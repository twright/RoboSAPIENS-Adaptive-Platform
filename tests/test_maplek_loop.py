"""End-to-end test of a dummy MAPLE-K loop built with rpclpy.

Five Node subclasses (Monitor, Analysis, Plan, Legitimate, Execute) are
wired together exactly as the README describes: each stage triggers the
next through an *event*, shares state through *knowledge*, and the final
actuation leaves the loop as a *message*. A simulated laser scan is
injected on the message bus and the tests assert that every event and
trigger in the chain fires — in the correct order and with the correct
data flowing through knowledge.

Scenario story (obstacle avoidance):
    /scan message ──► Monitor    writes 'laser_scan',  emits 'new_data'
                      Analysis   reads scan, if an obstacle is closer than
                                 ANOMALY_THRESHOLD writes 'anomaly' and
                                 emits 'anomaly_detected' (else loop stops)
                      Plan       picks an avoidance speed, writes
                                 'planned_action', emits 'new_plan'
                      Legitimate approves the plan iff the speed is safe,
                                 emits 'plan_legitimated' {approved: bool}
                      Execute    if approved, publishes /cmd_vel message

Three scenarios are covered:
    1. anomaly -> safe plan -> approved -> command executed  (full loop)
    2. no anomaly -> loop stops after Analysis               (no false triggers)
    3. anomaly -> unsafe plan -> rejected -> no command      (Legitimate veto)
"""

import json
import threading
from datetime import datetime

from conftest import next_message
from rpclpy.node import Node
from rpclpy.utils import timeit_callback

# Topics
MSG_SCAN = "/scan"
MSG_CMD = "/cmd_vel"
EVENT_NEW_DATA = "new_data"
EVENT_ANOMALY = "anomaly_detected"
EVENT_PLAN = "new_plan"
EVENT_LEGITIMATED = "plan_legitimated"

# Knowledge keys
K_SCAN = "laser_scan"
K_ANOMALY = "anomaly"
K_PLAN = "planned_action"

# Domain thresholds for the dummy adaptation logic
ANOMALY_THRESHOLD = 0.5   # an obstacle closer than this is an anomaly
CRITICAL_DISTANCE = 0.1   # closer than this -> Plan panics and picks an unsafe speed
MAX_SAFE_SPEED = 1.0      # Legitimate rejects any plan faster than this

STAGES = ("monitor", "analysis", "plan", "legitimate", "execute")


class LoopTrace:
    """Records which MAPLE-K stage fired, in which order, thread-safely."""

    def __init__(self):
        self._lock = threading.Lock()
        self.order = []
        self.fired = {stage: threading.Event() for stage in STAGES}
        self.executed_commands = []

    def record(self, stage):
        with self._lock:
            self.order.append(stage)
        self.fired[stage].set()


# ---------------------------------------------------------------------------
# Dummy MAPLE-K nodes, written the way a real application would write them
# ---------------------------------------------------------------------------

class DummyMonitor(Node):
    def __init__(self, config, trace):
        super().__init__(config)
        self.trace = trace
        self.register_message_callback(MSG_SCAN, self.handle_scan)

    def handle_scan(self, msg):
        self.trace.record("monitor")
        scan = json.loads(msg)
        self.write_knowledge(K_SCAN, json.dumps(scan))
        self.publish_event(EVENT_NEW_DATA, {"stage": "monitor"})


class DummyAnalysis(Node):
    def __init__(self, config, trace):
        super().__init__(config)
        self.trace = trace
        self.register_event_callback(EVENT_NEW_DATA, self.handle_new_data)

    @timeit_callback
    def handle_new_data(self, msg):
        self.trace.record("analysis")
        scan = self.read_knowledge(K_SCAN)
        min_range = min(scan["ranges"])
        anomaly = min_range < ANOMALY_THRESHOLD
        self.write_knowledge(
            K_ANOMALY, json.dumps({"anomaly": anomaly, "min_range": min_range})
        )
        if anomaly:
            self.publish_event(EVENT_ANOMALY, {"stage": "analysis"})


class DummyPlan(Node):
    def __init__(self, config, trace):
        super().__init__(config)
        self.trace = trace
        self.register_event_callback(EVENT_ANOMALY, self.handle_anomaly)

    def handle_anomaly(self, msg):
        self.trace.record("plan")
        anomaly = self.read_knowledge(K_ANOMALY)
        # Panic reaction: if the obstacle is critically close, plan an
        # (unsafe) escape speed — Legitimate is expected to veto it.
        speed = 2.0 if anomaly["min_range"] < CRITICAL_DISTANCE else 0.3
        self.write_knowledge(
            K_PLAN, json.dumps({"action": "avoid_obstacle", "speed": speed})
        )
        self.publish_event(EVENT_PLAN, {"stage": "plan"})


class DummyLegitimate(Node):
    def __init__(self, config, trace):
        super().__init__(config)
        self.trace = trace
        self.register_event_callback(EVENT_PLAN, self.handle_plan)

    def handle_plan(self, msg):
        self.trace.record("legitimate")
        plan = self.read_knowledge(K_PLAN)
        approved = plan["speed"] <= MAX_SAFE_SPEED
        self.publish_event(EVENT_LEGITIMATED, {"approved": approved})


class DummyExecute(Node):
    def __init__(self, config, trace):
        super().__init__(config)
        self.trace = trace
        self.register_event_callback(EVENT_LEGITIMATED, self.handle_legitimated)

    def handle_legitimated(self, msg):
        self.trace.record("execute")
        verdict = json.loads(msg)
        if verdict["approved"]:
            plan = self.read_knowledge(K_PLAN)
            command = {"linear": plan["speed"], "angular": 0.5}
            self.trace.executed_commands.append(command)
            self.publish_message(MSG_CMD, command)


# ---------------------------------------------------------------------------
# Test harness
# ---------------------------------------------------------------------------

def build_loop(make_node):
    """Instantiate and start all five stages; return the shared trace."""
    trace = LoopTrace()
    nodes = [
        make_node(cls, trace)
        for cls in (DummyMonitor, DummyAnalysis, DummyPlan,
                    DummyLegitimate, DummyExecute)
    ]
    for node in nodes:
        node.start()
    return trace, nodes


def inject_scan(raw_client, ranges):
    """Simulate a sensor probe publishing a laser scan on the message bus."""
    probe = raw_client(db=1)
    probe.publish(MSG_SCAN, json.dumps({"ranges": ranges}))


def test_full_loop_runs_all_stages_in_order(make_node, raw_client):
    trace, _ = build_loop(make_node)

    # Watch the actuation topic like a robot base controller would.
    actuator = raw_client().pubsub()
    actuator.subscribe(MSG_CMD)

    # Obstacle at 0.4 m -> anomaly, but not critical -> safe plan -> approved.
    inject_scan(raw_client, [1.2, 0.4, 2.0])

    assert trace.fired["execute"].wait(timeout=10), (
        f"loop did not complete; stages fired so far: {trace.order}"
    )
    assert trace.order == list(STAGES)

    # Knowledge propagated correctly through the loop (readable from any node).
    node = make_node()
    assert node.read_knowledge(K_SCAN) == {"ranges": [1.2, 0.4, 2.0]}
    assert node.read_knowledge(K_ANOMALY) == {"anomaly": True, "min_range": 0.4}
    assert node.read_knowledge(K_PLAN) == {"action": "avoid_obstacle", "speed": 0.3}

    # The actuation message really left the loop, with tracking metadata.
    msg = next_message(actuator)
    assert msg is not None, "no /cmd_vel message was published"
    command = json.loads(msg["data"])
    assert command["linear"] == 0.3
    assert len(command["uid"]) == 8
    datetime.fromisoformat(command["timestamp"])

    # The timeit_callback decorator recorded Analysis execution time.
    assert node.read_knowledge("DummyAnalysis:execution_time") is not None


def test_loop_stops_when_no_anomaly_detected(make_node, raw_client):
    trace, _ = build_loop(make_node)

    # Nothing closer than the anomaly threshold.
    inject_scan(raw_client, [2.0, 3.5, 1.8])

    assert trace.fired["analysis"].wait(timeout=10), "analysis never triggered"
    # Give downstream stages a moment to (wrongly) fire, then assert they didn't.
    assert not trace.fired["plan"].wait(timeout=1.0), (
        "plan was triggered although no anomaly was detected"
    )
    assert trace.order == ["monitor", "analysis"]

    node = make_node()
    assert node.read_knowledge(K_ANOMALY) == {"anomaly": False, "min_range": 1.8}


def test_legitimate_vetoes_unsafe_plan(make_node, raw_client):
    trace, _ = build_loop(make_node)

    actuator = raw_client().pubsub()
    actuator.subscribe(MSG_CMD)

    # Critically close obstacle -> Plan picks speed 2.0 -> above MAX_SAFE_SPEED.
    inject_scan(raw_client, [0.05, 1.0])

    assert trace.fired["execute"].wait(timeout=10), (
        f"loop did not reach execute; stages fired so far: {trace.order}"
    )
    assert trace.order == list(STAGES)

    # Execute was triggered but must not have actuated anything.
    assert trace.executed_commands == []
    assert next_message(actuator, timeout=1.0) is None, (
        "a command was published although the plan was rejected"
    )
