"""Tests for the in-process 'local' backend.

With ``protocol: "local"`` (communication) and ``knowledge_type: "local"``
(knowledge) a Node needs no broker at all: all nodes in the process share a
global LocalBus and in-memory knowledge stores. Event delivery is
synchronous — publish invokes subscribers inline — so a single trigger
drives an entire MAPLE-K loop to completion before returning.

The full-loop test reuses the exact same dummy stage classes as the Redis
version in test_maplek_loop.py, which proves that application code written
against the Node API is backend-agnostic: only the config changes.
"""

import copy
import json

import pytest

from rpclpy.node import Node
from test_node import RobotPose
from test_maplek_loop import (
    STAGES,
    MSG_SCAN,
    MSG_CMD,
    K_SCAN,
    K_ANOMALY,
    K_PLAN,
    LoopTrace,
    DummyMonitor,
    DummyAnalysis,
    DummyPlan,
    DummyLegitimate,
    DummyExecute,
)


@pytest.fixture
def local_node(make_node, local_config, reset_local_backend):
    """Build nodes on the local backend (fresh config copy per node)."""

    def _make(cls=Node, *extra_args):
        return make_node(cls, *extra_args, config=copy.deepcopy(local_config))

    return _make


# ---------------------------------------------------------------------------
# Node contract on the local backend
# ---------------------------------------------------------------------------

def test_local_knowledge_roundtrip(local_node):
    node = local_node()
    node.write_knowledge("robot_state", "moving")
    assert node.read_knowledge("robot_state") == "moving"

    payload = {"ranges": [1.0, 0.4]}
    node.write_knowledge("laser_scan", json.dumps(payload))
    assert node.read_knowledge("laser_scan") == payload

    assert node.read_knowledge("does_not_exist") is None


def test_local_knowledge_is_shared_between_nodes(local_node):
    writer, reader = local_node(), local_node()
    writer.write_knowledge("shared", "value")
    assert reader.read_knowledge("shared") == "value"


def test_local_object_knowledge_roundtrip(local_node):
    node = local_node()
    pose = RobotPose()
    pose.x, pose.y = 1.5, -2.0

    node.write_knowledge(pose)
    restored = node.read_knowledge(RobotPose)
    assert (restored.x, restored.y) == (1.5, -2.0)
    assert len(restored.uid) == 8


def test_local_event_pubsub_between_nodes(local_node):
    publisher, subscriber = local_node(), local_node()
    received = []
    subscriber.register_event_callback(
        "events/ping", lambda msg: received.append(json.loads(msg))
    )
    subscriber.start()
    publisher.start()

    publisher.publish_event("events/ping", {"n": 1})

    # Delivery is synchronous: the callback already ran.
    assert received and received[0]["n"] == 1
    assert len(received[0]["uid"]) == 8


def test_local_message_pubsub_between_nodes(local_node):
    publisher, subscriber = local_node(), local_node()
    received = []
    subscriber.register_message_callback(
        "/cmd_vel", lambda msg: received.append(json.loads(msg))
    )
    subscriber.start()
    publisher.start()

    publisher.publish_message("/cmd_vel", {"linear": 0.3})

    assert received and received[0]["linear"] == 0.3


def test_local_publish_before_start_raises(local_node):
    node = local_node()
    with pytest.raises(RuntimeError):
        node.event_manager.publish("events/ping", "{}")


def test_local_shutdown_unsubscribes(local_node):
    publisher, subscriber = local_node(), local_node()
    received = []
    subscriber.register_event_callback(
        "events/ping", lambda msg: received.append(msg)
    )
    subscriber.start()
    publisher.start()

    subscriber.shutdown()
    publisher.publish_event("events/ping", {"n": 1})

    assert received == [], "callback fired after the node was shut down"


# ---------------------------------------------------------------------------
# Whole MAPLE-K loop on the local bus — same dummy nodes as the Redis test
# ---------------------------------------------------------------------------

def test_full_maplek_loop_on_local_bus(local_node):
    trace = LoopTrace()
    nodes = [
        local_node(cls, trace)
        for cls in (DummyMonitor, DummyAnalysis, DummyPlan,
                    DummyLegitimate, DummyExecute)
    ]
    for node in nodes:
        node.start()

    # Helper node acting as sensor probe (input) and base controller (output).
    probe = local_node()
    commands = []
    probe.register_message_callback(
        MSG_CMD, lambda msg: commands.append(json.loads(msg))
    )
    probe.start()

    # One synchronous publish drives the whole loop to completion inline.
    probe.publish_message(MSG_SCAN, {"ranges": [1.2, 0.4, 2.0]})

    assert trace.order == list(STAGES)
    assert probe.read_knowledge(K_ANOMALY) == {"anomaly": True, "min_range": 0.4}
    assert probe.read_knowledge(K_PLAN) == {"action": "avoid_obstacle", "speed": 0.3}
    assert commands and commands[0]["linear"] == 0.3
    assert len(commands[0]["uid"]) == 8
