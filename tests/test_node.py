"""Contract tests for rpclpy.node.Node.

These tests pin down the public behaviour of Node — lifecycle, event and
message publishing (string-topic and class-based variants), callback
registration, and knowledge read/write in all three flavours (string, JSON,
pickled object). If a refactoring of the library internals breaks any part
of the Node API, one of these tests fails.

No external services are required: see conftest.py for the fakeredis setup.
"""

import json
import logging
import threading
from datetime import datetime

from conftest import next_message
from rpclpy.node import Node


# Classes used by the object-based APIs. They live at module level because
# the knowledge object round-trip relies on pickle, which needs importable
# classes.
class RobotPose:
    def __init__(self):
        self.name = "robot_pose"
        self.x = 0.0
        self.y = 0.0


class AnomalyEvent:
    def __init__(self):
        self.name = "anomaly_event"
        self.topic = "events/anomaly"
        self.severity = "high"


class StatusMessage:
    def __init__(self):
        self.name = "status_message"
        self.topic = "messages/status"
        self.status = "ok"


# ---------------------------------------------------------------------------
# Construction and lifecycle
# ---------------------------------------------------------------------------

def test_node_initializes_all_components(make_node):
    node = make_node()
    assert node.logger is not None
    assert isinstance(node.logger, logging.Logger)
    assert node.knowledge is not None
    assert node.event_manager is not None
    assert node.message_manager is not None
    assert node.event_manager.protocol == "redis"
    assert node.message_manager.protocol == "redis"


def test_start_and_shutdown_lifecycle(make_node):
    node = make_node()
    node.start()
    assert node.event_manager._redis_client is not None
    assert node.message_manager._redis_client is not None
    node.shutdown()  # must not raise


# ---------------------------------------------------------------------------
# Knowledge
# ---------------------------------------------------------------------------

def test_write_and_read_string_knowledge(make_node):
    node = make_node()
    assert node.write_knowledge("robot_state", "moving")
    assert node.read_knowledge("robot_state") == "moving"


def test_read_missing_knowledge_returns_none(make_node):
    node = make_node()
    assert node.read_knowledge("does_not_exist") is None


def test_json_knowledge_roundtrip(make_node):
    node = make_node()
    payload = {"ranges": [1.0, 0.4, 2.0], "frame": "laser"}
    node.write_knowledge("laser_scan", json.dumps(payload))
    assert node.read_knowledge("laser_scan") == payload  # deserialized back to dict


def test_object_knowledge_roundtrip(make_node):
    node = make_node()
    pose = RobotPose()
    pose.x, pose.y = 1.5, -2.0

    node.write_knowledge(pose)
    restored = node.read_knowledge(RobotPose)

    assert restored.x == 1.5
    assert restored.y == -2.0
    # write_knowledge_as_object stamps tracking metadata on the instance
    assert len(restored.uid) == 8
    datetime.fromisoformat(restored.timestamp)  # must be valid ISO-8601


# ---------------------------------------------------------------------------
# Events
# ---------------------------------------------------------------------------

def test_publish_event_attaches_uid_and_timestamp(make_node, raw_client):
    node = make_node()
    node.start()

    spy = raw_client().pubsub()
    spy.subscribe("events/new_data")

    node.publish_event("events/new_data", {"source": "monitor"})

    msg = next_message(spy)
    assert msg is not None, "event was never delivered"
    payload = json.loads(msg["data"])
    assert payload["source"] == "monitor"
    assert len(payload["uid"]) == 8
    datetime.fromisoformat(payload["timestamp"])


def test_publish_event_with_class(make_node, raw_client):
    node = make_node()
    node.start()

    spy = raw_client().pubsub()
    spy.subscribe(AnomalyEvent().topic)

    node.publish_event(AnomalyEvent)

    msg = next_message(spy)
    assert msg is not None, "class-based event was never delivered"
    payload = json.loads(msg["data"])
    assert payload["severity"] == "high"
    assert len(payload["uid"]) == 8


def test_publish_event_does_not_mutate_caller_dict(make_node):
    node = make_node()
    node.start()

    payload = {"source": "monitor"}
    node.publish_event("events/ping", payload)

    assert payload == {"source": "monitor"}, "caller's dict was mutated"


def test_publish_event_default_message_is_not_shared(make_node, raw_client):
    node = make_node()
    node.start()

    spy = raw_client().pubsub()
    spy.subscribe("events/tick")

    node.publish_event("events/tick")
    node.publish_event("events/tick")

    first = json.loads(next_message(spy)["data"])
    second = json.loads(next_message(spy)["data"])
    # Each call must get a fresh dict with its own uid — before the fix, a
    # shared mutable default leaked uid/timestamp between calls and nodes.
    assert first["uid"] != second["uid"]
    assert set(first) == {"uid", "timestamp"}


def test_register_event_callback_receives_events(make_node):
    node = make_node()
    received = []
    delivered = threading.Event()

    def on_event(msg):
        received.append(json.loads(msg))
        delivered.set()

    node.register_event_callback("events/ping", on_event)
    node.start()
    node.publish_event("events/ping", {"n": 1})

    assert delivered.wait(timeout=5), "callback was never invoked"
    assert received[0]["n"] == 1


# ---------------------------------------------------------------------------
# Messages
# ---------------------------------------------------------------------------

def test_register_message_callback_receives_messages(make_node):
    node = make_node()
    received = []
    delivered = threading.Event()

    def on_message(msg):
        received.append(json.loads(msg))
        delivered.set()

    node.register_message_callback("/cmd_vel", on_message)
    node.start()
    node.publish_message("/cmd_vel", {"linear": 0.3})

    assert delivered.wait(timeout=5), "callback was never invoked"
    assert received[0]["linear"] == 0.3
    assert len(received[0]["uid"]) == 8


def test_publish_message_with_instance_uses_topic_attribute(make_node, raw_client):
    node = make_node()
    node.start()

    status = StatusMessage()
    status.status = "degraded"

    spy = raw_client().pubsub()
    spy.subscribe(status.topic)

    node.publish_message(status)

    msg = next_message(spy)
    assert msg is not None, "instance-based message was never delivered"
    payload = json.loads(msg["data"])
    assert payload["status"] == "degraded"
    assert len(payload["uid"]) == 8
