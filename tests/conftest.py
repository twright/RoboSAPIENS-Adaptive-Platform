"""Shared fixtures for the rpclpy test suite.

The tests run without any external infrastructure. `fakeredis` provides an
in-process Redis emulation and `redis.Redis` is monkeypatched so that every
client the library creates (event manager, message manager, knowledge
manager) connects to the same FakeServer instance. This means the tests
exercise the *real* rpclpy code paths (pub/sub threads, serialization,
knowledge storage) end to end, just without a running Redis server.
"""

import copy
import pathlib
import sys
import time

import fakeredis
import pytest
import redis as redis_module

# Make the repo root importable even when rpclpy is not pip-installed.
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from rpclpy.node import Node  # noqa: E402


LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


@pytest.fixture
def fake_server(monkeypatch):
    """One in-process Redis server per test; all redis.Redis() calls land on it."""
    server = fakeredis.FakeServer()

    def fake_redis(*args, **kwargs):
        kwargs.pop("host", None)
        kwargs.pop("port", None)
        return fakeredis.FakeRedis(server=server, **kwargs)

    monkeypatch.setattr(redis_module, "Redis", fake_redis)
    return server


@pytest.fixture
def node_config():
    """A config dict mirroring the YAML layout documented in the README."""
    return {
        "Logger_Config": {
            "log_level": "INFO",
            "format": LOG_FORMAT,
            "logger_type": "terminal",
        },
        "Event_Manager_Config": {
            "protocol": "redis",
            "host": "localhost",
            "port": 6379,
            "db": 0,
        },
        "Message_Manager_Config": {
            "protocol": "redis",
            "host": "localhost",
            "port": 6379,
            "db": 1,
        },
        "Knowledge_Config": {
            "knowledge_type": "redis",
            "host": "localhost",
            "port": 6379,
            "db": 2,
        },
    }


@pytest.fixture
def local_config():
    """Config selecting the in-process 'local' backend — no broker needed."""
    return {
        "Logger_Config": {
            "log_level": "INFO",
            "format": LOG_FORMAT,
            "logger_type": "terminal",
        },
        "Event_Manager_Config": {"protocol": "local", "db": 0},
        "Message_Manager_Config": {"protocol": "local", "db": 1},
        "Knowledge_Config": {"knowledge_type": "local", "db": 2},
    }


@pytest.fixture
def reset_local_backend():
    """Isolate tests that use the process-global local bus and stores."""
    from rpclpy.CommunicationManager import LocalBus
    from rpclpy.KnowledgeManager import reset_local_stores

    LocalBus.reset()
    reset_local_stores()
    yield
    LocalBus.reset()
    reset_local_stores()


@pytest.fixture
def make_node(fake_server, node_config):
    """Factory that builds Node (sub)classes and tears them down after the test.

    Teardown also removes the handlers Node attaches to the class-named
    logger, otherwise repeated instantiations across tests would stack
    duplicate handlers on the same logger object.
    """
    created = []

    def _make(cls=Node, *extra_args, config=None):
        node = cls(config or copy.deepcopy(node_config), *extra_args)
        created.append(node)
        return node

    yield _make

    for node in created:
        try:
            node.shutdown()
        except Exception:
            pass
        node.logger.handlers.clear()


@pytest.fixture
def raw_client(fake_server):
    """Factory for plain redis clients on the same fake server.

    Used to observe what the library actually puts on the wire, or to
    inject traffic from 'outside' (e.g. a simulated sensor probe).
    """

    def _client(db=0, decode_responses=True):
        return fakeredis.FakeRedis(
            server=fake_server, db=db, decode_responses=decode_responses
        )

    return _client


def next_message(pubsub, timeout=5.0):
    """Poll a raw pubsub object until a real message arrives (or time out)."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        msg = pubsub.get_message(ignore_subscribe_messages=True, timeout=0.1)
        if msg is not None and msg["type"] == "message":
            return msg
    return None
