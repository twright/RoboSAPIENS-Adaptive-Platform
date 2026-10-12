"""Current callback API over the classic exact-reference journal."""
import json
import threading
import uuid

from rpio_observation.errors import ObservationError
from rpio_observation.core import canonical_text, freeze_json
from rpio_observation.outcomes import Aborted, Outcome
from rpio_observation.wire import (JsonCodec, knowledge_record, phase_record,
                                   reference, tagged)

SCHEMA_VERSION = 2
STAMPS = ("observation_epoch", "activation_id", "sequence_no")
RPCLPY_STAMPS = ("uid", "timestamp")


class _Activation:
    def __init__(self, caused_by):
        self.caused_by = caused_by
        self.start_seq = uuid.uuid4().hex  # stable activation identity, not a stream position
        self.emissions = 0
        self.acquired = []               # (object, immutable snapshot); held until End


class NodeObservation:
    def __init__(self, node_name, config, backend):
        self.namespace = config["namespace"]
        self.producer = config.get("producer", node_name)
        self.role = config.get("role", node_name)
        self.loop = list(config.get("loop", ["rpio"]))
        self.backend = backend
        self.epoch = backend.epoch
        self.run_id = config.get("run_id", self.epoch)
        self.codec = config.get("codec") or JsonCodec()
        if not self.epoch:
            raise ObservationError(f"namespace {self.namespace!r} was not created (create_run)")
        self.failed = False
        self.started = False
        self._local = threading.local()

    def error(self, message):
        self.failed = True
        return ObservationError(message)

    def _call(self, operation):
        try:
            return operation()
        except ObservationError:
            self.failed = True
            raise
        except Exception as exc:
            raise self.error(f"observation operation failed: {exc}") from exc

    def _stack(self):
        if not hasattr(self._local, "stack"):
            self._local.stack = []
        return self._local.stack

    def _current(self, what):
        stack = self._stack()
        if not stack:
            raise self.error(f"{what} outside an observed activation")
        return stack[-1]

    def _activation_json(self, act):
        return {"run_id": self.run_id, "activation_id": act.start_seq,
                "producer": self.producer, "role": self.role, "loop": self.loop,
                "caused_by": tagged("None") if act.caused_by is None else
                tagged("Some", act.caused_by)}

    def _phase(self, act, step, payload=None, refs=()):
        activation = self._activation_json(act)
        wire_step = tagged(step, payload)
        return self._call(lambda: self.backend.append_phase_record(
            lambda now: phase_record(activation, wire_step, refs, now)))

    def _writer(self, act):
        if act is None:
            return tagged("Node", {"run_id": self.run_id, "name": self.producer})
        return tagged("Activation", {"run_id": self.run_id, "activation_id": act.start_seq})

    @staticmethod
    def _key(cls):
        return cls.name if hasattr(cls, "name") else cls().name

    def start(self):
        self.started = True

    def write(self, obj):
        key = obj.name
        # Freeze before any store mutation. The caller may mutate its object later.
        value = self._call(lambda: freeze_json(obj.__dict__))
        typed = self._call(lambda: freeze_json(self.codec.knowledge(key, value)))
        stack = self._stack()
        if not stack and self.started:
            raise self.error(f"write of {key!r} outside an observed activation")
        act = stack[-1] if stack else None
        writer = self._writer(act)
        phase_builder = None if act is None else lambda ref, now: phase_record(
            self._activation_json(act), tagged("Write", {"key": key}), (ref,), now)
        commit = self._call(lambda: self.backend.commit(
            key, value, lambda ref, now: knowledge_record(ref, typed, writer, now),
            phase_builder=phase_builder))
        if act is not None:
            act.acquired.append((obj, commit.snapshot()))
        return True

    def read(self, cls):
        key = self._key(cls)
        stack = self._stack()
        if not stack:
            if self.started:
                raise self.error(f"read of {key!r} outside an observed activation")
            snapshot = self._call(lambda: self.backend.read_latest(key))
        else:
            act = stack[-1]
            activation = self._activation_json(act)
            snapshot, _ = self._call(lambda: self.backend.read_phase(
                key, lambda selected, now: phase_record(activation, tagged("Read", {"key": key}),
                           () if selected is None else (selected.ref,), now)))
        if snapshot is None:
            return None
        obj = cls.__new__(cls)
        # A read returns a fresh mutable object, not the immutable journal snapshot.
        obj.__dict__.update(json.loads(canonical_text(snapshot.value)))
        if stack:
            stack[-1].acquired.append((obj, snapshot))
        return obj

    def _resolve(self, act, obj):
        for held, snapshot in reversed(act.acquired):
            if held is obj:
                return snapshot.ref
        raise self.error(f"{type(obj).__name__} was neither read nor written by this activation")

    def publish(self, kind, topic, message, evidence):
        act = self._current(f"publication to {topic!r}")
        slots = {name: self._resolve(act, obj) for name, obj in (evidence or {}).items()}
        recorded = {k: v for k, v in (message or {}).items()
                    if k not in STAMPS and k not in RPCLPY_STAMPS and k != "_observation_emission"}
        validate = getattr(self.codec, "validate_evidence", None)
        if validate is not None:
            self._call(lambda: validate(kind, topic, {name: reference(ref) for name, ref in slots.items()}))
        emitted = self._call(lambda: freeze_json(self.codec.emitted(kind, topic, recorded)))
        n = act.emissions + 1
        emission = {"activation_id": act.start_seq, "ordinal": n}
        self._phase(act, "Event", {"name": topic, "payload": emitted, "emission": emission},
                    tuple(slots.values()))
        act.emissions = n
        return {"observation_epoch": self.epoch, "activation_id": act.start_seq,
                "sequence_no": n, "_observation_emission": emission}

    def service(self, service, request_id, outcome):
        act = self._current(f"service {service!r}")
        request_id = request_id or f"{service}/{self.epoch}/{act.start_seq}"
        payload = {"service": service, "request_id": request_id}
        if outcome is None:
            self._phase(act, "ServiceStarted", payload)
        else:
            if outcome not in ("Delivered", "NotDelivered"):
                raise self.error(f"unknown service outcome {outcome!r}")
            self._phase(act, "ServiceCompleted", dict(payload, outcome=tagged(outcome)))
        return request_id

    def wrap(self, kind, topic, callback):
        def run(message):
            act = self._begin(kind, topic, message)
            stack = self._stack()
            stack.append(act)
            try:
                try:
                    result = callback(message)
                except BaseException:
                    self.failed = True
                    try:
                        self._end(act, Aborted("callback_exception"))
                    except Exception:
                        pass
                    raise
                self._end(act, result)
                return result
            finally:
                stack.pop()
                act.acquired.clear()
        return run

    def _begin(self, kind, topic, message):
        if not self.started:
            raise self.error("callback delivered before start()")
        text = message.decode("utf-8", "replace") if isinstance(message, bytes) else message
        try:
            parsed = json.loads(text)
        except (TypeError, ValueError):
            parsed = text
        caused_by = None
        if isinstance(parsed, dict):
            envelope = parsed.pop("_observation_emission", None)
            stamps = [parsed.pop(k, None) for k in STAMPS]
            for k in RPCLPY_STAMPS:
                parsed.pop(k, None)
            if envelope is not None:
                caused_by = self._call(lambda: self._cause(envelope))
            elif None not in stamps:
                caused_by = {"activation_id": str(stamps[1]), "ordinal": stamps[2]}
        act = _Activation(caused_by)
        trigger = self._call(lambda: freeze_json(self.codec.trigger(kind, topic, parsed)))
        self._phase(act, "Start", {"trigger": trigger})
        return act

    @staticmethod
    def _cause(envelope):
        if (not isinstance(envelope, dict) or not isinstance(envelope.get("activation_id"), str)
                or not envelope["activation_id"] or isinstance(envelope.get("ordinal"), bool)
                or not isinstance(envelope.get("ordinal"), int) or envelope["ordinal"] < 1):
            raise ObservationError("invalid causal emission envelope")
        return {"activation_id": envelope["activation_id"], "ordinal": envelope["ordinal"]}

    def _end(self, act, result):
        if not isinstance(result, Outcome):
            raise self.error("an observed callback must return an rpio_observation.outcomes "
                             f"object, not {type(result).__name__}")
        refs = []
        def resolve(obj):
            ref = self._resolve(act, obj)
            refs.append(ref)
            return reference(ref)
        outcome = self._call(lambda: freeze_json(self.codec.outcome(result.wire(resolve))))
        self._phase(act, "End", {"outcome": outcome}, refs)
