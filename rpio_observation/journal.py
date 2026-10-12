"""Memory and Redis facades for the classic two-stream reference journal.

Knowledge values are committed once. Phase steps cite exact immutable refs.
Read selection and its Phase admission share one atomic publication boundary.
The two streams retain separate cursors; a file merge establishes delivery order,
not a new total-order version identity.
"""
import hashlib
import heapq
import json
import time
import uuid

from rpio_observation.core import (InMemoryJournalBackend, ObservationError,
                                  canonical_text, value_digest, WriteId)
from rpio_observation.redis_backend import RedisJournalBackend, _record_template, _text


def _entries(backend):
    # Read phases first: subsequently read Knowledge contains every cited commit.
    phase = backend.phase_journal()
    knowledge = backend.knowledge_journal()
    return list(heapq.merge(knowledge, phase, key=lambda entry: entry.journal_time))


class _Facade:
    def records(self):
        return self.epoch, [canonical_text(entry.record) for entry in self.entries()]

    def entries(self):
        return _entries(self)

    def current(self, key):
        """Compatibility inspection snapshot; observed callbacks use read_phase()."""
        snapshot = self.read_latest(key)
        if snapshot is None:
            return None
        from rpio_observation.wire import reference
        wanted = reference(snapshot.ref)
        entry = self.knowledge_entry(snapshot.ref)
        if entry is None:
            raise ObservationError(f"public history entry for {snapshot.ref} is missing")
        version = entry.record["payload"]["version"]
        return canonical_text({"ref": wanted, "value": snapshot.value,
                               "written_by": version["written_by"]})

    def commit(self, key, value, record_builder, write_id=None, phase_builder=None):
        return self.commit_knowledge_write(key, value, write_id or WriteId.new(),
                                           value_digest(value), record_builder, phase_record=phase_builder)


class MemoryJournal(_Facade, InMemoryJournalBackend):
    clock = "python:time.time_ns"

    def __init__(self):
        super().__init__(store_epoch="uninitialised", journal_clock=lambda: time.time_ns() / 1_000_000_000,
                         journal_clock_domain=self.clock)
        self.epoch = None

    def create(self, epoch):
        with self._lock:
            if self.epoch is not None:
                raise ObservationError("the memory journal already holds a run")
            self._store_epoch = self.epoch = epoch

    def read_phase(self, key, record_builder):
        with self._lock:
            snapshot = self.read_latest(key)
            entry = self.append_phase_record(lambda now: record_builder(snapshot, now))
            return snapshot, entry

    def close(self):
        pass


class RedisJournal(_Facade, RedisJournalBackend):
    clock = "redis.server.time"

    def __init__(self, config, namespace, *, _new_epoch=None):
        import redis
        from redis.retry import Retry
        from redis.backoff import NoBackoff
        self.ns = namespace
        args = dict(host=config.get("host", "localhost"), port=config.get("port", 6379),
                    db=config.get("db", 0), decode_responses=True)
        try:
            client = redis.Redis(retry=Retry(NoBackoff(), 0), **args)
        except TypeError:
            client = redis.Redis(**args)
        if _new_epoch is None and not client.exists(f"{namespace}:epoch"):
            client.close()
            raise ObservationError(f"namespace {namespace!r} was not created (create_run)")
        super().__init__(client, key_prefix=namespace, phase_stream=f"{namespace}:phase",
                         knowledge_stream=f"{namespace}:knowledge", initial_epoch=_new_epoch,
                         require_fresh=_new_epoch is not None)

    @property
    def epoch(self):
        return self.store_epoch

    def read_phase(self, key, record_builder):
        """Optimistic selection+Phase admission, retried only before admission.

        WATCH protects selected current state and lineage. Once MULTI commits,
        the script admits exactly one Read; commands are never retried after an
        ambiguous transport failure.
        """
        import redis
        with self.client.pipeline() as pipe:
            while True:
                try:
                    pipe.watch(self.keys["latest"], self.keys["epoch"], self.keys["streams"])
                    snapshot = self.read_latest(key)
                    parts = _record_template(record_builder(snapshot, 0.0), False)
                    pipe.multi()
                    self._append(keys=[self.phase_stream, self.keys["epoch"],
                                       self.keys["streams"], self.knowledge_stream],
                                 args=[self.journal_clock_domain, self.store_epoch, "2", *parts],
                                 client=pipe)
                    ((cursor, admitted),) = pipe.execute()
                    record = record_builder(snapshot, float(_text(admitted)))
                    from rpio_observation.core import JournalEntry
                    return snapshot, JournalEntry(self.phase_stream, _text(cursor),
                                                  float(_text(admitted)), self.clock, record)
                except redis.WatchError:
                    continue
                except Exception as exc:
                    raise self._translate(exc) from exc

    def close(self):
        self.client.close()


def create_run(namespace, *, backend=None, redis_config=None, epoch=None):
    if (backend is None) == (redis_config is None):
        raise ObservationError("create_run needs exactly one of backend= and redis_config=")
    epoch = epoch or uuid.uuid4().hex[:12]
    if backend is not None:
        backend.create(epoch)
    else:
        journal = RedisJournal(redis_config, namespace, _new_epoch=epoch)
        journal.close()
    return epoch


def dump(source, namespace, path):
    journal = source if isinstance(source, MemoryJournal) else RedisJournal(source, namespace)
    try:
        epoch, records = journal.records()
    finally:
        if journal is not source:
            journal.close()
    data = "".join(record + "\n" for record in records).encode("utf-8")
    with open(path, "wb") as out:
        out.write(data)
    return {"epoch": epoch, "through": len(records), "count": len(records),
            "sha256": hashlib.sha256(data).hexdigest()}


def dumps(value):
    return json.dumps(value, separators=(",", ":"), allow_nan=False)
