"""Reusable classic reference-journal store: immutable Knowledge versions,
atomic current/history publication, retry identity and separate Phase history.

Derived from ScrewDetection's measured producer at 0c10d85. Application and
callback vocabulary live in the configurable wire codec, not this store.
Knowledge references on the TC wire omit private key revisions and digests.
"""
from __future__ import annotations

import hashlib
import json
import threading
import time
import uuid
from dataclasses import dataclass, replace
from enum import Enum
from typing import Any, Callable, Mapping, Optional, Protocol, Sequence, runtime_checkable

__all__ = [
    "CODEC_ID",
    "SCHEMA_VERSION",
    "PHASE_STREAM",
    "KNOWLEDGE_STREAM",
    "SERVICE_OUTCOMES",
    "ObservationError",
    "CodecError",
    "WriteConflictError",
    "PhaseStateError",
    "PhaseClosedError",
    "ActiveExecutionError",
    "ExactReferenceError",
    "canonical_bytes",
    "canonical_text",
    "value_digest",
    "FrozenObject",
    "FrozenArray",
    "freeze_json",
    "thaw_json",
    "KnowledgeVersion",
    "KnowledgeRef",
    "WriteId",
    "PhaseExecution",
    "PhaseOutcome",
    "JournalEntry",
    "KnowledgeSnapshot",
    "KnowledgeCommit",
    "JournalBackend",
    "InMemoryJournalBackend",
    "MonitoredKnowledgeStore",
]

#: Versioned canonical-JSON codec identity carried by every monitored value.
CODEC_ID = "rpio-canonical-json/1"

#: Protocol version carried by every durable journal record.
SCHEMA_VERSION = 2

#: Default stream names. Two durable streams stay separate; no cross-stream
#: total order is implied by either cursor. Deployments may rename both.
PHASE_STREAM = "maple:observation:phase"
KNOWLEDGE_STREAM = "maple:observation:knowledge"

#: Raw service completion facts and their ``ServiceCompleted`` outcome tags.
SERVICE_OUTCOMES = {"delivered": "Delivered", "not_delivered": "NotDelivered"}

#: Journal record tags a stream entry may carry.
_RECORD_TAGS = frozenset(("Phase", "Knowledge", "Incomplete"))


# ===========================================================================
# ERRORS
# ===========================================================================
from rpio_observation.errors import ObservationError


class CodecError(ObservationError):
    """A value or wire object is not representable in the canonical JSON codec."""


class WriteConflictError(ObservationError):
    """A write ID was retried with a different key or a different value.

    A legitimate retry repeats the same write with the same identity. Anything
    else is an identity conflict, not a new revision.
    """


class PhaseStateError(ObservationError):
    """A phase observation context was used against its lifecycle contract."""


class PhaseClosedError(PhaseStateError):
    """The execution already logged its end record."""


class ActiveExecutionError(PhaseStateError):
    """The phase role already has an active execution (first version allows one)."""


class ExactReferenceError(ObservationError):
    """A reference was not an exact ``KnowledgeRef``."""


# ===========================================================================
# CANONICAL JSON CODEC
# ===========================================================================
def _check_json(value: Any, path: str = "value") -> None:
    """Reject anything that would not survive a JSON round trip unchanged.

    ``json.dumps`` silently coerces tuples to lists and non-string dict keys to
    strings. Both break exact equality of a decoded record, so they are errors
    here rather than quiet conversions.
    """
    if value is None or isinstance(value, bool):
        return
    if isinstance(value, int):
        return
    if isinstance(value, float):
        if value != value or value in (float("inf"), float("-inf")):
            raise CodecError(f"{path}: non-finite float is not canonical JSON")
        return
    if isinstance(value, str):
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            _check_json(item, f"{path}[{index}]")
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise CodecError(f"{path}: object keys must be str, got {type(key).__name__}")
            _check_json(item, f"{path}.{key}")
        return
    raise CodecError(f"{path}: {type(value).__name__} is not canonical JSON")


def canonical_text(value: Any) -> str:
    """Canonical JSON text: sorted keys, no insignificant whitespace, no NaN."""
    _check_json(value)
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False)


def canonical_bytes(value: Any) -> bytes:
    """UTF-8 canonical byte encoding of ``value``."""
    return canonical_text(value).encode("utf-8")


def value_digest(value: Any) -> str:
    """Digest of the canonical bytes, used to compare exact values, not meaning."""
    return "sha256:" + hashlib.sha256(canonical_bytes(value)).hexdigest()


def _readonly(self, *args, **kwargs):
    raise TypeError(f"{type(self).__name__} is immutable; use thaw_json() "
                    "for a mutable copy")


class FrozenObject(dict):
    """Deeply read-only JSON object.

    One committed value is shared by latest state, the history record and every
    reader, so mutating it in place would silently rewrite the journal. It still
    compares equal to a plain ``dict`` and encodes as a JSON object.
    """
    __slots__ = ()

    def __new__(cls, *args, **kwargs):
        value = dict(*args, **kwargs)
        _check_json(value)
        return cls._build((key, _frozen(item)) for key, item in value.items())

    def __init__(self, *args, **kwargs):
        pass  # filled once in __new__; re-running __init__ must not refill

    @classmethod
    def _build(cls, items) -> "FrozenObject":
        obj = dict.__new__(cls)
        dict.update(obj, items)
        return obj

    __setitem__ = __delitem__ = __ior__ = _readonly
    clear = pop = popitem = setdefault = update = _readonly

    def __hash__(self) -> int:
        return hash(canonical_text(self))

    def __reduce__(self):
        return (type(self), (thaw_json(self),))

    def __copy__(self):
        return self

    def __deepcopy__(self, memo):
        return self


class FrozenArray(list):
    """Deeply read-only JSON array; compares equal to a plain ``list``."""
    __slots__ = ()

    def __new__(cls, iterable=()):
        value = list(iterable)
        _check_json(value)
        return cls._build(_frozen(item) for item in value)

    def __init__(self, *args, **kwargs):
        pass

    @classmethod
    def _build(cls, items) -> "FrozenArray":
        obj = list.__new__(cls)
        list.extend(obj, items)
        return obj

    __setitem__ = __delitem__ = __iadd__ = __imul__ = _readonly
    append = extend = insert = pop = remove = reverse = sort = clear = _readonly

    def __hash__(self) -> int:
        return hash(canonical_text(self))

    def __reduce__(self):
        return (type(self), (thaw_json(self),))

    def __copy__(self):
        return self

    def __deepcopy__(self, memo):
        return self


def _frozen(value: Any) -> Any:
    """Freeze an already checked JSON value. Frozen containers are deep already."""
    if isinstance(value, (FrozenObject, FrozenArray)):
        return value
    if isinstance(value, list):
        return FrozenArray._build(_frozen(item) for item in value)
    if isinstance(value, dict):
        return FrozenObject._build((key, _frozen(item)) for key, item in value.items())
    return value


def freeze_json(value: Any) -> Any:
    """Check ``value`` is canonical JSON and return a deeply immutable copy."""
    _check_json(value)
    return _frozen(value)


def thaw_json(value: Any) -> Any:
    """Plain mutable ``dict``/``list`` copy of a (possibly frozen) JSON value."""
    if isinstance(value, list):
        return [thaw_json(item) for item in value]
    if isinstance(value, dict):
        return {key: thaw_json(item) for key, item in value.items()}
    return value


def _check_digest(value: Any, digest: Any, codec: str = CODEC_ID) -> None:
    _nonempty(digest, "digest")
    if codec == CODEC_ID and digest != value_digest(value):
        raise CodecError(f"digest {digest} does not match the canonical value")


def _check_time(value: Any, what: str) -> None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ObservationError(f"{what} must be a number")
    if value != value or value in (float("inf"), float("-inf")):
        raise ObservationError(f"{what} must be finite")


def _nonempty(text: str, what: str) -> str:
    if not isinstance(text, str) or not text:
        raise ObservationError(f"{what} must be a non-empty string")
    return text


# ===========================================================================
# VERSIONS, REFERENCES AND IDENTITIES
# ===========================================================================
@dataclass(frozen=True, slots=True)
class KnowledgeVersion:
    """``(store_epoch, global_write_sequence, key_revision)``.

    The epoch identifies persistent store lineage, not a loop invocation. The
    global sequence is the store-wide write order inside that epoch; the key
    revision counts writes of one key. Versions from different epochs are not
    comparable, so no ordering operators are defined.
    """
    store_epoch: str
    global_write_sequence: int
    key_revision: int

    def __post_init__(self) -> None:
        _nonempty(self.store_epoch, "store_epoch")
        for name in ("global_write_sequence", "key_revision"):
            number = getattr(self, name)
            if isinstance(number, bool) or not isinstance(number, int):
                raise ObservationError(f"{name} must be an integer")
            if number < 1:
                raise ObservationError(f"{name} must be >= 1, got {number}")

    def precedes(self, other: "KnowledgeVersion") -> bool:
        """Store-global order inside one epoch. Cross-epoch order is undefined."""
        if self.store_epoch != other.store_epoch:
            raise ObservationError(
                "cannot order versions across epochs "
                f"({self.store_epoch!r} vs {other.store_epoch!r})")
        return self.global_write_sequence < other.global_write_sequence

    def as_tuple(self) -> tuple[str, int, int]:
        return (self.store_epoch, self.global_write_sequence, self.key_revision)


@dataclass(frozen=True, slots=True)
class KnowledgeRef:
    """An exact reference: the key plus the version that was actually observed."""
    key: str
    version: KnowledgeVersion

    def __post_init__(self) -> None:
        _nonempty(self.key, "key")
        if not isinstance(self.version, KnowledgeVersion):
            raise ExactReferenceError("version must be a KnowledgeVersion")

    @property
    def store_epoch(self) -> str:
        return self.version.store_epoch

    @property
    def global_write_sequence(self) -> int:
        return self.version.global_write_sequence

    @property
    def key_revision(self) -> int:
        return self.version.key_revision

    def as_tuple(self) -> tuple[str, str, int, int]:
        """``(key, epoch, global_write_sequence, key_revision)``."""
        return (self.key,) + self.version.as_tuple()

    def __str__(self) -> str:
        return (f"{self.key}@{self.store_epoch}"
                f"/{self.global_write_sequence}/{self.key_revision}")


@dataclass(frozen=True, slots=True)
class WriteId:
    """Retry identity of one monitored write, independent of its version."""
    value: str

    def __post_init__(self) -> None:
        _nonempty(self.value, "write id")

    @classmethod
    def new(cls, prefix: str = "W") -> "WriteId":
        return cls(f"{prefix}{uuid.uuid4().hex}")

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True, slots=True)
class PhaseExecution:
    """Identity of one phase execution: ``(run_id, role, execution_id)``.

    ``run_id`` identifies the loop process; it is not the store epoch. The role
    is the lower-case phase role name (``"plan"``), and ``execution_id`` is the
    activation ID that every record of the execution carries.
    """
    run_id: str
    role: str
    execution_id: str

    def __post_init__(self) -> None:
        _nonempty(self.run_id, "run_id")
        _nonempty(self.role, "role")
        _nonempty(self.execution_id, "execution_id")

    @classmethod
    def new(cls, run_id: str, role: str, prefix: str = "X") -> "PhaseExecution":
        return cls(run_id=run_id, role=role,
                   execution_id=f"{prefix}{uuid.uuid4().hex}")


class PhaseOutcome(Enum):
    """How an execution ended. Both outcomes are explicit; neither is synthetic."""
    COMPLETED = "completed"
    ABORTED = "aborted"


# ===========================================================================
# JOURNAL RECORDS AND ENVELOPES
# ===========================================================================
def _journal_record(record: Any, tag: str) -> Any:
    """Check one durable record of ``tag`` and return a frozen copy.

    The record shapes themselves are built and checked by ``observation_wire``;
    the core only refuses anything that is not a record of this protocol.
    """
    if not isinstance(record, Mapping):
        raise ObservationError(f"a {tag} journal record must be a JSON object")
    if record.get("schema_version") != SCHEMA_VERSION or record.get("$tag") != tag:
        raise ObservationError(
            f"expected a schema_version {SCHEMA_VERSION} {tag} record, got "
            f"schema_version={record.get('schema_version')!r} "
            f"$tag={record.get('$tag')!r}")
    if not isinstance(record.get("payload"), Mapping):
        raise ObservationError(f"a {tag} journal record must carry an object payload")
    return freeze_json(dict(record))


def _knowledge_journal_record(record: Any, ref: KnowledgeRef) -> Any:
    """A ``Knowledge`` record that names exactly the version ``ref``."""
    frozen = _journal_record(record, "Knowledge")
    try:
        named = frozen["payload"]["version"]["ref"]
    except (KeyError, TypeError) as exc:
        raise ObservationError("a Knowledge record must name its version") from exc
    expected = {"key": ref.key, "store_epoch": ref.store_epoch,
                "global_write_sequence": ref.global_write_sequence}
    if not isinstance(named, Mapping) or dict(named) != expected:
        raise ObservationError(f"Knowledge record names {named!r}, committed {ref}")
    return frozen


@dataclass(frozen=True, slots=True)
class JournalEntry:
    """A record as admitted to one stream: source cursor and journal time.

    ``cursor`` is that stream's own opaque cursor (a Redis stream ID, or a
    decimal index in memory). Source order is the order in which a stream
    returns its entries; cursors are neither compared across streams nor
    assumed consecutive. ``journal_time`` is admission time, also carried by
    the record's ``arrival``, and is not assumed unique. ``record`` is the
    frozen ``schema_version`` 2 record exactly as it is stored.
    """
    stream: str
    cursor: str
    journal_time: float
    journal_clock: str
    record: Any

    def __post_init__(self) -> None:
        _nonempty(self.stream, "stream")
        _nonempty(self.cursor, "cursor")
        _check_time(self.journal_time, "journal_time")
        _nonempty(self.journal_clock, "journal_clock")
        if not isinstance(self.record, Mapping):
            raise ObservationError("entry record must be a journal record object")
        tag = self.record.get("$tag")
        if tag not in _RECORD_TAGS:
            raise ObservationError(f"entry record has unknown tag {tag!r}")
        object.__setattr__(self, "record", _journal_record(self.record, tag))


@dataclass(frozen=True, slots=True)
class KnowledgeSnapshot:
    """The exact value and reference a reader observed for one key."""
    ref: KnowledgeRef
    value: Any
    digest: str
    write_id: WriteId
    codec: str = CODEC_ID

    def __post_init__(self) -> None:
        _check_result(self)

    @property
    def key(self) -> str:
        return self.ref.key

    @property
    def version(self) -> KnowledgeVersion:
        return self.ref.version


def _check_result(result: Any) -> None:
    if not isinstance(result.ref, KnowledgeRef):
        raise ExactReferenceError("a Knowledge result requires an exact KnowledgeRef")
    if not isinstance(result.write_id, WriteId):
        raise ObservationError("write_id must be a WriteId")
    _nonempty(result.codec, "codec")
    object.__setattr__(result, "value", freeze_json(result.value))
    _check_digest(result.value, result.digest, result.codec)


@dataclass(frozen=True, slots=True)
class KnowledgeCommit:
    """The result of a monitored write.

    ``created`` is false for an idempotent retry: the same version and value
    were returned, no revision was allocated and no history record appended, so
    ``entry`` is the original admission entry.
    """
    ref: KnowledgeRef
    value: Any
    digest: str
    write_id: WriteId
    created: bool
    entry: JournalEntry
    codec: str = CODEC_ID

    def __post_init__(self) -> None:
        _check_result(self)
        if not isinstance(self.created, bool):
            raise ObservationError("created must be a boolean")
        if not isinstance(self.entry, JournalEntry):
            raise ObservationError("entry must be a JournalEntry")

    @property
    def key(self) -> str:
        return self.ref.key

    @property
    def version(self) -> KnowledgeVersion:
        return self.ref.version

    def snapshot(self) -> KnowledgeSnapshot:
        return KnowledgeSnapshot(ref=self.ref, value=self.value, digest=self.digest,
                                 write_id=self.write_id, codec=self.codec)


#: Builds the ``Phase`` record admitted at the given journal time.
PhaseRecordBuilder = Callable[[float], Mapping[str, Any]]
#: Builds the ``Knowledge`` record of the committed version admitted at a time.
KnowledgeRecordBuilder = Callable[[KnowledgeRef, float], Mapping[str, Any]]


# ===========================================================================
# JOURNAL BACKEND
# ===========================================================================
@runtime_checkable
class JournalBackend(Protocol):
    """Injectable durable side of the producer: streams plus the Knowledge store.

    An implementation owns the persistent epoch, the global write sequence, the
    per-key revision, retry identity, admission time and the atomic commit of
    latest state plus history. A caller supplies each durable record as a
    builder, because only the backend knows the admission time (and, for a
    write, the version) at the moment of admission.
    ``InMemoryJournalBackend`` is the reference implementation.
    """

    @property
    def store_epoch(self) -> str:
        """Persistent store lineage. Survives restarts; changes only on reset."""

    @property
    def global_write_sequence(self) -> int:
        """Highest allocated store-global write sequence (0 before any write)."""

    def key_revision(self, key: str) -> int:
        """Highest allocated revision of ``key`` (0 if never written)."""

    def append_phase_record(self, journal_record: PhaseRecordBuilder) -> JournalEntry:
        """Stamp journal time and append the ``Phase`` record built for it."""

    def commit_knowledge_write(
        self, key: str, value: Any, write_id: WriteId,
        digest: str, journal_record: KnowledgeRecordBuilder,
        phase_record: Optional[Callable[[KnowledgeRef, float], Mapping[str, Any]]] = None,
    ) -> KnowledgeCommit:
        """Atomically version one write, update latest state and append history.

        If supplied, ``phase_record`` builds the callback's Phase Write from
        the committed reference and admission time. The initial Knowledge
        commit and Phase Write are admitted atomically with the same time.
        Repeating ``write_id`` with the same key and value returns the committed
        version unchanged; it appends only the new Phase access, when supplied,
        at the retry's admission time. Repeating it with a different key or
        value raises ``WriteConflictError``. A failed admission must leave
        latest state and both history streams unchanged.
        """

    def read_latest(self, key: str) -> Optional[KnowledgeSnapshot]:
        """Exact snapshot of the latest committed version, or None if absent."""

    def resolve(self, ref: KnowledgeRef) -> Optional[KnowledgeSnapshot]:
        """The committed value at exactly ``ref``, or None. Never a later value."""

    def knowledge_entry(self, ref: KnowledgeRef) -> Optional[JournalEntry]:
        """The immutable public record at exactly ``ref``, without scanning history."""

    def phase_journal(self) -> tuple[JournalEntry, ...]:
        """Append-only phase stream in source order."""

    def knowledge_journal(self) -> tuple[JournalEntry, ...]:
        """Append-only versioned-Knowledge stream in source order."""


class InMemoryJournalBackend:
    """Process-local reference backend: two append-only streams and the store.

    Every mutation happens under one lock, and a write's version allocation,
    latest-state update and history append share a single publication point, so
    no observer can see latest state without its history record. ``commit_fault``
    exists to prove that: it is invoked after the version is computed and before
    anything is published, so raising from it must leave the backend unchanged.
    The journal clock is also read, and the record built, before publication for
    the same reason.
    """

    def __init__(self, store_epoch: str = "E1",
                 journal_clock: Callable[[], float] = time.monotonic,
                 journal_clock_domain: str = "time.monotonic",
                 commit_fault: Optional[Callable[[str, KnowledgeVersion], None]] = None,
                 phase_stream: str = PHASE_STREAM,
                 knowledge_stream: str = KNOWLEDGE_STREAM):
        self._store_epoch = _nonempty(store_epoch, "store_epoch")
        self.phase_stream = _nonempty(phase_stream, "phase_stream")
        self.knowledge_stream = _nonempty(knowledge_stream, "knowledge_stream")
        self._clock = journal_clock
        self._clock_domain = _nonempty(journal_clock_domain, "journal_clock_domain")
        self.commit_fault = commit_fault
        self._lock = threading.RLock()
        self._sequence = 0
        self._revisions: dict[str, int] = {}
        self._latest: dict[str, KnowledgeSnapshot] = {}
        self._history: dict[KnowledgeRef, KnowledgeSnapshot] = {}
        self._writes: dict[WriteId, KnowledgeCommit] = {}
        self._phase: list[JournalEntry] = []
        self._knowledge: list[JournalEntry] = []

    # -- identity and counters ---------------------------------------------
    @property
    def store_epoch(self) -> str:
        return self._store_epoch

    @property
    def global_write_sequence(self) -> int:
        with self._lock:
            return self._sequence

    def key_revision(self, key: str) -> int:
        with self._lock:
            return self._revisions.get(key, 0)

    # -- streams -----------------------------------------------------------
    def append_phase_record(self, journal_record: PhaseRecordBuilder) -> JournalEntry:
        if not callable(journal_record):
            raise ObservationError("a phase append requires a Phase record builder")
        with self._lock:
            admitted = float(self._clock())
            record = _journal_record(journal_record(admitted), "Phase")
            entry = JournalEntry(stream=self.phase_stream, cursor=str(len(self._phase)),
                                 journal_time=admitted,
                                 journal_clock=self._clock_domain, record=record)
            self._phase.append(entry)
            return entry

    def phase_journal(self) -> tuple[JournalEntry, ...]:
        with self._lock:
            return tuple(self._phase)

    def knowledge_journal(self) -> tuple[JournalEntry, ...]:
        with self._lock:
            return tuple(self._knowledge)

    # -- monitored writes --------------------------------------------------
    def commit_knowledge_write(self, key: str, value: Any, write_id: WriteId,
                               digest: str, journal_record: KnowledgeRecordBuilder,
                               phase_record=None) -> KnowledgeCommit:
        _nonempty(key, "key")
        if not isinstance(write_id, WriteId):
            raise ObservationError("write_id must be a WriteId")
        if not callable(journal_record):
            raise ObservationError("a Knowledge commit requires a Knowledge record builder")
        frozen = freeze_json(value)
        _check_digest(frozen, digest)
        with self._lock:
            prior = self._writes.get(write_id)
            if prior is not None:
                if prior.key != key or prior.digest != digest:
                    raise WriteConflictError(
                        f"write id {write_id.value!r} already committed "
                        f"{prior.key}={prior.digest}; retried with {key}={digest}")
                if phase_record is not None:
                    self.append_phase_record(lambda now: phase_record(prior.ref, now))
                return replace(prior, created=False)

            version = KnowledgeVersion(store_epoch=self._store_epoch,
                                       global_write_sequence=self._sequence + 1,
                                       key_revision=self._revisions.get(key, 0) + 1)
            ref = KnowledgeRef(key, version)
            admitted = float(self._clock())
            record = _knowledge_journal_record(journal_record(ref, admitted), ref)
            entry = JournalEntry(stream=self.knowledge_stream,
                                 cursor=str(len(self._knowledge)),
                                 journal_time=admitted,
                                 journal_clock=self._clock_domain, record=record)
            commit = KnowledgeCommit(ref=ref, value=frozen, digest=digest,
                                     write_id=write_id, created=True, entry=entry,
                                     codec=CODEC_ID)
            snapshot = commit.snapshot()
            phase_entry = None
            if phase_record is not None:
                phase = _journal_record(phase_record(ref, admitted), "Phase")
                phase_entry = JournalEntry(self.phase_stream, str(len(self._phase)),
                                           admitted, self._clock_domain, phase)
            if self.commit_fault is not None:
                # Injected before any state changes: the commit is all-or-nothing.
                self.commit_fault(key, version)

            # Single publication point; nothing below can fail.
            self._sequence = version.global_write_sequence
            self._revisions[key] = version.key_revision
            self._knowledge.append(entry)
            if phase_entry is not None:
                self._phase.append(phase_entry)
            self._latest[key] = snapshot
            self._history[commit.ref] = snapshot
            self._writes[write_id] = commit
            return commit

    def read_latest(self, key: str) -> Optional[KnowledgeSnapshot]:
        _nonempty(key, "key")
        with self._lock:
            return self._latest.get(key)

    def resolve(self, ref: KnowledgeRef) -> Optional[KnowledgeSnapshot]:
        if not isinstance(ref, KnowledgeRef):
            raise ExactReferenceError("resolve requires an exact KnowledgeRef")
        with self._lock:
            return self._history.get(ref)

    def knowledge_entry(self, ref: KnowledgeRef) -> Optional[JournalEntry]:
        if not isinstance(ref, KnowledgeRef):
            raise ExactReferenceError("knowledge_entry requires an exact KnowledgeRef")
        with self._lock:
            if ref not in self._history:
                return None
            # Every committed global write appends exactly one Knowledge entry;
            # retries allocate neither a new sequence nor a new history row.
            index = ref.global_write_sequence - 1
            if index >= len(self._knowledge):
                raise ObservationError(f"public history entry for {ref} is missing")
            entry = self._knowledge[index]
            _knowledge_journal_record(entry.record, ref)
            return entry


# ===========================================================================
# MONITORED KNOWLEDGE BOUNDARY
# ===========================================================================
class MonitoredKnowledgeStore:
    """Codec boundary over a :class:`JournalBackend`.

    Encoding is versioned canonical JSON; pickle is never produced here. The
    store validates and digests values and delegates versioning, admission,
    atomicity and retry identity to the backend. The caller supplies the
    ``Knowledge`` record builder, which names the typed value and its writer.
    """

    def __init__(self, backend: JournalBackend):
        self.backend = backend

    @property
    def store_epoch(self) -> str:
        return self.backend.store_epoch

    @property
    def codec(self) -> str:
        return CODEC_ID

    def write(self, key: str, value: Any,
              write_id: Optional[WriteId] = None, *,
              journal_record: KnowledgeRecordBuilder) -> KnowledgeCommit:
        """Commit one monitored write; retrying a write ID returns the same commit."""
        _nonempty(key, "key")
        if write_id is None:
            write_id = WriteId.new()
        if isinstance(write_id, str):
            write_id = WriteId(write_id)
        frozen = freeze_json(value)
        return self.backend.commit_knowledge_write(
            key=key, value=frozen, write_id=write_id, digest=value_digest(frozen),
            journal_record=journal_record)

    def read(self, key: str) -> Optional[KnowledgeSnapshot]:
        """Latest committed snapshot, with its exact reference, or None."""
        return self.backend.read_latest(_nonempty(key, "key"))

    def resolve(self, ref: KnowledgeRef) -> Optional[KnowledgeSnapshot]:
        """The committed value at exactly ``ref``, or None."""
        return self.backend.resolve(ref)


# ===========================================================================
# PHASE OBSERVATION CONTEXT
