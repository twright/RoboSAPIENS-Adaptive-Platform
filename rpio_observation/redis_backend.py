"""Redis :class:`~rpio_observation.core.JournalBackend` (redis-py).

Layout, with ``P`` the configured key prefix (default ``maple:observation``)
and the two stream keys configured separately:

=====================  ======  ==============================================
Key                    Type    Content
=====================  ======  ==============================================
``P:epoch``            string  Store lineage. Created once; never rewritten.
``P:seq``              string  Highest global write sequence.
``P:revisions``        hash    Knowledge key -> highest key revision.
``P:latest``           hash    Knowledge key -> stored version of the latest
                               committed write.
``P:writes``           hash    Write ID -> ``<stream ID>|<stored version>``
                               (retry identity).
``P:sequence-index``   hash    Global write sequence ->
                               ``<stream ID>|<stored version>``.
``P:streams``          hash    ``knowledge``/``phase`` stream names and the
                               ``schema_version`` of the epoch.
knowledge stream       stream  One entry per committed write.
phase stream           stream  One entry per phase record.
=====================  ======  ==============================================

Every stream entry has the fields ``record`` (canonical JSON of the
``schema_version`` 2 record), ``journal_time`` (Redis ``TIME``, seconds) and
``journal_clock``. The stream ID is the entry's opaque cursor. The record's
``arrival.journal_time`` is the same admission time, inserted by the script.

A journal record deliberately omits retry identity, digest and key revision.
The store keeps them privately as a *stored version*: canonical JSON of
``codec``, ``digest``, ``global_write_sequence``, ``key``, ``key_revision``,
``store_epoch``, ``value`` and ``write_id``. Redis stream IDs never contain
``|``, so ``<stream ID>|<stored version>`` splits unambiguously.

The epoch and its stream identity are created together. Reopening an epoch
with different stream names or another ``schema_version``, or finding
observation state (including either stream) without an epoch, fails stop, so
one epoch never splits its history.
Every read and append checks the live epoch in the same transaction or
script, so a reset or replaced lineage fails stop instead of mixing lineages.

A monitored write is one Lua script: it checks key types, counter
representations, stream capacity, the epoch, the stream identity and the
write ID, and only then allocates the sequence and revision, appends the
history entry, and updates latest state, the retry record and the sequence
index. When supplied, the callback's Phase Write is appended in the same
script with the same admission time. An idempotent retry retains the original
Knowledge commit and appends only the new Phase access. Redis runs a script
without interleaving, and every check precedes the first mutation, so no
reader can observe latest state without its history entry. Wrong-typed or
malformed state raises ``StoreIntegrityError`` and changes nothing. Requires Redis 7 (``#!lua`` scripts). Retention is not
bounded here: nothing trims either stream. Durability is whatever the Redis
server's persistence configuration provides.
"""
from __future__ import annotations

import json
import re
import secrets
import uuid
from typing import Any, Mapping, Optional

from rpio_observation.core import (
    CODEC_ID, KNOWLEDGE_STREAM, PHASE_STREAM, SCHEMA_VERSION, CodecError,
    ExactReferenceError, JournalEntry, KnowledgeCommit, KnowledgeRef,
    KnowledgeSnapshot, KnowledgeVersion, ObservationError, WriteConflictError,
    WriteId, _check_digest, _journal_record, _knowledge_journal_record, _nonempty,
    canonical_text, freeze_json)

__all__ = ["RedisJournalBackend", "StoreIntegrityError", "StoreLineageError",
           "DEFAULT_KEY_PREFIX", "REDIS_JOURNAL_CLOCK"]

DEFAULT_KEY_PREFIX = "maple:observation"
#: Clock domain of ``journal_time``: the Redis server's ``TIME`` (wall clock).
REDIS_JOURNAL_CLOCK = "redis.server.time"


class StoreIntegrityError(ObservationError):
    """Observation state in Redis is malformed (wrong type or representation)."""


class StoreLineageError(StoreIntegrityError):
    """The store lineage is missing, changed, split or inconsistent."""


# Shared preflight. Every script checks key types, counter representations and
# stream capacity before its first mutation, so a later command cannot fail
# and leave sequence, latest state and history partially updated. Scripts are
# declared with ``#!lua`` (Redis >= 7), which makes Redis refuse a writing
# script up front under OOM instead of after its first write.
_PREFLIGHT = """
local MAX_COUNTER = 9007199254740991
local MAX_ID = '18446744073709551615-18446744073709551615'
local function integrity(message)
  return redis.error_reply('INTEGRITY ' .. message)
end
local function lineage(message)
  return redis.error_reply('LINEAGE ' .. message)
end
local function check_types(keys, expected)
  for i = 1, #keys do
    local found = redis.call('TYPE', keys[i])['ok']
    if found ~= 'none' and found ~= expected[i] then
      return keys[i] .. ' has type ' .. found .. ', expected ' .. expected[i]
    end
  end
  return nil
end
local function counter(value)
  if not value then return 0 end
  if value ~= '0' and not string.match(value, '^[1-9]%d*$') then return nil end
  if #value > 16 or tonumber(value) >= MAX_COUNTER then return nil end
  return tonumber(value)
end
local function stream_full(key)
  if redis.call('EXISTS', key) == 0 then return false end
  local info = redis.call('XINFO', 'STREAM', key)
  for i = 1, #info, 2 do
    if info[i] == 'last-generated-id' then return info[i + 1] == MAX_ID end
  end
  return true
end
local function canonical_time()
  local now = redis.call('TIME')
  local fraction = string.format('%06d', now[2])
  fraction = string.gsub(fraction, '0+$', '')
  if fraction == '' then fraction = '0' end
  return tostring(now[1]) .. '.' .. fraction
end
"""

# KEYS: epoch, seq, revisions, latest, writes, index, knowledge stream,
#       stream identity, phase stream
# ARGV[1-5]: new epoch, knowledge stream name, phase stream name,
#            schema version, require-fresh flag
_INIT_EPOCH = "#!lua\n" + _PREFLIGHT + """
local bad = check_types(KEYS, {'string', 'string', 'hash', 'hash', 'hash', 'hash',
                               'stream', 'hash', 'stream'})
if bad then return integrity(bad) end
local epoch = redis.call('GET', KEYS[1])
if epoch then
  if ARGV[5] == '1' then return lineage('namespace already holds a run') end
  local identity = redis.call('HMGET', KEYS[8], 'knowledge', 'phase', 'schema_version')
  if not identity[1] or not identity[2] then
    return integrity('epoch ' .. epoch .. ' has no recorded stream identity')
  end
  if identity[1] ~= ARGV[2] or identity[2] ~= ARGV[3] then
    return lineage('epoch ' .. epoch .. ' belongs to streams knowledge=' .. identity[1]
                   .. ' phase=' .. identity[2] .. '; configured knowledge=' .. ARGV[2]
                   .. ' phase=' .. ARGV[3])
  end
  if identity[3] ~= ARGV[4] then
    return lineage('epoch ' .. epoch .. ' holds schema_version ' .. tostring(identity[3])
                   .. '; this producer writes schema_version ' .. ARGV[4])
  end
  return epoch
end
for i = 2, #KEYS do
  if redis.call('EXISTS', KEYS[i]) == 1 then
    return lineage('observation state exists without an epoch: ' .. KEYS[i])
  end
end
redis.call('HSET', KEYS[8], 'knowledge', ARGV[2], 'phase', ARGV[3],
           'schema_version', ARGV[4])
redis.call('SET', KEYS[1], ARGV[1])
return ARGV[1]
"""

# Shared lineage check of the mutating scripts: the live epoch, both stream
# names and the schema version must be the ones this backend opened.
# KEYS as for _INIT_EPOCH.
_CHECK_LINEAGE = """
local function check_lineage(epoch_key, identity_key, knowledge, phase, expected, schema)
  local epoch = redis.call('GET', epoch_key)
  if epoch ~= expected then
    return 'store epoch is ' .. tostring(epoch) .. ', expected ' .. expected
  end
  local identity = redis.call('HMGET', identity_key, 'knowledge', 'phase', 'schema_version')
  if identity[1] ~= knowledge then
    return 'knowledge stream ' .. knowledge .. ' is not the stream of epoch ' .. epoch
  end
  if identity[2] ~= phase then
    return 'phase stream ' .. phase .. ' is not the stream of epoch ' .. epoch
  end
  if identity[3] ~= schema then
    return 'epoch ' .. epoch .. ' holds schema_version ' .. tostring(identity[3])
           .. '; this producer writes schema_version ' .. schema
  end
  return nil
end
"""

# KEYS: epoch, seq, revisions, latest, writes, index, knowledge stream,
#       stream identity, phase stream
# ARGV[1-8]: epoch, key, write id, stored-version prefix/middle/suffix,
#            journal clock, schema version
# ARGV[9-11]: Knowledge record prefix/middle/suffix
# ARGV[12-14]: optional Phase record prefix/middle/suffix
# ARGV[15]: value digest for retry conflict checks
_COMMIT = "#!lua\n" + _PREFLIGHT + _CHECK_LINEAGE + """
local bad = check_types(KEYS, {'string', 'string', 'hash', 'hash', 'hash', 'hash',
                               'stream', 'hash', 'stream'})
if bad then return integrity(bad) end
local wrong = check_lineage(KEYS[1], KEYS[8], KEYS[7], KEYS[9], ARGV[1], ARGV[8])
if wrong then return lineage(wrong) end
local function append_phase(seq_text, journal_time)
  local phase = ARGV[12] .. journal_time .. ARGV[13] .. seq_text .. ARGV[14]
  redis.call('XADD', KEYS[9], '*', 'record', phase,
             'journal_time', journal_time, 'journal_clock', ARGV[7])
end
local prior = redis.call('HGET', KEYS[5], ARGV[3])
if prior then
  if ARGV[12] ~= '' then
    if stream_full(KEYS[9]) then return integrity(KEYS[9] .. ' cannot accept entries') end
    local separator = string.find(prior, '|', 1, true)
    if not separator then return integrity('malformed retry identity') end
    local stored = string.sub(prior, separator + 1)
    local ok, value = pcall(cjson.decode, stored)
    if not ok then return integrity('malformed retry version') end
    -- A conflicting retry appends nothing; Python returns its usual conflict.
    if value.key == ARGV[2] and value.digest == ARGV[15] then
      local seq_text = string.match(stored, '"global_write_sequence":(%d+)')
      if not seq_text then return integrity('malformed retry sequence') end
      append_phase(seq_text, canonical_time())
    end
  end
  return {'replay', prior}
end
local seq = counter(redis.call('GET', KEYS[2]))
if not seq then return integrity(KEYS[2] .. ' is not a valid sequence counter') end
local rev = counter(redis.call('HGET', KEYS[3], ARGV[2]))
if not rev then return integrity(KEYS[3] .. '[' .. ARGV[2] .. '] is not a valid revision') end
seq, rev = seq + 1, rev + 1
local seq_text, rev_text = string.format('%d', seq), string.format('%d', rev)
if redis.call('HEXISTS', KEYS[6], seq_text) == 1 then
  return integrity('sequence ' .. seq_text .. ' already has history')
end
if stream_full(KEYS[7]) then return integrity(KEYS[7] .. ' cannot accept entries') end
if ARGV[12] ~= '' and stream_full(KEYS[9]) then
  return integrity(KEYS[9] .. ' cannot accept entries')
end
local journal_time = canonical_time()
local stored = ARGV[4] .. seq_text .. ARGV[5] .. rev_text .. ARGV[6]
local record = ARGV[9] .. journal_time .. ARGV[10] .. seq_text .. ARGV[11]
-- Mutations: none of these can fail after the preflight above.
redis.call('SET', KEYS[2], seq_text)
redis.call('HSET', KEYS[3], ARGV[2], rev_text)
local id = redis.call('XADD', KEYS[7], '*', 'record', record,
                      'journal_time', journal_time, 'journal_clock', ARGV[7])
redis.call('HSET', KEYS[4], ARGV[2], stored)
-- Redis stream IDs never contain '|', while the stored JSON may.
local identity = id .. '|' .. stored
redis.call('HSET', KEYS[5], ARGV[3], identity)
redis.call('HSET', KEYS[6], seq_text, identity)
if ARGV[12] ~= '' then append_phase(seq_text, journal_time) end
return {'created', id, record, journal_time, stored}
"""

# KEYS: phase stream, epoch, stream identity, knowledge stream
# ARGV: journal clock, epoch, schema version, record prefix/suffix
_APPEND = "#!lua\n" + _PREFLIGHT + _CHECK_LINEAGE + """
local bad = check_types(KEYS, {'stream', 'string', 'hash', 'stream'})
if bad then return integrity(bad) end
local wrong = check_lineage(KEYS[2], KEYS[3], KEYS[4], KEYS[1], ARGV[2], ARGV[3])
if wrong then return lineage(wrong) end
if stream_full(KEYS[1]) then return integrity(KEYS[1] .. ' cannot accept entries') end
local journal_time = canonical_time()
local record = ARGV[4] .. journal_time .. ARGV[5]
local id = redis.call('XADD', KEYS[1], '*', 'record', record,
                      'journal_time', journal_time, 'journal_clock', ARGV[1])
return {id, journal_time}
"""

_NUMBERED = ("global_write_sequence", "key_revision")
_STORED_FIELDS = frozenset(("codec", "digest", "global_write_sequence", "key",
                            "key_revision", "store_epoch", "value", "write_id"))


def _text(value: Any) -> str:
    return value.decode("utf-8") if isinstance(value, bytes) else value


def _stored_template(fields: Mapping[str, Any]) -> tuple[str, str, str]:
    """Canonical stored-version text split around the two counters.

    The script concatenates ``prefix + seq + middle + rev + suffix``. Keys are
    emitted in canonical (sorted) order, so the result is byte-identical to
    ``canonical_text`` of the complete stored version.
    """
    fields = dict(fields)
    fields.update({name: None for name in _NUMBERED})
    parts, current = [], "{"
    for index, name in enumerate(sorted(fields)):
        current += ("," if index else "") + json.dumps(name, ensure_ascii=False) + ":"
        if name in _NUMBERED:
            parts.append(current)
            current = ""
        else:
            current += canonical_text(fields[name])
    parts.append(current + "}")
    assert len(parts) == 3
    return parts[0], parts[1], parts[2]


def _stored_version(text: str) -> KnowledgeSnapshot:
    """Decode a stored version into the snapshot it records."""
    try:
        fields = json.loads(text)
        if not isinstance(fields, dict) or set(fields) != _STORED_FIELDS:
            raise ValueError(f"fields {sorted(fields) if isinstance(fields, dict) else fields}")
        version = KnowledgeVersion(fields["store_epoch"], fields["global_write_sequence"],
                                   fields["key_revision"])
        return KnowledgeSnapshot(ref=KnowledgeRef(fields["key"], version),
                                 value=fields["value"], digest=fields["digest"],
                                 write_id=WriteId(fields["write_id"]),
                                 codec=fields["codec"])
    except (TypeError, ValueError, ObservationError) as exc:
        raise StoreIntegrityError(f"malformed stored version: {exc}") from exc


def _stored_identity(identity: str, what: str) -> tuple[str, KnowledgeSnapshot]:
    """Split ``<stream ID>|<stored version>``."""
    cursor, separator, stored = identity.partition("|")
    if not separator or not cursor:
        raise StoreIntegrityError(f"{what} has malformed history identity")
    return cursor, _stored_version(stored)


def _record_template(record: Mapping[str, Any], with_sequence: bool,
                     sequence_path=("payload", "version", "ref", "global_write_sequence")) -> tuple[str, ...]:
    """Split canonical record JSON around values inserted atomically by Lua."""
    mutable = json.loads(canonical_text(dict(record)))
    original = canonical_text(mutable)

    def marker(label: str) -> str:
        # Application strings may contain any legal JSON text. Generate a
        # fresh marker and prove its quoted token absent from this record
        # before using it as the sole structural substitution site.
        # ``secrets`` rather than ``uuid``: callers may make uuid4 deterministic.
        while True:
            candidate = f"__RPIO_{label}_{secrets.token_hex(16)}__"
            if canonical_text(candidate) not in original:
                return candidate

    time_marker = marker("JOURNAL_TIME")
    mutable["payload"]["arrival"]["journal_time"] = time_marker
    sequence_marker = None
    if with_sequence:
        sequence_marker = marker("SEQUENCE")
        target = mutable
        for key in sequence_path[:-1]:
            target = target[key]
        target[sequence_path[-1]] = sequence_marker
    text = canonical_text(mutable)
    time_token = canonical_text(time_marker)
    if text.count(time_token) != 1:
        raise CodecError("admission-time template is not structurally unique")
    if with_sequence:
        sequence_token = canonical_text(sequence_marker)
        if text.count(sequence_token) != 1:
            raise CodecError("sequence template is not structurally unique")
        prefix, rest = text.split(time_token, 1)
        middle, suffix = rest.split(sequence_token, 1)
        return prefix, middle, suffix
    prefix, suffix = text.split(time_token, 1)
    return prefix, suffix


class RedisJournalBackend:
    """Journal backend over one Redis database; safe to share across threads.

    The epoch is created, with the configured stream names and the schema
    version, on first use of an empty namespace and then only read, so it
    survives producer restarts. If counters, latest state or either stream
    exist without an epoch, or the epoch belongs to other streams or another
    schema version, construction fails.
    """

    def __init__(self, client, key_prefix: str = DEFAULT_KEY_PREFIX,
                 phase_stream: str = PHASE_STREAM,
                 knowledge_stream: str = KNOWLEDGE_STREAM,
                 journal_clock_domain: str = REDIS_JOURNAL_CLOCK,
                 initial_epoch: str | None = None, require_fresh: bool = False):
        self.client = client
        self.key_prefix = _nonempty(key_prefix, "key_prefix")
        self.phase_stream = _nonempty(phase_stream, "phase_stream")
        self.knowledge_stream = _nonempty(knowledge_stream, "knowledge_stream")
        if self.phase_stream == self.knowledge_stream:
            raise ObservationError("phase and knowledge streams must be distinct")
        self.journal_clock_domain = _nonempty(journal_clock_domain, "journal_clock_domain")
        self.keys = {name: f"{self.key_prefix}:{name}" for name in
                     ("epoch", "seq", "revisions", "latest", "writes", "sequence-index",
                      "streams")}
        for label, stream in (("phase", self.phase_stream),
                              ("knowledge", self.knowledge_stream)):
            if stream in self.keys.values():
                raise ObservationError(
                    f"{label} stream {stream!r} collides with a store metadata key")
        self._store_keys = [self.keys[name] for name in
                            ("epoch", "seq", "revisions", "latest", "writes",
                             "sequence-index")] + [self.knowledge_stream,
                                                   self.keys["streams"]]
        self._init_epoch = client.register_script(_INIT_EPOCH)
        self._commit = client.register_script(_COMMIT)
        self._append = client.register_script(_APPEND)
        try:
            epoch = self._init_epoch(
                keys=self._store_keys + [self.phase_stream],
                args=[initial_epoch or f"E{uuid.uuid4().hex}", self.knowledge_stream,
                      self.phase_stream, str(SCHEMA_VERSION), "1" if require_fresh else "0"])
        except Exception as exc:
            raise self._translate(exc) from exc
        self._store_epoch = _text(epoch)

    @classmethod
    def from_url(cls, url: str, **kwargs) -> "RedisJournalBackend":
        import redis
        return cls(redis.Redis.from_url(url), **kwargs)

    def _checked(self, *commands):
        """Run read commands in one transaction with the live epoch check."""
        pipe = self.client.pipeline(transaction=True)
        pipe.get(self.keys["epoch"])
        for name, *args in commands:
            getattr(pipe, name)(*args)
        try:
            epoch, *results = pipe.execute()
        except Exception as exc:
            raise self._translate(exc) from exc
        if _text(epoch) != self._store_epoch:
            raise StoreLineageError(
                f"store epoch is {_text(epoch)!r}, expected {self._store_epoch!r}")
        return results

    @staticmethod
    def _translate(exc: Exception) -> Exception:
        text = str(exc)
        if "LINEAGE" in text:
            return StoreLineageError(text)
        if "INTEGRITY" in text or "WRONGTYPE" in text:
            return StoreIntegrityError(text)
        return exc

    @staticmethod
    def _counter(value: Any, name: str) -> int:
        text = _text(value)
        if text is None:
            return 0
        if not re.fullmatch(r"0|[1-9][0-9]*", text):
            raise StoreIntegrityError(f"{name} is not a valid counter: {text!r}")
        return int(text)

    # -- identity and counters ---------------------------------------------
    @property
    def store_epoch(self) -> str:
        return self._store_epoch

    @property
    def global_write_sequence(self) -> int:
        (value,) = self._checked(("get", self.keys["seq"]))
        return self._counter(value, self.keys["seq"])

    def key_revision(self, key: str) -> int:
        (value,) = self._checked(("hget", self.keys["revisions"], _nonempty(key, "key")))
        return self._counter(value, f"{self.keys['revisions']}[{key}]")

    # -- decoding ----------------------------------------------------------
    def _entry(self, stream: str, cursor: Any, fields: Any) -> JournalEntry:
        # redis-py parses XRANGE into a dict; inside a script it is a flat list.
        pairs = fields.items() if isinstance(fields, Mapping) else zip(fields[::2], fields[1::2])
        fields = {_text(k): _text(v) for k, v in pairs}
        try:
            record = json.loads(fields["record"])
            journal_time = float(fields["journal_time"])
            clock = fields["journal_clock"]
            return JournalEntry(stream=stream, cursor=_text(cursor),
                                journal_time=journal_time, journal_clock=clock,
                                record=record)
        except (KeyError, ValueError, AttributeError, ObservationError) as exc:
            raise CodecError(f"malformed {stream} entry {_text(cursor)}: {exc}") from exc

    def _range(self, stream: str) -> tuple[JournalEntry, ...]:
        (entries,) = self._checked(("xrange", stream))
        return tuple(self._entry(stream, cursor, fields) for cursor, fields in entries)

    def _history(self, cursor: str, stored: KnowledgeSnapshot) -> JournalEntry:
        """The public history entry at ``cursor``, checked against ``stored``."""
        (entries,) = self._checked(("xrange", self.knowledge_stream, cursor, cursor))
        if not entries:
            raise StoreLineageError(f"history entry {cursor} for {stored.ref} is missing")
        entry = self._entry(self.knowledge_stream, entries[0][0], entries[0][1])
        try:
            _knowledge_journal_record(entry.record, stored.ref)
        except ObservationError as exc:
            raise StoreLineageError(
                f"public history entry {cursor} disagrees with {stored.ref}: {exc}") from exc
        return entry

    # -- streams -----------------------------------------------------------
    def append_phase_record(self, journal_record) -> JournalEntry:
        if not callable(journal_record):
            raise ObservationError("a phase append requires a Phase record builder")
        prefix, suffix = _record_template(_journal_record(journal_record(0.0), "Phase"),
                                          False)
        try:
            cursor, journal_time = self._append(
                keys=[self.phase_stream, self.keys["epoch"], self.keys["streams"],
                      self.knowledge_stream],
                args=[self.journal_clock_domain, self._store_epoch, str(SCHEMA_VERSION),
                      prefix, suffix])
        except Exception as exc:
            raise self._translate(exc) from exc
        admitted = float(_text(journal_time))
        return JournalEntry(stream=self.phase_stream, cursor=_text(cursor),
                            journal_time=admitted,
                            journal_clock=self.journal_clock_domain,
                            record=journal_record(admitted))

    def phase_journal(self) -> tuple[JournalEntry, ...]:
        return self._range(self.phase_stream)

    def knowledge_journal(self) -> tuple[JournalEntry, ...]:
        return self._range(self.knowledge_stream)

    # -- monitored writes --------------------------------------------------
    def commit_knowledge_write(self, key: str, value: Any, write_id: WriteId,
                               digest: str, journal_record, phase_record=None) -> KnowledgeCommit:
        _nonempty(key, "key")
        if not isinstance(write_id, WriteId):
            raise ObservationError("write_id must be a WriteId")
        if not callable(journal_record):
            raise ObservationError("a Knowledge commit requires a Knowledge record builder")
        # Validates value, digest and the record before anything reaches Redis.
        frozen = freeze_json(value)
        _check_digest(frozen, digest)
        probe = KnowledgeRef(key, KnowledgeVersion(self._store_epoch, 1, 1))
        stored_parts = _stored_template({
            "codec": CODEC_ID, "digest": digest, "key": key,
            "store_epoch": self._store_epoch, "value": frozen,
            "write_id": write_id.value})
        record_parts = _record_template(
            _knowledge_journal_record(journal_record(probe, 0.0), probe), True)
        phase_parts = ("", "", "") if phase_record is None else _record_template(
            _journal_record(phase_record(probe, 0.0), "Phase"), True,
            ("payload", "knowledge", "payload", "global_write_sequence"))
        try:
            reply = self._commit(keys=self._store_keys + [self.phase_stream],
                                 args=[self._store_epoch, key, write_id.value,
                                       *stored_parts, self.journal_clock_domain,
                                       str(SCHEMA_VERSION), *record_parts, *phase_parts, digest])
        except Exception as exc:
            raise self._translate(exc) from exc
        kind = _text(reply[0])
        if kind == "replay":
            cursor, prior = _stored_identity(_text(reply[1]), f"write id {write_id.value!r}")
            if prior.key != key or prior.digest != digest:
                raise WriteConflictError(
                    f"write id {write_id.value!r} already committed "
                    f"{prior.key}={prior.digest}; retried with {key}={digest}")
            entry = self._history(cursor, prior)
            created = False
        else:
            cursor, text, journal_time = (_text(item) for item in reply[1:4])
            prior = _stored_version(_text(reply[4]))
            if prior.key != key or prior.digest != digest:
                raise StoreIntegrityError("created stored version disagrees with the request")
            record = json.loads(text)
            if canonical_text(record) != text:
                raise CodecError("script produced a non-canonical record")
            entry = JournalEntry(stream=self.knowledge_stream, cursor=cursor,
                                 journal_time=float(journal_time),
                                 journal_clock=self.journal_clock_domain, record=record)
            try:
                _knowledge_journal_record(entry.record, prior.ref)
            except ObservationError as exc:
                raise StoreLineageError(
                    f"created history record disagrees with {prior.ref}: {exc}") from exc
            created = True
        return KnowledgeCommit(ref=prior.ref, value=prior.value, digest=prior.digest,
                               write_id=write_id, created=created, entry=entry,
                               codec=CODEC_ID)

    def read_latest(self, key: str) -> Optional[KnowledgeSnapshot]:
        (text,) = self._checked(("hget", self.keys["latest"], _nonempty(key, "key")))
        if text is None:
            return None
        snapshot = _stored_version(_text(text))
        if snapshot.key != key or snapshot.version.store_epoch != self._store_epoch:
            raise StoreLineageError(f"latest state of {key!r} is {snapshot.ref}")
        return snapshot

    def _resolve_record(self, ref: KnowledgeRef):
        if not isinstance(ref, KnowledgeRef):
            raise ExactReferenceError("history lookup requires an exact KnowledgeRef")
        if ref.store_epoch != self._store_epoch:
            return None
        (identity,) = self._checked(
            ("hget", self.keys["sequence-index"], ref.global_write_sequence))
        if identity is None:
            return None
        cursor, stored = _stored_identity(
            _text(identity), f"sequence {ref.global_write_sequence}")
        if stored.version.global_write_sequence != ref.global_write_sequence:
            raise StoreLineageError(
                f"sequence {ref.global_write_sequence} indexes {stored.ref}")
        entry = self._history(cursor, stored)
        return (stored, entry) if stored.ref == ref else None

    def resolve(self, ref: KnowledgeRef) -> Optional[KnowledgeSnapshot]:
        result = self._resolve_record(ref)
        return result[0] if result is not None else None

    def knowledge_entry(self, ref: KnowledgeRef) -> Optional[JournalEntry]:
        """Resolve through the existing version index and one exact XRANGE row.

        The epoch and public history identity are checked on every lookup;
        there is no process cache that can outlive a reset or hide lost history.
        """
        result = self._resolve_record(ref)
        return result[1] if result is not None else None
