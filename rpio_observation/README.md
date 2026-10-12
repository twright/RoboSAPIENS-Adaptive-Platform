# rpio_observation

The observation package requires Python 3.10 or later.

The paper branch uses the classic reference journal with the current generic
`rpclpy.Node` observation API. Callback activity is recorded on a Phase stream;
immutable Knowledge values and their writers are committed once on a separate
Knowledge stream. Reads, publications and outcomes cite exact versions.

## Configuration

```yaml
Observation_Config:
  namespace: trial-0042
  role: Monitor
  loop: [screw_detection]
Knowledge_Config: {knowledge_type: redis, host: localhost, port: 6379, db: 0}
```

Create the run before constructing its nodes. Redis uses `{namespace}:phase`,
`{namespace}:knowledge` and the version-store metadata under the same namespace.
A namespace is bound to its epoch, stream names and protocol. Use a fresh
namespace for a fresh run. Redis 7 is required for the preflighted Lua scripts.
Local nodes share one `MemoryJournal` passed as `observation_backend=journal`.

```python
from rpio_observation import MemoryJournal, create_run, dump, outcomes

journal = MemoryJournal()
epoch = create_run("trial-0042", backend=journal)
# Or: create_run("trial-0042", redis_config={"host": "localhost", "port": 6379, "db": 0})
node = Cls(config, observation_backend=journal)
node.start()
# Deliver messages and await application completion.
node.shutdown()
assert not node.observation_failed
dump(journal, "trial-0042", "run.jsonl")
```

## Callback API

Every registered event/message callback has an activation identity and explicit
Start/End. Nested synchronous callbacks retain separate identities and causal
publication links. An observed callback returns one of the `outcomes` objects.

On observed nodes, typed Knowledge reads and writes use the observation store
(`{namespace}:latest` in Redis), rather than the ordinary KnowledgeManager key
space; unobserved nodes and the dashboard do not see those typed values through
that key space. Raw-string `read_knowledge` and `write_knowledge` still use the
KnowledgeManager and do not record an observed acquisition or commit.

- `read_knowledge(Cls)` returns a fresh mutable object or `None`; its Read cites
  the exact selected immutable snapshot. Selection and Read admission are atomic.
- `write_knowledge(obj)` atomically commits the Knowledge version, current state
  and the callback's Write step. Both stream records carry the same admission
  time. Before `start()` writes are standalone Knowledge commits and reads do
  not admit Phase steps. Typed access outside callbacks after start raises.
- `publish_event(topic, payload, evidence={name: obj})` and `publish_message`
  accept only objects actually read or written by that activation. A repeated
  write of the same object selects its most recent acquisition. Publication
  records cite refs; the immutable values remain in Knowledge history.
- `service_started(service, request_id=None)` and
  `service_completed(service, request_id=None, outcome="Delivered")` record
  request/completion boundaries. Completion outcomes also include `NotDelivered`.

The returned callback outcome names acquired Knowledge objects, for example
`Monitored(observation)`, `Nominal()`, `Anomaly(evidence)`, `Generated(plan)`,
`NoPlan()`, `Accepted(plan, legitimate_result)`, `Rejected(plan,
legitimate_result)`, `Applied(plan, effect)`, `NotApplied(plan, reason)` and
`Aborted(reason)`. A reason is a string or `BudgetExhausted()`.

## Application vocabulary

Set `Observation_Config["codec"]` to an application codec in the Python config.
The default `JsonCodec` preserves JSON fields; a typed TC specification supplies
its own codec to match the declared unions and optional fields. The codec has:

```python
knowledge(key, raw_value)        # tagged Knowledge value; raw store remains unchanged
trigger(kind, topic, message)    # typed callback trigger
emitted(kind, topic, message)    # typed event payload
outcome(wire)                    # outcome with exact acquired refs, not values
validate_evidence(kind, topic, slots)  # optional; slots map names to exact ref dicts
```

Codecs declare application types and evidence slots. Version allocation and
actual acquisition tracking belong to RPIO. The wire is classic schema 2:
Phase `knowledge` is `Nothing`, `One(ref)` or `Several(refs)`; Knowledge contains
`version.ref`, its typed value and writer. Refs contain epoch, key and a global
**write** sequence. Activations have string IDs independent of stream cursors.
Read/Write carry the accessed key, including absent reads; activations retain
the actual producer name for the consistency properties.

Business messages still travel through the application's event/message manager.
They carry `_observation_emission` plus the compatible epoch/activation/sequence
stamps. RPIO records the publication before sending it, allowing a synchronous
receiver to cite the emission. That record means a publication was admitted;
it does not independently establish transport delivery. A direct TC transport
subscription is a separate input configuration.

## Storage, failure and replay

The reusable store retains immutable snapshots and retry identities. Redis
checks key types, lineage and stream capacity before mutation. Current state,
history and Phase Write cannot be partially published. Atomic Read uses WATCH:
a competing write before admission reselects the snapshot; an ambiguous network
failure after admission is never retried. Observation failures set
`node.observation_failed`, and callback exceptions best-effort record an explicit
abort before being re-raised. The harness checks the flag before accepting a run.

`dump` writes raw canonical records as JSONL and reports epoch/count/SHA256.
It reads Phase first and then Knowledge, merging by admission time while keeping
each stream's order. At equal timestamps the file
places Knowledge before Phase, preserving visible evidence before its use. Live
stream delivery still resolves exact references independently of arrival order. Stream cursors and clock
identity remain available through `backend.entries()`; no artificial global
journal order is introduced. Retention remains unbounded for the whole run.

The classic store/backend were extracted from the measured ScrewDetection
producer at `0c10d85`; this branch retains the later acquisition/callback API and
strengthens Read/Write admission atomicity across the classic streams. The
previous inline journal implementation is preserved on
`archive/seams-2027-twright-before-v2-20261012`.

Tests: `RPIO_TEST_REDIS_URL=redis://localhost:6379/0 pytest tests`.
The portable reference capture is `tests/fixtures/journal-v2/nominal.jsonl`.
