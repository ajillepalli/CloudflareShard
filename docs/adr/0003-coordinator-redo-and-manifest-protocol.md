# ADR-3: Coordinator redo envelope and manifest registration protocol

- Status: Accepted
- Date: 2026-08-05
- Decision owners: CoordinatorDO, JournalManifestDO, and `@cloudflareshard/transaction-contracts`
- Depends on: ADR-1
- Required before: participant commit under the monotonic protocol

> 2026-08-05 amendment: the Manifest V2 section below supersedes the original
> V1 write path for new state-model-2 transactions. V1 remains an expand-first
> recovery format and is never reinterpreted as V2.

## Context

A commit witness stored only inside participant objects cannot reconstruct a
cross-shard transaction when point-in-time recovery predates prepare. The
canonical redo data therefore belongs in the transaction's existing
CoordinatorDO. Those one-per-transaction objects are not enumerable by cutoff,
so a small, deterministic JournalManifestDO index must record every committed
decision before any participant commit effect.

This ADR freezes the redo, hashing, routing, registration, retention, error, and
compatibility contracts. It does not authorize fleet cutoff enumeration, restore
execution, bucket sealing workflows, R2 archival, or any other control-plane
namespace.

The original decision above describes the minimum V1 deployment. The accepted
Manifest V2 amendment now authorizes cutoff sealing and enumeration preparation;
restore preview/execution and R2 archival remain outside this ADR.

## Decision

### Canonical redo envelope

CoordinatorDO is the canonical recovery journal. Before any participant receives
commit, the coordinator atomically persists state `commit_decided`, the version 1
redo envelope below, and a forward-work row for manifest registration.

| Field | Version 1 rule |
|---|---|
| `protocol_version` | Current shared protocol version; writes require 1 |
| `format_version` | Redo envelope format 1 |
| `tx_id` | Non-empty immutable transaction identity |
| `fleet_id` | Non-empty manifest/restore boundary |
| `coordinator_id` | Non-empty deterministic CoordinatorDO identity |
| `decision` | Literal `commit` |
| `decision_epoch` | Positive safe integer shared by all participants |
| `commit_decided_at` | Canonical UTC ISO timestamp with millisecond precision |
| `retention_deadline` | At least 35 days after `commit_decided_at` |
| `operation_hash` | Lowercase SHA-256 of canonical participant operations |
| `participants` | 1-8 entries, unique and sorted by `participant_id` |

Each participant contains `participant_id`, the identical decision epoch, and at
least one redo intent. Intents use contiguous zero-based `intent_seq` and contain
the executable SQL, JSON parameters, tenant, table, partition key, nullable
vBucket, nullable structured-operation kind, and nullable mirror target. This is
the minimum information needed to reconstruct every participant mutation when a
participant PITR bookmark predates prepare.

Objects are serialized as UTF-8 canonical JSON: object keys sort
lexicographically, array order is preserved, and only finite JSON values are
allowed. Participants sort by ID; intent sequence preserves execution order.
`envelope_hash` is lowercase SHA-256 over those exact canonical bytes. The
envelope does not contain its own hash.

The qualification ceiling is 256 KiB (262,144 canonical UTF-8 bytes), inclusive,
and eight participant keys. Oversize work is rejected before prepare with
`413 TX_ENVELOPE_TOO_LARGE`. Measurement may lower this ceiling; raising it
requires a new engineering review and protocol qualification.

### Manifest record and deterministic routing

The minimum JournalManifestDO stores version 1 records with:

| Field | Version 1 rule |
|---|---|
| `protocol_version` | Current shared protocol version |
| `format_version` | Manifest record format 1 |
| `fleet_id` | Same value as the redo envelope |
| `utc_day` | `YYYY-MM-DD` from `commit_decided_at` in UTC |
| `partition` | `sha256(tx_id) mod 16` |
| `partition_count` | Literal 16, encoded in every record |
| `routing_key` | `<utc_day>:<two-digit partition>` |
| `tx_id` | Same value as the redo envelope |
| `coordinator_id` | Same value as the redo envelope |
| `commit_decided_at` | Same canonical timestamp as the envelope |
| `decision_epoch` | Same epoch as the envelope |
| `envelope_hash` | SHA-256 of the canonical redo envelope |
| `retention_deadline` | Same deadline as the envelope |

For modulo routing, SHA-256 is interpreted as an unsigned big-endian integer;
modulo 16 is therefore the low nibble of the final digest byte. Version 1 always
uses 16 partitions. A partition-count or routing-function change requires a new
protocol version, saturation benchmark, and mixed-version routing tests.

`record_hash` is SHA-256 of the canonical manifest record and accompanies the
registration request; it is not hashed into itself. The durable identity is
`(fleet_id, utc_day, partition, tx_id)`. A retry with the same identity and
`record_hash` is idempotent. The same identity with different content returns
`409 TX_MANIFEST_CONFLICT`, preserves the original row, and quarantines the
transaction for manual inspection.

### Commit and recovery ordering

The required order is:

1. validate protocol versions, canonical operation hash, participant ordering,
   epoch equality, retention, and envelope byte length;
2. persist `preparing(E)` and participants, then prepare every participant;
3. compare-and-set to `prepared(E)`;
4. pre-arm the recovery alarm;
5. atomically persist `commit_decided(E)`, the immutable redo envelope, and the
   forward manifest-registration row;
6. synchronously register the identical content-addressed manifest record;
7. after an unambiguous acknowledgment, persist `manifest_registered(E)`;
8. persist `committing(E)` and only then send participant commit with epoch and
   operation hash;
9. persist participant acknowledgments and terminal state.

There is no participant commit call before step 7. A decision write failure
returns `503 TX_DECISION_UNAVAILABLE` with zero manifest or participant effects.
Manifest unavailability or ambiguous acknowledgment leaves the immutable commit
decision intact, persists/retains `commit_pending_manifest`, returns HTTP 202,
and retries the identical registration. It never aborts and never permits a
participant commit based on an ambiguous manifest response.

The alarm is durably armed before the decision transaction. An alarm with no
work is a no-op. Decision and forward-work persistence are atomic. Every failed
alarm attempt durably records bounded backoff and re-arms; duplicate and late
alarms cannot regress state or duplicate participant application.

After `manifest_registered`, the coordinator and participant recovery paths may
reconcile commit. Missing acknowledgments return `committed_pending_ack`; they
cannot change the decision. Participant sweeps treat `manifest_registered`,
`committing`, `committed_pending_ack`, and `committed` as authorization to
reconcile commit, while `commit_decided` and `commit_pending_manifest` remain
blocked behind the manifest barrier.

### Admission, retention, and lifecycle

Three manifest registration failures within 30 seconds open the per-coordinator
admission circuit before new prepare. Recovery work has priority. Half-open state
permits one bounded probe; failed probes back off to a maximum five-minute
cooldown. New work rejected before prepare receives
`503 TX_MANIFEST_UNAVAILABLE`; already-decided work remains pollable and retried.

Coordinator envelopes, manifest records, and participant decision tombstones are
retained for at least 35 days. The namespaces are excluded from participant PITR.
Deletion is alarm-driven and idempotent, and may occur only when the record is
past `retention_deadline`, has no unresolved forward work, and satisfies the
configured archive-policy check. Lifecycle failure delays deletion; it never
changes a transaction decision.

The minimum manifest slice supports registration, idempotent lookup, conflict
quarantine, and lifecycle. Bucket sealing, no-gap cutoff enumeration, restore,
and optional encrypted R2 batch archives remain deferred.

### Compatibility and ownership

| Contract | Source owner | Durable owner | Version rule |
|---|---|---|---|
| State names and transitions | `packages/contracts` | CoordinatorDO | current write; current/N-1 read |
| Participant phase message | `packages/contracts` | RPC payload | current write; current/N-1 read |
| Participant tombstone | `packages/contracts` | ShardDO | current write; current/N-1 read |
| Redo envelope | `packages/contracts` | CoordinatorDO | envelope format + protocol version |
| Manifest record/registration | `packages/contracts` | JournalManifestDO | manifest format + protocol version |
| Typed transaction errors | `packages/contracts` | API/RPC response | error schema version 1 |
| Result replay storage | ADR-1/core schema | ShardDO/CoordinatorDO | expand-first; current/N-1 reader |
| Quarantine detail | ADR-1/runtime schema | object detecting conflict | preserves prior decision and both hashes |

Schema changes are expand-first. Readers must understand legacy `preparing`,
`prepared`, `committed`, and `aborted` rows and map them through an explicit
migration policy before enabling version 1 writes. Unknown newer rows produce
`503 TX_VERSION_UNSUPPORTED` before mutation. Code rollback is permitted only
while stored and wire formats remain N-1 readable; otherwise recovery uses a
forward fix.

### Typed protocol behavior

| Condition | Typed behavior |
|---|---|
| Decision storage unavailable | `503 TX_DECISION_UNAVAILABLE` |
| Manifest admission unavailable before prepare | `503 TX_MANIFEST_UNAVAILABLE` |
| Manifest acknowledgment unavailable/ambiguous after decision | HTTP 202 `commit_pending_manifest` |
| Same manifest identity, different content | `409 TX_MANIFEST_CONFLICT`, quarantine |
| Force-abort after commit decision | `409 TX_COMMIT_ALREADY_DECIDED` |
| Participant epoch is stale | `409 TX_EPOCH_STALE` |
| Participant epoch attempts to replace a decision | `409 TX_EPOCH_CONFLICT` |
| Participant phase contradicts tombstone | `409 TX_DECISION_CONFLICT` |
| Operation/envelope hash differs | `409 TX_ENVELOPE_HASH_MISMATCH` |
| Envelope exceeds 256 KiB | `413 TX_ENVELOPE_TOO_LARGE` before prepare |
| Unknown durable/wire format | `503 TX_VERSION_UNSUPPORTED` |

The shared package owns HTTP status and retryability. HTTP 202 statuses are typed
nonterminal outcomes, not errors and not participant-commit authorization.

## Required verification

- Fixed-vector canonical hashes and UTC-day/16-way routing.
- Idempotent same-record registration and same-ID/different-hash quarantine.
- Exact 256 KiB acceptance and greater-than-256 KiB pre-prepare rejection.
- Crash injection before and after alarm, decision, forward-work, manifest
  acknowledgment, `manifest_registered`, and every participant commit.
- No participant commit count increase during ambiguous manifest outcomes.
- Duplicate/late alarm no-op and durable bounded rescheduling after failure.
- Current/N-1 fixture reads and unknown-newer rejection before mutation.
- Retention/lifecycle tests at just before, exactly at, and after the deadline.
- Qualification measurement of p95 manifest overhead at or below 100 ms on the
  reference deployment; a miss blocks release and cannot bypass the manifest.

**Qualification status (2026-08-05): pending.** Local tests, type checks, and
Wrangler dry runs do not satisfy the reference-deployment gate. Before this
release is qualified, a sanitized artifact must name the deployment and build,
record the measurement method and sample size, and demonstrate p95 manifest
overhead at or below 100 ms. Until that evidence exists, release qualification
is incomplete.

## Consequences

Every cross-shard commit adds one synchronous manifest registration plus any
explicit idempotent retry after ambiguity. This latency and cost are accepted to
make durable decisions enumerable and recoverable. The protocol may block after
commit decision, but it cannot tear or reverse. The fixed 16-way UTC-day routing
is intentionally an A-phase contract and must be re-benchmarked before broader
scale claims.

## Manifest V2 amendment: reservation, sealing, and enumeration

New state-model-2 transactions reserve a frozen manifest route before participant
prepare. Route assignment and reservation are separate idempotent RPCs: the
coordinator durably stores the assigned `reserved_at`, reservation day,
partition, partition-config hash, canonical reservation, and reservation hash
before the catalog or bucket is mutated. Catalog activation precedes bucket
acknowledgement. An ambiguous reserve remains `manifest_reserving` and replays
the exact stored bytes; participant prepare is forbidden until acknowledgement.

After prepare, the coordinator atomically enters irreversible
`commit_deciding` and stores the exact finalize intent. `JournalManifestDO`, not
the coordinator, assigns `(commit_decided_at_ms, decision_sequence)` using its
trusted clock and decision floor. The returned record and hash are durable in
the coordinator before participant commit. Abort before that boundary requires
an acknowledged reservation cancellation; ambiguous cancellation remains
`aborted_pending_manifest_cancel`.

Each daily bucket has distinct `decision_floor_ms` and `sealed_through_ms`
watermarks. Starting an exact-cutoff seal raises the decision floor immediately;
publishing the immutable receipt advances `sealed_through_ms` only after the
eligible record digest and the identical `FINALIZED AND commit_decided_at_ms <=
cutoff` count are rechecked transactionally. Later finalizations therefore land
strictly above the cutoff. Exact-prefix receipts are derived when a covering
receipt exists; covering evidence alone never authorizes completeness.

`FleetManifestCatalogDO` is the candidate-set authority. It serializes bucket
activation against hash-chained cutoff snapshots, pins append-only partition
configuration, materializes all 16 partitions for every newly eligible day,
and owns deterministic close progress plus the final fleet root. Enumeration is
bucket-serial and keyset-paged. Its cursor binds the request, catalog snapshot,
conflict root, seal receipt, and retention epoch. Missing receipts, unresolved
quarantine, changed epochs, deleted coverage, and pre-reservation history return
typed non-complete outcomes.

The first V2 route assignment establishes the fleet's reservation-required
boundary and permanently closes new V1 admission. Each bucket certifies its
local V1 ledger (or `NO_LEGACY`) before V2 reservation or sealing. The catalog's
daily horizon advances only after all 16 exact receipts are committed. V1 rows
and model-1 coordinators remain readable and recoverable, but they are never
reinterpreted as V2 evidence.
