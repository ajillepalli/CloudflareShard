# ADR-1: Idempotency and monotonic transaction state machine

- Status: Accepted
- Date: 2026-08-05
- Decision owners: core transaction runtime and `@cloudflareshard/transaction-contracts`
- Required before: Phase A fault-floor and partner trials

## Context

CloudflareShard already has request-result storage and two-phase commit, but its
historical coordinator states did not encode an irreversible decision before
participant effects. Retrying `/begin`, forcing an abort, or delivering late
participant messages could therefore select a phase from a status string rather
than from a durable decision. The Phase A safety floor requires at-most-once
application, durable result replay, all-or-none cross-shard outcome, and explicit
behavior after response loss or restart.

This ADR freezes the state, idempotency, epoch, tombstone, quarantine, and
compatibility rules. ADR-3 owns the commit redo envelope and manifest protocol.

## Decision

### Protocol ownership and versioning

`packages/contracts` is the sole source-code owner of transaction state names,
legal transitions, decision predicates, participant message/tombstone formats,
canonical hashing helpers, and typed transaction errors. Runtime modules own
storage and RPC adapters but must not duplicate or widen those contracts.

Protocol version 1 is the only writable version. Readers support the current and
N-1 protocol versions for one release window. Because version 1 has no
predecessor, the current readable set is `{1}`. Unknown newer and expired older
formats fail closed with `TX_VERSION_UNSUPPORTED`; they never fall through to a
default phase. A future version bump must add N-1 fixtures before changing the
shared version constants.

### Idempotency and durable result replay

The request identity is `(tenant_id, request_id)` and the immutable content
identity is a SHA-256 hash of the canonical, versioned logical operation. The
caller supplies a unique request ID; the server derives and stores the hash.

The configurable idempotency window is an integer from 1 through 30 days,
inclusive, with a default of 7 days. Within that window:

1. First application durably records the request hash and exact typed result in
   the same object transaction as the application mutation.
2. A retry with the same identity and hash returns the stored result without
   reapplying any mutation.
3. The same identity with a different hash returns
   `409 TX_ID_REQUEST_MISMATCH`; it cannot replace the original hash or result.
4. Cleanup cannot remove a record before its configured expiry. Cleanup is
   idempotent and alarm-driven; failed cleanup does not affect correctness.
5. A poisoned/quarantined identity returns its stable quarantine error rather
   than attempting the operation again.

Coordinator redo records and participant decision tombstones use ADR-3's longer
35-day minimum retention. Consequently a cross-shard transaction may remain
recognizable after its configured result-replay window; reusing its request ID
does not authorize replacing the retained decision.

### Transaction states

The durable state vocabulary is version 1:

| State | Meaning | Durable decision |
|---|---|---|
| `new` | Conceptual state before a coordinator row exists | none |
| `preparing` | Epoch and participant set are durable; prepare may proceed | none |
| `prepared` | Every participant acknowledged prepare for the same epoch/hash | none |
| `abort_decided` | Immutable abort decision and forward abort work are durable | abort |
| `aborting` | Abort effects are being reconciled | abort |
| `aborted` | Abort reconciliation is terminal | abort |
| `commit_decided` | Immutable commit redo envelope and forward work are durable | commit |
| `commit_pending_manifest` | Manifest registration is ambiguous or unavailable | commit |
| `manifest_registered` | The identical manifest record is durably acknowledged | commit |
| `committing` | Participant commit effects are being reconciled | commit |
| `committed_pending_ack` | Commit is durable; at least one acknowledgment is missing | commit |
| `committed` | All participant commit acknowledgments are durable | commit |
| `quarantined` | A version/hash/epoch/decision conflict requires inspection | preserved from prior state |

Same-state replay is an idempotent no-op. The only non-idempotent transitions are:

| From | To |
|---|---|
| `new` | `preparing`, `quarantined` |
| `preparing` | `prepared`, `abort_decided`, `quarantined` |
| `prepared` | `commit_decided`, `abort_decided`, `quarantined` |
| `abort_decided` | `aborting`, `quarantined` |
| `aborting` | `aborted`, `quarantined` |
| `aborted` | `quarantined` |
| `commit_decided` | `commit_pending_manifest`, `manifest_registered`, `quarantined` |
| `commit_pending_manifest` | `manifest_registered`, `quarantined` |
| `manifest_registered` | `committing`, `quarantined` |
| `committing` | `committed_pending_ack`, `committed`, `quarantined` |
| `committed_pending_ack` | `committed`, `quarantined` |
| `committed` | `quarantined` |
| `quarantined` | none |

Quarantine is not a new transaction decision. A quarantine record must preserve
the prior state, durable decision (if any), epoch, original hashes, conflicting
hash/format, reason code, and timestamp. A committed or aborted transaction that
later quarantines remains committed or aborted for recovery purposes.

No commit-decided state can transition to an abort state. No abort-decided state
can transition to a commit state. `/begin`, `/force-abort`, coordinator alarms,
participant sweeps, and status handlers all use explicit shared transition
validation; no unknown status may use a catch-all resume branch.

### Durable decision ordering

Before prepare, the coordinator validates the immutable operation and the ADR-3
envelope byte ceiling. It then:

1. persists `preparing(E)`, the operation hash, and participant set;
2. sends prepare with epoch `E` and the operation hash;
3. compare-and-sets `preparing(E)` to `prepared(E)` after all votes;
4. pre-arms recovery;
5. atomically persists either a decision plus its forward-work row;
6. only then emits the corresponding participant or manifest effect.

For abort, step 5 stores `abort_decided(E)` before any abort RPC. For commit, it
stores `commit_decided(E)` and ADR-3's immutable redo envelope before manifest
registration. Participant commit remains forbidden until the manifest has been
acknowledged and `manifest_registered(E)` is durable.

Decision persistence failure produces `503 TX_DECISION_UNAVAILABLE` and zero
decision effects. Duplicate or late forward work is a no-op after its durable
target state is reached.

### Epochs and participant tombstones

An epoch is a positive safe integer scoped to a transaction ID. The initial
coordinator generation is epoch 1. Retries of the same generation reuse the same
epoch. An explicit compare-and-set recovery takeover may advance an undecided
generation; after `abort_decided` or `commit_decided`, the decision epoch is
immutable and cannot advance.

Every prepare, commit, abort, status, and recovery message carries
`{protocol_version, tx_id, epoch, phase, operation_hash}`. Every participant
persists the highest decision epoch as a versioned tombstone with the decision,
operation hash, decision time, and retention deadline. Tombstones live for at
least 35 days.

A participant:

- rejects a lower epoch with `TX_EPOCH_STALE`;
- rejects a higher epoch that attempts to replace an existing decision with
  `TX_EPOCH_CONFLICT`;
- rejects a different operation hash with `TX_ENVELOPE_HASH_MISMATCH`;
- rejects prepare/abort after a commit tombstone and prepare/commit after an
  abort tombstone with `TX_DECISION_CONFLICT`;
- accepts same-epoch, same-hash status/recovery and same-decision replay as
  idempotent reconciliation.

An abort tombstone must be durable before participant intent/lock cleanup. This
is what prevents a late prepare from recreating locks after force-abort.

### Typed outcomes

- Manifest ambiguity: HTTP 202, `status: commit_pending_manifest`, pollable
  transaction ID; this is a committed decision but not permission to commit a
  participant yet.
- Participant acknowledgment ambiguity: HTTP 202,
  `status: committed_pending_ack`; the outcome is committed.
- Force-abort after commit decision: `409 TX_COMMIT_ALREADY_DECIDED`.
- A quarantined transaction: `409 TX_QUARANTINED`, with no automatic overwrite.
- A known abort: `409 TX_ABORTED`, with stable result replay.

The complete transaction error-code/status/retryability map is owned by
`packages/contracts/src/transaction.ts`. Renderers may add user guidance but may
not change code meaning or retryability.

## Required verification

- Exhaustively test every state pair, including same-state replay.
- Race begin against force-abort and prove exactly one decision wins its CAS.
- Crash before and after decision persistence; no effect may precede decision.
- Send stale, future, hash-mismatched, and contradictory messages to every
  participant phase.
- Prove tombstones reject late prepare throughout the 35-day window.
- Prove same request/hash replays the exact result and a different hash never
  mutates the stored record.
- Read N-1 fixtures and reject unknown newer formats before mutation.

## Consequences

The protocol can block while a manifest or participant is unavailable, which is
the safe property of the selected atomicity contract. Operators can inspect and
poll blocked work, but cannot force an abort after commit is decided. Additional
durable rows, tombstone retention, and explicit recovery scheduling are accepted
costs. Restore execution and fleet enumeration remain outside this ADR.
