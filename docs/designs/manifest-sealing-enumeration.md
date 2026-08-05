---
status: IMPLEMENTED_LOCALLY
---

# Manifest sealing and no-gap enumeration

Generated from the T5 roadmap item and the focused `/plan-eng-review` on 2026-08-05.

## Outcome

Make the journal manifest enumerable without ever claiming completeness while a
commit decision can still appear late. A coordinator reserves a manifest bucket
before participant prepare and durably enters an irreversible, commit-only
recovery state before asking that bucket to finalize. The bucket atomically
assigns the canonical `commit_decided_at` and writes the manifest record. A cutoff
seal fences the bucket's decision clock, so every later finalization is assigned
strictly after the sealed cutoff.

This is the bounded T5a slice. It deliberately stops before restore preview,
restore execution, participant reconciliation, and live qualification.

## Capacity and scope declaration

- Capacity: four implementation lanes plus one integration/review owner.
- Displaced package: manual evidence work remains partner-gated; T7 and T6 wait.
- New infrastructure: one fleet-scoped `FleetManifestCatalogDO` namespace behind
  `ControlPlaneWorker`, selected in D5 as the authoritative active-bucket index.
- New durable concepts: one reservation state machine, a bucket-owned monotonic
  decision clock, immutable `sealed_through` generations inside each daily
  reservation bucket, and catalog cutoff snapshots/hash chains.

Although implementation spans more than eight files, it adds no public route or
deployable Worker. D5 deliberately adds one Durable Object namespace because a
bucket-issued decision may live in an arbitrarily old reservation-day bucket and
there is otherwise no bounded, complete discovery source.

## What already exists

- `CoordinatorDO` validates the full redo envelope before prepare, persists a
  monotonic commit or abort decision, and retains durable recovery work.
- `ControlPlaneWorker` is an internal service-binding boundary and already
  routes a manifest to `fleet + UTC day + sha256(tx_id) mod 16`.
- `JournalManifestDO` already provides idempotent content-addressed registration,
  conflict quarantine, lookup, lifecycle release, and 35-day alarm cleanup.
- No fleet-scoped active-bucket authority exists yet; D5 adds it rather than using
  Cloudflare's account-level object listing API.
- Shared transaction contracts already own canonical JSON, hashing, protocol
  validation, routing, and expand-first compatibility rules.
- Workerd/Vitest suites already exercise ambiguous acknowledgements, retries,
  conflict quarantine, alarm fallback, and lifecycle retention.

The implementation extends these paths. It does not build a parallel recovery
store or use Cloudflare's account-level Durable Object listing API as a runtime
index.

## Architecture decision

Selected approach: **reserve before prepare, with the manifest bucket assigning
the canonical commit-decision time during finalization**.

`ControlPlaneWorker` assigns the reservation route and `reserved_at` from its
trusted clock before addressing the daily bucket; callers cannot supply or
backdate them. The returned route token is frozen and never recomputed from the
later bucket-issued `commit_decided_at`. The reservation content is immutable and
content-addressed from:

- protocol and reservation format versions;
- fleet, UTC day, partition count, partition, and routing key;
- transaction and coordinator IDs;
- operation hash and decision epoch; and
- reservation timestamp.

Before the bucket may acknowledge that reservation, `ControlPlaneWorker` calls
`FleetManifestCatalogDO.activateBucket(reservation_day, partition,
activation_key)`. Catalog activation is idempotent and precedes bucket durability;
an activation without a later reservation is a safe false positive. An ambiguous
activation is retried and participant prepare remains forbidden.

The catalog owns a monotonic decision-time floor. `snapshotThrough(T,
idempotency_key)` serializes against activation: an entry activated first is in
the immutable snapshot and must produce a bucket receipt through `T`; an
activation serialized after the fence returns `required_decision_floor >= T`,
which the target bucket must merge into its durable `decision_floor_ms` before
acknowledging the reservation. This ordering makes a missing candidate impossible
even when a new reservation bucket appears concurrently with fleet sealing.

The coordinator persists a local `manifest_reserving` state, frozen route, exact
canonical reservation bytes, and durable recovery action before the external
reservation RPC. An ambiguous response is retried byte-for-byte. Participant
prepare cannot start until the reservation is acknowledged.

After every participant prepares, the coordinator atomically persists an
irreversible `commit_deciding` state, the immutable redo envelope without its
bucket-owned decision fields, and exact finalize request bytes. From that point it
may only recover toward commit; force-abort is forbidden. The bucket finalization
transaction assigns `(commit_decided_at_ms, decision_sequence)` as
`(max(DO now, last_assigned_ms + 1, decision_floor_ms + 1), next sequence)`,
derives the 35-day retention deadline, writes `ManifestRecordV2`, and returns the
canonical record and hash. An ambiguous response is retried byte-for-byte and can
never fall back to abort. Only after the identical finalization is acknowledged
does the coordinator persist the returned decision fields and propagate
participant commit.

```text
client /begin
    |
    v
CoordinatorDO: validate complete request + compute operation hash
    |
    v
persist manifest_reserving + canonical reservation bytes + recovery schedule
    |
    v
ControlPlaneWorker.reserveManifest()
    |
    +--> control plane assigns reserved_at + route
    |                         -> FleetManifestCatalogDO.activateBucket()
    |                         -> returned decision floor
    |                         -> JournalManifestDO initialize floor + reserve
    |                              |
    |                              +--> insert identical reservation
    |                              +--> identical retry = success
    |                              +--> hash mismatch = quarantine
    |
    +--> acknowledged -> persist preparing -> prepare participants
    |                         |
    |                         +--> durable abort -> abort participants
    |                         |                    -> cancel reservation
    |                         |                    -> terminal aborted
    |                         +--> persist irreversible commit_deciding
    |                                              -> finalize reservation
    |                                              -> bucket assigns decision time
    |                                              -> persist returned decision
    |                                              -> commit participants
    |
    +--> ambiguous/unavailable -> 202 manifest_reserving + alarm retry
```

No cross-object atomic transaction is claimed. A crash before `commit_deciding`
may still recover to abort; a crash after it can only replay the exact finalize
request and then commit. A cutoff seal and finalization serialize inside the same
bucket: if finalization wins, its assigned decision time is visible to that seal;
if sealing wins, finalization is assigned strictly after the cutoff.

## Reservation state machine

```text
CANONICAL STATE (membership owner)
ABSENT --reserve--> RESERVED --finalize + assign time--> FINALIZED
                          \--cancel after durable abort--> CANCELLED

EVIDENCE OVERLAY (never changes canonical membership by itself)
quarantine_state=NONE --content/transition conflict--> UNRESOLVED
UNRESOLVED --audited resolution attestation---------> RESOLVED
             conflict_root and every candidate remain append-only
```

RESERVED is the ordinary non-terminal bucket state. FINALIZED stores the immutable
bucket-issued `(commit_decided_at_ms, decision_sequence)` and canonical record.
A conflicting transition never overwrites that record; it appends candidate and
conflict hashes and sets the separate `quarantine_state=UNRESOLVED` plus
`conflict_root`. It never changes canonical `state`. The overlay can move to
RESOLVED only through the trusted audited operation below. FINALIZED, CANCELLED,
and SEALED never regress.
Duplicate identical transitions are idempotent.
Conflicting terminal transitions quarantine rather than overwrite evidence.

`resolveQuarantine(tx_id, resolution, selected_hash, evidence_hash, actor, reason,
coordinator_state_hash, idempotency_key)` is route-less and available only through
the trusted control plane. The control plane reads the CoordinatorDO's durable
state/hash and binds it to the request; unavailable or changed state fails closed.
The bucket preserves every candidate and conflict event, then appends an immutable,
hash-chained resolution attestation.

FINALIZED resolution is legal only for a coordinator in the monotonic
commit-authorized set (`commit_deciding`, `commit_pending_manifest`,
`manifest_registered`, `committing`, `committed_pending_ack`, or `committed`).
CANCELLED resolution is legal only for an `aborted*` coordinator. An existing
canonical FINALIZED or CANCELLED state can only be confirmed, never reversed. If
canonical state is still RESERVED, the authorized resolution atomically performs
the matching finalize/cancel transition; a new finalized record receives a bucket
decision time strictly above all current floors. Illegal state/outcome pairs are
stable typed conflicts, not operator judgment calls.

`cancel` while `quarantine_state=UNRESOLVED` returns the typed domain result
`QUARANTINED_PENDING_RESOLUTION`; it is neither a retryable transport error nor a
successful cancellation. The coordinator remains parked in
`aborted_pending_manifest_cancel` with no scheduled retry. Only an acknowledged
resolved-CANCELLED attestation releases it to terminal `aborted`; alarm recovery
after an ambiguous resolution acknowledgement replays the resolution key rather
than hot-looping cancel.

The transaction state-model version advances. Expand-first readers accept the
immediately previous model, while new writers use
`manifest_reserving -> preparing -> prepared`. Abort cleanup gains
`aborted_pending_manifest_cancel`; `aborted` is not terminal until cancellation
is acknowledged. Commit becomes
`prepared -> commit_deciding -> commit_pending_manifest -> manifest_registered`.
`commit_deciding` is locally irreversible but is not yet a PITR-visible commit
decision; it records that only bucket finalization and participant commit recovery
remain legal. `manifest_registered` persists the bucket-issued decision time,
sequence, record, and hash before participant commit.

The reservation ledger is separate from the existing v1 rows so expand-first
reads remain possible. Finalization writes a versioned `ManifestRecordV2` whose
frozen reservation route, reservation hash, bucket-issued decision time, and
decision sequence are part of its canonical content. V2 `envelope_hash` pins the
canonical pre-decision envelope intent; the matching completed redo envelope is
built from the bucket-issued timestamps, fully validated, and stored before
participant commit. A V1-to-V2 bridge retains its already-canonical predecessor
envelope and completed-envelope hash rather than reinterpreting that evidence.
V1 continues to validate and recover under its coordinator-issued
`commit_decided_at` rules, but it is never reinterpreted as V2 and never
contributes to a complete sealed window.

## Bucket seal contract

A daily reservation bucket maintains two monotonic decision-time watermarks rather
than waiting for UTC day-end:

- `decision_floor_ms` advances immediately when a catalog floor is installed or a
  seal generation begins. Every later finalization is assigned strictly above it.
- `sealed_through` advances only in the transaction that publishes an immutable
  completed receipt. The invariant is always
  `sealed_through <= decision_floor_ms`.

The caller sends
`closeThrough(T, idempotency_key)`; the DO allocates the next generation and
returns a covering receipt when `T <= sealed_through`; it rejects a future cutoff
or reports the durable identity of a different close already in progress. A
replayed key returns the stored generation or receipt.

Starting the generation atomically advances `decision_floor_ms` to `T` while
leaving `sealed_through` at the last published receipt. Reservations,
finalizations, and cancellations remain writable. A
finalization serialized after that transaction is assigned
`commit_decided_at_ms > T`, so it cannot alter the set attested by this generation.
A finalization serialized before it is already durable and is included exactly
when its bucket-issued decision time is at or before `T`. This makes a noon cutoff
provable without closing the rest of the day or waiting for unresolved
reservations whose eventual decisions necessarily belong after the cutoff.

`JournalManifestDO.sealThrough()` advances one generation through bounded,
durable work:

1. Reject if partition metadata does not match the requested fleet/day/partition.
2. Reject a future cutoff. If `sealed_through >= T`, return the immutable covering
   receipt as fencing success. `complete=true` still requires an exact-cutoff
   receipt; if none exists, run the bounded `deriveExactReceipt(T)` path described
   below. Otherwise require `T <= DO now`. The exact time-validation tolerance
   never permits a future seal.
3. Allocate `generation = current_generation + 1` inside the DO. Return the stored
   generation or receipt for an identical idempotency key. A competing key returns
   `SEAL_IN_PROGRESS` with the durable in-progress generation, key, and cutoff so
   the driver can adopt rather than wedge the bucket.
4. In the generation-start transaction, persist the cutoff and set
   `decision_floor_ms = max(decision_floor_ms, T)`. No later finalization can
   receive a decision time at or below it; `sealed_through` is unchanged.
5. Use the shared `manifestMemberAt(T)` predicate from `packages/contracts`:
   `state = FINALIZED AND commit_decided_at_ms <= T`.
   `(commit_decided_at_ms, decision_sequence, tx_id)` keyset batches digest exactly
   that set. RESERVED and CANCELLED are not members. An UNRESOLVED quarantine
   overlay fails the generation closed without removing a canonical member; a
   RESOLVED overlay requires its resolution-attestation hash in the digest entry.
6. Persist generation-fenced digest progress after each bounded batch. Resume the
   same generation after a crash; never reuse its cursor for another cutoff.
7. In one synchronous SQLite transaction, recheck that no unresolved quarantine
   affects `<= T`, count with the identical `manifestMemberAt(T)` predicate and
   assert that member count equals the digest count, write
   finalized count, maximum decision sequence, retention bound, prior seal hash,
   current conflict/resolution root, and the canonical receipt hash, then publish
   the immutable receipt and advance `sealed_through = T`.
8. A later cutoff starts a strictly newer generation chained to that receipt.

`deriveExactReceipt(T, idempotency_key)` is an evidence-only generation for
`T < sealed_through` when no exact receipt exists. Because `decision_floor_ms` is
already above `T`, the member set cannot grow. It uses the same
`manifestMemberAt(T)` digest/count/page predicate and bounded crash-resumable
progress, then appends an exact-cutoff receipt without changing either watermark.
Its deterministic key is derived from the bucket close key plus `"exact-prefix"`.
A covering receipt proves fencing only and can never by itself authorize
`complete=true` for a smaller cutoff.

Single-threaded Durable Object execution plus synchronous SQLite transactions
establish the race boundary. A generation never loads an unbounded record set into
memory, and its cursor is generation-fenced so retries resume identical work after
a crash. The fence precedes digesting, so resumed scans cannot acquire new
`<= T` records.

```text
OPEN THROUGH T0 --closeThrough(T1,key1)--> DRAINING g1 THROUGH T1
       |                                        +--> reserve -> accept
       |                                        +--> finalize -> assigned > T1
       |                                        +--> cancel -> terminal
       |                                        +--> exact close replay -> g1/receipt
       |                                        +--> quarantine <=T1 -> fail closed
       |                                        +--> digest complete -> SEALED T1
       |
       +-- later same-day decisions continue strictly above T1

SEALED T1 --closeThrough(T2,key2), T2>T1--> DRAINING g2
SEALED T1 --same idempotency key----------> identical g1 receipt
SEALED T1 --new cutoff <T1----------------> covering fence + exact-prefix receipt
```

### Legacy coverage rule

The fleet catalog owns an immutable, hash-chained V1/V2 cutover record with phases
`OPEN -> FENCING -> IMPORTING -> SEALED`. The selected D6 bridge protocol preserves
the retained V1 window without allowing a V1 row to arrive behind a V2 seal:

1. Deploy expand-first readers, V2 bridge recovery, and bucket/catalog cutover
   methods everywhere while V1 remains open.
2. Every V1 registration, including one routed to an uninitialized or future-day
   bucket, first calls the catalog's `admitLegacyRegistration`. While phase is
   OPEN, that transaction activates the exact bucket and returns a hash-bound
   admission token required by the bucket. Any later phase returns `V1_CLOSED`.
3. Enter `FENCING`; the catalog snapshot includes every bucket for which it issued
   a token. Materialize those entries plus every deterministic V1 day/partition
   bucket in the supported 35-day recovery window, including empty buckets.
4. Call `closeLegacy(cutover_id)` on each bucket. It serializes against V1
   registration: an insertion that wins is included; one that loses receives
   `V1_CLOSED`. Exact replay of a pre-fence row remains readable and idempotent.
5. A legacy coordinator with no acknowledged V1 manifest may not send participant
   commit. On `V1_CLOSED`, it activates a catalog bucket and creates a V2 bridge
   reservation/finalization. The bridge record retains `legacy_decided_at`, V1
   envelope/hash/version, and migration evidence, while its bucket-issued V2
   decision time is the authoritative PITR membership time.
6. A legacy transaction prepared but not yet commit-decided also adopts the bridge
   path before choosing commit. This is the only migration exception to
   reserve-before-prepare and is explicit in the record. It may still abort before
   entering `commit_deciding`.
7. Import every closed V1 bucket's immutable row digest/receipt into the catalog
   cutover generation. Enter `SEALED` only after all expected receipts verify and
  the catalog root hash commits to the complete set.

The admission token is safe across the phase race because token issuance activates
the bucket before `FENCING` can snapshot it. A token that reaches its bucket after
`closeLegacy` is rejected and bridges; a bucket absent from the materialized
35-day grid still cannot accept a V1 row without a catalog token. This is the
namespace-wide fence for forward-skewed clocks and midnight rollover.

Pre-catalog V1 rows may already exist in arbitrarily future-dated buckets. The
catalog therefore retains `legacy_scanned_through_day` after initial cutover.
On first V2 use of any bucket, one synchronous local initialization transaction
inspects its V1 ledger and writes either an imported legacy digest/receipt or an
immutable `NO_LEGACY` certificate before accepting the reservation or seal. The
certificate is therefore immediately available without waiting for catalog-wide
aggregation; V1 insertion is already permanently fenced, so the local result
cannot become stale.

Before fleet close can complete cutoff `T`, it materializes all 16 buckets for
every day from the prior horizon through `utcDay(T)`. Each missing bucket performs
that same inline certification, then the driver derives its exact-cutoff receipt.
Only after all 16 certificate/receipt pairs are committed to the catalog hash chain
does the catalog advance `legacy_scanned_through_day`. Aggregation and optional
prewarming are asynchronous and never gate reservations. This permanent
16-checks-per-fleet-day cadence requires no historical clock-skew assumption and
catches a future-dated orphan exactly when its declared decision day becomes
eligible.

`reservation_required_since`, the cutover generation/hash, and the earliest
retained V1 decision time are copied into catalog snapshots and bucket receipts.
The boundary write is serialized in the fleet catalog after all route-building
awaits and is strictly greater than `legacy_admitted_through_ms`, the maximum V1
decision accepted by the preceding catalog transactions. A V1 row therefore
cannot land at or above a boundary later reported as complete.
Enumeration fails closed on any mismatch. Requests older than the imported
retention window return `coverage: unproven_legacy_window`; they never produce
`complete`. Rollback after any bucket fence must preserve V2 bridge recovery and
must never reopen V1 insertion.

## Fleet enumeration

`FleetManifestCatalogDO` is the authoritative candidate-set source. Its immutable
cutoff snapshot contains every activated `(reservation_day, partition)` serialized
before the catalog's decision-time fence plus the required decision floor for
later activations. Every snapshot entry must carry a bucket receipt with
`sealed_through >= cutoff`. Enumeration never infers existence from the account
API or from whichever buckets happen to respond.

Catalog snapshots are generation allocated, idempotency-keyed, hash-chained, and
produced through bounded keyset batches. Entries remain discoverable while their
bucket has an unresolved reservation, quarantine, unexpired finalized record, or
seal evidence required by the supported recovery window. Retirement requires a
bucket-issued empty/retention-safe certificate and is itself retained in catalog
history long enough to validate every supported cursor and cutoff.

Every seal receipt records the bucket's conflict/resolution root for audit.
Unresolved quarantine is rechecked by each local page and blocks enumeration;
later audited resolutions do not change immutable membership or invalidate an
already returned cursor page.

The finalize decision and sequence are assigned before the canonical record hash
is materialized. If a conflicting cancel arrives during that hash await, record
materialization still completes while the unresolved overlay keeps the finalize
result and sealing path quarantined. Audited `resolution=FINALIZED` repair thus
remains satisfiable without exposing unresolved evidence.

The catalog owns append-only `partition_config_history` records containing
`effective_from_day`, protocol version, partition count, prior hash, and config
hash. V2 starts at 16 partitions; a future reshard requires a new history entry
and cannot reinterpret an older route. A new entry must satisfy
`effective_from_day > current_utc_day + 1` when written. The control plane pins the
selected config hash into every frozen reservation route; later activation and
finalization validate that pinned hash rather than re-deriving the currently
effective config. Bucket metadata, receipts, snapshots, and cursors must agree
with the pinned/effective history entry or fail closed.

Each bucket owns `records_deleted_through_ms`, `retention_epoch`, and a
retention-evidence root. In the same synchronous SQLite transaction that deletes
one or more finalized rows, lifecycle cleanup advances all three through the
maximum deleted decision time. Every local page request carries the expected
retention epoch and revalidates it after serialization in the bucket. If
`coverage_start <= records_deleted_through_ms`, the authoritative bucket returns
`retention_expired`; a stale catalog mirror or seal receipt can never override it.
The live epoch is audit evidence, not part of the cursor's stable request hash:
a deletion strictly below `coverage_start` may advance the epoch without
invalidating a cursor whose requested window remains fully retained.

Seal and snapshot hashes retain the chain commitment after their recoverable
detail is garbage-collected. Superseded seal/snapshot/close generations,
cancelled or abandoned reservations, conflict detail, and activation idempotency
keys are removed in bounded alarm batches only after the 35-day recovery window;
the newest chain head is retained. Per-transaction route assignments have a
separate one-hour crash-recovery lease and are released on every terminal or
quarantined path rather than scaling with the data-retention horizon.
Catalog propagation of a new
retention epoch may be asynchronous because fleet enumeration always checks the
bucket before paging. A caller cannot materialize buckets to manufacture expired
history.

```text
enumerate(fleet, coverage_start, cutoff, cursor, limit)
    |
    +--> validate version, time window, page limit, cursor/request hash
    +--> reject pre-reservation legacy coverage as unprovable
    +--> load/fence immutable fleet catalog snapshot through cutoff
    +--> derive every cataloged (reservation day, partition)
    +--> inspect buckets in deterministic reservation-day/partition order
    |      +--> missing/uninitialized -> GAP
    |      +--> sealed_through<cutoff -> INCOMPLETE
    |      +--> unresolved quarantine overlay -> QUARANTINED
    |      +--> sealed                -> page finalized records decided <=cutoff
    +--> emit bounded page + next cursor + per-bucket seal evidence
    +--> complete=true only when every expected bucket is sealed and gap-free
```

The cursor is versioned and bound to a canonical request hash covering fleet,
coverage start, cutoff, protocol version, partition-configuration hash, catalog
snapshot generation/hash, conflict/resolution root, bucket retention epoch, and the selected seal
generation/hash for every bucket already entered. A cursor
reused for a different request, catalog snapshot, or receipt chain fails closed.
Fleet records are ordered by
`(reservation_utc_day, partition, commit_decided_at_ms, decision_sequence, tx_id)`
to match deterministic bucket-serial traversal and keep the cursor bounded to one
bucket/local key. Consumers that require global decision-time order must perform a
separate merge; this API does not claim that ordering. A local bucket page uses
`(commit_decided_at_ms, decision_sequence, tx_id)` keyset pagination. Offset
pagination is forbidden. The page query, seal digest, and final count must all use
the same shared `manifestMemberAt(cutoff)` predicate.

The trusted close operation first fences the catalog through the requested cutoff,
then explicitly materializes every snapshot entry, including false-positive empty
buckets, and advances each through the same decision cutoff. Buckets activated
after the catalog fence are initialized above that cutoff and cannot contribute a
backdated decision.

`ControlPlaneWorker.closeFleetThrough(fleet, cutoff)` is the internal route-less
driver and `FleetManifestCatalogDO` is its durable operation owner. The catalog
derives `catalog_close_key = sha256("catalog-close", fleet, cutoff, protocol,
partition_config_hash)`, stores the snapshot generation and per-entry progress,
and resumes bounded work by alarm or caller retry. Each bucket key is
`sha256("bucket-close", fleet, cutoff, catalog_generation, reservation_day,
partition)`, so a driver crash cannot lose adoption identity.

`SEAL_IN_PROGRESS` returns its generation, key, and cutoff. A bucket already
sealed beyond the requested cutoff returns the immutable covering receipt as
fencing success and then derives or fetches the exact-cutoff receipt. Catalog
progress records both identities but advances completeness only on exact evidence.
For each newly eligible legacy day the close order is strictly
`initialize + local legacy certificate -> exact-cutoff receipt -> catalog proof ->
legacy_scanned_through_day`. The close operation returns pending diagnostics until
every snapshot entry has an exact receipt and every required day has 16 cataloged
legacy proofs, then returns the immutable fleet root used by enumeration.
Enumeration itself never manufactures history; a missing or insufficient seal
receipt is a gap, not an empty result. A partial or overloaded response never sets
`complete=true`.

## Shared contracts and errors

Add versioned runtime-validated contracts for:

- `ManifestReservationV1`, the bucket-finalize intent, bucket-issued
  `ManifestRecordV2`, and their canonical hashes;
- catalog activate/snapshot/retire, partition-config, reserve, finalize, cancel,
  quarantine-resolution, seal, local-page, and fleet-enumeration requests;
- versioned cursors and request hashes;
- per-bucket coverage evidence and overall `complete | incomplete | quarantined |
  unproven_legacy_window | retention_expired` results; and
- stable errors for future/nonmonotonic cutoffs, seal-in-progress, reservation or
  terminal conflict, cursor mismatch, coverage gap, unsupported version, and
  temporary unavailability.

Unknown fields, unsafe integers, noncanonical UTC timestamps, non-SHA-256 hashes,
wrong routes, oversized page limits, and incompatible versions fail before any
durable mutation.

## Error and recovery map

| Codepath | Failure | Durable result | Caller sees | Recovery |
|---|---|---|---|---|
| Coordinator reserve | RPC fails before/after durability | `manifest_reserving` + recovery row | 202 pending | Retry identical reservation with backoff |
| Catalog activation | Ack lost before/after activation | Safe false-positive entry may exist | 202 pending | Retry same activation before bucket reserve |
| Bucket initialization | Legacy inspection/certificate transaction unavailable | No V2 reservation or seal mutation | Retryable `LEGACY_CERTIFICATION_UNAVAILABLE` | Stay `manifest_reserving`; bounded recovery backoff |
| Reserve | Same ID, different immutable hash | Reservation quarantined | 409 conflict | Operator investigation; seal blocked |
| Reserve | Hard rejection before bucket row exists | Zero participant effects | Stable rejected/aborted result | Remove recovery work; do not retry forever |
| Prepare | Participant rejects after reservation | Durable abort decision | 409 aborted/pending cleanup | Abort participants, then cancel reservation |
| Finalize | Ack lost after bucket assigned decision | `commit_deciding` is irreversible; reservation may be finalized | 202 pending manifest | Replay exact finalize bytes; persist returned record; commit |
| Cancel | Ack lost after cancellation | Reservation may already be cancelled | Abort remains durable | Retry identical cancel; converge |
| Cancel | Reservation overlay is quarantined | Parked `aborted_pending_manifest_cancel`; no retry scheduled | Pending operator resolution | resolved-CANCELLED attestation releases terminal abort |
| Finalize vs cancel | Conflicting terminal transition | Reservation quarantined | 409 conflict | Never overwrite; seal blocked |
| Quarantine repair | Ack lost after attestation append | Resolution may already be durable | Pending resolution | Replay identical idempotency key and evidence |
| Quarantine repair | Different evidence/outcome for same key | No overwrite | 409 repair conflict | Operator must use a new reviewed resolution |
| Seal generation | Reservation remains unresolved | It can only receive a decision time after the fence | No cutoff blocker | Coordinator recovery continues independently |
| Seal generation | Existing eligible conflict/quarantine | No complete receipt published | Quarantined coverage | Audited repair transition required |
| Seal generation | Crash during bounded digest/page work | Durable generation cursor remains | Pending/incomplete | Resume the identical generation and cutoff |
| Catalog snapshot | Activation races cutoff fence | Serialized into snapshot or returned floor | Stable inclusion/exclusion | Included bucket seals through T; later bucket initializes above T |
| V1 cutover | Registration races bucket fence | Row is durably pre-fence or receives `V1_CLOSED` | Imported V1 or V2 bridge | Never reopen fence; exact V1 replay remains readable |
| V1 import | Bucket/receipt missing or hash mismatch | Catalog cutover stays `IMPORTING` | Incomplete with exact bucket | Repair evidence or re-read; never seal partial root |
| Enumerate | Missing/uninitialized bucket | No completeness claim | Gap with exact day/partition | Materialize and seal expected bucket |
| Enumerate | Coverage predates retained floor | No history manufactured | `retention_expired` | Choose a supported cutoff/window or external archive |
| Enumerate | One bucket overloads/times out | Partial diagnostics only | Incomplete/503 | Resume same request/cursor after cooldown |
| Enumerate | Cursor belongs to another request | No reads after validation | 400 cursor mismatch | Restart with matching cursor/request |
| Lifecycle alarm | Sealed data still inside retention | No early deletion | No user-visible loss | Retention rules continue to apply |

No row may have `test=no`, `handling=no`, and `visibility=silent`.

### Alarm multiplexing

Each Durable Object has one physical alarm slot. No feature calls `setAlarm`
directly after initialization. `JournalManifestDO` and
`FleetManifestCatalogDO` persist an `alarm_schedule(purpose PRIMARY KEY,
fire_at_ms, generation, payload_hash)` table; `alarm()` processes every due
purpose in bounded work and arms the minimum remaining time. Purposes include
retention cleanup, seal/snapshot resume, cutover import, catalog retirement, and
resolution propagation. `CoordinatorDO` extends its existing durable recovery
queue with reserve/finalize/cancel/bridge actions and likewise arms only the
minimum due row. Scheduling one purpose must not delete or postpone another.
An optional next-day legacy prewarm purpose reduces cold-start work but is never a
correctness or admission prerequisite.

## Code-quality constraints

- Contract validation and hashing have one owner in `packages/contracts`.
- Routing helpers accept the control-plane-issued frozen reservation route
  explicitly; callers do not reconstruct it from `commit_decided_at`.
- Only `JournalManifestDO` assigns V2 decision time and sequence. Coordinators
  validate and persist the returned canonical record but never manufacture those
  fields locally.
- Storage methods perform one state transition per transaction and return typed
  domain results instead of throwing expected conflicts.
- Service-binding adapters translate transport ambiguity once; the coordinator
  does not duplicate bucket result interpretation.
- Coordinators persist exact canonical reservation/finalize/cancel bytes before
  their first RPC. Hard reserve rejection with no bucket row transitions directly
  to `aborted`; a rejection that created quarantine transitions to
  `aborted_pending_manifest_cancel`. Finalize conflict in `commit_deciding` parks
  for audited resolution and never hot-loops or autonomously aborts.
- Existing v1 reads and in-flight transaction recovery remain supported.
- Complex coordinator and bucket state transitions receive inline ASCII diagrams.
- No catch-all may swallow overload metadata; temporary availability and protocol
  conflicts stay distinct.

## Test coverage diagram

```text
SHARED CONTRACTS
  +-- reservation canonicalization/hash/route ........ unit: valid + malformed + boundary
  +-- state transition table ........................ unit: every valid/invalid pair
  +-- seal-generation/page/enumeration/cursor validation unit: unknown fields + mismatch
  +-- catalog activation/snapshot/retirement contracts  unit: valid + malformed + replay
  +-- v1 bridge/cutover contracts .................... unit: versions + hashes + boundaries

COORDINATOR
  +-- new transaction -> reserve -> prepare .......... integration
  +-- reservation ack lost -> identical retry ........ fault integration
  +-- hard reserve rejection reaches terminal abort .. integration
  +-- restart replays byte-identical request despite clock movement fault integration
  +-- crash at every local/RPC boundary .............. deterministic fault matrix
  +-- prepare rejection -> abort -> cancel ........... integration
  +-- commit_deciding -> bucket assigns time -> commit  integration
  +-- finalize ambiguity never regresses to abort ..... fault integration
  +-- force-abort/resume races in reserving state .... concurrency integration
  +-- predecessor prepared adoption -> reserve first . compatibility integration

JOURNAL MANIFEST BUCKET
  +-- reserve idempotency/conflict ................... unit/integration
  +-- finalize/cancel idempotency + conflict ......... unit/integration
  +-- audited resolve commit/cancel + evidence replay  integration
  +-- coordinator-state hash rejects outcome mismatch . transition/integration
  +-- quarantine overlay never mutates member state ... invariant integration
  +-- post-seal repair cannot backdate decision ....... deterministic concurrency
  +-- one membership predicate drives digest/count/page contract integration
  +-- seal empty bucket through same-day cutoff ...... integration
  +-- unresolved reservation does not block cutoff ... integration
  +-- finalize before/after fence gets <=T/>T ......... deterministic concurrency
  +-- monotonic generation/hash chain + crash resume . integration
  +-- stable keyset pages at exact cutoff ............ integration
  +-- lifecycle never deletes required seal evidence . alarm integration
  +-- retention + seal-resume alarms coexist .......... alarm integration

FLEET MANIFEST CATALOG
  +-- activation before reservation acknowledgement . service integration
  +-- activation vs cutoff fence serialization ....... deterministic concurrency
  +-- false-positive entry materializes empty bucket . integration
  +-- bounded snapshot/hash chain + crash resume ...... integration
  +-- retirement waits for bucket safety certificate . lifecycle integration
  +-- partition config history/hash mismatch fails .... contract/integration
  +-- snapshot + cutover + retirement alarms coexist . alarm integration
  +-- V1 cutover state/hash chain cannot skip phase .. transition/integration
  +-- 35-day x 16 V1 import includes empty buckets ... service integration
  +-- future/uninitialized V1 route rejected after fence concurrency integration
  +-- daily legacy horizon finds pre-catalog future row  compatibility integration
  +-- first post-midnight reserve certifies inline ..... deterministic concurrency
  +-- delayed prewarm never blocks reservation ......... alarm integration

FLEET ENUMERATOR
  +-- deterministic close key adopts crash-lost driver  fault integration
  +-- covering fence derives exact-prefix receipt ...... integration
  +-- one day x 16 including empty buckets ........... service integration
  +-- cross-day + same-day cutoff boundaries ......... service integration
  +-- missing/open/quarantined/overloaded bucket ..... fault integration
  +-- retention-expired differs from true gap ......... boundary integration
  +-- cursor resume, tamper, request mismatch ........ service integration
  +-- imported V1 window + V2 bridge ordering ........ compatibility integration
  +-- pre-import-retention request -> unproven ........ compatibility integration
```

The full deterministic matrix includes crashes immediately before and after:
local reserving persistence, reservation RPC durability, preparing transition,
abort decision, cancellation RPC durability, irreversible `commit_deciding`,
bucket decision assignment, returned-record persistence, participant commit,
V1 bucket fence, V1 receipt import, catalog cutover phase transition, seal
transaction, and page/cursor emission.

## Performance plan

- Add `(route_at, state, tx_id)` and terminal-identity indexes for reservation
  recovery and conflict checks.
- Add `(commit_decided_at_ms, decision_sequence, tx_id)` on manifest records for
  sealing and keyset pages.
- Default local page size: 100; accepted range: 1..500.
- Default fleet concurrency: 4; accepted range: 1..8. Do not issue unbounded
  catalog-entry parallel RPCs.
- A fleet page inspects a bounded number of buckets and records, and its cursor
  resumes at an exact bucket/local key.
- Seal generations fence the bucket decision clock before deterministic bounded
  digest batches. They never load an unbounded bucket into memory. Digest progress
  is durable, chained to the prior receipt, and generation-fenced; the final
  transaction rechecks row counts before publishing the immutable receipt.
- Report inspected buckets, records, elapsed milliseconds, and incomplete counts
  as structured logs. Live p95 and overload qualification remain a later gate.

## Deployment and compatibility

1. Deploy read-compatible contracts, schema migration, V2 bridge recovery, and
   mixed-version tests first.
2. Deploy `FleetManifestCatalogDO` and control-plane activation/snapshot/cutover
   methods.
3. Deploy bucket reserve/finalize/cancel plus `closeLegacy` while V1 registration
   remains accepted in open legacy buckets.
4. Deploy coordinator `manifest_reserving` and `commit_deciding`; new transactions
   activate the catalog and reserve before participant prepare, while legacy
   prepared/pending-manifest states know how to bridge.
5. Create the durable fleet cutover record, fence all retained V1 buckets, bridge
   losing legacy registrations, import and verify every V1 receipt, then seal the
   catalog cutover generation.
6. Establish `reservation_required_since` from that immutable generation and
   enable enumeration only for its provable retained window and later V2 traffic.

Rollback before step 5 may return writers to V1 registration and leaves V2 rows
readable. Once any V1 bucket fence is durable, rollback must preserve V2 bridge
recovery and must never reopen V1 insertion. A partial cutover remains visibly
`FENCING` or `IMPORTING` and cannot authorize completeness. No destructive schema
rollback is required.

## NOT in scope

- PITR preview, execution, participant replay, and reconciliation: T6 after these
  contracts stabilize.
- T7 fleet-wide overload control, schema matrix, SLO alerts, and live B1 gate:
  separate bounded reliability slices.
- Cloudflare account API enumeration: privileged control-plane inventory is not
  a correctness source.
- R2 journal archive: optional T14 and never on the commit path.
- Public HTTP/admin restore routes or UI: this slice remains internal and route-less.
- Claims that pre-reservation v1 history is complete: mathematically unprovable.
- Production deployment or live qualification: requires separate authorization.

## Parallel implementation

| Lane | Work | Depends on |
|---|---|---|
| A | Shared catalog/reservation/seal/enumeration contracts and tests | None |
| B | ADR-3/SPEC protocol text and compatibility matrix | Selected architecture |
| C | FleetManifestCatalogDO schema/snapshot/retirement implementation | Lane A contract freeze |
| D | JournalManifestDO schema/state/seal/page implementation | Lane A contract freeze |
| E | Coordinator reserving/finalize/cancel integration | Lane A contract freeze |
| F | Fleet close driver, enumeration service, and integration tests | Lanes C, D, and E |

Launch A and B in parallel. After A freezes exports, run C, D, and E in parallel
with single owners for shared files. Merge those lanes, then run F and the
aggregate gate.

## Implementation tasks

- [ ] **T1 (P1, human ~3d / Codex ~6h)** - contracts - Add catalog,
  reservation, terminal-transition, seal, page, cursor, coverage, and typed error
  contracts.
- [ ] **T2 (P1, human ~3d / Codex ~6h)** - catalog - Add fleet activation,
  cutoff snapshots, hash chains, safe false positives, and certified retirement.
- [ ] **T3 (P1, human ~3d / Codex ~6h)** - manifest bucket - Add expand-first
  reservation schema, bucket-owned decision clock, monotonic `sealed_through`
  generations, and idempotent reserve/finalize/cancel transitions.
- [ ] **T4 (P1, human ~3d / Codex ~6h)** - coordinator - Persist reserving before
  prepare, persist irreversible `commit_deciding` before finalize, and reconcile
  bucket-issued decisions/cancellations through durable recovery.
- [ ] **T5 (P1, human ~3d / Codex ~6h)** - fleet close/enumeration - Add the
  route-less durable close driver, bounded sealing, stable local pages,
  deterministic fleet coverage, and fail-closed cursors.
- [ ] **T6 (P1, human ~3d / Codex ~6h)** - verification - Add contract,
  crash-boundary, concurrency, compatibility, gap, overload, and pagination tests.
- [ ] **T7 (P2, human ~1d / Codex ~2h)** - documentation - Update ADR-3, SPEC,
  operator guidance, roadmap status, diagrams, and release notes.

## Unresolved decisions

None. D4-D11 selected the complete proof-bearing contracts. Inline bucket legacy
certification removes catalog aggregation from admission while preserving the
continuous scan horizon; optional prewarming affects latency only.

## GSTACK REVIEW REPORT

| Review | Trigger | Why | Runs | Status | Findings |
|---|---|---|---:|---|---|
| CEO Review | `/plan-ceo-review` | Scope and strategy | 2 prior + focused routing | CLEAN | T5a selected as the user-authorized alternative to the partner gate |
| Codex Review | `/codex review` | Independent second opinion | 5 Claude outside-voice passes | CLEAN | 20 findings resolved; focused D11 confirmation returned `GO` |
| Eng Review | `/plan-eng-review` | Architecture and tests, required | 2 + 9 resumed decisions | CLEAN | D4-D11 selected; all known proof, migration, availability, repair, retention, and orchestration gaps closed |
| Design Review | `/plan-design-review` | UI/UX gaps | 1 prior | NOT REQUIRED | Internal route-less recovery infrastructure; no UI scope |
| DX Review | `/plan-devex-review` | Developer experience gaps | 0 | NOT REQUIRED | No new public developer surface |

- **CODEX:** Five Claude outside-voice passes drove 20 corrections across decision
  time, discovery, V1 cutover, sealing, retention, quarantine, ordering, alarms,
  orchestration, and midnight availability; the final focused gate returned `GO`.
- **VERDICT:** CEO + ENG + independent review CLEARED - ready to implement bounded
  T5a. Design/DX review remain not required for this internal route-less scope.

NO UNRESOLVED DECISIONS
