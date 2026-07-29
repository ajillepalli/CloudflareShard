# Operator's Guide: Running a CloudflareShard Cluster

This is a runbook for keeping a **deployed** CloudflareShard cluster healthy
in production — your own Deploy-to-Cloudflare instance or a larger
hand-rolled deployment. It assumes the cluster already exists and someone is
on the hook for it staying up, staying fast, and not draining a bank account.

If you're building an application on top of a cluster (writing schema,
choosing partition keys, calling `/v1/mutate`/`/v1/tx`), that's
`docs/guides/infrastructure-builder.md`, not this document. This document is
about the cluster itself: deployment hardening, telemetry, reshard
operations, incident response, backup posture, and cost.

Everything below assumes familiarity with the architecture in
[`docs/SPEC.md`](../SPEC.md) — this guide links to specific sections rather
than re-deriving them.

## 1. Deploying and initial hardening

Deploy mechanics (the "Deploy to Cloudflare" button, what it provisions, the
`/admin/init` call, pointing a second Worker at it via service binding) are
covered in the README's
[**"Deploy your own cluster"**](../REFERENCE.md#deploy-your-own-cluster)
section and in
[`examples/shardscope/docs/deploy/README.md`](../../examples/shardscope/docs/deploy/README.md).
Don't duplicate that here — go straight to hardening once the cluster
exists.

### 1.1 Set a genuinely strong `ADMIN_TOKEN`

`ADMIN_TOKEN` is a single Worker secret (`env.ADMIN_TOKEN`) that gates the
**entire** `/admin/*` surface, plus the admin-only `/v1/sql` and
`/v1/scatter` routes (§7, §14). There is no per-operation scoping and no
separate roles — one bearer secret grants init, reshard (split/drain/
migrate), tenant registration/revocation, raw SQL, and cross-tenant scatter
reads. Treat it like a database root password, because functionally it is
one.

- Generate it with `openssl rand -hex 32` (this is what the README and the
  deploy-button `.env.example` recommend) — don't hand-type something
  memorable.
- Store it in a secrets manager, not in a chat message, ticket, or shell
  history file.
- If the Worker returns `500 ADMIN_TOKEN is not configured`, the secret was
  never set — the entire admin surface is unusable (not open — the check
  fails closed) until you set it.

### 1.2 There is no rate limiting on `/admin/*` beyond the token itself

Unlike `/v1/table-scan`, which has an explicit per-tenant token-bucket rate
limiter (§7, "Per-tenant rate limiting"), the `/admin/*` surface has none.
Anyone holding `ADMIN_TOKEN` can call `/admin/drain-shard`,
`/v1/scatter`, or `/v1/sql` as fast as they want, with no built-in throttle.
This means:

- A leaked `ADMIN_TOKEN` is not just a confidentiality problem — it's an
  availability and integrity problem, immediately, with no rate-limit
  friction slowing down abuse.
- If you're scripting admin operations (a CI job, a cron-based drain, a
  benchmark harness), you are your own rate limiter. Nothing on the server
  side will slow you down if you fire off a reshape operation in a loop by
  accident.

### 1.3 Rotating `ADMIN_TOKEN` if it's ever leaked

`ADMIN_TOKEN` is a plain Worker secret compared by `requireAdminAuth` /
`requireAdminAuthFromHeader` against the request's `authorization` header —
it is **not** the same token model as tenant tokens. Concretely:

- **No grace period.** Tenant token rotation (`POST /admin/register-tenant
  {"rotate": true}`, §14) keeps the old token valid for
  `TENANT_TOKEN_ROTATION_GRACE_MS` (5 minutes) so in-flight callers don't
  break. `ADMIN_TOKEN` has no equivalent — updating the secret (`wrangler
  secret put ADMIN_TOKEN` or via the dashboard) takes effect on the next
  request and the old value stops working immediately. Any script or
  service still holding the old value starts getting 401s right away.
- **No revocation list, no partial invalidation.** There's exactly one
  active value at a time; rotating replaces it wholesale. There is nothing
  to "revoke" beyond setting a new value.
- **Practical rotation procedure if `ADMIN_TOKEN` leaks:**
  1. Generate a new value (`openssl rand -hex 32`).
  2. `wrangler secret put ADMIN_TOKEN` against the deployed Worker (or the
     dashboard's Settings → Variables and Secrets).
  3. Update every legitimate caller (deploy scripts, your own operator
     tooling, the Shardscope dashboard's admin client config) with the new
     value — all of them break the instant the secret is updated, since
     there's no grace window.
  4. Assume any operation the leaked token could have triggered
     (`/admin/drain-shard`, `/v1/scatter`, `/v1/sql` writes against internal
     tables would have been blocked, but reads and any tenant-table write
     were fully in scope) may have happened; check Workers Logs (§2) for
     `catalog.admin_action` and `http.request` entries in the exposure
     window.

Tenant tokens (`tenant_auth`, §14) are a structurally separate trust
boundary — rotating or revoking a tenant's token (`POST
/admin/register-tenant {"rotate": true}` / `POST /admin/revoke-tenant`) has
no effect on `ADMIN_TOKEN` and vice versa. `POST /admin/revoke-tenant` is
the one exception to "rotation is graceful" — it invalidates the current
token **and** any still-in-grace previous token immediately, no grace
period.

## 2. Health monitoring

### 2.1 What "healthy" looks like, using real response fields

`POST /admin/status` (ADMIN_TOKEN) is the top-level cluster health check. It
fans out to every catalog shard's own `/status` and fails closed — if any
one catalog shard's `/status` call itself errors, the whole request fails
with that catalog shard's own status/body rather than silently returning a
partial view (§7). Response shape:

```
{
  initialized: boolean,        // AND of every catalog shard's own "initialized" flag
  catalogShardCount: number,
  shards: { total, active, draining },   // summed across every catalog shard
  catalogs: [
    { catalogShardId, initialized, totalVBuckets?, metadataVersion?, initializedAt?,
      shards?: { total, active, draining } }
    // ...one entry per catalog shard; every field but catalogShardId/initialized
    // is absent for a catalog shard that hasn't been /admin/init'd yet
  ]
}
```

Read this as: **healthy** = `initialized: true`, every entry in `catalogs`
has `initialized: true`, and `shards.draining` is 0 (or a number you
expect, because you know a drain is in progress). **Degraded / suspect**
signals:

- Any `catalogs[i].initialized === false` after the cluster was supposed to
  be fully bootstrapped — that catalog shard never got `/admin/init`, or its
  state was lost.
- `shards.draining > 0` when you didn't just kick off a drain — either a
  drain is stuck (see §4.4) or someone ran one you don't know about.
- The whole `/admin/status` call itself failing (not returning a body at
  all) — this means at least one catalog shard is unreachable, which is
  worse than any per-field signal above.

For a single shard's own footprint (row counts, idempotency-table size,
in-flight-transaction count, index backlog), use `POST /admin/shard-stats
{shardId}` (§7):

```
{ ok, tables: [{table, rowCount}], idempotencyTableSize, pendingIntentCount,
  indexPendingJobCount, indexEntryCount, rowOwnerCount }
```

`pendingIntentCount` is the number of distinct in-flight 2PC transactions
touching that shard — a number that should hover near zero outside of
active write bursts and never grow unboundedly (see §4.1 if it does).
`indexPendingJobCount` is the async index-maintenance retry backlog — a
sustained nonzero value here means index writes are failing to land, not
just running slowly. Note this route does **not** validate `shardId` against
the live topology first (unlike `/admin/fault-inject`) — a typo'd shard ID
silently cold-starts a brand-new, empty `ShardDO` rather than 404ing, so a
suspiciously-empty result from this route is worth double-checking against
`/admin/vbucket-map` or `/admin/status` before concluding a shard is
actually empty.

`POST /admin/topology-lock-status` (§7, and §3 below) tells you whether a
reshape operation currently holds the cluster-wide topology lock.
`POST /admin/tx-status {txId}` tells you the state of one transaction if
you already have its ID; there's no "list all in-flight transactions"
route — you learn about a stuck one from `pendingIntentCount` climbing on a
particular shard, from application-side timeout/error logs carrying the
`txId`, or from `TX_PARTICIPANT_LOCKED` spikes (§4.2) naming the row.

`POST /admin/drain-shard-status {catalogShardId, shardId}` reports drain
progress: `{shardId, vbucketsRemaining, ringsRemaining, status}` where
`status` is `active | migrating-vbuckets | evacuating-rings | complete`.

### 2.2 Logs: what's already there, and what to watch for

The reference doc's [**Observability**](../REFERENCE.md#observability) section
covers the mechanics — `wrangler tail --format json`, or the dashboard's
Workers Logs view (`[observability]` in `wrangler.toml` is set to
`head_sampling_rate = 1`, i.e. every request is logged, not sampled). Don't
re-derive the tail commands here; see that section.

What's worth actually watching for, once you're tailing:

- **Every request** logs `http.request` with `{path, method, status,
  durationMs}` — this alone gives you a per-route latency and error-rate
  signal without any extra instrumentation. A normal pattern is a tight
  cluster of `durationMs` values per route with occasional outliers during
  reshape operations (migration/checksum passes read a lot of rows) or cold
  starts. A **concerning** pattern is `durationMs` trending upward across
  many consecutive requests to the same route with no reshape in flight —
  that's a shard getting slower, not a one-off.
- `catalog.admin_action` — logged for every admin mutation
  (`/init`, `/register-table`, `/split-vbucket`, `/drain-shard`, etc., also
  what `/admin/audit-log` aggregates across catalog shards, §4). A burst of
  these you didn't initiate is either a scripted process you forgot about
  or a `ADMIN_TOKEN` compromise (see §1.3).
- `shard.prepare_guard_mismatch` — logged every time a `/v1/tx` prepare
  hits a `TX_PARTICIPANT_GUARD_MISMATCH` (§4.3). A low steady background
  rate is normal on hot rows; watch for a sustained, climbing rate against
  the *same* `table:partitionKey` pair specifically.
- Status codes in `http.request`: occasional `409`s on `/v1/tx` and
  `/v1/mutate` (lock contention, guard mismatches) are expected under
  concurrent load. A sustained run of `5xx`s, or `502 SHARD_UNREACHABLE` on
  `/v1/table-scan`/`/v1/scatter`, means a shard or catalog shard is actually
  unreachable — that's an incident, not routine contention.

## 3. Running a reshard operation safely

The mechanics of split/drain/migrate are in
[`docs/SPEC.md` §11](../SPEC.md#11-rebalancing-split-and-drain-milestone-3--shipped);
this section is about running them **as an operator**, safely, on a live
cluster.

### 3.1 The topology lock — why it exists

Split, drain, migrate, create-index, and drop-index all reshape the
cluster's topology (the vbucket map or an index's placement ring). Running
two of these concurrently against the same catalog shard could race each
other's preconditions — e.g. a split and a drain both trying to move the
same vbucket. To prevent that, every one of these operations acquires a
durable, TTL'd (`TOPOLOGY_LOCK_TTL_MS` = 30s) cluster-wide lock living in
`catalog-0`'s own `topology_lock` table before mutating, and heartbeats it
once per orchestration tick while the operation is in flight (§7).

This means: **only one topology-reshaping operation can run at a time,
cluster-wide** (not just per-catalog-shard) — attempting a second one while
the lock is held fails until the first completes or the lock is released.

### 3.2 Checking lock state

```
POST /admin/topology-lock-status   (ADMIN_TOKEN, no body)
-> 200 {held: false}
-> 200 {held: true, operationId, operationType, acquiredAt, heartbeatAt, expiresAt, expired}
```

Check this before kicking off a reshape if you're not sure whether one is
already running, and check it whenever a reshape operation you started
seems to be taking longer than expected. `expired: true` on a `held: true`
response means the lock's TTL lapsed without a heartbeat — normally a sign
the operation that held it crashed or got stuck; a live, healthy operation
heartbeats well inside the 30s TTL.

### 3.3 `/admin/force-release-topology-lock` — last resort, not routine

```
POST /admin/force-release-topology-lock   (ADMIN_TOKEN)
Request: {operationId}
-> 200 {ok: true, released}   // released: false is an idempotent no-op — the
                               // given operationId didn't match the current
                               // holder, or nothing is held. Not an error.
-> 400 {error: {code: "MISSING_FIELDS", ...}}
```

This is explicitly framed in `docs/SPEC.md` as "the same class of manual
escape hatch as `/admin/tx-force-abort` for a stuck 2PC transaction" —
**not** a tool you reach for because a reshape operation is merely slow.
Force-releasing while the original operation is still alive and genuinely
working (e.g. a large migration whose checksum pass over hundreds of pages
is just taking a while) can let a second topology operation start
concurrently against state the first one is still mutating — exactly the
race the lock exists to prevent.

**Only force-release when you've confirmed the lock is actually stuck, not
just slow:**

1. `POST /admin/topology-lock-status` — note `operationId`, `operationType`,
   `heartbeatAt`, `expiresAt`, `expired`.
2. If `expired: false`, the operation is still heartbeating — it is alive
   and progressing by definition (a crashed process can't heartbeat). Do
   not force-release. If it seems slow, check the relevant status route
   instead (`/admin/migrate-vbucket-status` for a migration/split,
   `/admin/drain-shard-status` for a drain) to see whether it's actually
   making progress (`rowsCopied`/`vbucketsRemaining`/`ringsRemaining`
   moving between polls) before concluding anything is wrong.
3. If `expired: true` **and** you've independently confirmed the process
   that started it is actually gone (a deploy that interrupted a `waitUntil`,
   a crashed script, an operator who ran the curl command and then lost the
   connection) — only then force-release, using the exact `operationId`
   from step 1:
   ```
   curl -X POST https://<worker>/admin/force-release-topology-lock \
     -H "authorization: Bearer $ADMIN_TOKEN" -H "content-type: application/json" \
     -d '{"operationId": "<the-id-from-status>"}'
   ```
4. After releasing, re-check whatever the stuck operation was doing (a
   migration's status, a drain's status) — it may be resumable via a retry
   of the same operation, or it may have left partial state that needs
   manual inspection (see §4.4).

### 3.4 The actual workflow

1. **Split** (`POST /admin/split-vbucket {catalogShardId, vbucket,
   newShardId?}`) — creates a target shard and starts a real online
   migration (dual-write backfill + fenced checksum-verified cutover, §11);
   routing only flips once cutover completes. Response includes
   `migrationStarted: true`.
2. **Migrate** (`POST /admin/migrate-vbucket {catalogShardId, vbucket,
   targetShardId?}`) is the same underlying primitive, usable directly (not
   just via split) — e.g. to move a vbucket for load-balancing reasons
   rather than growth. Gated 409 `VBUCKET_PROVENANCE_INCOMPLETE` if the
   source shard has unattributed rows for any registered table — run
   `/admin/backfill-provenance` first if you hit this.
3. **Watch it** — `POST /admin/migrate-vbucket-status {catalogShardId,
   vbucket}` returns `{vbucket, status, fromShard, toShard, rowsCopied,
   mirrorQueueDepth, startedAt}`. `status` moves through `backfilling` →
   `cutover` → gone (map flipped, `migration_status` back to `none`).
4. **Abort if needed, but only before cutover completes** — `POST
   /admin/migrate-vbucket-abort {catalogShardId, vbucket}` is safe any time
   before the map flip (source never stopped being authoritative; target's
   partial rows + provenance are wiped, fence lifted, queued mirrors
   purged). After the flip it returns 409 `MIGRATION_ALREADY_COMMITTED` — a
   committed migration is reversed by migrating the vbucket back, not by
   aborting.
5. **Drain** (`POST /admin/drain-shard {catalogShardId, shardId}`) — full
   evacuation: every vbucket on the shard migrates off sequentially via the
   same primitive above, then every index whose pinned placement ring
   includes the shard gets a deterministic substitute. Pre-checked 409s
   before any durable change: `SHARD_HAS_IN_FLIGHT_TRANSACTIONS`,
   `SHARD_HAS_PENDING_INDEX_JOBS`, `RING_EVACUATION_NO_CANDIDATE`. Track
   with `POST /admin/drain-shard-status {catalogShardId, shardId}`.

A drain of a busy shard is inherently a slow, multi-step operation — expect
it to take meaningfully longer than a single split, since it's N sequential
vbucket migrations plus ring evacuation. Don't force-release the topology
lock just because a drain is still `migrating-vbuckets` after a while;
confirm via `/admin/drain-shard-status` that `vbucketsRemaining` is
actually decreasing between polls first.

## 4. Incident playbook

### 4.1 A transaction stuck in `preparing`/`prepared` for a long time

**Symptom:** a client is waiting on a `/v1/tx` call that never resolves, or
you notice `pendingIntentCount` on `/admin/shard-stats` for a shard staying
elevated rather than draining back toward zero.

1. Get the `txId` (from the client's own error/timeout logs, or from
   application logs that recorded it at the time `/v1/tx` was called).
2. `POST /admin/tx-status {txId}` → `{found, status}` where `status` is one
   of `preparing / prepared / committing / committed / aborted`.
3. If `status` is `committed` or `aborted`, the transaction actually
   resolved — whatever's "stuck" is downstream (a client that gave up
   waiting, or a delayed ack — see §7's note that `committed_pending_ack`
   is still a fully durable commit, only the shard-side ack is outstanding
   and retried by `CoordinatorDO`'s own `alarm()`). Nothing to force here.
4. If `status` is genuinely `preparing` or `prepared` for well past a
   reasonable window (seconds to low tens of seconds under normal
   conditions, not minutes), the coordinator's own recovery loop hasn't
   resolved it — treat it as stuck.
5. `POST /admin/tx-force-abort {txId}` → aborts every participant shard,
   marks the transaction `aborted`. Rejects 409 if it already committed (so
   this is safe to attempt even if you're not 100% sure — it won't
   accidentally abort something that already succeeded).
6. Confirm the client-side write did not apply (re-read the row) and have
   the caller retry from scratch with a fresh `requestId`.

### 4.2 `TX_PARTICIPANT_LOCKED` errors spiking

**What it means:** a `/v1/tx` (or `/v1/mutate`/`/v1/sql`) call tried to
touch a row already locked by a *different* in-flight coordinated
transaction. This is row-level lock contention — expected, ordinary
behavior under concurrent writes to the same row, not a bug. The response
includes `retryAfterMs`; a well-behaved client backs off and retries.

**When it's fine:** a low background rate, concentrated on rows you know
are genuinely hot by design (a shared counter, a warehouse-level stock
row in a TPC-C-style workload) — this is the system doing exactly what
optimistic/pessimistic row locking is supposed to do.

**When it's a capacity or design problem:** a *sustained*, climbing rate,
especially if it's concentrated on one specific `table:partitionKey` and
correlates with rising p95/p99 latency or client-visible timeouts. That's a
signal the row is hotter than the current topology can serve — the row
itself can't be split (a single logical row lives on one shard by
definition), so the fix is architectural: reduce the contention at the
application level (batch fewer competing writers onto the same key, e.g.
per-district rather than per-warehouse counters), not a cluster operation.
Cross-reference against `/admin/shard-stats`' `pendingIntentCount` for the
shard that owns the hot row, and against Workers Logs `http.request`
latency for the affected route.

### 4.3 `TX_PARTICIPANT_GUARD_MISMATCH` (409) — verified against current `shard.ts`

As of the current `main` (commit `524a986` / `67879bc`, merged in PR #48,
"fix/tx-where-guard-silent-noop"), `ShardDO`'s `/prepare` handler
(`src/shard.ts`, the block implementing `handlePrepare`'s guard check) does
the following: for any base-row `update`/`delete` intent in a `/v1/tx`
batch, if executing that intent's compiled SQL during prepare's validation
pass affects **zero rows** — i.e. the caller's `where` clause (almost
always an optimistic-concurrency / compare-and-swap guard against a
previously-read row state) no longer matches anything — prepare fails that
participant with:

```
409 { error: { code: "TX_PARTICIPANT_GUARD_MISMATCH",
      message: "Row <table>:<partitionKey> changed concurrently — its WHERE
                 guard no longer matched any row.",
      fix: "Re-read the row's current state and retry with fresh values." } }
```

The coordinator aborts the whole transaction on this response, exactly as
it does for `TX_PARTICIPANT_LOCKED` — the client gets a real, retryable
error instead of a false "committed" for a write that silently no-op'd.
(Before this fix, a 0-row `WHERE` match wasn't treated as an error at all —
it's ordinary, non-throwing SQL — so a stale CAS attempt sailed through
prepare, got recorded, and `handleCommit` applied it as a no-op while the
transaction still reported success. See the code comment at the top of
this check in `src/shard.ts` for the full history, including why the fix
short-circuits on the *first* mismatch found in a batch rather than letting
later intents in the same batch throw a different, misleading error.)

**Read this as a legitimate, expected, retryable concurrency-conflict
signal — not, by itself, an operator-actionable error.** A well-behaved
client that reads a row, computes a guarded update, and retries on this
exact response is working as designed; this is the system correctly
catching a stale compare-and-swap instead of silently dropping it. Do not
page on an isolated occurrence, and do not treat a low background rate as
an incident.

**When it *is* worth investigating:** a **sustained, high rate** of this
error concentrated on one specific row (visible via the
`shard.prepare_guard_mismatch` log line's `table`/`partitionKey` fields,
§2.2) indicates real hot-row contention — many concurrent writers racing to
CAS the same row, most of them losing. That's the same underlying signal as
§4.2's `TX_PARTICIPANT_LOCKED` guidance: ask whether the contention is
inherent to the workload (and needs an application-level redesign to spread
writes across more keys) or whether it's evidence a shard split would help
by isolating the hot row's neighbors onto a less-contended shard (splitting
does not, by itself, reduce contention on a single row that stays on one
shard — but if the hot row is sharing a shard with other traffic that's
also suffering from the contention, isolating it can still help the
neighbors).

### 4.4 A reshard operation stuck mid-migration

**Symptom:** `/admin/migrate-vbucket-status` shows `rowsCopied` or
`mirrorQueueDepth` not moving between polls, or `status` stuck at
`backfilling`/`cutover` well past when the data volume would suggest it
should finish.

1. `POST /admin/migrate-vbucket-status {catalogShardId, vbucket}` — check
   `rowsCopied` and `mirrorQueueDepth` across two polls a reasonable
   interval apart. Progress (even slow progress) means it's working, not
   stuck — a large table or a slow checksum pass over many 500-row pages is
   legitimately slow, not broken. `mirrorQueueDepth` not draining while
   `status` is `cutover` blocks that phase specifically (§11 step 2 of
   cutover) — check whether the target shard is reachable at all.
2. `POST /admin/topology-lock-status` — confirm whether the migration still
   holds and is heartbeating the topology lock (§3.2). If it's not
   heartbeating and the lock has expired, that's strong evidence the
   process driving it (the catalog shard's own alarm loop) actually
   stalled, not just slowly working through pages.
3. **If truly stuck before cutover's map flip (§11 step 2.4):** `POST
   /admin/migrate-vbucket-abort {catalogShardId, vbucket}` is safe — it
   wipes the target's partial rows/provenance, lifts the fence, purges
   queued mirrors, and leaves the source fully authoritative, as if the
   migration never started. Retry the migration (or split) fresh afterward.
4. **If stuck after the map flip has already happened** (routing moved to
   the target, but cleanup — unfencing the source, deleting old rows —
   hasn't finished): `/admin/migrate-vbucket-abort` now returns 409
   `MIGRATION_ALREADY_COMMITTED` — this is not a bug, it's telling you the
   move already happened. Data is live and correct on the target; what's
   incomplete is only the source-side teardown. Check
   `/admin/migrate-vbucket-status` again after a beat — the alarm-driven
   cleanup should catch up. If the topology lock looks stuck (not
   heartbeating, expired) at this stage specifically, this is one of the
   few situations where `/admin/force-release-topology-lock` (§3.3) is
   genuinely appropriate, since the operation's own recovery loop may have
   died after the point of no return rather than before it.
5. When genuinely unsure whether it's safe to abort vs. force-release vs.
   just wait, default to **waiting and re-polling** — both status routes
   are cheap and side-effect-free, and every abort/force-release path is
   documented above as either fully safe (pre-flip abort) or a deliberate
   last resort (force-release). There's no time pressure that make either
   destructive option "expire" in value the way delaying a production
   outage response would.

## 5. Backup and disaster-recovery posture — the honest version

**There is no built-in backup or snapshot mechanism in this project today.**
This isn't an oversight buried somewhere hard to find — `docs/SPEC.md` §15
("Migration Path to Production") lists "Add backups and restore drills per
shard" as an explicit, still-open, un-struck-through item, distinct from
the other §15 items that have since shipped (secondary indexes, the
split/drain controller) and are marked with strikethrough. `TODOS.md` does
not track it as a planned increment either.

What this means concretely for an operator:

- Cluster state lives entirely in each `ShardDO`/`CatalogDO`'s own SQLite
  storage. Durable Objects storage is itself durable and replicated by
  Cloudflare's infrastructure — this is not "data on a laptop that
  disappears on crash" — but that is infrastructure-level durability
  against hardware failure, **not** an application-level backup/restore
  story. It protects you against Cloudflare losing your data; it does
  nothing for a bad migration, a mistaken `DROP TABLE` via `/v1/sql`, a
  drain that goes wrong, or wanting to restore to a point five minutes ago
  after a bug corrupted rows.
- There is no export/snapshot route, no point-in-time-restore mechanism, and
  no documented restore drill anywhere in this codebase.
- If you need recoverability beyond "Cloudflare's own storage durability,"
  you have to build it yourself today — e.g. periodically exporting rows
  via `/v1/scatter` or `/v1/table-scan` per tenant/table into external
  storage, or scripting your own snapshot process against `/v1/sql` reads.
  Nothing in this repo does that for you.
- Treat any destructive admin operation (`/v1/sql` `DROP`/`DELETE` against
  application tables, an aggressive drain) as unrecoverable via any
  built-in mechanism. Test destructive operations against a throwaway
  cluster first, not production.

If this gap matters for your deployment, it's worth raising as a feature
request rather than assuming it's handled — as of this writing it plainly
is not.

## 6. Cost management

### 6.1 The billing model, plainly

- Durable Objects require the **Workers Paid** plan. This is not optional
  for running this project at all — the free-plan DO request-volume quota
  is irrelevant here, not something to plan around, because Paid is a hard
  prerequisite.
- Everything the cluster does is billed to whichever Cloudflare account
  deployed it: the Worker's own request volume, and every Durable Object's
  requests, duration, and storage — across all three DO classes (`CATALOG`,
  `SHARD`, `COORDINATOR`). Workers Logs (if left at `head_sampling_rate = 1`,
  the shipped default) adds its own cost for high request volumes.
- This is a real, metered resource in your account from the moment it's
  deployed, not a sandboxed trial. Idle cost is low but not zero; load
  costs money in direct proportion to request volume and DO activity.
  Tear it down (`npx wrangler delete --name <worker>`, or
  `examples/shardscope/docs/deploy/teardown.sh`) when a cluster is no longer
  needed.

### 6.2 What's request-heavy — be deliberate about these

- **`/admin/drain-shard`** touches every row on the shard being drained
  (every vbucket migrates, each involving a full backfill copy plus a
  checksum pass reading every row on both source and target, per table),
  plus every index whose ring includes that shard. A drain of a large,
  busy shard is one of the most DO-request-intensive operations this
  system performs — expect it, don't run it casually or repeatedly as a
  routine maintenance habit.
- **`/v1/scatter`** fans out a `SELECT` to *every* shard cluster-wide,
  unconditionally — it's the admin escape hatch for querying without an
  index, and its cost scales with shard count on every single call. Don't
  build a recurring/scheduled job around it without recognizing that cost
  scales with cluster size, not query result size.
- **`/v1/table-scan`** fans out to every shard in one tenant's catalog-shard
  pool (not cluster-wide) — cheaper than scatter, but still N-shard
  requests per page for a single tenant's scan. It has a built-in per-
  tenant rate limiter (§7) precisely because it's a fan-out read a tenant
  could otherwise hammer.
- **`/admin/create-index`** does a single-pass backfill across every
  existing row on every shard for the indexed table (§7 — explicitly not
  chunked, "pre-product-scale simplification"). Creating an index on a
  large, already-populated table is a one-time but potentially large
  request/duration cost.
- **`/admin/backfill-provenance`** (full-cluster, `catalogShardId` omitted)
  scans every shard's rows to attribute ownership — another full-cluster
  read pass, not something to schedule frequently once a table's
  `provenance_complete` is already `1`.
- Migration's checksum step (part of every split/migrate cutover, §11) reads
  every row of every registered table on both the source and target shard,
  in 500-row pages, to compute a comparison hash — factor this into the
  cost of a split, not just the backfill copy itself.

None of these are "wrong" to run — they're the tools this system provides
for exactly these jobs — but each one's cost is proportional to data
volume or shard count, not to how small the underlying operational need
felt when you clicked the button. Budget for that before running one
against a large, live cluster for the first time.
