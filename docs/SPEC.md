# Cloudflare Durable Object Sharded SQL: Concrete Spec (MVP + v1)

## 1) Goals

- Provide one logical SQL endpoint to application developers.
- Automatically route I/O to many Durable Object SQLite shards.
- Keep strict ACID within a shard.
- Allow online scale-out before any shard reaches 10 GB.
- Keep control-plane metadata strongly consistent via a single catalog Durable Object.

## 2) Non-goals (MVP)

- Full ANSI SQL parser/planner.
- Transparent cross-shard joins.
- Zero-downtime live data movement in first cut.

Cross-shard transactional writes (2PC) were originally scoped out of the MVP (see prior
revision) but were reclassified as a non-negotiable day-one requirement, alongside global
secondary indexes, per the founder's explicit decision during product review — see
Milestone 1/2 in the `feature/next-stage` design doc. Section 10 reflects this.

## 3) Components

- Gateway Worker
  - Public endpoint.
  - AuthN/AuthZ hook.
  - Request validation.
  - Routes to catalog and shard DOs.

- CatalogDO (control plane)
  - Owns table registry.
  - Owns vBucket to shard map.
  - Owns metadata version.
  - Exposes route and rebalance APIs.

- ShardDO (data plane)
  - Owns one SQLite database.
  - Executes SQL statements serially.
  - Maintains idempotency table for at-least-once retries.

## 4) Logical Data Partitioning

- Partition function input:
  - tenantId
  - table
  - partitionKey

- Hashing:
  - FNV-1a 32-bit on composite key tenantId:table:partitionKey.

- Virtual partitions:
  - totalVBuckets default 1024 (configurable).
  - vbucket = hash(composite) mod totalVBuckets.

- Mapping:
  - Catalog maps vbucket -> shardId.
  - Rebalance changes only mapping entries, not hash function.

## 5) Catalog Schema

Table: cluster_config
- singleton INTEGER PRIMARY KEY CHECK(singleton = 1)
- total_vbuckets INTEGER NOT NULL
- metadata_version INTEGER NOT NULL DEFAULT 1
- initialized_at TEXT NOT NULL

Table: shards
- shard_id TEXT PRIMARY KEY
- status TEXT NOT NULL (active, draining)
- created_at TEXT NOT NULL

Table: vbucket_map
- vbucket INTEGER PRIMARY KEY
- shard_id TEXT NOT NULL
- map_version INTEGER NOT NULL
- updated_at TEXT NOT NULL
- migration_status TEXT NOT NULL DEFAULT 'none' (none | backfilling | cutover — Milestone 3)
- target_shard_id TEXT (the in-flight migration's destination — Milestone 3)
- migration_rows_copied INTEGER NOT NULL DEFAULT 0 (Milestone 3)
- migration_started_at TEXT (Milestone 3)

Table: table_rules
- table_name TEXT PRIMARY KEY
- partitioning TEXT NOT NULL (hash for MVP)
- created_at TEXT NOT NULL
- partition_key_column TEXT NOT NULL (default sentinel `__unset__` for pre-upgrade rows)
- schema_sql TEXT (the CREATE TABLE statement captured at /admin/create-table — Milestone 3
  migration backfill applies it to a target shard created after the original fan-out,
  e.g. a split target; NULL for tables registered before this column existed. PR review
  round 11: can only ever be non-NULL alongside a probe-verified `partition_key_unique = 1`
  via `/admin/create-table`'s own atomic push-then-verify flow — see that column below and
  §7's `/admin/register-table`/`/admin/set-partition-key-column` entries for the invariant
  and its documented trade-off)
- provenance_complete INTEGER NOT NULL DEFAULT 0 (Milestone 4: 1 once a full-cluster
  /admin/backfill-provenance run has reported zero orphaned/ambiguous rows for this table;
  never reset to 0 automatically. PR review round 11: `/admin/create-table` no longer
  auto-sets this to 1 at creation — its DDL is `CREATE TABLE IF NOT EXISTS`, which silently
  no-ops if the table name already physically existed with legacy rows predating row
  provenance, so auto-certifying complete regardless would hide that collision behind a
  false `provenance.complete: true`. A newly created table now starts at 0 like any other
  table and earns certification through the normal `/admin/backfill-provenance` mechanism
  (trivially, for a genuinely brand-new/empty table — nothing to find). Read by
  POST /v1/table-scan's `provenance.complete` response field.)
- partition_key_unique INTEGER NOT NULL DEFAULT 0 (Milestone 4: 1 iff `partition_key_column`
  is verified backed by a real `UNIQUE` constraint, sole `PRIMARY KEY`, or a non-partial
  UNIQUE index — merely being part of a composite unique key, or "unique" only via a
  partial/`WHERE`-conditioned index, does not qualify. Computed automatically, never
  client-supplied, at /admin/create-table, /admin/set-partition-key-column, and
  /admin/register-table time via live `PRAGMA table_info`/`index_list`/`index_info`/
  `sqlite_master` introspection against a representative shard (`checkPartitionKeyUnique` in
  index.ts); fails closed to 0 on any introspection error or ambiguity. Gates
  POST /v1/table-scan: a table where this isn't 1 is rejected 409 `PARTITION_KEY_NOT_UNIQUE`,
  since `/tenant-scan-page`'s join keys purely by partition-key value against
  `__cf_row_owners` — a non-unique column would let two tenants' rows share a value and the
  join attribute both to whichever tenant currently owns that key, a cross-tenant read leak.)

Table: index_rules (Milestone 2, extended in Milestone 3)
- index_name TEXT PRIMARY KEY
- table_name TEXT NOT NULL
- columns_json TEXT NOT NULL
- status TEXT NOT NULL DEFAULT 'building' (building | ready)
- created_at TEXT NOT NULL
- placement_ring_json TEXT NOT NULL DEFAULT '[]' (Milestone 3: the ordered shard-id array
  active at /admin/create-index time, pinned for the index's lifetime — `indexShardIdForKey`
  hashes over THIS ring, never the live shard set, so splits/drains can't reshuffle
  existing entries; drain substitutes a ring member deterministically instead)

## 6) Shard Schema

Table: applied_requests
- request_id TEXT PRIMARY KEY
- request_hash TEXT NOT NULL DEFAULT ''
- result_json TEXT NOT NULL
- applied_at TEXT NOT NULL

Table: __cf_row_owners (Milestone 3, Chunk 0 — row provenance)
- table_name    TEXT NOT NULL
- partition_key TEXT NOT NULL
- tenant_id     TEXT NOT NULL
- vbucket       INTEGER NOT NULL
- updated_at    TEXT NOT NULL
- PRIMARY KEY (table_name, partition_key)

Written transactionally with every write path (`/v1/sql` mutation, `/v1/mutate`, `/v1/tx`
commit-apply); deletes remove the row's provenance. This is the only place a stored row's
logical `(tenantId, vbucket)` identity exists — `vbucket = hash(tenantId:table:partitionKey)`
is unrecoverable from the row itself (§14 trust model). Migration export, the cutover
checksum, and index backfill all select rows through it. Known limitation, inherited not
widened: the PK mirrors the base tables' own physical layout, which already collides if two
tenants use the same partition key on the same shard (§14). Rows written before this table
existed are re-attributed by `/admin/backfill-provenance` (mechanical: candidate tenants ×
hash → exactly one match writes provenance; multiple → reported `ambiguous` for
`/admin/set-row-owner`; zero → reported `orphaned`).

Indexed (Milestone 4) `(tenant_id, table_name, partition_key)` — `idx_cf_row_owners_tenant_scan`
— so `POST /tenant-scan-page` (below) is a range scan, not a full table scan, of this table.

`POST /tenant-scan-page` (Milestone 4, internal — ShardDO route driven by the Worker's
`POST /v1/table-scan`): `{table, partitionKeyColumn, tenantId, afterPartitionKey, limit}` ->
`{rows: [{partitionKey, row}], ownerRowsScanned, lastOwnerKeyScanned?}`. Joins the base table
against `__cf_row_owners` filtered by `tenant_id` + `table_name`, paged by
`partition_key > afterPartitionKey` (mirrors `/migrate-export`'s join pattern, filtered by
tenant instead of vbucket). `limit` is clamped to `min(requested, 100)` — the per-shard page
cap, independent of `/v1/table-scan`'s overall response limit (up to 500), which is enforced
by merging across shards. `ownerRowsScanned` (the raw `__cf_row_owners` row count before any
base-row lookups) and `lastOwnerKeyScanned` (that query's last returned partition key) report
this shard's true scan position separately from `rows.length` — a base row deleted between
the owner-row read and the base-table lookup drops that row from `rows` without shrinking
`ownerRowsScanned`, so the Worker's "did this shard's own page get fully consumed" check
can't be corrupted by a skip (round-4 fix; see §7's cursor invariant). A table not physically
present on this shard returns `{rows: [], ownerRowsScanned: 0}` (same established
`/migrate-export` precedent).

Table: __cf_mirror_pending (Milestone 3, Chunk 3 — dual-write retry queue, on the SOURCE shard)
- job_id          INTEGER PRIMARY KEY AUTOINCREMENT
- target_shard_id TEXT NOT NULL
- sql             TEXT NOT NULL
- params_json     TEXT NOT NULL
- request_id      TEXT NOT NULL
- vbucket         INTEGER NOT NULL
- next_attempt_at TEXT NOT NULL
- attempt_count   INTEGER NOT NULL DEFAULT 0
- created_at      TEXT NOT NULL

Retried by the shard's alarm loop: exponential backoff starting 1s, cap 60s, no attempt
limit — cutover gates on the queue reaching zero for the migrating vbucket, so jobs must
eventually land. Mirrored writes reuse the original requestId; the target's
applied_requests dedupe makes mirror + backfill + retry safely re-appliable in any order.

Table: __cf_fenced_vbuckets (Milestone 3, Chunk 4 — cutover write fence)
- vbucket   INTEGER PRIMARY KEY
- fenced_at TEXT NOT NULL

A data write whose payload vbucket appears here is rejected 409 `VBUCKET_FENCED`
(retryable). Enforced at the data, not at routing: a write that resolved its route before
the fence and physically arrives after it is still caught.

Table: __cf_indexes (Milestone 2, extended in Milestone 3)
- gains `tenant_id TEXT NOT NULL DEFAULT ''` (Milestone 3, Chunk 2): the logical identity
  hydration re-routes through at read time (`vbucket = hash(tenant_id:table:partition_key)`
  → vbucket_map → current shard), so moved base rows are always found. The old
  `source_shard_id` column stays physically (additive-migration convention) but is unread.

Application tables are created by tenant SQL statements routed to target shards.

## 7) Public HTTP API (Gateway Worker)

POST /admin/init
Request:
- numShards number (default 8, clamped to 1-256)
- totalVBuckets number (default 1024, clamped to 64-65536)
- force boolean (optional)

Response:
- ok boolean
- numShards
- totalVBuckets

POST /admin/register-table
Request:
- table string
- partitioning string (optional, default hash)
- partitionKeyColumn string (required — the column holding each row's partition key)

Response:
- ok
- table
- metadataVersion

This route is metadata-only (it doesn't create anything on any shard), but it still computes
and caches `table_rules.partition_key_unique` against one active shard's existing schema for
`table`, exactly as `/admin/create-table` does (see §5) — falling back to unverified (0) if no
shards exist yet or listing them fails, since registration itself must succeed even when
nothing can be verified. Any `partitionKeyUnique` present in the request body is always
ignored (Codex P1 fix: accepting a client-supplied value would let a caller lie about
uniqueness and bypass the check `POST /v1/table-scan` relies on).

**Rejects repointing an already-configured partition key column (PR review round 7).** The
same rule `/admin/set-partition-key-column` enforces (see below) applies here too: if `table`
already has a real (non-`'__unset__'`) `partition_key_column` in `table_rules` and the
request's `partitionKeyColumn` names a *different* column, the call is rejected with 409
`PARTITION_KEY_ALREADY_SET` and `table_rules` is left untouched. Re-registering with the
*same* `partitionKeyColumn` value remains allowed (idempotent re-registration, e.g. a
metadata-only re-sync), as does registering a brand-new table or upgrading one still carrying
the `'__unset__'` sentinel. Before this fix, `/admin/register-table`'s `INSERT OR REPLACE`
took `partitionKeyColumn` unconditionally, silently repointing the column through a route
round 6's guard didn't cover — the same stale-`__cf_row_owners`-provenance / cross-tenant leak
described below, just reachable through a second endpoint.

**`schema_sql` can only ever pair with a probe-verified `partition_key_unique = 1` via
`/admin/create-table` (PR review round 11 fundamental fix, replacing rounds 8-10's escalating
guard patches).** `table_rules.schema_sql` is exactly what a future split/migration backfill
executes verbatim to provision this table on a freshly-created target shard (see
`/admin/create-table` below). It can only ever be trustworthy alongside `partition_key_unique
= 1` when the two were established TOGETHER, atomically, by `/admin/create-table`'s own
push-then-verify flow — it pushes this exact DDL to every shard, THEN verifies uniqueness
against a shard that just received it, so the two are structurally guaranteed to correspond.
`/admin/register-table`'s own live probe (`checkPartitionKeyUnique`, run against whatever
ALREADY physically exists on a representative shard right now) is structurally disconnected
from whatever `schemaSql` text this or an earlier call submitted for storage, so it can never
make the same guarantee. Earlier rounds tried to patch this gap with reject/preserve guards
compared against the previously-stored value (round 8's `SCHEMA_SQL_ALREADY_VERIFIED`
rejection, round 9's preserve-on-omission fallback, round 10's skip-the-probe-when-schemaSql-
present rule) — each closed one call sequence while leaving another reachable (a second,
schemaSql-omitting call could still verify `partition_key_unique = true` while an earlier
call's stored `schema_sql` sat there unconfirmed; a client-supplied empty-string `schemaSql`
slipped past the "is schemaSql present" check while still landing as a stored non-NULL
garbage value). The round-11 fix is a simpler invariant instead of more comparisons: whenever
`/admin/register-table`'s OWN probe verifies `partition_key_unique = 1` in a given call (which
only happens when `schemaSql` is omitted or empty in that same call — a real `schemaSql`
always skips the probe and demotes `partition_key_unique` to 0, per round 10's original,
still-correct behavior), `schema_sql` is explicitly NULLed in that same write, regardless of
what was previously stored — this route's live-state check can never vouch for arbitrary
stored text. `/admin/set-partition-key-column` (below) applies the identical rule for its own
probe. Submitting a real `schemaSql` in any `/admin/register-table` call always stores it as
submitted and always demotes `partition_key_unique` to 0 in the same write — there is no
longer a 409 rejection for "changing schema_sql on an already-verified table," since the
demotion itself means there is never a stale-pairing risk to guard against.

**Trade-off.** A table verified via `/admin/register-table`'s or `/admin/set-partition-key-
column`'s own live probe (not `/admin/create-table`) can be table-scan-eligible
(`partition_key_unique = 1`) but ends up with `schema_sql = NULL` — a future split/migration
backfill can't auto-provision that table on a new target shard from stored DDL; the table
must already exist there some other way, or an operator applies the schema manually.

POST /admin/create-table
Request:
- table string
- schema string (must be a `CREATE TABLE` statement whose table name matches `table`)
- partitionKeyColumn string (required — validated via `PRAGMA table_info` against the created schema; the table is dropped from every shard and the call fails 400 if the column doesn't exist. The rollback also clears the create-table idempotency-cache entry on each shard via `/invalidate-request`, so a retry with a corrected `partitionKeyColumn` genuinely re-creates the table rather than replaying a stale cached "success" for a table that no longer exists. The rollback's own `DROP TABLE` requestId is unique per attempt, not a stable per-table key — otherwise a second failed retry for the same table would replay the *first* rollback's cached success and leave the just-recreated table behind despite returning 400)

Response:
- ok
- table

Also computes and caches `table_rules.partition_key_unique` (§5) from the schema just
created, checked against the same representative shard as the column-exists validation above
— this is what gates whether `POST /v1/table-scan` will later accept this table (409
`PARTITION_KEY_NOT_UNIQUE` if not verified unique). This is the ONE route whose
`partition_key_unique = 1` is always paired with a real, non-NULL `schema_sql` — see the
round-11 invariant above.

**No longer auto-certifies `provenance_complete` (PR review round 11, P2 fix).** Previously
set `provenanceComplete: true` unconditionally when registering the table it just created —
wrong when `table` already physically existed (its DDL is `CREATE TABLE IF NOT EXISTS`, which
silently no-ops against a pre-existing table with legacy rows predating row-provenance
tracking), since it would certify `provenance_complete = 1` regardless, hiding those legacy
rows' absence from `__cf_row_owners` behind a false `provenance.complete: true` on
`POST /v1/table-scan`. A newly created table now starts at `provenance_complete = 0` like any
other table (see §5); a genuinely brand-new (empty) table's first `POST /admin/backfill-
provenance` run trivially finds zero orphaned/ambiguous rows and certifies it complete through
the normal mechanism — same end state as before for the legitimate case, one extra (cheap,
near-no-op) admin call, with no false-positive risk for a colliding table name.

POST /admin/set-partition-key-column (ADMIN_TOKEN)
Request:
- table string
- partitionKeyColumn string (validated via `PRAGMA table_info` against a live shard's schema)

Response:
- ok
- table
- partitionKeyColumn

Also nulls out any previously-stored `schema_sql` in the same `UPDATE` whenever this call's own
probe verifies `partition_key_unique = 1` (PR review round 11) — this route's live-state check
can't guarantee whatever `schema_sql` happens to already be on file corresponds to the column it
just verified, so it must not be left in place. Left untouched when unverified (0).

Upgrades a table still carrying the `'__unset__'` sentinel (registered before `partitionKeyColumn` was mandatory, including anything live from `v1.0.0.0`) — such tables are otherwise rejected from `/v1/mutate` and coordinated transactions with a 409. Also recomputes `table_rules.partition_key_unique` (§5) for the newly-set column.

**One-time upgrade only (PR review round 6).** This route is strictly a one-time `'__unset__'` → real-column upgrade, never a general "repoint an already-configured table's partition key column" operation. If `table_rules.partition_key_column` is already a real (non-`'__unset__'`) value for `table`, the call is rejected outright with 409 `PARTITION_KEY_ALREADY_SET` and `table_rules` is left untouched — it does not matter whether the requested `partitionKeyColumn` matches the current one or names something different. There is no legitimate use case for repointing an already-working column: any existing `__cf_row_owners` entries for the table were written keyed by the OLD column's values, and `POST /v1/table-scan` enumerates those entries but looks up base rows via `WHERE "<column>" = ?` against whatever column is *currently* configured — repointing to a different column would let a stale, now-mismatched `partition_key` value resolve to an unrelated row under the new column, a cross-tenant data leak. Rather than attempting to safely migrate or clear provenance for a repoint, the endpoint refuses the operation entirely.

POST /admin/create-index (ADMIN_TOKEN) — Milestone 2, Chunk 1
Request:
- indexName string
- table string (must already be registered with an upgraded `partitionKeyColumn`)
- columns array of string (composite-capable — validates against the table's actual schema via `PRAGMA table_info`)

Response:
- ok
- indexName
- table
- columns

Registers a secondary index and backfills it against every existing row on every shard (single-pass, not chunked — see the Milestone 2 design doc's stated pre-product-scale simplification). Index entries live in each shard's internal `__cf_indexes` table, placed on a shard chosen by hashing `(table, indexName, indexKeyJson)` — independent of the base row's own shard, so a future index-query lookup resolves on one shard instead of scattering. Once any index is registered on a table, raw `/v1/sql` mutations against that table are rejected 409 (`TABLE_HAS_INDEX`) — raw SQL bypasses every index-maintenance mechanism, so it's blocked outright rather than silently desyncing the index; use `/v1/mutate` or `/v1/tx` instead. `/v1/mutate`'s async maintenance is live as of Chunk 2 below; `/v1/tx`'s 2PC-piggyback maintenance is live as of Chunk 3.

The index is registered in `CatalogDO` *before* the backfill scan runs, not after — a row written between the two would otherwise be missed by the scan and never trigger async maintenance either, leaving it permanently unindexed. Registering first means any concurrent write during backfill is already covered by the live async/2PC maintenance path; backfill's own writes are redundant-but-idempotent (`INSERT OR REPLACE`) in that case. To avoid backfill clobbering a concurrent write's fresher data with the value it captured in its initial bulk scan, backfill re-reads each row immediately before writing its index entry rather than trusting the scanned snapshot — this narrows the staleness window to a single read-then-write round trip, the same order of hazard the async maintenance path already accepts elsewhere, not a new or larger one.

**`building` / `ready` status (eng-review fix).** `index_rules.status` starts `'building'` the moment `CatalogDO` registers the index (before backfill runs) and flips to `'ready'` only once the Worker's backfill loop has fully completed on every shard (`CatalogDO./mark-index-ready`, the last step of `/admin/create-index`). Write-path maintenance (`/v1/mutate` async, `/v1/tx` piggyback) is **not** gated on status — it's live from the moment of registration, which is what makes registering before backfill correct. Only the read path is gated: `/lookup-index` (and therefore `/v1/index-query`) rejects a `'building'` index with 425 `INDEX_BUILDING`, rather than silently returning partial results for rows backfill hasn't reached yet. If backfill fails partway, the index stays `'building'` forever until a retried `/admin/create-index` call (idempotent, per above) succeeds all the way through.

**Index placement is pinned, and both former topology blocks are removed (Milestone 3, Chunk 2 — supersedes the Milestone 2 eng-review mitigation).** Milestone 2 shipped `indexShardIdForKey` hashing over the globally *active* shard set, which made any shard-count change silently reshuffle index placement — mitigated then by blocking both topology operations 409 (`SHARD_DRAIN_BLOCKED_BY_INDEXES` / `SPLIT_BLOCKED_BY_INDEXES`) while any index existed. Milestone 3 resolves the underlying topology question instead: each index's placement ring (`index_rules.placement_ring_json`) is captured once at `/admin/create-index` from the then-active shard set and pinned for the index's lifetime; every placement computation (write maintenance, `/v1/index-query`, backfill) hashes over that ring, never the live set. `__cf_indexes` entries additionally carry `tenant_id`, and hydration re-routes per entry at read time (`vbucket = hash(tenant_id:table:partition_key)` → `vbucket_map` → current shard) instead of following the stale physical `source_shard_id` snapshot. Splits therefore never affect index placement; drains substitute a ring member deterministically (Section 11). Both 409 blocks are gone.

**Non-unique only (Milestone 2, Chunk 7 — decided, not a gap).** Every registered index is non-unique: multiple base rows may share the same indexed-column value(s), and `__cf_indexes` writes use `INSERT OR REPLACE` with no constraint check. Unique-index support (rejecting a write that would violate uniqueness, which needs either a real `UNIQUE` constraint at the index-shard level or explicit pre-check-plus-lock coordination to close the race between two concurrent writes both claiming to be first) was scoped out of Milestone 2 entirely — no chunk in the plan allocated space to build it, consistent with this milestone's own founding premise (Chunk 1's Demand Evidence: no validated need exists yet for even the general-purpose composite-index scope this milestone already ships ahead of demand). Tracked in `TODOS.md` as a future increment if real usage ever calls for it.

POST /admin/list-indexes (ADMIN_TOKEN) — Milestone 2, Chunk 1
Response:
- indexes: array of `{indexName, table, columns, status, createdAt, placementRing}` (`status` is `'building'` or `'ready'` — see below; `placementRing` is the pinned shard-id array, Milestone 3)

POST /admin/drop-index (ADMIN_TOKEN) — Milestone 2, Chunk 6
Request:
- indexName string

Response:
- ok
- indexName
- warning string (only present if physical cleanup failed on one or more shards — the index is still unregistered and unqueryable either way)

Unregisters the index in `CatalogDO` (fanned to every catalog shard) *before* fanning out physical `__cf_indexes` row cleanup — so `/v1/index-query` and raw `/v1/sql` mutations against the table both see the index as gone (404 / no longer blocked, respectively) immediately, even while physical cleanup is still in flight across shards. A write already in progress when the drop runs may still land one last `__cf_indexes` row after cleanup passes over it — a known, accepted eventual-consistency window for this rare admin operation, not a linearizability guarantee this milestone makes.

`/v1/mutate` async index maintenance — Milestone 2, Chunk 2 (no new route; extends the existing `/v1/mutate` handler)
When the target table has any registered index, `handleV1Mutate` computes the resulting `__cf_indexes` deltas and dispatches them via the Worker's own `ctx.waitUntil()` — after the base row's write has already succeeded and the response is on its way back to the caller, so this never adds latency to the base write. For `update`/`delete`/`upsert` (which may hit the `ON CONFLICT UPDATE` path), the row's prior indexed-column values are read once before the mutation runs, so removing a now-stale index entry doesn't need a second read-after-write — whatever column wasn't in the caller's `values` is taken from that pre-read. If the best-effort write to the computed index shard fails, it's recorded via `ShardDO./enqueue-index-job` on the *base* shard (not the index shard, which may be the one that's unreachable) and retried by that shard's own `alarm()` with exponential backoff (`index_pending_jobs` table) — the same recovery-queue pattern `CoordinatorDO` already uses for 2PC. A write to a table with no registered index pays none of this cost. **Eng-review fix:** maintenance only dispatches if the base write's own `rowsAffected > 0` — a `StructuredMutation`'s optional `where` can narrow `update`/`delete` beyond the `partitionKey`, and a `where` that matches nothing must not still delete/rewrite the row's index entry based on a beforeRow snapshot that describes a row nothing actually touched.

`/v1/tx` index piggyback — Milestone 2, Chunk 3 (no new route; extends the existing `/v1/tx` handler)
When a mutation's table carries any registered index, its `__cf_indexes` deltas (same `computeIndexDeltas` logic Chunk 2 uses, so both write paths agree on what "the index changed" means) are added to the *same* 2PC transaction as extra participants — `CoordinatorDO./begin` sees them as ordinary intents with a synthetic lock key (`table` = `"__cf_indexes:" + indexName`, `partitionKey` = the index key JSON), so the base row and every index entry it affects commit or abort atomically, with no separate consistency model to reason about. These synthetic index-participant keys do **not** count against `MAX_TX_PARTICIPANT_KEYS` — that cap bounds what the caller asked to touch; index maintenance is bookkeeping this system adds on the caller's behalf, not additional caller-requested scope. A transaction that worked before a table had any index keeps working once one is added.

**Zero-row `update`/`delete` fix (eng-review).** Unlike `/v1/mutate`, `/v1/tx`'s index-delta participants must be decided *before* `/begin` runs — before the base mutation has executed at all, so there's no `rowsAffected` to gate on the way Chunk 2's path does. Instead, the pre-read that produces `beforeRow` now filters by the exact same predicate `compileMutation` will use (`mutationWhereClause`, shared by both) — partition key AND any caller-supplied `where` — so a `where` that won't match correctly comes back with `beforeRow = null` instead of a real row's stale snapshot. `computeIndexDeltas` treats a null `beforeRow` on `update` as a hard no-op (no delta at all): without this, a null `beforeRow` is ambiguous between "insert, no prior row can exist yet" (where a new entry should legitimately be written from `afterValues`) and "update whose predicate matched nothing" (where nothing happened and nothing should be written) — conflating the two would synthesize a phantom `__cf_indexes` entry for a base-row change that never occurred. A phantom entry is asymmetric with the original bug it replaces: `/v1/index-query`'s staleness re-check (below) always filters it back out at read time, so it can never cause a wrong-answer or a live row to vanish — at most it's a harmless orphan row, the same class of residual accepted for the backfill-vs-concurrent-write race above. `delete` was never at risk of this: its `newKeyJson` is unconditionally `null` regardless of `beforeRow`.

**Indexed columns must be explicit on insert/upsert (eng-review fix, Codex-found).** An insert/upsert that omits an indexed column and relies on a SQL `DEFAULT` would otherwise get indexed as `null` instead of whatever SQLite actually assigns — `computeIndexDeltas` has no way to know the real stored value without reading the row back, and reading back after the write isn't an option for `/v1/tx` (the index delta is a 2PC participant decided *before* `/begin`, so there is no "after the write" moment to read from without breaking the atomicity guarantee the piggyback exists to provide). Both `/v1/mutate` and `/v1/tx` therefore reject an insert/upsert on an indexed table 400 `INDEXED_COLUMN_REQUIRES_VALUE` if any indexed column is missing from `values` — turning a silent wrong-index into an explicit, immediate error, and keeping both write paths behaving identically rather than one silently working around it and the other not.

**Same-row multi-mutation tracking within one `/v1/tx` call (eng-review fix, Codex-found).** Each mutation's `beforeRow` pre-read hits the real database, which only reflects what's already committed — never what an *earlier mutation in the same batch* is about to do, since `/begin` hasn't run yet when these pre-reads happen. Without accounting for this, two mutations on the same row in one `/v1/tx` call (e.g. insert `v='a'` then update `v='b'`) could compute the second mutation's delta against a stale or nonexistent prior state, silently losing the index entry for the row's actual final value. `handleV1Tx` now tracks a simulated per-row state (`Map` keyed the same way as `rowKey()`/`participantKey()`) seeded from the first real pre-read for each row and updated in place as each mutation in the batch is processed — mirroring how SQLite itself sees each statement's effects within the same transaction (later statements observe earlier ones' writes). A later mutation's `where` is matched against the simulated state the same way the database would evaluate it against the real row once `/begin` actually runs the batch in order.

Benchmark — Milestone 2, Chunk 7 (finalizes the design doc's Success Criterion 2)
`src/index.test.ts`'s "benchmark" describe block measures p50 latency for indexed vs. unindexed `/v1/mutate` inserts and asserts the regression stays under a bar combining an absolute floor (25ms, absorbing test-environment timing noise on an already-fast baseline) and a percentage cap (10%). It also asserts zero rows ever land in `index_pending_jobs` across a clean run — the "repair-debt backlog" half of the criterion — since a nonzero count under non-failure conditions would mean the async dispatch itself is silently degrading, not just slow. Because index maintenance is dispatched via `ctx.waitUntil()` *after* the response is already prepared (Chunk 2), the expected result is that indexed writes are indistinguishable from unindexed ones on the caller-observed path; this benchmark exists to catch a regression if that property is ever accidentally broken (e.g. an future change that awaits the index write before responding), not because a real difference is expected today.

Benchmark — Milestone 3, Chunk 6 (observational, no pass/fail thresholds)
`src/index.test.ts`'s migration-benchmark test migrates a vbucket carrying a few hundred rows while issuing sustained writes against it and records: backfill throughput (rowsCopied / migration wall time), the cutover fence-window duration (first `VBUCKET_FENCED` rejection to first post-flip success, 0 if no write happened to race the fence), and the peak `__cf_mirror_pending` depth sampled per orchestration tick. Deliberately observational — the numbers are workload- and environment-dependent, so the test only sanity-asserts they're well-formed (migration completed, all rows landed) rather than gating on absolute values; run vitest with `--silent=false` to see the recorded numbers.

POST /admin/split-vbucket (ADMIN_TOKEN)
Request:
- catalogShardId string (vbucket numbering is local to a catalog shard)
- vbucket number
- newShardId string (optional — default `{catalogShardId}-shard-split-{Date.now()}`)

Response:
- ok
- vbucket
- fromShard
- toShard
- metadataVersion
- migrationStarted boolean (Milestone 3: split now creates the target shard and starts a
  real data migration; routing flips only when the fenced cutover completes)

Milestone 3 topology routes (all ADMIN_TOKEN-gated, standard `{error:{code,message,fix}}` shape):

POST /admin/migrate-vbucket        {catalogShardId, vbucket, targetShardId?}
  targetShardId omitted -> default `{catalogShardId}-shard-split-{Date.now()}`
  -> 200 {ok, vbucket, fromShard, toShard, status:"backfilling"}
  -> 409 {error:{code:"VBUCKET_PROVENANCE_INCOMPLETE", unattributedRows:N, ...}}
  -> 409 {error:{code:"MIGRATION_IN_PROGRESS", ...}}   (one migration per vbucket)

POST /admin/migrate-vbucket-status {catalogShardId, vbucket}
  -> 200 {vbucket, status, fromShard, toShard, rowsCopied, mirrorQueueDepth, startedAt}

POST /admin/migrate-vbucket-abort  {catalogShardId, vbucket}
  -> 200 {ok, vbucket, status:"aborted"}    (only before the map flip; after it ->
     409 MIGRATION_ALREADY_COMMITTED — reverse a committed migration by migrating back)

POST /admin/backfill-provenance    {catalogShardId?}            (omitted = all catalog shards)
  -> 200 {attributed:N, ambiguous:[{catalogShardId,shardId,table,partitionKey,candidateTenants}], orphaned:[...]}
  (Milestone 4: only a full-cluster run — catalogShardId omitted — can flip a table's
  `table_rules.provenance_complete` to 1, and only for tables it scanned with zero
  orphaned/ambiguous rows across every catalog shard processed. A scoped single-catalog-shard
  run never flips it, since it only ever sees that one catalog shard's own shard pool.)

POST /admin/set-row-owner          {catalogShardId, shardId, table, partitionKey, tenantId}
  -> 200 {ok}  / 409 ROW_OWNER_SHARD_MISMATCH if the claimed tenant does not hash to a
     vbucket on that shard

POST /admin/drain-shard            {catalogShardId, shardId}
  -> 200 {ok, shardId, metadataVersion, evacuationStarted:true}
  -> 409 {error:{code:"RING_EVACUATION_NO_CANDIDATE", ...}}  (pre-checked before any
     durable state change)
  (existing 409s SHARD_HAS_IN_FLIGHT_TRANSACTIONS / SHARD_HAS_PENDING_INDEX_JOBS still
   apply, checked Worker-side first)

POST /admin/drain-shard-status     {catalogShardId, shardId}
  -> 200 {shardId, vbucketsRemaining, ringsRemaining, status}
     status: active | migrating-vbuckets | evacuating-rings | complete

POST /admin/register-tenant (ADMIN_TOKEN)
Request:
- tenantId string
- rotate boolean (optional — issues a new token for an already-registered tenant, invalidating the old one immediately)

Response:
- ok
- tenantId
- token string (plaintext, returned exactly once)

POST /admin/revoke-tenant (ADMIN_TOKEN)
Request:
- tenantId string

Response:
- ok
- tenantId
- revoked boolean

POST /v1/sql (ADMIN_TOKEN — admin-only, operator/debugging escape hatch)
Request:
- sql string
- params array (optional)
- table string
- tenantId string (routing/hashing only — NOT authenticated against the caller; the caller is the operator)
- partitionKey string (required for all writes and single-shard reads)
- requestId string (optional)

As of Milestone 3, /v1/sql is admin-only (reads AND writes) — see §14. The
per-tenant SQL guard was structurally unwinnable and a raw partition-scoped
SELECT leaked cross-tenant data (base rows carry no physical tenant_id), so the
trust-based tenant path was removed entirely; tenants use /v1/mutate + /v1/tx
(writes) and /v1/index-query (reads). A light guardrail remains even for the
operator: a mutation whose write TARGET is an internal bookkeeping table
(applied_requests / pending_intents / row_locks / __cf_* / sqlite_*) is rejected
403, so a fat-fingered query can't corrupt fence/provenance/mirror state;
internal-table READS and cross-table access are allowed (admin is trusted).

Response:
- route { shardId, vbucket, metadataVersion }
- requestId
- result (query or mutation payload)

POST /v1/mutate (tenant bearer token)
Request (a single StructuredMutation — see structured-op.ts):
- op string ("insert" | "update" | "delete" | "upsert")
- table string
- tenantId string
- partitionKey string
- where object (optional — additional predicates; never substitutes for the partition-key predicate, which is always injected)
- values object (optional — required for insert/upsert; the partition-key column is always force-set here regardless of what's supplied)
- conflictColumns array (optional, upsert only — defaults to [partitionKeyColumn])
- requestId string (optional — shares /v1/sql's requestId-based idempotency contract)

Response:
- ok boolean
- rowsAffected number

Row-ownership is structural, not conditional: `compileMutation` always ANDs the partition-key predicate into `update`/`delete`'s `WHERE` clause and force-sets it in `insert`/`upsert`'s `values` — an absent `where` still only ever touches the one partitioned row/set, never the whole table. Rejected 409 (`PARTITION_KEY_COLUMN_UNSET`) against a table still carrying the `'__unset__'` sentinel.

POST /v1/tx (tenant bearer token)
Request:
- mutations array of StructuredMutation (see /v1/mutate above), 1 or more, bounded to 8 distinct (tenantId, table, partitionKey) rows
- requestId string (required — the transaction's idempotency key; a retry with the same requestId returns the prior outcome rather than re-running 2PC)

Response (200, committed):
- ok: true
- txId string
- status: "committed" (or "committed_pending_ack" if a commit acknowledgement from one or more shards is still outstanding and queued for alarm-driven retry — the transaction is durably committed either way, only the ack is pending)

Response (409, aborted):
- error { code: "TX_ABORTED", message, details } — prepare failed on at least one participant shard; every participant was rolled back (or had nothing to roll back, given /prepare's shadow-write design)

Drives `CoordinatorDO`'s two-phase commit across every shard touched by the mutation set: each mutation is individually routed and validated (so a cross-tenant mutation in the same batch 401s the same way a lone `/v1/mutate` call would, with no separate check needed), grouped by shardId, then `/begin` fans out `/prepare` to all participants, aborts everyone on any failure, or fans out `/commit` on universal success.

POST /admin/tx-status (ADMIN_TOKEN)
Request:
- txId string

Response:
- found boolean
- status string (if found — preparing/prepared/committing/committed/aborted)

POST /admin/tx-force-abort (ADMIN_TOKEN)
Request:
- txId string

Response:
- ok boolean
- txId
- status: "aborted"

Manual escape hatch for a transaction stuck past a reasonable window (visible via `/admin/tx-status`) — aborts every participant shard and marks the transaction aborted. Rejects 409 if the transaction already committed.

POST /v1/index-query (tenant bearer token) — Milestone 2, Chunk 4
Request:
- table string
- indexName string
- tenantId string
- values object (a value for every column the index covers — exact full-tuple lookups only, leftmost-prefix not yet supported)
- limit number (optional, default 20, capped at 100)

Response:
- rows: array of the matching base rows (full row data, not just partition keys)

The first tenant-facing, non-partition-key query path this platform has — resolves in three hops (`CatalogDO` validates the tenant token and the index's columns, the computed index shard resolves matching `(partitionKey, sourceShardId)` pairs, each match's base row is read from its own shard), never `/v1/scatter`'s admin-only full-cluster fan-out. Because `/v1/mutate`'s index maintenance (Chunk 2) is async, a matched entry can be stale by the time it's read; the base row is re-verified against the queried tuple before being returned, so a stale delete/update is silently excluded — never surfaced as a wrong result. Rejects 425 `INDEX_BUILDING` if the index hasn't finished its initial backfill yet (see the `building`/`ready` status note in the `/admin/create-index` section above). `/v1/scatter` remains the admin-only fallback for querying by a column that has no registered index; the two coexist rather than one deprecating the other.

**Paging past stale entries (eng-review fix).** `limit` no longer bounds the raw `__cf_indexes` scan directly — it used to apply `LIMIT` before the staleness re-check ran, so a run of stale entries sorted first could starve out live matches that exist further down the index, silently under-filling or emptying a result even though enough live rows exist. Raw entries are now paged (ordered by `partition_key` for a stable cursor) and re-verified batch by batch until `limit` verified rows are collected or the index is exhausted, bounded by a `limit * 5` raw-scan cap so a pathologically stale index (e.g. after a delete burst whose async cleanup hasn't caught up) can't make one query scan unboundedly. Within each batch, every match's hydrate read is dispatched concurrently (`Promise.all`) rather than one round trip at a time — each match is an independent read (different partition keys, potentially different shards), and `Promise.all` preserves the batch's `partition_key` order so the result stays deterministic across repeated calls.

POST /v1/table-scan (tenant bearer token) — Milestone 4
Request:
- tenantId string
- table string
- limit number (optional, default 100, hard max 500)
- cursor string (optional, opaque — from a prior response's `nextCursor`)

Response (200):
- rows: array of the tenant's own matching base rows (full row data)
- nextCursor string (present iff any shard in the pool may have more rows; omitted when exhausted)
- provenance: `{complete: boolean, fix?: string}` (`fix` present only when `complete` is `false`)
- scan: `{catalogShardId, shardCount, successCount, scanMs}`

Errors: 400 `MISSING_FIELDS` / `LIMIT_EXCEEDED` (`limit` isn't a positive integer, or exceeds 500); 401 (tenant token doesn't match
`tenantId`, identical to `/v1/index-query`'s check); 404 `TABLE_NOT_REGISTERED`; 409
`PARTITION_KEY_COLUMN_UNSET`; 409 `PARTITION_KEY_NOT_UNIQUE` (`table_rules.partition_key_unique`
isn't 1 for this table — see §5 and the paragraph below); 403 `INTERNAL_TABLE_ACCESS_FORBIDDEN`
(defense-in-depth — `table_rules` should never contain a `__cf_*`/`sqlite_*` name); 400
`INVALID_CURSOR` (cursor fails to base64/JSON-decode, or names a shard no longer in the
catalog shard's current active set — e.g. topology changed between calls; a client that gets
this restarts with no cursor); 400 `NO_SHARDS` (the tenant's catalog shard has no shards at
all yet — same pre-cluster-init condition every other shard-listing route rejects); 502
`SHARD_UNREACHABLE` (naming the shard — any shard failure fails the whole request, no
silently-partial result in this MVP).

Lists a tenant's own rows in a registered table, with no arbitrary filters — the query is
mechanically constructed (`table` + `tenantId` + `cursor` + `limit` only), the same
safe-by-construction pattern `compileMutation` established for writes, rather than the
raw-SQL pattern that failed for the removed tenant read path (see §14 and `TODOS.md`'s
Completed section). Auth reuses `CatalogDO.checkTenantAuth` exactly as `/v1/index-query`
does (via a new combined `/lookup-table-scan` route playing the same role `/lookup-index`
does: auth check + `table_rules` gate in one round trip). Resolves the tenant's catalog
shard (`catalogShardIdForTenant`) and its shard pool (`/list-shards` with `{includeDraining:
true}` — a tenant's rows still physically exist on a shard mid-`/admin/drain-shard` until
its vbuckets finish migrating, so excluding draining shards would silently omit them; matches
`/admin/backfill-provenance` and index-creation's existing pattern), then fans out to every
shard's internal `POST /tenant-scan-page` (§6) at `SHARD_FANOUT_CONCURRENCY` concurrency (the
same constant/`batchedMap` helper reused throughout this codebase).

**Partition-key-uniqueness precondition (structural, not incidental).** `/tenant-scan-page`
keys its `__cf_row_owners` join purely by partition-key VALUE, filtered by
`(tenant_id, table_name)` — if `partitionKeyColumn` isn't actually guaranteed unique, two
different tenants' rows could share the same value and the join would attribute both to
whichever tenant currently owns that key in `__cf_row_owners`, a cross-tenant read leak. This
is why `table_rules.partition_key_unique` (§5) exists: it's computed automatically — never
accepted from a client — at `/admin/create-table`, `/admin/set-partition-key-column`, and
`/admin/register-table` time via live schema introspection (`checkPartitionKeyUnique` in
index.ts: a real `UNIQUE` constraint, sole `PRIMARY KEY`, or non-partial UNIQUE index counts;
composite-key membership and partial/`WHERE`-conditioned unique indexes do not), and fails
closed to unverified (0) on any introspection error or ambiguity. `POST /v1/table-scan`
checks this flag on every call (via `/lookup-table-scan`) and rejects 409
`PARTITION_KEY_NOT_UNIQUE` for any table where it isn't 1 — a hard requirement for using this
route safely, not a soft recommendation.

**Cursor and the "advance only on emit" invariant (the largest correctness risk in this
feature).** The opaque `cursor` is a base64-JSON `{table, shardCursors: {[shardId]:
afterPartitionKey}}` — `table` binds the cursor to the table it was issued against (a cursor
from scanning one table is rejected 400 `INVALID_CURSOR` if replayed against another) and
`shardCursors` holds one position per shard in the pool at issuance time; a shard absent from the map starts at
`""` (scan from the beginning). Results from every shard are merged ascending by
`partition_key` (ties broken by `shardId` ascending — a same-`partition_key` tie across two
different shards for the same tenant/table cannot happen in steady state, since a given key
hashes to exactly one shard; it can happen transiently during the documented migration-
duplicate window, where the tie-break only decides which copy sorts first) and truncated to
the requested `limit`. Each shard's cursor then advances only to the `partition_key` of the
last row **from that shard actually included** in the truncated response — a shard whose
rows were fetched internally but then cut by the overall-limit truncation keeps its OLD
cursor position, so the next call re-fetches (never skips) them. `nextCursor` is omitted only
when there is no reason to believe more rows exist: every shard's own fetch returned fewer
rows than its per-shard cap (`min(requested, 100)`), *and* nothing fetched this call was
truncated away by the overall limit. (A real bug surfaced during implementation: checking
only "did any shard hit its own per-shard cap" is insufficient — two shards can each return
fewer rows than their own cap while the overall-limit truncation still cuts one of those
already-fetched rows; the fix also checks whether the merge itself truncated anything.)

**Provenance visibility (§14, Milestone 3 Chunk 0/1).** A row lacking a `__cf_row_owners`
entry (pre-Chunk-0, never backfilled) is invisible to `/tenant-scan-page`'s join by
construction — its owner is definitionally unknown, so it cannot be attributed to any
tenant. `provenance.complete` (from `table_rules.provenance_complete`, a cheap column read)
reports whether this table has zero such gaps anywhere in the cluster; `provenance.fix`
names `/admin/backfill-provenance` as the remediation until a full-cluster run clears them,
at which point it flips `true` permanently (see §5).

**Non-goals (explicit, matching the design decision).** No arbitrary column filtering (only
`table` + `tenantId` + pagination — a future milestone can add a structured equality-filter
map if real usage demands it). No fix to §14's partition-key collision limitation (two
tenants sharing a partition key on the same shard — `__cf_row_owners` reflects only the last
writer either way; this milestone inherits, not worsens, that limitation). Migration-window
duplicates are inherited, not solved fresh, exactly like `/v1/scatter`'s existing documented
limitation. No per-tenant rate limiting on this fan-out-shaped route yet (the
catalog-shard-scoped pool is the v1 blast-radius control). No partial-result mode (`SHARD_UNREACHABLE`
fails the whole request rather than returning `{partial: true, errors: [...]}`).

POST /v1/scatter (ADMIN_TOKEN — reads across every tenant indiscriminately, so this is an admin operation, not a data-plane one)
Request:
- sql string (SELECT only)
- params array (optional)
- limit number (optional)

Response:
- shardCount
- rows
- perShard

## 8) Routing Algorithm

1. Validate table, tenantId, partitionKey.
2. Catalog verifies table is registered.
3. Compute vbucket.
4. Read vbucket_map to find shardId.
5. Forward request to ShardDO named shardId.
6. Return shard response with routing metadata.

## 9) Write Idempotency Contract

- Client may provide requestId.
- If omitted, Gateway generates UUID.
- For mutating SQL:
  - Shard checks applied_requests by requestId.
  - If found and the stored request hash matches the incoming (sql, params), returns the previously stored result.
  - If found but the hash differs, rejects the replay with a 409 instead of returning a stale or wrong result.
  - If not found, executes mutation in transaction and records requestId plus a hash of (sql, params).

This prevents duplicate writes after network retries and stops a reused requestId from being replayed against different SQL/params.

## 10) Transaction Semantics

- Single-shard operations:
  - Strong, local ACID semantics.

- Cross-shard operations (Milestone 1 — Transaction Coordinator):
  - Full 2PC is the only mode, not feature-flagged. Cross-shard transactional writes were
    reclassified from "future, saga-first" to a non-negotiable day-one requirement (see
    Section 2). A caller derives a `StructuredMutation` set; the coordinator resolves
    participant shards and drives prepare/commit/abort across all of them atomically.
  - Bounded to at most 8 participant shards per transaction.
  - Requires the structured mutation contract (Section 7, `/v1/mutate`) and a mandatory
    partition-key-column convention on every registered table — both shipped.
  - **Shard-level primitives (implementation status: shipped and reachable via `/v1/tx`).**
    Each `ShardDO` exposes internal `/prepare`, `/commit`, `/abort`, `/tx-status`,
    `/pending-intent-count`, and `/invalidate-request` routes implementing the participant
    side of 2PC: `/prepare`
    validates a mutation by executing it inside a transaction and forcing a rollback (so a
    concurrent read never sees it), then durably records a lock + pending intent in a
    separate transaction; `/commit` re-executes for real; `/abort` has nothing to undo,
    since prepare never left anything applied. These are DO-binding-only, called from
    `CoordinatorDO`, never directly by the Worker.
  - **CoordinatorDO (Milestone 1 Chunk 3 — shipped).** One `CoordinatorDO` instance per
    transaction (`env.COORDINATOR.idFromName(txId)`, no sharding — see the cost-model
    decision in the milestone plan: Cloudflare DO billing has no per-instantiation cost, so
    the simpler keying wins over a sharded pool at this project's realistic near-term scale).
    `txId = sha256Hex([mutations[0].tenantId, requestId])`. `/begin` persists the
    transaction and its participants, fans out `/prepare` to every participant shard, aborts
    everyone on any failure, or fans out `/commit` on universal success. A commit
    acknowledgement that fails to reach one or more shards is queued in `recovery_queue` and
    retried via `alarm()` with exponential backoff — the transaction is already durably
    committed at that point, so this only affects when the shard-side state catches up, not
    correctness. `/v1/tx` (Section 7) is the public entry point. `CoordinatorDO` stores a
    hash of the participant/mutation set alongside each `txId`; retrying an existing `txId`
    (any status — in-flight, committed, or aborted) with a different mutation set rejects 409
    (`TX_ID_REQUEST_MISMATCH`) instead of silently resuming 2PC with the new data or replaying
    a stale "committed" for content that was never actually applied — mirrors `/v1/sql`'s
    existing `request_hash` mismatch rejection for the same class of bug (found by a Codex
    review pass against the merged milestone). `CoordinatorDO`'s schema migration guard
    (`ensureColumn`, the same pattern `catalog.ts`/`shard.ts` already use) backfills
    `operation_hash` on a pre-existing `transactions` table rather than crashing — a
    `/begin` retry against a pre-migration row degrades to the same fail-closed mismatch
    rejection instead of a 500 (found by a second Codex pass against this fix itself).
  - A batch may legitimately contain multiple mutations against the same row (e.g. insert
    then update in one `/v1/tx` call — nothing caps mutation count, only distinct participant
    keys); `ShardDO./prepare` acquires each row lock with `INSERT OR IGNORE` rather than a
    plain `INSERT`, so this no longer crashes on a `row_locks` primary-key violation (also
    found by the same Codex pass).
  - Raw `/v1/sql` and `/v1/mutate` mutations against a row locked by an in-flight
    coordinated transaction reject 409 (`TX_PARTICIPANT_LOCKED`).
  - **Draining interaction (Milestone 1 Chunk 4 — shipped).** A new transaction targeting an
    already-draining shard is rejected 503 by `CatalogDO`'s existing `/route` check (shard
    status must be `active`) — no new code needed there. The other direction — draining a
    shard that has in-flight prepared 2PC intents — is handled in the Worker's
    `handleAdminDrainShard`, not by adding a `SHARD` binding to `CatalogDO`: it calls the
    target `ShardDO`'s `/pending-intent-count` first, and only proceeds to `CatalogDO`'s
    `/drain-shard` if that count is 0; otherwise it rejects 409
    (`SHARD_HAS_IN_FLIGHT_TRANSACTIONS`). This preserves the "Worker orchestrates, DOs don't
    call each other directly" invariant. Relies on Chunk 3's recovery loop (bounded time) or
    `/admin/tx-force-abort` (manual escape hatch) to unblock a stuck retry.
  - **Index-shard drain interaction (Milestone 2 Chunk 5 — shipped).** Index-shard placement
    (`indexShardIdForKey`) is a pure hash over the current shard pool, independent of
    `vbucket_map`/`shards.status` — draining a shard in the catalog sense doesn't stop the
    underlying `ShardDO` instance or its `alarm()` from continuing to exist and run. The real
    risk is an operator draining a shard ahead of decommissioning it while that shard still
    has unresolved `index_pending_jobs` (Chunk 2's retry queue) — silently letting those keep
    retrying against a shard the catalog no longer routes traffic to. `handleAdminDrainShard`
    now also checks `/pending-intent-count`'s (extended) `indexPendingJobCount` field and
    rejects 409 (`SHARD_HAS_PENDING_INDEX_JOBS`) if it's nonzero, mirroring the 2PC
    in-flight-transaction check exactly. `/admin/shard-stats` also reports
    `indexPendingJobCount` and `indexEntryCount` for observability.

## 11) Rebalancing, Split, and Drain (Milestone 3 — shipped)

Trigger conditions (future automation — see TODOS.md "Automatic split heuristics"):
- shard DB size > threshold (example 7 GB soft limit)
- sustained write QPS above threshold
- p95 latency above threshold

### vbucket migration (`/admin/migrate-vbucket`, and what `/admin/split-vbucket` now does)

`CatalogDO` owns the migration state machine (it already owns `vbucket_map`) and drives
`ShardDO`'s internal `/migrate-export`, `/migrate-import`, `/migrate-checksum(s)`,
`/fence-vbucket`, `/unfence-vbucket`, and `/delete-vbucket-rows` endpoints from its own
alarm — a deliberate, spec'd exception to the earlier "CatalogDO and ShardDO never call
each other" convention (orchestration from a stateless Worker request would die with the
request). One migration per vbucket at a time (409 `MIGRATION_IN_PROGRESS`); starting is
gated on the source shard having zero unattributed rows for any registered table
(409 `VBUCKET_PROVENANCE_INCOMPLETE`, count included).

Phases:
1. **backfilling** — writes keep landing on the source (authoritative); the gateway
   mirrors each applied write to the target with the same requestId (dual-write; mirror
   failure never fails the client write, it enqueues on the source's `__cf_mirror_pending`).
   `/v1/tx` intents on migrating vbuckets are mirrored post-commit by `CoordinatorDO`,
   same queue on failure. Meanwhile the catalog pages `/migrate-export` (rows selected via
   `__cf_row_owners`, 500-row pages, stable partition-key cursor) into `/migrate-import`
   (INSERT OR REPLACE + provenance, idempotent). If the target shard was created mid-life
   (a split target), each table's captured `schema_sql` is applied there first — a table
   whose `schema_sql` is NULL (PR review round 11: only `/admin/create-table`-verified tables
   are guaranteed to have one — see §7) can't be auto-provisioned this way and must already
   exist on the target some other way, or be applied manually by an operator.
2. **cutover** (formal 5-step ordering):
   1. Catalog sets `migration_status='cutover'` and synchronously writes a fence row to
      the source (`/fence-vbucket`). From this instant the source rejects any data write
      whose payload vbucket matches, 409 `VBUCKET_FENCED` (retryable). The fence is
      enforced at the data, not at routing — a write that resolved its route before the
      fence still physically arrives at the source and is caught there.
   2. Source drains `__cf_mirror_pending` for that vbucket to zero (catalog polls).
   3. Verify: for each registered table, both shards compute a content checksum — sha256
      over the concatenation of (partition_key, canonical row JSON) ordered by partition
      key, streamed in the same 500-row pages. Canonical row JSON = JSON.stringify with
      keys sorted lexicographically. Any mismatch aborts the attempt: fence lifted, target
      wiped, status back to `backfilling` (a later pass re-copies and retries).
   4. Flip `vbucket_map.shard_id` to the target, `migration_status='none'`, bump
      `metadata_version`.
   5. Unfence the source, then delete the vbucket's rows + provenance from it.

`/admin/migrate-vbucket-abort` at any point before step 4 is safe: the source never
stopped being authoritative; the target's rows + provenance for the vbucket are wiped,
the fence lifted, and the source's queued-but-unsent mirrors purged. After the flip it
rejects 409 `MIGRATION_ALREADY_COMMITTED` — a committed migration is reversed by migrating
the vbucket back (same primitive, reversed). `/admin/split-vbucket` keeps its name and
request shape but now creates the target shard and starts this migration (response gains
`migrationStarted: true`) instead of repointing the map and stranding rows.

Reads stay on the source until the flip. `/v1/scatter` may observe duplicate rows during
an active migration window (documented limitation).

### Drain v2 (`/admin/drain-shard` + `/admin/drain-shard-status`)

Draining a shard now performs full evacuation, alarm-driven:
1. Mark draining (existing 503 behavior for newly-routed work).
2. Migrate every vbucket mapped to the shard off it, sequentially, via the migration
   primitive above (targets rotated deterministically across the catalog's remaining
   active shards).
3. Ring evacuation: for each index whose pinned `placement_ring_json` contains the shard,
   pick the replacement deterministically — the active shard not already in that ring with
   the smallest `hashKey(indexName + ":" + shardId)` (candidates gathered cluster-wide;
   an index's ring pins every shard active at its creation, so viable substitutes are
   shards added later, e.g. by a split). Substitute at the same ring position, copy the
   draining shard's `__cf_indexes` rows for that index to the substitute (idempotent
   INSERT OR REPLACE), repoint the ring on every catalog shard, then delete the source
   copies. If no candidate exists, `/admin/drain-shard` rejects 409
   `RING_EVACUATION_NO_CANDIDATE` up front, before any durable state change.

Drain reports completion only when both loops finish; `/admin/drain-shard-status` returns
`{shardId, vbucketsRemaining, ringsRemaining, status}`. Both former topology blocks
(`SPLIT_BLOCKED_BY_INDEXES`, `SHARD_DRAIN_BLOCKED_BY_INDEXES`) are removed — index
placement's pinned ring (Milestone 3, Chunk 2) makes them unnecessary.

**Upgrade flow for indexes created before Milestone 3** (one-time, operator-run): pre-M3
`__cf_indexes` entries carry no `tenant_id` and pre-M3 `index_rules` rows have no pinned
ring — run `/admin/drop-index` then `/admin/create-index` per index. The index is
unqueryable from drop until backfill flips it back to `ready` (the same availability
contract index creation already has); `/v1/index-query` 404s during the window exactly as
for a never-created index.

## 12) Query Planning Rules (MVP)

- Writes must be single-shard and require partitionKey.
- Reads without partitionKey are rejected on /v1/sql.
- Admin fan-out reads allowed only through /v1/scatter and should be capped. /v1/table-scan
  (Milestone 4) is a separate, tenant-scoped fan-out read, bounded to one tenant's own rows
  across its catalog shard's pool rather than every shard cluster-wide.

## 13) Observability (required for production)

Emit structured logs and counters for:
- route_lookup_ms
- shard_execute_ms
- shard_row_count
- scatter_shard_count
- duplicate_request_hits
- metadata_version

## 14) Security and Multi-tenancy

- Require authenticated principal at Gateway — implemented via `tenant_auth` bearer tokens (`/admin/register-tenant`), checked in `CatalogDO.handleRoute` before any routing info is returned. `ADMIN_TOKEN` is accepted there as a universal bypass (the operator may route as any tenant), used by the admin-only `/v1/sql`.
- Verify tenantId belongs to principal before route — implemented: the caller's bearer token is hashed and compared against the claimed `tenantId`'s stored hash; missing/wrong/revoked tokens are all rejected with 401.
- This is a per-deployment authorization boundary (isolating apps/environments within one self-hosted deployment), not a multi-customer-SaaS boundary — see README.md's "Tenant authorization" section for the operator/tenant distinction this milestone's distribution model assumes.
- **Raw `/v1/sql` is ADMIN-ONLY (Milestone 3), reads AND writes.** The trust-based tenant SQL path was removed rather than continue an unwinnable guard. Two things forced it: (1) the per-tenant write guard (denylist → allowlist against a passthrough SQL string) leaked six times — mixed case, inter-token comments, `schema.` qualifiers, a spaced+quoted internal name, double-quoted internal identifiers; and (2) there is **no safe tenant `SELECT` over arbitrary SQL**, because base rows carry no physical `tenant_id` column — the shard cannot add a `WHERE tenant_id = ?` predicate to an arbitrary query, so a partition-scoped raw read could return another tenant's rows that hash into the same vbucket. `/v1/sql` now requires `ADMIN_TOKEN` (operator/debugging); tenants write via `/v1/mutate` + `/v1/tx` (which force the partition-key predicate structurally) and read via `/v1/index-query` (exact-tuple lookups) and `/v1/table-scan` (Milestone 4 — lists a tenant's own rows, mechanically constructed and filtered by `tenant_id` via the `__cf_row_owners` join rather than a physical `tenant_id` column on the base row; see §7 and §6). This closes the general-read gap without needing the physical-`tenant_id` schema change originally assumed necessary — `TODOS.md`'s Completed section records why the join approach was chosen over adding that column. Even for the operator, a `/v1/sql` mutation whose write TARGET is an internal bookkeeping table is rejected 403 (internal reads and cross-table access are allowed — admin is trusted).
- **`/v1/table-scan`'s isolation depends on `__cf_row_owners.tenant_id`, not a physical column on the base row.** Every query `/tenant-scan-page` runs filters `WHERE ro.tenant_id = ?` before joining to the base table — but that filter alone is not sufficient when two tenants' rows share a literal partition-key value on the same shard (§14's pre-existing collision: `__cf_row_owners`'s primary key is `(table_name, partition_key)`, no tenant in the key, so the last writer's attribution wins): the join would then attribute both rows to whichever tenant currently owns that key. The actual mechanism that closes this is `table_rules.partition_key_unique`, computed by `checkPartitionKeyUnique` (`index.ts`) at `/admin/create-table`, `/admin/set-partition-key-column`, and `/admin/register-table` time and failing closed to 0 on any ambiguity — `/v1/table-scan` rejects 409 `PARTITION_KEY_NOT_UNIQUE` for any table where the flag isn't 1, so a tenant's scan against a table with a verified-unique partition key can never join into another tenant's row for a colliding value in the first place. This is the same class of leak that motivated removing raw `/v1/sql`, closed by rejecting the unsafe case rather than by another guard.
- Enforce SQL policy allowlist in production (MVP currently permissive).

## 15) Migration Path to Production

- Replace permissive SQL with parsed/validated subset.
- Add query planner and local pre-aggregation for scatter reads.
- ~~Add global secondary index service.~~ Shipped (Milestone 2).
- ~~Add automated split controller and background mover.~~ Shipped (Milestone 3): vbucket
  migration with dual-write backfill and fenced cutover, split-as-migration, drain-shard
  evacuation. Automatic split *heuristics* (deciding when to split) remain future work —
  TODOS.md "Automatic split heuristics".
- Add backups and restore drills per shard.
