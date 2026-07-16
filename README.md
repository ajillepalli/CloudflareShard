# CloudflareShard MVP

A concrete MVP for a sharded SQL layer on top of Cloudflare Durable Objects (SQLite-backed).

## What this prototype demonstrates

- One logical SQL endpoint in a Worker.
- Catalog DO as control plane for table registry and vBucket map.
- Shard DOs as single-threaded SQLite execution nodes.
- Deterministic single-shard routing via tenantId + table + partitionKey.
- Scatter read endpoint for fan-out SELECT.
- Online vBucket migration: dual-write backfill with a fenced, checksum-verified cutover —
  `/admin/split-vbucket` performs a real data move, and `/admin/drain-shard` fully evacuates a
  shard (vbuckets first, then secondary-index placement rings via deterministic substitution,
  protected by a per-index write fence so no index entry is stranded on a shard mid-evacuation).
  See `docs/SPEC.md` §11 for the backfill/cutover/drain algorithm.
- A durable, TTL'd topology-operation lock serializes drain/split/migrate/create-index/drop-index
  so two concurrent cluster-reshaping operations can't race each other's preconditions;
  `/admin/topology-lock-status` and `/admin/force-release-topology-lock` give an operator
  visibility and recovery.
- Mutation idempotency via requestId, rejecting replay with a mismatched SQL/params pair instead of returning a stale result.

## Project layout

- `src/index.ts`: Gateway worker router and public API.
- `src/catalog.ts`: Catalog durable object (metadata, routing, map changes).
- `src/shard.ts`: Shard durable object (SQLite execution + idempotency).
- `docs/SPEC.md`: Concrete architecture and protocol spec.
- `examples/rpc-consumer/`: Demo Worker calling the tenant data path over a Durable Object RPC / service binding instead of HTTP.

## Prerequisites

- Node.js 20+
- Cloudflare account + Wrangler authentication

## Setup

```powershell
git clone https://github.com/ajillepalli/CloudflareShard.git
cd CloudflareShard
npm install
```

## Run locally

```powershell
npm run dev
```

## Deploy

```powershell
npm run deploy
```

## API quickstart

### 1) Initialize cluster metadata and shard map

```bash
curl -X POST http://127.0.0.1:8787/admin/init \
  -H "content-type: application/json" \
  -H "authorization: Bearer $ADMIN_TOKEN" \
  -d '{"numShards":4,"totalVBuckets":256}'
```

`numShards` is clamped to 1-256 and `totalVBuckets` to 64-65536 (defaults 8 and 1024)
so a single call can't provision an oversized, unrollbackable cluster.

### 2) Register a logical table

Every table requires a `partitionKeyColumn` — the column that holds each
row's partition key. This is mandatory, not optional: it's what lets
`/v1/mutate` and coordinated transactions structurally enforce
that a mutation only ever touches the one row/partition it claims to.

```bash
curl -X POST http://127.0.0.1:8787/admin/register-table \
  -H "content-type: application/json" \
  -H "authorization: Bearer $ADMIN_TOKEN" \
  -d '{"table":"events","partitionKeyColumn":"id"}'
```

### 3) Create the table's schema (admin-mediated, applies to every shard)

Schema changes (`CREATE TABLE`, etc.) are not allowed through `/v1/sql` — that
endpoint's deny-list blocks `CREATE`/`DROP`/`ALTER` to keep tenant-supplied SQL
restricted to data operations. Use `/admin/create-table` instead; it registers
the table and applies the schema to every physical shard across every catalog
shard so any tenant's rows land on a shard that already has the table.

```bash
curl -X POST http://127.0.0.1:8787/admin/create-table \
  -H "content-type: application/json" \
  -H "authorization: Bearer $ADMIN_TOKEN" \
  -d '{
    "table":"events",
    "schema":"CREATE TABLE events (id TEXT PRIMARY KEY, user_id TEXT, body TEXT, created_at TEXT)",
    "partitionKeyColumn":"id"
  }'
```

The `schema`'s `CREATE TABLE` name must match `table` exactly — a mismatch is
rejected with a 400 rather than silently creating a differently-named table.
`partitionKeyColumn` is validated against the schema's actual columns (via
`PRAGMA table_info`, not a hand-rolled parser) after the table is created on
every shard — if it doesn't exist, the table is rolled back (dropped from
every shard) and the call fails with a 400, rather than registering a table
whose structured/transactional paths could never work.

`schema` must **not** use `CREATE TABLE IF NOT EXISTS` — rejected with 400
`SCHEMA_IF_NOT_EXISTS_NOT_ALLOWED`. This route's
verification step trusts that its own DDL push actually applied everywhere,
and `IF NOT EXISTS` can silently no-op on a shard where `table` already
physically exists (e.g. a naming collision with a legacy table), leaving that
trust misplaced. Use a plain `CREATE TABLE`: if the table genuinely doesn't
exist yet it's created normally; if it already exists somewhere, the call now
fails loudly with SQLite's own "table already exists" error instead of
silently no-oping — use `/admin/register-table` for an existing table, or
resolve the naming conflict first.

Tables registered before this validation existed carry a `'__unset__'`
sentinel and are rejected from `/v1/mutate` and coordinated transactions with
a 409 until an operator runs:

```bash
curl -X POST http://127.0.0.1:8787/admin/set-partition-key-column \
  -H "content-type: application/json" \
  -H "authorization: Bearer $ADMIN_TOKEN" \
  -d '{"table":"events","partitionKeyColumn":"id"}'
```

Raw `/v1/sql` against an unupgraded table is unaffected — this gate only
blocks the new structured paths.

This is a strictly **one-time** upgrade: it only works on a table that's
still carrying the `'__unset__'` sentinel. Calling it again on a table whose
`partitionKeyColumn` has already been set (whether via `/admin/create-table`
or a prior call to this endpoint) is rejected with 409
`PARTITION_KEY_ALREADY_SET`, and `table_rules` is left unchanged. There's no
supported way to repoint an already-configured table to a different
partition key column — doing so would leave `__cf_row_owners`' existing
provenance entries keyed by the old column's values while `/v1/table-scan`
looks up rows by the new column, a cross-tenant data leak.

![Terminal output showing steps 1-3 of the quickstart — cluster init, table registration, and schema creation — each returning HTTP 200 with real JSON responses](docs/images/quickstart-cluster-init.png)

Steps 1-3 run end to end against the actual live deployment
(`https://cloudflare-shard-mvp.<account>.workers.dev`) — this is real `curl`
output, not fabricated example data.

### 4) Register a tenant

`/v1/mutate`, `/v1/tx`, `/v1/index-query`, and `/v1/table-scan` are the tenant
data-plane routes and require a tenant bearer token, not `ADMIN_TOKEN` — this
isolates apps/environments within one deployment (see "Tenant authorization"
below). `/v1/sql` is admin-only — see "Tenant authorization" below for why.
`/register-tenant` returns the plaintext token exactly once; store it.

```bash
curl -X POST http://127.0.0.1:8787/admin/register-tenant \
  -H "content-type: application/json" \
  -H "authorization: Bearer $ADMIN_TOKEN" \
  -d '{"tenantId":"t1"}'
```

Response: `{"ok":true,"tenantId":"t1","token":"<save this>"}`. Export it for
the rest of this walkthrough:

```bash
export TENANT_TOKEN=<token from the response above>
```

### 5) Insert data

Tenants write through the structured data plane (`/v1/mutate`, below, and
`/v1/tx`), not raw SQL — `/v1/sql` is admin-only; see "Tenant authorization"
below for why:

```bash
curl -X POST http://127.0.0.1:8787/v1/mutate \
  -H "content-type: application/json" \
  -H "authorization: Bearer $TENANT_TOKEN" \
  -d '{
    "op":"insert",
    "table":"events",
    "tenantId":"t1",
    "partitionKey":"e1",
    "requestId":"req-1",
    "values":{"user_id":"user-1","body":"hello","created_at":"2026-06-29T00:00:00Z"}
  }'
```

### 6) Query a partition

There is still no general partition-scoped tenant `SELECT` (tenants can't use
raw `/v1/sql` — see "Tenant authorization" below), but tenants have two
structured read paths: exact-tuple lookups via `/v1/index-query` (see
`docs/SPEC.md` §7 for its request/response shape), and listing a tenant's own
rows in a table via `POST /v1/table-scan` (cursor-paginated, no arbitrary
filters). See "Tenant-scoped table scan" below.

### 7) Tenant-scoped table scan

`POST /v1/table-scan` lists a tenant's own rows in a registered table,
cursor-paginated, with the query mechanically constructed from
`table + tenantId + cursor + limit` only — no arbitrary filtering, the same
safe-by-construction pattern `/v1/mutate` already uses for writes. It's the
general tenant read path for tables with no registered index (see "Known
limitations" below and `TODOS.md`'s Completed section): a tenant's rows are
found via `__cf_row_owners` (filtered by `tenant_id` + `table_name`, no
physical `tenant_id` column needed on the base row), fanned out to every shard
in the tenant's catalog shard's pool.

Only tables whose `partitionKeyColumn` has TEXT or BLOB type affinity, is
verified UNIQUE, and is verified to collate as BINARY can use this route.
`/admin/create-table`, `/admin/set-partition-key-column`, and
`/admin/register-table` each automatically check (a client-supplied
uniqueness claim is never trusted) whether the column is backed by a real
`UNIQUE` constraint, the table's sole `PRIMARY KEY` column, or a non-partial unique index — a
column that's only one part of a composite primary/unique key, or "unique" only via a
partial/`WHERE`-conditioned index, does not count. INTEGER/NUMERIC/REAL-affinity columns are
rejected outright regardless of uniqueness, since SQLite's numeric-string coercion (`'01'`,
`'1'`, and `'1.0'` all matching the same row) is an unbounded ambiguity space; and the column's
real collation is verified with a live probe against the shard's actual SQLite engine, not by
parsing DDL text, so a column that behaves as `NOCASE` or `RTRIM` is rejected too. All three
checks cache into `table_rules.partition_key_unique`, failing closed (unverified) on any
introspection error. Call `/v1/table-scan` against a table where that flag
isn't set and you'll get 409 `PARTITION_KEY_NOT_UNIQUE`: without a verified-unique,
BINARY-collated, TEXT/BLOB partition key, `/tenant-scan-page`'s join against `__cf_row_owners`
(keyed purely by partition-key value) could return one tenant's row to
another tenant who happens to share that value, so the route refuses to run
rather than risk a cross-tenant read. (`docs/SPEC.md` §5/§7 has the
per-shard verification details for each of the three routes above, including the affinity and
collation checks.)

**Trade-off:** a table verified via `/admin/register-table`'s or
`/admin/set-partition-key-column`'s own probe (not `/admin/create-table`)
is `table-scan`-eligible but ends up with `schema_sql = NULL` — a future
split/migration backfill can't auto-provision that table on a new target
shard from stored DDL; the table must already exist there some other way,
or an operator applies the schema manually.

```bash
curl -X POST http://127.0.0.1:8787/v1/table-scan \
  -H "content-type: application/json" \
  -H "authorization: Bearer $TENANT_TOKEN" \
  -d '{"tenantId":"t1","table":"events","limit":100}'
```

Response:

```json
{
  "rows": [{"id":"e1","user_id":"user-1","body":"hello"}],
  "nextCursor": "eyJzaGFyZEN1cnNvcnMiOnsi...",
  "provenance": {"complete": true},
  "scan": {"catalogShardId":"catalog-0","shardCount":4,"successCount":4,"scanMs":12}
}
```

`nextCursor` is present iff any shard in the pool may have more rows; pass it
back on the next call to continue, and omit it (or pass `null`) to restart from
the beginning. It's an opaque, per-shard cursor map — each shard's position
advances only to the last row from that shard actually returned in a given
response, never to a row that was fetched internally but cut by the overall
`limit`, so pagination can't silently drop a row even when a page truncates
mid-shard. A cursor that fails to decode, or names a shard no longer in the
catalog shard's active set (topology changed between calls), is rejected 400
`INVALID_CURSOR` — omit `cursor` to restart the scan.

`provenance.complete` reports whether this table has any rows anywhere in the
cluster with no recorded owner (from before row-provenance tracking existed)
— such rows are always hidden from every tenant's scan (their owner is
definitionally unknown), and `provenance.fix` names the remediation
(`/admin/backfill-provenance`) until a full-cluster run reports zero
remaining gaps for the table, at which point it flips `true` permanently. A
brand-new table (`/admin/create-table`) starts `false`: it earns
certification the same way any other table does, through a
`/admin/backfill-provenance` run — trivial for a genuinely brand-new (empty)
table, since it finds nothing orphaned.

Any shard in the pool failing to respond fails the whole request 502
`SHARD_UNREACHABLE` (naming the shard) rather than return a silently-partial
result — a deliberate MVP simplification (a `partial: true` mode with
per-shard errors is a documented future option, not a blocker today).

![Terminal output from a live run: table-scan on a brand-new table returns provenance.complete: false, then a backfill-provenance call reports 0 orphaned/ambiguous rows, then the same table-scan call returns provenance.complete: true](docs/images/tenant-table-scan-live.png)

`provenance.complete: false` right after inserting data is expected, not a
bug: a brand-new table always starts unverified until an explicit
`/admin/backfill-provenance` run certifies it clean, as shown above.

### 8) Structured mutation (row-owned, single-shard)

`/v1/mutate` is an alternative to raw SQL for writes: instead of a SQL string,
you describe the operation (`insert`/`update`/`delete`/`upsert`) and its
target. The partition-key column is always forced into the affected row —
`compileMutation` ANDs it into the `WHERE` clause for `update`/`delete` (even
if you supply no `where` at all, it only ever touches the one partitioned
row/set) and force-sets it in `values` for `insert`/`upsert`. This is what
raw `/v1/sql` cannot guarantee, since it's a trust-based passthrough.

```bash
curl -X POST http://127.0.0.1:8787/v1/mutate \
  -H "content-type: application/json" \
  -H "authorization: Bearer $TENANT_TOKEN" \
  -d '{
    "op":"insert",
    "table":"events",
    "tenantId":"t1",
    "partitionKey":"e1",
    "values":{"user_id":"user-1","body":"hello","created_at":"2026-06-29T00:00:00Z"}
  }'
```

Response: `{"ok":true,"rowsAffected":1}`.

### 9) Cross-shard atomic transaction

`/v1/tx` atomically commits a batch of structured mutations that may span
multiple shards, via two-phase commit (`CoordinatorDO`). Every mutation must
share the same `tenantId`; the batch is capped at 8 distinct
`(tenantId, table, partitionKey)` rows. `requestId` is required and doubles as
the transaction's idempotency key — retrying the same `requestId` returns the
prior outcome instead of re-running 2PC.

```bash
curl -X POST http://127.0.0.1:8787/v1/tx \
  -H "content-type: application/json" \
  -H "authorization: Bearer $TENANT_TOKEN" \
  -d '{
    "mutations": [
      {"op":"insert","table":"events","tenantId":"t1","partitionKey":"e1","values":{"user_id":"user-1","body":"a"}},
      {"op":"insert","table":"events","tenantId":"t1","partitionKey":"e2","values":{"user_id":"user-1","body":"b"}}
    ],
    "requestId": "req-tx-1"
  }'
```

Response: `{"ok":true,"txId":"...","status":"committed"}`. If any participant
shard fails to prepare, every participant is rolled back and the call returns
409 (`TX_ABORTED`) — nothing is left half-applied.

An operator can check on or force-abort a stuck transaction:

```bash
curl -X POST http://127.0.0.1:8787/admin/tx-status \
  -H "content-type: application/json" -H "authorization: Bearer $ADMIN_TOKEN" \
  -d '{"txId":"..."}'

curl -X POST http://127.0.0.1:8787/admin/tx-force-abort \
  -H "content-type: application/json" -H "authorization: Bearer $ADMIN_TOKEN" \
  -d '{"txId":"..."}'
```

### 10) Fan-out query (all shards)

`/v1/scatter` reads across every tenant indiscriminately, so it's an admin
operation — it requires `ADMIN_TOKEN`, not a tenant token.

```bash
curl -X POST http://127.0.0.1:8787/v1/scatter \
  -H "content-type: application/json" \
  -H "authorization: Bearer $ADMIN_TOKEN" \
  -d '{
    "sql":"SELECT id, user_id, body FROM events",
    "params":[],
    "limit":100
  }'
```

### 11) Move one vBucket to a new shard

The cluster is partitioned across a fixed set of catalog shards (see
"Catalog sharding" below); `vbucket` numbering is local to one catalog shard,
so `catalogShardId` is required.

```bash
curl -X POST http://127.0.0.1:8787/admin/split-vbucket \
  -H "content-type: application/json" \
  -H "authorization: Bearer $ADMIN_TOKEN" \
  -d '{"catalogShardId":"catalog-0","vbucket":42,"newShardId":"shard-hotfix-1"}'
```

This starts a real online data migration (`migrationStarted: true` in the
response), not a routing-only repoint: the catalog backfills the vbucket's
rows to the target shard while new writes dual-write to both (same
requestId — the target dedupes), then runs a fenced, checksum-verified
cutover before deleting the source copy. See `docs/SPEC.md` §11 for the full
backfill/cutover algorithm. `/admin/migrate-vbucket` is the same primitive
with an explicit target; watch progress or bail out pre-flip:

```bash
curl -X POST http://127.0.0.1:8787/admin/migrate-vbucket-status \
  -H "content-type: application/json" \
  -H "authorization: Bearer $ADMIN_TOKEN" \
  -d '{"catalogShardId":"catalog-0","vbucket":42}'
# -> {"vbucket":42,"status":"backfilling","fromShard":"...","toShard":"...","rowsCopied":N,"mirrorQueueDepth":0,"startedAt":"..."}

curl -X POST http://127.0.0.1:8787/admin/migrate-vbucket-abort \
  -H "content-type: application/json" \
  -H "authorization: Bearer $ADMIN_TOKEN" \
  -d '{"catalogShardId":"catalog-0","vbucket":42}'
```

Migration selects rows by provenance (`__cf_row_owners`, written automatically
with every write). Rows written before provenance tracking existed must be
re-attributed once:

```bash
curl -X POST http://127.0.0.1:8787/admin/backfill-provenance \
  -H "content-type: application/json" \
  -H "authorization: Bearer $ADMIN_TOKEN" \
  -d '{"catalogShardId":"catalog-0"}'
# -> {"attributed":N,"ambiguous":[...],"orphaned":[...]}
# resolve an ambiguous row manually:
curl -X POST http://127.0.0.1:8787/admin/set-row-owner \
  -H "content-type: application/json" \
  -H "authorization: Bearer $ADMIN_TOKEN" \
  -d '{"catalogShardId":"catalog-0","shardId":"catalog-0-shard-0","table":"events","partitionKey":"user-1","tenantId":"tenant-1"}'
```

Migrating a vbucket whose source shard still has unattributed rows is rejected 409
`VBUCKET_PROVENANCE_INCOMPLETE` (with the count) rather than silently leaving rows behind.

**Upgrading indexes created before pinned placement rings existed** (one-time,
operator-run): such index entries carry no logical tenant identity and no
pinned placement ring — run `/admin/drop-index` then `/admin/create-index`
per index. The index is unqueryable from drop until backfill completes (the
same availability contract index creation already has).

## Catalog sharding

The cluster is partitioned across a fixed, well-known set of catalog shards
(`catalog-0`, `catalog-1`, ... — count controlled by the `CATALOG_SHARD_COUNT`
var, default 4). A tenant's catalog shard is computed by hashing `tenantId` —
there's no lookup step, which avoids the bootstrapping problem of sharding the
metadata store itself. Cluster-wide admin operations (`/admin/init`,
`/admin/register-table`, `/admin/create-table`, `/admin/status`) fan out to
every catalog shard; shard-scoped operations (`/admin/split-vbucket`,
`/admin/drain-shard`) require an explicit `catalogShardId` since vBucket/shard
identifiers are local to one catalog shard.

`/admin/audit-log` fans out to every catalog shard and merges each shard's
last 100 admin actions (`/init`, `/register-table`, `/split-vbucket`,
`/drain-shard`) into one list sorted newest-first, tagged with the
`catalogShardId` that logged each entry.

`/admin/drain-shard` first checks the target shard's in-flight `/v1/tx`
transaction count and rejects with 409 (`SHARD_HAS_IN_FLIGHT_TRANSACTIONS`)
if any are still prepared — retry once they resolve, or use
`/admin/tx-force-abort` to unstick one manually. A shard that's already
draining rejects any *new* `/v1/tx`/`/v1/sql` write with 503.

Draining a shard is a full evacuation, not just a routing marker: every
vbucket mapped to the shard is migrated off it sequentially (same primitive as
`/admin/migrate-vbucket`), and any secondary index whose pinned placement ring
contains the shard gets it substituted out for another active shard not
already in that ring. If no substitute candidate exists, the drain is
rejected up front with 409 `RING_EVACUATION_NO_CANDIDATE` (add a shard via a
split first). See `docs/SPEC.md` §11 for the substitution algorithm.
`/admin/drain-shard-status {catalogShardId, shardId}` reports
`{vbucketsRemaining, ringsRemaining, status}` until `status: "complete"`.

## Tenant authorization

The tenant data-plane routes — `/v1/mutate`, `/v1/tx`, `/v1/index-query`, and
`/v1/table-scan` — require a tenant bearer token (`POST /admin/register-tenant {"tenantId":
"..."}`, `ADMIN_TOKEN`-gated), separate from `ADMIN_TOKEN` itself. This
isolates apps/environments *within* one deployment — it is not a
multi-customer-SaaS boundary. In this project's current self-hosted
distribution model, the deploying developer holds both the operator role
(`ADMIN_TOKEN`) and every tenant role (`tenant_auth` tokens) — but the two are
kept structurally distinct in the code so a future hosted layer (a genuinely
separate operator) could be added without a rewrite.

`POST /admin/register-tenant {"tenantId": "...", "rotate": true}` issues a
new token for an already-registered tenant, invalidating the old one
immediately (no grace period — a scheduled rotation can break in-flight
callers with zero overlap window; a known limitation, not yet addressed).
`POST /admin/revoke-tenant {"tenantId": "..."}` disables a tenant's access.

**`/v1/sql` and `/v1/scatter` are admin-only** (`ADMIN_TOKEN`-gated operator
routes, not tenant paths). Raw `/v1/sql` (reads *and* writes) requires
`ADMIN_TOKEN` rather than a tenant token: a per-tenant SQL guard proved
structurally unwinnable (it leaked six times across review — mixed case,
inter-token comments, `schema.` qualifiers, a spaced+quoted internal name,
double-quoted internal identifiers), and a raw partition-scoped `SELECT`
could return another tenant's rows because base rows carry no physical
`tenant_id` column (see SPEC §14). Tenants use `/v1/mutate` + `/v1/tx` for
writes and `/v1/index-query` + `/v1/table-scan` for reads instead — all three
are safe-by-construction (the partition-key / tenant-id predicate is always
injected, never trusted from the caller). Even for the operator, `/v1/sql`
still refuses *writes* to internal bookkeeping tables (fence/provenance/
mirror), while allowing reads of them for debugging. `/v1/scatter` reads
across every tenant indiscriminately, so it requires `ADMIN_TOKEN` for the
same reason.

## RPC / Worker service-binding access (additive, not a replacement)

Every route above is also reachable without HTTP by a Worker running in the
same Cloudflare account, via a [service binding](https://developers.cloudflare.com/workers/runtime-apis/bindings/service-bindings/rpc/)
to `CloudflareShardRpc` — a named `WorkerEntrypoint` export in `src/index.ts`
exposing `mutate()`, `tableScan()`, and `indexQuery()` (the tenant data path;
admin/topology operations aren't exposed this way yet — see the tracked
follow-up issue). A consumer Worker declares this in its own `wrangler.toml`:

```toml
[[services]]
binding = "SHARD_API"
service = "cloudflare-shard-mvp"
entrypoint = "CloudflareShardRpc"
```

...and calls it directly, with no HTTP request or `Authorization` header to
build:

```ts
const result = await env.SHARD_API.mutate(tenantToken, {
  table: "events", tenantId, partitionKey, op: "insert", values: { ... },
});
```

The security boundary shifts from "possessing a bearer token per request" to
"this binding is wired into the caller's own config" — but the tenant token
is still required and still checked: it's passed as an explicit argument and
validated internally via the same `CatalogDO.checkTenantAuth` path the HTTP
routes use. Holding the binding alone is not sufficient authorization.

A full working example — a second Worker, wired via service binding, with a
real integration test proving the round trip over the actual binding (not an
in-process mock) — lives in [`examples/rpc-consumer/`](examples/rpc-consumer/).

## Known limitations

- No SQL parser or policy sandboxing yet.
- Cross-shard transactions (`/v1/tx`) are bounded to 8 participant rows.
- `/v1/scatter` may observe duplicate rows during an active migration window (reads fan
  out to all shards, and a migrating vbucket's rows exist on both source and target until
  cutover completes).
- Row provenance (`__cf_row_owners`) inherits — does not widen — the documented §14
  trust-model limitation: two tenants sharing a partition key on the same shard collide
  in the base table's own physical layout already. `/v1/table-scan` inherits the same
  limitation unaffected: it doesn't fix or worsen it (see §14).
- `/v1/table-scan` supports only `table + tenantId + cursor + limit` — no arbitrary
  column filtering (a future increment can add a structured equality-filter map if real
  usage demands it), and no per-tenant rate limiting on the fan-out yet (the
  catalog-shard-scoped pool is the v1 blast-radius control).
- `/v1/table-scan` may return the same row twice during an active migration window, for
  the same reason `/v1/scatter` can: a migrating vbucket's rows exist on both source and
  target until cutover completes. It never *loses* a row for this reason.
- When to split (hot-shard detection) is still a manual operator decision — the migration
  mechanism exists, but the heuristics for deciding when to trigger it don't (see `TODOS.md`).

## Next production steps

1. ~~Add authenticated tenant authorization in Gateway.~~ Done (Milestone 1 Chunk 0).
2. Introduce SQL allowlist/parser and bounded query plans.
3. ~~Add automated split controller with backfill and dual-write cutover.~~ Done
   (Milestone 3): `/admin/split-vbucket` and `/admin/migrate-vbucket` perform real online
   migration with dual-write backfill and a fenced, checksum-verified cutover;
   `/admin/drain-shard` fully evacuates a shard. Automatic split *heuristics* remain open.
4. ~~Add index service~~ Done (Milestone 2); query planner enhancements remain open.
5. Add observability and SLO alerting per shard and per route.

## License

Apache-2.0 — see `LICENSE`.
