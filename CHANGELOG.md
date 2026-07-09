# Changelog

All notable changes to this project are documented in this file.

## [1.0.0.0] - 2026-07-08

First shippable release of the sharded-SQL platform: multi-shard catalog routing, an admin control plane, and a security-hardened SQL execution path.

### Added
- Catalog sharding: the control plane is now split across multiple catalog-shard Durable Objects (`catalog-0..N-1`), routed by a pure hash function on tenant ID — no lookup or bootstrapping step required.
- `/admin/create-table` — admin-mediated schema creation, fanned out to every shard.
- `/admin/shard-stats` — per-shard row counts and idempotency-table size.
- `/admin/audit-log` — queryable audit trail of admin actions (`/init`, `/register-table`, `/split-vbucket`, `/drain-shard`).
- Structured JSON logging across the Worker, Catalog, and Shard layers.
- A non-destructive post-deploy smoke test (`scripts/smoke-test.mjs`).
- vitest + `@cloudflare/vitest-pool-workers` test suite (114 tests) covering routing, admin auth, idempotency, and error boundaries.
- `TODOS.md` tracking open decisions: distribution/positioning, automatic split heuristics, cross-tenant analytics.

### Changed
- Every admin endpoint now requires a bearer admin token, enforced both per-route and structurally at the Worker's `/admin/*` gate.
- Admin token comparison is constant-time, closing a timing side-channel.
- Shard/vbucket fan-out during `/admin/init` is now batched with a concurrency cap instead of unbounded parallel requests, preventing resource exhaustion on large clusters.
- `/admin/init` clamps `numShards` and `totalVBuckets` to a bounded range so a single call can't provision an oversized, unrollbackable cluster.
- Error responses no longer leak raw SQL or driver error messages to callers.

### Fixed
- Closed a critical bypass where a SQL comment (e.g. `-- x\nDELETE FROM t`) defeated mutation classification, letting an unauthenticated `/v1/scatter` call execute destructive writes across every shard as if it were a read. Shard execution now derives the mutation classification itself instead of trusting the caller.
- Closed the same class of bypass via a leading `WITH`/CTE clause (e.g. `WITH x AS (SELECT 1) DELETE FROM events`), found by a Codex adversarial pass against the final diff.
- Idempotent mutation replay now detects a `requestId` reused with different SQL/params and rejects the replay instead of silently returning a stale result. The mismatch check now uses SHA-256 instead of a 32-bit hash, which collided easily enough to defeat the check.
- `/admin/create-table` now verifies the `CREATE TABLE` statement's table name matches the declared table name, rejecting a schema that would create a differently-named table.
- `/admin/init` now rejects non-numeric `numShards`/`totalVBuckets` instead of silently propagating `NaN` into shard/vbucket provisioning (which mapped every vbucket to a nonexistent `shard-NaN` while still returning 200 OK).
- `/admin/audit-log` is now actually reachable through the Worker — it was documented and tested against `CatalogDO` directly, but never registered in the Worker's route table.
- Replaced raw `BEGIN`/`COMMIT` SQL (rejected by Durable Object SQLite storage) with `ctx.storage.transactionSync()`.
- Fixed `/stats` querying Cloudflare's internal `_cf_METADATA` system table.
- Added a schema migration guard so pre-existing catalog/shard tables get missing columns backfilled instead of failing.
- Added error boundaries so malformed request bodies return a clean error response instead of an unhandled crash.
