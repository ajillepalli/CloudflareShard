# TODOS

## Architecture

### Ring-evacuation vs. stale fixed-target index writes (residual risk)

**What:** During a shard drain's ring evacuation, `advanceDrain` copies the draining shard's `__cf_indexes` entries to the deterministic substitute, repoints `placement_ring_json` cluster-wide, runs a reconcile loop to catch entries that raced onto the draining shard during the evacuation's async gaps, then deletes the source copies. This closes the dominant race (a write whose `/route` resolved with the old ring landing on the draining shard mid-evacuation is caught by the reconcile loop, since after the repoint no *new* route targets the draining shard). **Residual:** a `index_pending_jobs` retry enqueued before the repoint carries a *fixed* target shard id (the draining shard) and does not re-resolve the ring, so if it fires after the reconcile loop's final pass it writes an entry to the draining shard that the delete then removes — the base row survives but its index entry is lost. The reconcile loop is capped (`RING_EVAC_RECONCILE_MAX_PASSES`); pathological churn leaves source rows in place (unreachable, not deleted) rather than losing them.

**Why not fully closed here:** the complete fix is a ring-version stamped at index-write time, rejected by the index shard if stale, with the writer (including the `index_pending_jobs` retry) re-resolving the ring on rejection — a cross-cutting change to the index-write path, the retry-queue schema, and `/route`. Out of proportion to a rare admin operation (drain) racing an async index-maintenance retry whose target shard is simultaneously being evacuated.

**Mitigations already in place:** index writes are best-effort and self-healing — a subsequent write to the same row re-derives and re-writes the entry against the current ring; and the drain's own feasibility checks plus the reconcile loop bound the window to the narrow interval between the final reconcile pass and the source delete.

**Effort:** M
**Priority:** P3
**Depends on:** a ring-version mechanism on the index-write path.

### Unique-index support for the Index Service

**What:** Support rejecting a write that would violate a declared uniqueness constraint on a registered index, instead of today's non-unique-only model (`__cf_indexes` uses `INSERT OR REPLACE`, no constraint check).

**Why:** Deliberately scoped out of Milestone 2 entirely (Chunk 7) — no chunk in that plan allocated space to build it. Real uniqueness enforcement needs either a `UNIQUE` constraint at the index-shard level or explicit pre-check-plus-lock coordination to close the race between two concurrent writes both claiming to be first; both are genuine design work, not a small addition.

**Context:** Milestone 2 itself shipped ahead of validated demand (see its design doc's Demand Evidence — no named user or query exists yet). Building uniqueness enforcement on top of an already-ahead-of-demand feature would compound that, not reduce it. Revisit once a real adopter's schema actually needs a unique secondary index (e.g. "email must be unique per tenant").

**Effort:** M
**Priority:** P3
**Depends on:** A real adopter's use case — not committed work.

### Re-evaluate CoordinatorDO keying against real transaction volume

**What:** Confirm one-DO-per-transaction (`env.COORDINATOR.idFromName(txId)`, no sharding) stays the right choice once real transaction volume exists, or re-introduce a sharded pool (`coordinator-${hash(txId) % N}`) if cold-start latency becomes a measurable problem at scale.

**Why:** The cost-model analysis that chose one-DO-per-transaction was reasoning from Cloudflare's published billing model and this project's realistic near-term (self-hosted, pre-product, low-volume) scale — not from observed production numbers, since none exist yet.

**Context:** If sustained transaction volume gets high enough that coordinator cold-starts become a measurable latency cost, re-introducing sharding is a bounded, mostly-mechanical change (only the DO-keying line changes; the WAL schema and 2PC orchestration logic are unaffected). Re-key is more disruptive once live transaction state exists — better to check before volume grows, not after.

**Effort:** S
**Priority:** P3
**Depends on:** Milestone 1 shipping with real usage data available.

### Structured tenant read API + physical `tenant_id`

**What:** Add a structured, isolation-enforced tenant READ path — a partition-scoped `SELECT` (single vbucket) that the shard executes with an enforced `tenant_id` predicate — to replace the general tenant read path removed when `/v1/sql` became admin-only. Today tenants can only read via `/v1/index-query` (secondary-index lookups); there is no way to do a partition-scoped scan of one's own rows.

**Why:** Removing raw tenant `/v1/sql` (see Completed) closed a cross-tenant read leak but left tenants with no general read path. The blocker for a safe one is physical: **base rows carry no `tenant_id` column** (SPEC §14) — the logical identity lives only in `__cf_row_owners`, so a shard can't add `WHERE tenant_id = ?` to a tenant's `SELECT`, and two tenants' rows can share a vbucket. A safe tenant read therefore needs base rows to carry a physical `tenant_id` (written on every mutation, indexed), after which the gateway can offer a structured partition-scoped read that injects the tenant predicate the same way `/v1/mutate` injects the partition-key predicate.

**Context:** This is the replacement for the removed read path, not a nice-to-have — without it tenants cannot read arbitrary rows at all. Adding a physical `tenant_id` is a schema/write-path change (touches `/v1/mutate`, `/v1/tx` apply, and migration import) and should be scoped with the read API together.

**Effort:** L
**Priority:** P2
**Depends on:** physical `tenant_id` on base rows (schema + write paths).

### CLI/SDK wrapper or write-path consolidation

**What:** Decide whether `/v1/sql`, `/v1/mutate`, and `/v1/tx` should stay three separate raw-HTTP contracts developers learn individually, or whether a CLI/SDK wrapper (or eventual deprecation of raw SQL mutations) is needed to keep the developer experience coherent as the write-path count grows.

**Why:** Both DX-review voices independently flagged three coexisting write paths with different guarantees (raw-SQL trust-based, structured-DSL row-owned, coordinated-tx atomic) as confusing without a unifying interface, and noted the lack of any SDK/CLI feels primitive against competitors (Turso/libSQL, PlanetScale-style tooling) that ship one.

**Context:** Not urgent for Milestone 1's initial ship, but worth revisiting once Milestone 2 (Index Service) adds a fourth structured-mutation consumer (index maintenance) — the case for consolidation gets stronger each time a new path is added rather than reusing the existing ones.

**Effort:** M
**Priority:** P3
**Depends on:** None — can be decided independently, informed by real developer feedback post-Milestone-1.

### Automatic split heuristics

**What:** Auto-detect a hot or oversized shard and trigger a split, instead of requiring a manual `/admin/split-vbucket` call.

**Why:** The project's own README has listed this as an unbuilt "next production step" since the MVP was first written.

**Context:** Natural v3 item once Approach B's Milestones 1 and 2 ship. Building heuristics before the underlying split mechanism is proven would be premature — heuristics tuning only makes sense once splits themselves are reliable. **Update (Milestone 3, 2026-07-10):** the mechanism now fully exists — `/admin/split-vbucket` performs a real online migration (dual-write backfill, fenced checksum-verified cutover) and `/admin/drain-shard` fully evacuates a shard. What remains is exactly this TODO's original scope: deciding *when* to trigger a split (shard size / QPS / latency thresholds, per SPEC §11's trigger conditions), which needs real usage data to tune against.

**Effort:** L
**Priority:** P3
**Depends on:** ~~Milestone 1 (Transaction Coordinator) and Milestone 2 (Index Service).~~ Mechanism shipped (Milestone 3); heuristics need production usage data.

### Cross-tenant/cross-shard analytics aggregation

**What:** Support queries like "total orders across all tenants yesterday" without requiring a separate analytics store.

**Why:** README explicitly documents this as unsupported by design — per-tenant/per-shard isolation is the point of the architecture; `/v1/scatter` + app-level aggregation is the only current path.

**Context:** Landscape research during `/office-hours` found the same limitation acknowledged industry-wide across other D1-sharding write-ups. Capturing this prevents a future contributor from assuming it's already solved. May never be worth building directly into the sharded transactional path — a real analytics need might be better served by exporting to an external OLAP store than adding this here.

**Effort:** M
**Priority:** P4
**Depends on:** None — a future scope decision, not committed work.

## Completed

### Evaluate deprecating raw `/v1/sql` mutations in favor of the structured DSL

**What:** Decide whether raw-SQL and the structured DSL (`/v1/mutate`, `/v1/tx`) coexist long-term, or whether raw SQL should be deprecated/admin-only.

**Why:** Two first-class write paths with different row-ownership guarantees (raw SQL: trust-based; structured: enforced) is a non-auditable correctness story and a security footgun — a caller could always fall back to the weaker path — and raw `/v1/sql` mutations also bypass every index-maintenance mechanism.

**Resolution (Milestone 3 — went further than "mutations"):** raw `/v1/sql` is now **fully admin-only — reads AND writes**, not just mutations. The per-tenant write guard against a passthrough SQL string proved structurally unwinnable (six leaked bypasses), and there is no safe tenant `SELECT` while base rows carry no physical `tenant_id` (a partition-scoped raw read could return another tenant's rows in the same vbucket). The trust-based tenant path was removed entirely: tenants write via `/v1/mutate` + `/v1/tx` and read via `/v1/index-query`; `/v1/sql` requires `ADMIN_TOKEN` (operator/debugging), with a residual guardrail blocking operator writes to internal bookkeeping tables. The follow-on "Structured tenant read API + physical `tenant_id`" item (Architecture) tracks the replacement read path.

**Completed:** Milestone 3 (2026-07-12)

### Index-entry topology: physical source_shard_id vs. logical identity + read-time routing

**What:** Decide whether `__cf_indexes` entries store the physical `source_shard_id` they were written on (the Milestone 2 design), or store logical identity and resolve the owning shard at read time instead.

**Why:** Storing `source_shard_id` bakes topology into the data — every split, drain, or backfill has to rewrite index entries just because placement changed. Flagged by an independent Codex outside-voice pass during Milestone 2's `/plan-eng-review`; the post-implementation eng-review confirmed the concrete failure (index placement hashing over the *live* active shard set orphans entries on any shard-count change), which Milestone 2 mitigated by 409-blocking both topology operations while any index existed.

**Resolution (Milestone 3, Chunk 2 — logical identity won, plus a pinning the question didn't originally include):** `__cf_indexes` entries now carry `tenant_id`, and `/v1/index-query` hydration re-routes per entry at read time (`hash(tenant_id:table:partition_key)` → `vbucket_map` → current shard) — moved base rows are always found, and no topology change ever rewrites entries for placement's sake. Index-shard *placement* is additionally pinned per index (`index_rules.placement_ring_json`, captured at `/admin/create-index`), so the live shard set never re-hashes existing entries; a drained ring member is substituted deterministically with its entries copied (Chunk 5). Both former 409 blocks (`SPLIT_BLOCKED_BY_INDEXES`, `SHARD_DRAIN_BLOCKED_BY_INDEXES`) are removed. The accepted cost is the predicted one: one extra routing hop per hydrated match on `/v1/index-query`.

**Completed:** Milestone 3 (2026-07-10)

### Choose an OSS license

**What:** Pick and add a LICENSE file (MIT/Apache-2.0/etc.) now that the distribution model is resolved to self-hosted/OSS-first.

**Why:** A public self-hosted template without a stated license leaves adopters unsure what they're allowed to do with the code.

**Resolution:** Apache-2.0, added at the repo root in Milestone 3's docs chunk — the explicit patent grant suits infrastructure software that adopters embed in their own deployments.

**Completed:** Milestone 3 (2026-07-10)

### Distribution/positioning decision

**What:** Decide open source library, hosted control plane, or embedded Worker template.

**Why:** Architecturally load-bearing — determines who owns DO namespaces, migrations, secrets, upgrades, and customer data access.

**Resolution:** Ship self-hosted/OSS-first now, with an explicit operator/tenant identity seam so a hosted control plane can be added later without a rewrite. Resolved via a dedicated `/office-hours` session after both CEO-review voices flagged it as blocking Milestone 1's coordinator design.

**Completed:** v1.0.0.0 (2026-07-09)
