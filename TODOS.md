# TODOS

## Architecture

### Re-evaluate CoordinatorDO keying against real transaction volume

**What:** Confirm one-DO-per-transaction (`env.COORDINATOR.idFromName(txId)`, no sharding) stays the right choice once real transaction volume exists, or re-introduce a sharded pool (`coordinator-${hash(txId) % N}`) if cold-start latency becomes a measurable problem at scale.

**Why:** The cost-model analysis that chose one-DO-per-transaction was reasoning from Cloudflare's published billing model and this project's realistic near-term (self-hosted, pre-product, low-volume) scale — not from observed production numbers, since none exist yet.

**Context:** If sustained transaction volume gets high enough that coordinator cold-starts become a measurable latency cost, re-introducing sharding is a bounded, mostly-mechanical change (only the DO-keying line changes; the WAL schema and 2PC orchestration logic are unaffected). Re-key is more disruptive once live transaction state exists — better to check before volume grows, not after.

**Effort:** S
**Priority:** P3
**Depends on:** Milestone 1 shipping with real usage data available.

### Evaluate deprecating raw `/v1/sql` mutations in favor of the structured DSL

**What:** Decide whether the raw-SQL mutation path and the new structured-mutation DSL (`/v1/mutate`, `/v1/tx`) should coexist long-term, or whether raw SQL mutations should be deprecated/admin-only once the structured path covers the common cases.

**Why:** Codex flagged that two first-class write paths with different row-ownership guarantees (raw SQL: trust-based; structured: enforced) is a non-auditable correctness story and a security-footgun risk — a caller could always fall back to the weaker path. Milestone 2's `/plan-eng-review` added a third reason: raw `/v1/sql` mutations bypass every index-maintenance mechanism entirely, silently desyncing any index on a table they touch — Milestone 2's Chunk 1 closes this specific hole with a 409 rejection, but it's the same underlying dual-write-path problem surfacing again.

**Context:** Milestone 2 (Index Service) will also depend on structured mutations for index maintenance, which may tip the balance toward deprecating raw SQL mutations entirely. Not urgent for Milestone 1 since raw `/v1/sql` isn't used for coordinated transactions, but worth deciding before the dual-path pattern calcifies into tenant-facing API surface people build against.

**Effort:** M
**Priority:** P3
**Depends on:** Milestone 2 (Index Service) — revisit once its structured-mutation dependency is clear.

### CLI/SDK wrapper or write-path consolidation

**What:** Decide whether `/v1/sql`, `/v1/mutate`, and `/v1/tx` should stay three separate raw-HTTP contracts developers learn individually, or whether a CLI/SDK wrapper (or eventual deprecation of raw SQL mutations) is needed to keep the developer experience coherent as the write-path count grows.

**Why:** Both DX-review voices independently flagged three coexisting write paths with different guarantees (raw-SQL trust-based, structured-DSL row-owned, coordinated-tx atomic) as confusing without a unifying interface, and noted the lack of any SDK/CLI feels primitive against competitors (Turso/libSQL, PlanetScale-style tooling) that ship one.

**Context:** Not urgent for Milestone 1's initial ship, but worth revisiting once Milestone 2 (Index Service) adds a fourth structured-mutation consumer (index maintenance) — the case for consolidation gets stronger each time a new path is added rather than reusing the existing ones.

**Effort:** M
**Priority:** P3
**Depends on:** None — can be decided independently, informed by real developer feedback post-Milestone-1.

### Choose an OSS license

**What:** Pick and add a LICENSE file (MIT/Apache-2.0/etc.) now that the distribution model is resolved to self-hosted/OSS-first.

**Why:** A public self-hosted template without a stated license leaves adopters unsure what they're allowed to do with the code.

**Context:** Surfaced as a deferred item during the `/office-hours` session that resolved the distribution/positioning decision (2026-07-09). Small, independent decision — doesn't block Milestone 1 implementation.

**Effort:** S
**Priority:** P2
**Depends on:** None.

### Automatic split heuristics

**What:** Auto-detect a hot or oversized shard and trigger a split, instead of requiring a manual `/admin/split-vbucket` call.

**Why:** The project's own README has listed this as an unbuilt "next production step" since the MVP was first written.

**Context:** Natural v3 item once Approach B's Milestones 1 and 2 ship. Building heuristics before the underlying split mechanism is proven would be premature — heuristics tuning only makes sense once splits themselves are reliable.

**Effort:** L
**Priority:** P3
**Depends on:** Milestone 1 (Transaction Coordinator) and Milestone 2 (Index Service).

### Index-entry topology: physical source_shard_id vs. logical identity + read-time routing

**What:** Decide whether `__cf_indexes` entries store the physical `source_shard_id` they were written on (current Milestone 2 design), or store logical identity `(tenantId, table, partitionKey)` and resolve the owning shard at read time instead.

**Why:** Storing `source_shard_id` bakes topology into the data — every split, drain, or backfill has to rewrite index entries just because placement changed. The logical-identity alternative avoids that rewrite cost at the price of an extra routing hop on every index query. Flagged by an independent Codex outside-voice pass during Milestone 2's `/plan-eng-review`.

**Context:** Real enough to matter once splits start happening on a table with a live index, but deciding now (before Chunk 2/5 implementation experience exists) would be premature — noted as an Open Question in the Milestone 2 design doc for those chunks to weigh directly, not deferred past Milestone 2 entirely.

**Effort:** S (a design decision, not implementation work itself)
**Priority:** P3
**Depends on:** Milestone 2 Chunks 2 and 5 (async maintenance, drain interaction).

### Cross-tenant/cross-shard analytics aggregation

**What:** Support queries like "total orders across all tenants yesterday" without requiring a separate analytics store.

**Why:** README explicitly documents this as unsupported by design — per-tenant/per-shard isolation is the point of the architecture; `/v1/scatter` + app-level aggregation is the only current path.

**Context:** Landscape research during `/office-hours` found the same limitation acknowledged industry-wide across other D1-sharding write-ups. Capturing this prevents a future contributor from assuming it's already solved. May never be worth building directly into the sharded transactional path — a real analytics need might be better served by exporting to an external OLAP store than adding this here.

**Effort:** M
**Priority:** P4
**Depends on:** None — a future scope decision, not committed work.

## Completed

### Distribution/positioning decision

**What:** Decide open source library, hosted control plane, or embedded Worker template.

**Why:** Architecturally load-bearing — determines who owns DO namespaces, migrations, secrets, upgrades, and customer data access.

**Resolution:** Ship self-hosted/OSS-first now, with an explicit operator/tenant identity seam so a hosted control plane can be added later without a rewrite. Resolved via a dedicated `/office-hours` session after both CEO-review voices flagged it as blocking Milestone 1's coordinator design.

**Completed:** v1.0.0.0 (2026-07-09)
