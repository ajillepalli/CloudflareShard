# TODOS

## Architecture

### Validate CoordinatorDO sharded-pool choice against real transaction volume

**What:** Confirm the sharded coordinator pool (`coordinator-${hash(txId) % N}`, many transactions per DO instance) is actually justified versus the simpler one-DO-per-transaction alternative, using real observed transaction volume once Milestone 1 ships.

**Why:** Both CEO-review voices independently flagged this as asserted rather than argued — no cost model (expected tx/sec, DO instantiation cost) exists anywhere in the Milestone 1 plan. It was a deliberate, informed choice at plan time, but it's a decision worth re-checking with data rather than aesthetics once there's real usage to look at.

**Context:** If per-transaction DOs turn out to be cheap enough and volume is low, the sharded pool's added complexity (CPU-budget management per alarm tick, cross-transaction interference risk within one shard) may not be earning its keep. Re-key is possible but disruptive once live transaction state exists — better to check early.

**Effort:** S
**Priority:** P3
**Depends on:** Milestone 1 shipping with real usage data available.

### Evaluate deprecating raw `/v1/sql` mutations in favor of the structured DSL

**What:** Decide whether the raw-SQL mutation path and the new structured-mutation DSL (`/v1/mutate`, `/v1/tx`) should coexist long-term, or whether raw SQL mutations should be deprecated/admin-only once the structured path covers the common cases.

**Why:** Codex flagged that two first-class write paths with different row-ownership guarantees (raw SQL: trust-based; structured: enforced) is a non-auditable correctness story and a security-footgun risk — a caller could always fall back to the weaker path.

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

### Autoincrement-safe coordinated transactions

**What:** Extend Milestone 1's `pending_intents` prepare/abort mechanism to handle `INTEGER PRIMARY KEY AUTOINCREMENT` tables without leaking sequence gaps on every aborted transaction.

**Why:** SQLite's `ROLLBACK` doesn't reset `sqlite_sequence`, so the prepare-then-forced-rollback validation trick in Milestone 1's Chunk 2 permanently consumes an autoincrement ID even when the transaction never commits. Milestone 1 documents this as a known boundary and excludes such tables from coordinated transactions rather than fixing it.

**Context:** Only matters if/when a tenant wants a table with gapless or tightly-packed autoincrement IDs to participate in cross-shard transactions. Likely low priority — most tenant schemas can use non-sequential keys (UUIDs, the existing partition-key convention) instead.

**Effort:** M
**Priority:** P4
**Depends on:** Milestone 1 (Transaction Coordinator) shipping first.

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
