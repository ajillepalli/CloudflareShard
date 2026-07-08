# TODOs

## Distribution/positioning decision

**What:** Decide open source library, hosted control plane, or embedded Worker template.
**Why:** Architecturally load-bearing — determines who owns DO namespaces, migrations, secrets, upgrades, and customer data access — but explicitly unresolved.
**Pros:** Cheap to decide early; unblocks downstream architecture decisions that currently assume "figure it out later."
**Cons:** None specific — it's genuinely undecided, not a tradeoff being avoided.
**Context:** Raised in the `feature/next-stage` design doc's Open Questions and independently flagged by a Codex outside-voice review as something "the architecture cannot be finalized while unresolved." The Assignment's customer interviews (see design doc) may surface signal on this.
**Depends on:** Nothing blocking; can be decided independently, though customer interview feedback may inform it.

## Automatic split heuristics

**What:** Auto-detect a hot or oversized shard and trigger a split, instead of requiring a manual `/admin/split-vbucket` call.
**Why:** The project's own README has listed this as an unbuilt "next production step" since the MVP was first written.
**Pros:** Removes manual ops toil once the automated split/rebalance machinery (Milestone 1/2 in the design doc) exists.
**Cons:** Building heuristics before the underlying split mechanism is proven would be premature — heuristics tuning only makes sense once splits themselves are reliable.
**Context:** Natural v3 item once Approach B's Milestones 1 and 2 ship.
**Depends on:** Milestone 1 (Transaction Coordinator) and Milestone 2 (Index Service).

## Cross-tenant/cross-shard analytics aggregation

**What:** Support queries like "total orders across all tenants yesterday" without requiring a separate analytics store.
**Why:** README explicitly documents this as unsupported by design — per-tenant/per-shard isolation is the point of the architecture; `/v1/scatter` + app-level aggregation is the only current path.
**Pros:** Capturing this prevents a future contributor from assuming it's already solved.
**Cons:** May never be worth building directly into the sharded transactional path — a real analytics need might be better served by exporting to an external OLAP store than adding this here.
**Context:** Landscape research during `/office-hours` found the same limitation acknowledged industry-wide across other D1-sharding write-ups.
**Depends on:** Nothing blocking; a future scope decision, not committed work.
