# cloudflare-shard-tpcc-benchmark

A TPC-C-derived OLTP demo and benchmark for CloudflareShard (issue #16), adapted to this project's actual query primitives (`/v1/mutate`, `/v1/tx`, `/v1/index-query`, `/v1/table-scan`) over the existing HTTP API.

**This is not TPC-DS** (the analytics/decision-support benchmark originally suggested — CloudflareShard has no join or aggregation engine, deliberately, so it doesn't fit). **This is also not an official or certifiable TPC-C submission** — several deliberate, documented adaptations were needed to fit this project's architecture. Read "Deviations from official TPC-C" below before quoting any numbers this produces as if they were a real TPC-C result.

## What this actually demonstrates

CloudflareShard's `CoordinatorDO` 2PC and sharded `mutate` path under a standard, realistic OLTP transaction mix: 9 tables (warehouse/district/customer/order/order-line/stock/etc.), all 5 TPC-C transaction types, run at a configurable scale against a local `wrangler dev` instance or a live deployment.

## Quick start

```bash
cd examples/tpc-c-benchmark

# 1) Seed a cluster (needs /admin/init already called against the target — see the main README's quickstart step 1)
node src/run.mjs seed --base-url http://localhost:8787 --admin-token $ADMIN_TOKEN

# 2) Run the benchmark
node src/run.mjs benchmark --base-url http://localhost:8787 --duration 30
```

`seed` writes tenant tokens + cached reference data to `.tpcc-tenants.json` (gitignored — it holds plaintext bearer tokens, never commit it). `benchmark` reads that same file, so you don't need to re-seed between runs unless you want fresh data.

### `seed` options

| Flag | Default | Notes |
|---|---|---|
| `--warehouses` | 2 | One tenant per warehouse (see "Tenancy model" below) |
| `--districts-per-warehouse` | 10 | Matches the official spec |
| `--customers-per-district` | 100 | Official spec: 3000 — reduced for faster demo seeding |
| `--items` | 200 | Official spec: 100,000 — reduced for faster demo seeding, and to keep New-Order's stock-row collision rate low (see below) |
| `--tenants-file` | `.tpcc-tenants.json` | Where tenant tokens + cached reference data are written |
| `--start-warehouse` | 1 | Warehouse IDs (and their tenant IDs / row keys) are deterministic, so seeding always starting at 1 means you can't add a *fresh* batch of warehouses to a cluster that already has warehouse 1..K seeded without colliding — pick a higher starting ID instead |
| `--seed-orders` | off | Also seeds one pre-existing order (+5-15 order lines) per customer, ~30% left undelivered — matches the official spec's initial-load convention, but is by far the most expensive part of seeding (thousands of extra `order_line` rows, and a proportionally slower index backfill). Off by default so the out-of-the-box seed stays fast; New-Order transactions create real orders for Delivery to act on within seconds of a benchmark run starting anyway |

**How long seeding actually takes**: measured locally (Windows, `wrangler dev`) at the defaults above (2 warehouses, no `--seed-orders`): **~1m45s**. Each HTTP round trip to a local `wrangler dev` instance costs roughly 300-500ms (Durable Object dispatch overhead, not something this benchmark's own code controls), and seeding issues on the order of 2,800 individual `/v1/mutate` calls at that scale — the time is almost entirely round-trip count × per-call latency, not computation. Scaling `--warehouses`/`--customers-per-district`/`--items` up multiplies seed time roughly proportionally; a live deployment will typically have lower per-call latency than a local dev instance, but network round-trip time enters instead.

### `benchmark` options

| Flag | Default | Notes |
|---|---|---|
| `--duration` | *(required)* | Seconds to run |
| `--concurrency` | 10 | Concurrent workers, each looping continuously |
| `--tenants-file` | `.tpcc-tenants.json` | Must match what `seed` wrote |
| `--base-url` | *(from tenants file)* | Override if reusing a tenants file against a different host |

Each worker picks one of the 5 transaction types at random, weighted to the standard TPC-C mix (45% New-Order, 43% Payment, 4% Order-Status, 4% Delivery, 4% Stock-Level), runs it against a randomly chosen warehouse/district/customer, and records `{type, ok, latencyMs, error?}`.

## Reading the report

```
=== TPC-C-derived benchmark report (CloudflareShard, issue #16) ===
Duration: 30s
Total transactions attempted: 142
tpmC-equivalent (successful New-Order transactions / minute): 118.00

Per-transaction-type latency (ms, successful attempts only) and outcome counts:
type            n     ok    err      p50      p95      p99
payment           61    61     0    1001.7   1493.6   1899.8
new-order         61    59     2    1969.2   3202.0   3717.4
order-status       5     5     0     445.0    667.3    667.3
stock-level        7     7     0     132.9    310.6    310.6
delivery           8     8     0   20179.7  20485.0  20485.0
```

- **tpmC-equivalent** mirrors real TPC-C's actual throughput metric: *successful New-Order transactions per minute* specifically, not total transaction count.
- **Per-type latency** is computed over successful attempts only; `err` counts failures separately (see below for why New-Order shows some).
- **Delivery is expected to be by far the slowest transaction type** — it's not a bug. One Delivery "transaction" here processes every district in the warehouse in one call (matching the real spec's batch-style semantics), each district potentially doing several index-query round trips plus a commit plus per-line updates. The official TPC-C spec itself acknowledges Delivery is the heavy one: it's the one transaction type the spec explicitly permits to run deferred/asynchronously rather than interactively, for exactly this reason.
- **A small New-Order `TX_ABORTED` rate is expected, not a bug**: every New-Order transaction's first step (incrementing the district's `d_next_o_id`) contends on that district's single row — the same well-known "hot row" property real TPC-C New-Order has. At this benchmark's default scale (2 warehouses × 10 districts = 20 contend-able rows) under concurrency 10, expect roughly low-single-digit percent of New-Order attempts to abort and need a retry (this benchmark does not auto-retry — a caller of this benchmark, or a real application, could choose to). This rate drops further with more warehouses (more districts spreads contention out) and rises with higher concurrency relative to warehouse count — if you see a much higher rate, check your `--warehouses`/`--concurrency` ratio before assuming something's broken.

## Tenancy model

**One tenant per warehouse** (`tpcc-w0001`, `tpcc-w0002`, ...). This is a CloudflareShard-specific design choice, not part of the TPC-C spec: it makes every warehouse's tables naturally isolated (a `table-scan` or `index-query` scoped to one warehouse's tenant only ever sees that warehouse's own rows, with no extra filtering needed), and it's the only way this benchmark can use CloudflareShard's actual tenant-scoped read primitives at all.

## Deviations from official TPC-C

These are deliberate, not oversights — each is a direct consequence of CloudflareShard's real constraints (documented in code comments at the point of each deviation, in `src/transactions.mjs` and `src/generate.mjs`):

1. **Order-Status and Payment's by-customer-*name* lookup variants are dropped; both are ID-only.** The official spec's by-name variant needs a substring/text-search capability CloudflareShard deliberately doesn't have (its only query primitives are exact-tuple `index-query`, cursor-paginated `table-scan`, and mechanically-constructed `mutate` — see the main repo README's "Tenant authorization" section for why raw SQL search isn't offered to tenants).
2. **Stock-Level's server-side aggregate is replaced with `table-scan` + client-side counting.** No aggregation pushdown exists in CloudflareShard at all (this is the same architectural reason TPC-DS was ruled out for this project entirely — see the main issue).
3. **New-Order's atomicity is split, not one single all-encompassing transaction.** `/v1/tx` caps a single call at 8 distinct rows (`MAX_TX_PARTICIPANT_KEYS` in `src/index.ts`); a New-Order with the spec's default 5-15 order lines could touch 30+ distinct rows if attempted as one transaction. Instead: one small transaction covers the order header (district update + order insert + new-order insert, 3 rows), and each order line's insert + its stock-row update is its OWN 2-row transaction. Per-line atomicity is preserved (an order line and its stock decrement can never land as only one of the two); whole-order atomicity across all lines is not.
4. **Remote (cross-warehouse) order lines fall back to two independent, non-atomic writes.** `/v1/tx` requires every mutation in one call to share the same tenant; combined with the one-tenant-per-warehouse model, an order line supplied from a different warehouse can never be one atomic transaction (its `order_line` row and the supply warehouse's `stock` row belong to two different tenants). This affects ~1% of order lines, matching the official spec's own cross-warehouse rate.
5. **Reduced default cardinalities** (see the `seed` options table above) for faster demo seeding — scale them up via CLI flags if you want numbers closer to the official spec's.
6. **Immutable reference data (item name/price) is cached client-side after seeding, rather than re-read from the server on every transaction.** The official spec mandates fresh reads of these fields on every transaction; since they never change post-load, caching them is a deliberate simplification that meaningfully reduces round trips without affecting correctness of the actual transaction semantics being measured (writes to `district`/`warehouse`/`customer`'s *mutable* fields — tax-adjacent YTD counters, balances — are always read fresh, since those genuinely do change).
7. **The item catalog is duplicated per warehouse-tenant rather than shared globally.** Real TPC-C has one global item catalog across all warehouses; CloudflareShard's tenant-scoped read primitives have no cross-tenant read path, so each warehouse-tenant gets its own identical copy instead.
8. **Pre-existing orders at seed time are opt-in (`--seed-orders`), not default-on** — see the `seed` options table above for why.
9. **Counter updates (stock quantity/YTD, warehouse/district YTD, customer balance/payment/delivery counts) use a compare-and-swap `where` guard, converting a possible silent-corruption race into a silent no-op instead.** CloudflareShard's structured mutations only support SETting a column to a literal client-supplied value — there's no server-side arithmetic UPDATE (`col = col - ?`) — so every counter increment/decrement here is computed client-side from a prior read, then written back. Two concurrent transactions reading the same hot row (a popular item's stock, a warehouse's YTD total) before either commits could otherwise silently overwrite each other's effect with a stale value. Each such write here includes a `where` clause matching every field it read: if another write beat it to the row, the predicate won't match and the write becomes a no-op instead of corrupting the counter. Delivery's customer-credit and New-Order's stock decrement additionally have real, checked recovery paths built on top of this guard (see items below) — this isn't just documentation, it's load-bearing for both.
10. **New-Order's `new_order` marker (what makes an order visible to Delivery) is inserted last, only once every order line has committed and, if any line failed, the already-committed lines and header have been compensated (reversed) first.** Two real bugs were found and fixed here across three rounds of review, each verified live against fresh warehouses under contention: (1) inserting the marker alongside the header, up front, let a concurrent Delivery pass observe an order that was still mid-flight, summing an incomplete set of lines and orphaning whichever hadn't landed yet — fixed by deferring the marker insert to the very end. (2) `runPool`'s own `Promise.all` used to reject on the first line to fail without waiting for other still-in-flight lines, so other lines could keep committing in the background after the function had already thrown past the marker insert — fixed by catching each line's own error inside the pool instead of letting it propagate, so every line is always awaited to completion first. (3) even after both of those fixes, a genuinely failed line still left its already-committed siblings and the header permanently uncompensated — fixed with a real compensating-transaction step (`compensateFailedOrder`): each already-committed line's stock update is reversed via its own compare-and-swap (matching exactly the values that line applied, so it safely no-ops instead of corrupts if a different write touched the row again meanwhile), its order_line row is deleted, and the orphaned `orders` row is deleted (leaving an expected, harmless gap in the o_id sequence — `d_next_o_id` is deliberately not decremented back, since a different, newer order may have already claimed the incremented value). Verified live: zero orphaned/incomplete orders found by cross-referencing orders/order_line/new_order server-side after a high-concurrency, small-item-pool stress run against fresh warehouses. Genuinely residual after all three fixes: a third concurrent write landing in the narrow window between a line's update and its own compensation reversal makes that reversal safely no-op rather than fully undo — doubly unlikely, and results in a stranded stock decrement, not silent corruption.
11. **Delivery claims a district's oldest undelivered order via its own checked `/v1/mutate` delete (not as part of the closing transaction) before touching anything else**, so a concurrent Delivery worker that read the same "oldest" order (before the first worker's claim committed) sees `rowsAffected: 0` on its own claim attempt and correctly skips the order as already handled — rather than proceeding to re-credit the customer for an order someone else already delivered. This was a real bug (found by review): the marker's delete used to be bundled into the closing `/v1/tx` alongside the order/customer updates, whose response doesn't report per-mutation `rowsAffected` — a second, stale-read worker's delete would silently no-op there while its customer credit could still land.

None of this changes what the benchmark actually demonstrates: real `CoordinatorDO` 2PC transactions, real sharded `mutate` writes, real `index-query`/`table-scan` reads, under a realistic (if not officially-compliant) OLTP transaction mix and cardinality.
