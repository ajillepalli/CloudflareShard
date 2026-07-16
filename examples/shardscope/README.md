# Shardscope

Shardscope is CloudflareShard's mission-control dashboard — a live view into a running cluster's topology, health, and resharding behavior. It is a separate demo Worker, service-bound to the main `cloudflare-shard-mvp` Worker's RPC entrypoint (the same pattern `../rpc-consumer` demonstrates), not part of the CloudflareShard core in `../../src`.

The memorable thing it's built around (see `./DESIGN.md` for the full design system): **"it healed itself under fire and lost nothing."** The hero moment is watching a cluster reshard under load — and under attack — while a scoreboard never leaves `lost: 0`. Composure is the product; red never means "the system is unhealthy," only "this shard is hot" or "an attack is landing."

## The four rooms

Per `./DESIGN.md`'s layout section, Shardscope is a single-page app with a fixed shell (top invariant scoreboard, left icon rail, center canvas, right console) and four rooms reachable from the icon rail:

- **App** — overview / landing.
- **Topology** — the living topology canvas: shard ownership, vBucket placement, in-flight migrations. The primary object on screen; not a metric-tile grid.
- **Reshard** — operator controls and status for driving/observing a resharding operation.
- **Playground / Chaos** — inject load and simulated faults ("break it") and watch the invariant scoreboard hold.

None of the rooms are built yet. This skeleton only proves the Worker boots, serves a themed placeholder page, and has a wired (but stubbed) path from browser to Durable Object over SSE.

## Two-tier auth model

Shardscope holds two distinct secrets server-side; the browser never sees either:

- **`ADMIN_TOKEN`** — authorizes this Worker's calls into `cloudflare-shard-mvp`'s `/admin/*` surface (topology reads today; any topology-mutating control this dashboard grows must pass this the same way the admin HTTP API or `CloudflareShardRpc`'s `adminXxx(adminToken, ...)` RPC methods require).
- **`SHARDSCOPE_GATE_TOKEN`** — authorizes a browser to talk to Shardscope itself (open `/api/stream`, eventually drive controls). Distinct question from the one above: "is this viewer allowed to watch/operate Shardscope" vs. "is this call allowed to touch the cluster."

See `src/index.ts`'s header comment and `src/env.d.ts` for the full explanation and the TODO marking where `SHARDSCOPE_GATE_TOKEN` enforcement needs to land before this ships for real.

## Running it locally

Same two-process pattern as `../rpc-consumer`:

```bash
# Terminal 1, from the repo root:
npm run dev  # main Worker, defaults to http://localhost:8787

# Terminal 2, from this directory:
npm install
npm run dev -- --port 8789  # Shardscope
```

Wrangler's local dev registry connects `SHARD_API` automatically once both dev servers are up — watch for `env.SHARD_API (cloudflare-shard-mvp#CloudflareShardRpc) [connected]` in this Worker's dev output.

## Status: Phase 1 skeleton

This is the initial scaffold only. What's real vs. stubbed:

- **Real:** package/tsconfig/wrangler config; the `SHARD_API` service binding; the `AGGREGATOR` Durable Object binding + migration; a dark-themed placeholder page at `/`; an `/api/stream` route that forwards to a singleton `TopologyAggregator` DO and gets back one real SSE `hello` event.
- **Stubbed, pending `/admin/vbucket-map`:** `TopologyAggregator.alarm()` in `src/aggregator.ts` — the real shared poll of `/admin/status` + `/admin/vbucket-map` + fanned `/admin/shard-stats`, merged into one snapshot and fanned out to all SSE subscribers. `/admin/vbucket-map` doesn't exist yet (a separate in-flight task); `/admin/status` and `/admin/shard-stats` already exist in the main Worker. See the TODO comments in `src/aggregator.ts` for the exact target shape.
- **Not started:** the real SPA (all four rooms above are placeholder-only); `SHARDSCOPE_GATE_TOKEN` enforcement on `/api/stream`; the load-generation/chaos engine behind the Playground room; the `[assets]` static-serving switch noted (commented out) in `wrangler.toml`.

Building the real aggregator polling or the SPA on top of this skeleton should not require changing `wrangler.toml`'s bindings or `src/env.d.ts`'s `Env` shape, only filling in the TODOs already left in `src/aggregator.ts` and `src/index.ts`.
