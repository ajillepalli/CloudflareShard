# Shardscope

Shardscope is CloudflareShard's mission-control dashboard — a live view into a running cluster's topology, health, and resharding behavior. It is a separate demo Worker, service-bound to the main `cloudflare-shard-mvp` Worker's RPC entrypoint (the same pattern `../rpc-consumer` demonstrates), not part of the CloudflareShard core in `../../src`.

The memorable thing it's built around (see `./DESIGN.md` for the full design system): **"it healed itself under fire and lost nothing."** The hero moment is watching a cluster reshard under load — and under attack — while a scoreboard never leaves `lost: 0`. Composure is the product; red never means "the system is unhealthy," only "this shard is hot" or "an attack is landing."

## The rooms

Per `./DESIGN.md`'s layout section, Shardscope is a single-page app with a fixed shell (top invariant scoreboard, left icon rail, center canvas, right console). The icon rail has five slots; all five are built and wired:

- **App** *(built)* — Room 1, the DX story: a real multi-tenant view (pick a demo warehouse/tenant, browse a live slice of its customers/stock via `/v1/table-scan`) plus one honest same-tenant `/v1/tx` transaction, and a "how little code this takes" panel showing the actual, copy-accurate client snippets. See "The App room" below.
- **Topology** *(built)* — the living topology canvas: shard ownership, vBucket placement, in-flight migrations. The primary object on screen; not a metric-tile grid. Live via the aggregator's SSE poll (see below).
- **Reshard** *(built)* — operator controls (split / migrate / drain, abort, lock status/force-release) for driving a resharding operation, with the "CHAOS — BREAK IT" attack panel folded into the same room rather than living in a separate Playground.
- **Edge** *(built)* — a Phase-3 global-latency readout for the current request's own edge/geo data (`GET /api/edge`); honest about having no live multi-region probe network beyond that.
- **Play / Playground** *(built)* — an interactive console for every primitive (`mutate`, `tx`, `index-query`, `table-scan`, plus the operator-only `sql`/`scatter`) against a controlled demo tenant, and a routing inspector that resolves + highlights a key's real owning shard.

## The App room

Room 1 (`data-hook="app-wrap"` in `public/index.html`, the "App room" section of `public/app.js`) is the pitch to a developer deciding whether to build on CloudflareShard: the same live cluster the other rooms watch, presented as a working app screen instead of a raw console.

- **Multi-tenant view:** a warehouse/tenant picker (1/2/3 — whitelisted, same demo tenants the Playground room uses) drives two live reads through `POST /api/play/table-scan` (`src/play.ts`'s `playTableScan`): the first 5 `tpcc_customer` rows and the first 5 `tpcc_stock` rows for that tenant, rendered as small tables, not a JSON dump. Switching tenant re-queries live.
- **One honest 2PC action:** "Restock" fires a real `POST /api/play/tx` (`playTx`) — a same-tenant, multi-row (2-3 rows, well under `/v1/tx`'s real 8-participant cap) transactional update against the stock rows just displayed, guarded by an optimistic-concurrency `where` clause per row. It is explicitly labeled as **not** a full TPC-C order and **not** a cross-tenant transaction — see the panel's own help text.
- **"How little code" callout:** two copy-accurate client snippets (a `/v1/table-scan` read, a `/v1/tx` write) matching the real CloudflareShard API shapes documented in the project root's `README.md` ("Tenant-scoped table scan" / "Cross-shard atomic transaction") — rendered as escaped text, never executed.

No new backend was needed: the room reuses `/api/play/table-scan` and `/api/play/tx` verbatim, the same gate-protected, whitelisted, tenant-scoped routes the Playground room already exercises.

## Two-tier auth model

Shardscope holds two distinct secrets server-side; the browser never sees either:

- **`ADMIN_TOKEN`** — authorizes this Worker's calls into `cloudflare-shard-mvp`'s `/admin/*` surface (topology reads today; any topology-mutating control this dashboard grows must pass this the same way the admin HTTP API or `CloudflareShardRpc`'s `adminXxx(adminToken, ...)` RPC methods require).
- **`SHARDSCOPE_GATE_TOKEN`** — authorizes a browser to talk to Shardscope itself (open `/api/stream`, drive the load/reshard/chaos controls). Distinct question from the one above: "is this viewer allowed to watch/operate Shardscope" vs. "is this call allowed to touch the cluster."

`SHARDSCOPE_GATE_TOKEN` enforcement is live: every `/api/*` route fails closed behind it (see `src/gate.ts`'s `isGateAuthorized` and the routing in `src/index.ts`). Only `GET /` and `POST /login`/`/logout` are deliberately ungated — see `src/index.ts`'s header comment and `src/env.d.ts` for the full explanation.

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

## Status

Shardscope is fully built — all five rooms are wired end to end:

- **Real:** the static SPA in `public/`, served via the active `[assets]` block in `wrangler.toml` (GET `/` and all other static paths are matched before falling through to the Worker; `src/index.ts`'s inline `placeholderHtml()` is dead code, kept only as a documented fallback). The App, Topology, Reshard, Edge, and Playground rooms are all wired end to end. `TopologyAggregator.alarm()` (`src/aggregator.ts`) runs the real shared poll of `/admin/status` + `/admin/vbucket-map` + fanned `/admin/shard-stats`, merges it into one snapshot, and fans it out over SSE to every subscriber. The Worker-native TPC-C load engine and hot-shard skew driver (`src/load/`), the durable per-tenant token store (`src/load/tenant-token-store.ts`), the live correctness meter (`src/load/correctness.ts`), the Reshard console ops (`src/reshard.ts`: split/migrate/drain/abort, lock status/force-release), the chaos attack engine (`src/chaos.ts`, folded into the Reshard room's "BREAK IT" panel), the Edge room's real edge/geo readout (`src/edge.ts`), the Playground's primitive console (`src/play.ts`), and the App room's multi-tenant view + same-tenant transaction (also `src/play.ts`, reused) are all live. `SHARDSCOPE_GATE_TOKEN` enforcement is live on every `/api/*` route.

Extending an already-wired room should not require changing `wrangler.toml`'s bindings or `src/env.d.ts`'s `Env` shape.
