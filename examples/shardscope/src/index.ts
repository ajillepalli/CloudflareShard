/** Shardscope — mission-control dashboard for CloudflareShard.
 *
 * ============================================================================
 * TWO-TIER AUTH MODEL (read this before touching routing below)
 * ============================================================================
 * This Worker sits between a browser and cloudflare-shard-mvp's admin API,
 * and holds two *different* secrets that must never be confused:
 *
 *   1. ADMIN_TOKEN — authorizes calls *from this Worker* to
 *      cloudflare-shard-mvp's /admin/* surface (topology reads today; any
 *      topology-mutating admin control, once built here, must pass this the
 *      same way). This is the "am I allowed to operate the cluster" token.
 *
 *   2. SHARDSCOPE_GATE_TOKEN — authorizes a *browser* to talk to Shardscope
 *      itself at all (e.g. open /api/stream). This is the "is this viewer
 *      allowed to watch/operate Shardscope" token.
 *
 * Both are secrets bound server-side only (see src/env.d.ts) — the browser
 * never receives either token in any response body, header, or inline
 * script; the browser only ever holds a SHARDSCOPE_GATE_TOKEN-derived
 * session artifact (see src/gate.ts). All admin-facing calls
 * (SHARD_API.adminXxx(...)) happen inside this Worker or a Durable Object
 * (TopologyAggregator, LoadDriver, TenantTokenStore), never in client-side
 * JS.
 *
 * T5: every /api/* route is now gated behind SHARDSCOPE_GATE_TOKEN (see
 * src/gate.ts's isGateAuthorized, and the routing below) — this covers both
 * mutating routes (/api/load/start, /api/load/stop — starting a load run is
 * a cluster-affecting action) and read-only ones (/api/stream, topology is
 * sensitive; /api/load/status). GET / (the page shell) and POST /login,
 * /logout (how a browser obtains the gate artifact) are deliberately NOT
 * gated. src/gate.ts's header comment explains exactly what this gate is —
 * and, importantly, is NOT (a real multi-user auth system).
 *
 * TODO(shardscope): once topology-mutating admin controls exist beyond the
 * load engine (force a reshard, drain a shard — the eventual "Reshard" /
 * "Chaos" room controls per DESIGN.md), each such route must ALSO thread
 * ADMIN_TOKEN through to the underlying SHARD_API call (the call is allowed
 * to touch cloudflare-shard-mvp) in addition to the SHARDSCOPE_GATE_TOKEN
 * check every /api/* route already gets.
 * ============================================================================
 *
 * STATUS: Phase 1 skeleton. See README.md's "Status" section for exactly
 * what is stubbed vs. real.
 */
import { TopologyAggregator } from "./aggregator";
import { LoadDriver } from "./load/load-driver";
import { TenantTokenStore } from "./load/tenant-token-store";
import { isGateAuthorized, handleLogin, handleLogout } from "./gate";
import type { Env } from "./env";

export { TopologyAggregator, LoadDriver, TenantTokenStore };

// Shardscope palette (DESIGN.md) — inlined here only because this is a
// placeholder stub page. The real SPA build should pull these from a shared
// CSS/token file, not duplicate them inline like this.
const BG = "#0A0E14";
const SAFE = "#35E3B0";
const TEXT = "#E6EAF2";
const MUTED = "#8A94A6";

function placeholderHtml(): string {
  return `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>Shardscope</title>
<style>
  html, body {
    margin: 0;
    height: 100%;
    background: ${BG};
    color: ${TEXT};
    font-family: ui-monospace, "JetBrains Mono", "Berkeley Mono", monospace;
    font-variant-numeric: tabular-nums;
  }
  body {
    display: flex;
    align-items: center;
    justify-content: center;
    flex-direction: column;
    gap: 12px;
  }
  .brand {
    font-size: 13px;
    letter-spacing: .12em;
    text-transform: uppercase;
    color: ${MUTED};
  }
  .status {
    font-size: 16px;
    color: ${SAFE};
  }
  .dot {
    display: inline-block;
    width: 8px;
    height: 8px;
    border-radius: 50%;
    background: ${SAFE};
    margin-right: 8px;
    animation: breathe 4s ease-in-out infinite;
  }
  @keyframes breathe {
    0%, 100% { opacity: .55; }
    50% { opacity: 1; }
  }
</style>
</head>
<body>
  <div class="brand">Shardscope</div>
  <div class="status"><span class="dot"></span>booting</div>
</body>
</html>
`;
  // TODO(shardscope): this is a Phase 1 stub only. The real dashboard is a
  // four-room SPA (App / Topology / Reshard / Playground per DESIGN.md's
  // layout section) served as static assets once it exists — see the
  // commented-out [assets] block in wrangler.toml. Do not build the real UI
  // inline in this string; this placeholder exists only to prove the Worker
  // boots and to keep the dark theme correct from the very first commit.
}

const json = (data: unknown, status = 200): Response =>
  new Response(JSON.stringify(data, null, 2), { status, headers: { "content-type": "application/json" } });

/** Forwards to the LoadDriver singleton DO (see src/load/load-driver.ts).
 * LOAD_DRIVER is declared on Env (env.d.ts) and bound in wrangler.toml. */
function forwardToLoadDriver(request: Request, env: Env): Promise<Response> {
  const id = env.LOAD_DRIVER.idFromName("singleton");
  const stub = env.LOAD_DRIVER.get(id);
  return stub.fetch(request);
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    if (request.method === "GET" && url.pathname === "/") {
      return new Response(placeholderHtml(), {
        status: 200,
        headers: { "content-type": "text/html; charset=utf-8" },
      });
    }

    // POST /login, /logout: how a browser obtains (or clears) the
    // SHARDSCOPE_GATE_TOKEN session artifact — see src/gate.ts. Deliberately
    // NOT behind the /api/* gate below; that's how you'd get the artifact in
    // the first place.
    if (request.method === "POST" && url.pathname === "/login") {
      return handleLogin(request, env);
    }
    if (request.method === "POST" && url.pathname === "/logout") {
      return handleLogout();
    }

    // Every /api/* route requires SHARDSCOPE_GATE_TOKEN (header or session
    // cookie — see src/gate.ts). This fails CLOSED by default for any
    // current or future /api/* route, not just the specific ones matched
    // below — see this file's header comment for what this gate is (and
    // isn't).
    if (url.pathname.startsWith("/api/") && !isGateAuthorized(request, env)) {
      return json({ error: "Unauthorized. Provide 'authorization: Bearer <SHARDSCOPE_GATE_TOKEN>', or log in via POST /login first." }, 401);
    }

    // GET /api/stream: Server-Sent Events stream of live topology snapshots.
    //
    // Target architecture (see src/aggregator.ts for the DO side of this):
    // a single TopologyAggregator DO instance ("singleton") owns the one
    // shared poll of cloudflare-shard-mvp's admin API on an alarm() tick,
    // merges the result into one snapshot, and fans it out over SSE to every
    // subscriber connected here. This Worker's job on this route is just to
    // forward the browser's SSE connection into that DO — it must NOT poll
    // the admin API itself (that would defeat the whole point of having one
    // shared poller instead of one poll per open tab).
    if (request.method === "GET" && url.pathname === "/api/stream") {
      const id = env.AGGREGATOR.idFromName("singleton");
      const stub = env.AGGREGATOR.get(id);
      return stub.fetch(request);
    }

    // /api/load/*: forwards to the LoadDriver singleton DO (see
    // src/load/load-driver.ts) — a Worker-native TPC-C-style load engine
    // with a deterministic hot-shard skew mode. Exactly one instance for the
    // whole Worker (idFromName("singleton")), mirroring AGGREGATOR's own
    // singleton pattern above: one shared load run, not one per caller.
    if (
      (request.method === "POST" && (url.pathname === "/api/load/start" || url.pathname === "/api/load/stop")) ||
      (request.method === "GET" && url.pathname === "/api/load/status")
    ) {
      return forwardToLoadDriver(request, env);
    }

    return json({ error: `Unknown route: ${url.pathname}` }, 404);
  },
};
