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
 * STATUS: the SPA (public/) is built and live for three of its five rail
 * slots — Topology, Reshard (with the chaos "BREAK IT" panel folded in), and
 * Edge; App and Playground remain disabled rail items ("Not built yet"). The
 * SHARDSCOPE_GATE_TOKEN gate, the load/chaos engine, the Reshard console ops,
 * and the aggregator's SSE poll are all live. See README.md's "Status"
 * section for the full breakdown.
 */
import { TopologyAggregator } from "./aggregator";
import { LoadDriver } from "./load/load-driver";
import { TenantTokenStore } from "./load/tenant-token-store";
import { isGateAuthorized, handleLogin, handleLogout } from "./gate";
import { buildEdgeInfo } from "./edge";
import {
  ReshardValidationError,
  parseSplitVbucketInput,
  splitVbucket,
  parseMigrateVbucketInput,
  migrateVbucket,
  parseMigrateVbucketStatusQuery,
  migrateVbucketStatus,
  parseMigrateVbucketAbortInput,
  migrateVbucketAbort,
  parseDrainShardInput,
  drainShard,
  parseDrainShardStatusQuery,
  drainShardStatus,
  topologyLockStatus,
  parseForceReleaseTopologyLockInput,
  forceReleaseTopologyLock,
} from "./reshard";
import {
  ChaosPreconditionError,
  parseDoubleSubmitInput,
  runDoubleSubmitAttack,
  parseMismatchedReplayInput,
  runMismatchedReplayAttack,
  parseDrainHotNodeInput,
  runDrainHotNodeAttack,
  parseSplitHotVbucketInput,
  runSplitHotVbucketAttack,
  parseMigrateHotVbucketInput,
  runMigrateHotVbucketAttack,
  parseAbortMigrationInput,
  runAbortMigrationAttack,
  parseBlipShardOfflineInput,
  runBlipShardOfflineAttack,
} from "./chaos";
import {
  PlayValidationError,
  parsePlayMutateInput,
  playMutate,
  parsePlayTxInput,
  playTx,
  parsePlayIndexQueryInput,
  playIndexQuery,
  parsePlayTableScanInput,
  playTableScan,
  parsePlaySqlInput,
  playSql,
  parsePlayScatterInput,
  playScatter,
  parsePlayRouteInspectInput,
  playRouteInspect,
} from "./play";
import { generateScaffoldFiles, buildZip } from "./build";
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
  // NOTE(shardscope): dead code kept as a documented fallback. The real
  // dashboard now ships from public/ via the active [assets] block in
  // wrangler.toml, which matches GET / before this Worker ever runs — this
  // function is unreachable in practice. Left in place (dark theme intact)
  // only in case [assets] is ever disabled; do not build real UI inline in
  // this string.
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

/** Parses a Reshard console POST body, turning invalid JSON into the same
 * ReshardValidationError -> 400 path runReshardOp gives every other bad
 * request from the browser (see reshard.ts's header comment). */
async function readReshardJsonBody(request: Request): Promise<unknown> {
  try {
    return await request.json();
  } catch {
    throw new ReshardValidationError("Invalid JSON body.");
  }
}

/** Parses a Chaos panel POST body — deliberately MORE tolerant than
 * readReshardJsonBody above: every chaos attack input is optional with a
 * sane default (see src/chaos.ts's parse*() functions), so a one-click
 * attack button firing an EMPTY body (or no body at all) is the expected
 * common case, not a client error. Malformed/absent JSON just falls back to
 * `{}`, letting each attack's own defaults apply, rather than 400ing a
 * perfectly normal "fire this attack" click. */
async function readChaosJsonBody(request: Request): Promise<unknown> {
  const text = await request.text();
  if (!text) return {};
  try {
    return JSON.parse(text);
  } catch {
    return {};
  }
}

/** Runs one Reshard console operation (src/reshard.ts) and turns any failure
 * into a calm JSON error response instead of a 500 or an unhandled
 * rejection — this is the "handle errors, calm inline messages" contract
 * the Reshard console UI depends on. Two failure classes are handled
 * distinctly:
 *   - ReshardValidationError (a malformed request FROM the browser, caught
 *     by reshard.ts's parse*() functions before ever touching SHARD_API) ->
 *     400 with that message.
 *   - Everything else is assumed to be env.SHARD_API's RPC call rejecting.
 *     CloudflareShardRpc's own unwrapForRpc (main repo's src/index.ts) turns
 *     any non-2xx HTTP response from the catalog into a thrown Error whose
 *     message is `CloudflareShard RPC error <status>: <JSON body>` — that
 *     JSON body is exactly the structured `{ error: {...} }` shape the
 *     catalog itself returned (e.g. 409 MIGRATION_IN_PROGRESS when another
 *     op already holds the topology lock), so it's unpacked and forwarded
 *     with its original status rather than collapsed into a generic 500. */
/** Shared by runReshardOp and runChaosOp below: runs `fn`, mapping any thrown
 * error into a calm JSON Response instead of a 500 or an unhandled
 * rejection. `isRequestError` decides which error CLASSES mean "malformed or
 * currently-unsatisfiable request FROM the browser" (-> 400) as opposed to
 * "the cluster/RPC layer itself rejected this" (the CloudflareShard RPC error
 * <status>: <body> pattern, unpacked and forwarded below with its original
 * status) or an unrecognized failure (-> 502). */
async function runOperatorOp(fn: () => Promise<unknown>, isRequestError: (err: unknown) => boolean): Promise<Response> {
  try {
    const result = await fn();
    return json(result);
  } catch (err) {
    if (isRequestError(err)) {
      return json({ error: err instanceof Error ? err.message : String(err) }, 400);
    }
    const message = err instanceof Error ? err.message : String(err);
    const match = /^CloudflareShard RPC error (\d+): ([\s\S]*)$/.exec(message);
    if (match) {
      const status = Number(match[1]) >= 400 && Number(match[1]) < 600 ? Number(match[1]) : 502;
      try {
        return json(JSON.parse(match[2]), status);
      } catch {
        return json({ error: match[2] }, status);
      }
    }
    // Unrecognized failure (a runtime/DO/RPC-transport error, not a validated
    // request error or a structured CloudflareShard error): don't leak internal
    // detail to the browser. Log it server-side (wrangler tail) and return a
    // generic 502. (pre-PR Codex integration finding [P2].)
    console.error("shardscope: operator op failed with an unexpected error:", message);
    return json({ error: "Operator request failed unexpectedly. Check the Worker logs." }, 502);
  }
}

async function runReshardOp(fn: () => Promise<unknown>): Promise<Response> {
  return runOperatorOp(fn, (err) => err instanceof ReshardValidationError);
}

/** Same calm-error contract as runReshardOp, plus src/chaos.ts's
 * ChaosPreconditionError (a chaos attack that can't currently be attempted —
 * e.g. no skew load running to derive a "hot shard" from — is a 400, not a
 * 500 or a fabricated result). This is also where a lock-busy 409 from
 * ./reshard.ts's admin wrappers (drain-hot-node / split-hot-vbucket /
 * migrate-hot-vbucket / abort-migration all call straight into ./reshard.ts)
 * naturally surfaces to the browser: the SAME "CloudflareShard RPC error
 * <status>: <body>" unpacking above already handles it, so the Reshard
 * console's existing topology-lock UI conventions apply here unchanged — no
 * new lock-busy handling needed. */
async function runChaosOp(fn: () => Promise<unknown>): Promise<Response> {
  return runOperatorOp(fn, (err) => err instanceof ReshardValidationError || err instanceof ChaosPreconditionError);
}

/** Same calm-error contract as runReshardOp/runChaosOp above, for the
 * Playground room (src/play.ts): a PlayValidationError (malformed input, or
 * a warehouse/table/index/SQL-shape outside src/play.ts's whitelists) is a
 * 400 with that message; everything else (including the idempotent-mismatch
 * 409 src/play.ts's playMutate doc comment describes) flows through
 * runOperatorOp's existing "CloudflareShard RPC error <status>: <body>"
 * unpacking unchanged — this is what lets the Playground's requestId-replay
 * demo show the REAL contract to the browser instead of a simulated one. */
async function runPlayOp(fn: () => Promise<unknown>): Promise<Response> {
  return runOperatorOp(fn, (err) => err instanceof PlayValidationError);
}

/** Parses a Playground POST body — same strict contract as
 * readReshardJsonBody (every Playground call is an explicit, developer-typed
 * request, not a one-click button with a sane empty-body default like
 * Chaos's readChaosJsonBody), so invalid/absent JSON is a PlayValidationError
 * -> 400, not a silently-defaulted `{}`. */
async function readPlayJsonBody(request: Request): Promise<unknown> {
  try {
    return await request.json();
  } catch {
    throw new PlayValidationError("Invalid JSON body.");
  }
}

export default {
  // Request<unknown, IncomingRequestCfProperties>: narrows request.cf from
  // the bare Request default (CfProperties<unknown>, a union that also
  // includes the OUTGOING-fetch-only RequestInitCfProperties shape) down to
  // the real INCOMING shape GET /api/edge relies on (see src/edge.ts).
  async fetch(request: Request<unknown, IncomingRequestCfProperties>, env: Env): Promise<Response> {
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

    // GET /api/edge: the Edge room's (T11) real edge/geo readout for THIS
    // request — see src/edge.ts's header comment for the full honesty
    // contract this route exists to uphold. request.cf is real Cloudflare
    // data or undefined (local dev/miniflare); buildEdgeInfo is a pure,
    // directly-unit-tested function (src/edge.test.ts) that turns either
    // shape into an honest response — this line is deliberately the only
    // thing connecting it to a real Request.
    if (request.method === "GET" && url.pathname === "/api/edge") {
      return json(buildEdgeInfo(request.cf, Date.now()));
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

    // /api/reshard/*: the Reshard room's manual operator controls (T8) —
    // split / migrate / drain, their status/abort forwarders, and the
    // cluster-wide topology-lock status + force-release escape hatch (see
    // src/reshard.ts). GATING CONFIRMATION: every path under here is a
    // /api/* route, so it already went through the `isGateAuthorized` check
    // at the top of this function (this code is unreachable otherwise) —
    // these ARE gated, same as /api/load/*. They additionally thread
    // ADMIN_TOKEN through to SHARD_API inside src/reshard.ts (never here,
    // never in the browser), completing the TODO in this file's header
    // comment: "each such route must ALSO thread ADMIN_TOKEN through to the
    // underlying SHARD_API call in addition to the SHARDSCOPE_GATE_TOKEN
    // check every /api/* route already gets."
    if (url.pathname.startsWith("/api/reshard/")) {
      if (request.method === "POST" && url.pathname === "/api/reshard/split") {
        return runReshardOp(async () => splitVbucket(env, parseSplitVbucketInput(await readReshardJsonBody(request))));
      }
      if (request.method === "POST" && url.pathname === "/api/reshard/migrate") {
        return runReshardOp(async () => migrateVbucket(env, parseMigrateVbucketInput(await readReshardJsonBody(request))));
      }
      if (request.method === "POST" && url.pathname === "/api/reshard/drain") {
        return runReshardOp(async () => drainShard(env, parseDrainShardInput(await readReshardJsonBody(request))));
      }
      if (request.method === "GET" && url.pathname === "/api/reshard/migrate-status") {
        return runReshardOp(() => migrateVbucketStatus(env, parseMigrateVbucketStatusQuery(url.searchParams)));
      }
      if (request.method === "GET" && url.pathname === "/api/reshard/drain-status") {
        return runReshardOp(() => drainShardStatus(env, parseDrainShardStatusQuery(url.searchParams)));
      }
      if (request.method === "GET" && url.pathname === "/api/reshard/lock-status") {
        return runReshardOp(() => topologyLockStatus(env));
      }
      if (request.method === "POST" && url.pathname === "/api/reshard/migrate-abort") {
        return runReshardOp(async () => migrateVbucketAbort(env, parseMigrateVbucketAbortInput(await readReshardJsonBody(request))));
      }
      if (request.method === "POST" && url.pathname === "/api/reshard/force-release-lock") {
        return runReshardOp(async () => forceReleaseTopologyLock(env, parseForceReleaseTopologyLockInput(await readReshardJsonBody(request))));
      }
    }

    // /api/chaos/*: the Reshard room's "CHAOS — BREAK IT" panel (T9) — see
    // src/chaos.ts's header comment for the full thesis. GATING
    // CONFIRMATION: every path under here is a /api/* route, so it already
    // passed the `isGateAuthorized` check at the top of this function (this
    // code is unreachable otherwise) — same as /api/reshard/* above. These
    // are the MOST DANGEROUS routes this Worker exposes (they fire real
    // destructive writes and real topology ops against the live cluster), so
    // this comment exists to make that double-checkable at a glance: nothing
    // under /api/chaos/* is reachable without SHARDSCOPE_GATE_TOKEN, on top
    // of ADMIN_TOKEN being threaded through server-side only (src/chaos.ts's
    // reused ./reshard.ts wrappers for c/d/e; a real tenant bearer token via
    // src/load/tenant-token-store.ts for a/b) — never sent to or read by the
    // browser, exactly like every other admin-facing route in this file.
    if (url.pathname.startsWith("/api/chaos/")) {
      if (request.method === "POST" && url.pathname === "/api/chaos/double-submit") {
        return runChaosOp(async () => runDoubleSubmitAttack(env, parseDoubleSubmitInput(await readChaosJsonBody(request))));
      }
      if (request.method === "POST" && url.pathname === "/api/chaos/mismatched-replay") {
        return runChaosOp(async () => runMismatchedReplayAttack(env, parseMismatchedReplayInput(await readChaosJsonBody(request))));
      }
      if (request.method === "POST" && url.pathname === "/api/chaos/drain-hot-node") {
        return runChaosOp(async () => runDrainHotNodeAttack(env, parseDrainHotNodeInput(await readChaosJsonBody(request))));
      }
      if (request.method === "POST" && url.pathname === "/api/chaos/split-hot-vbucket") {
        return runChaosOp(async () => runSplitHotVbucketAttack(env, parseSplitHotVbucketInput(await readChaosJsonBody(request))));
      }
      if (request.method === "POST" && url.pathname === "/api/chaos/migrate-hot-vbucket") {
        return runChaosOp(async () => runMigrateHotVbucketAttack(env, parseMigrateHotVbucketInput(await readChaosJsonBody(request))));
      }
      if (request.method === "POST" && url.pathname === "/api/chaos/abort-migration") {
        return runChaosOp(async () => runAbortMigrationAttack(env, parseAbortMigrationInput(await readChaosJsonBody(request))));
      }
      // "Blip shard offline" — makes a real shard's Durable Object genuinely
      // unreachable via the core's admin-gated fault-injection primitive
      // (env.SHARD_API.adminFaultInject; see src/chaos.ts's header comment
      // and env.d.ts's doc comment on that method). Off by default (the core
      // Worker's FAULT_INJECTION_ENABLED must be "true"); a disabled cluster
      // rejects with 403, which runBlipShardOfflineAttack classifies via
      // classifyBlipFaultInjectError into a calm ChaosPreconditionError (400,
      // via runChaosOp below) explaining the flag requirement — never a
      // fabricated success or a generic 502.
      if (request.method === "POST" && url.pathname === "/api/chaos/blip-shard-offline") {
        return runChaosOp(async () => runBlipShardOfflineAttack(env, parseBlipShardOfflineInput(await readChaosJsonBody(request))));
      }
    }

    // /api/play/*: the Playground room's backend (src/play.ts) — a
    // gate-protected proxy letting a browser drive CloudflareShard's
    // developer primitives (mutate/tx/index-query/table-scan under a
    // controlled demo tenant; sql/scatter server-side under ADMIN_TOKEN,
    // read-only). GATING CONFIRMATION: every path under here is a /api/*
    // route, so it already passed the `isGateAuthorized` check at the top of
    // this function (this code is unreachable otherwise) — same as
    // /api/reshard/* and /api/chaos/* above. See src/play.ts's header
    // comment for the full security model (never a browser-supplied
    // token/tenant identity; whitelisted demo tenants/tables/indexes;
    // read-only enforcement on the operator SQL/scatter routes).
    if (url.pathname.startsWith("/api/play/")) {
      if (request.method === "POST" && url.pathname === "/api/play/mutate") {
        return runPlayOp(async () => playMutate(env, parsePlayMutateInput(await readPlayJsonBody(request))));
      }
      if (request.method === "POST" && url.pathname === "/api/play/tx") {
        return runPlayOp(async () => playTx(env, parsePlayTxInput(await readPlayJsonBody(request))));
      }
      if (request.method === "POST" && url.pathname === "/api/play/index-query") {
        return runPlayOp(async () => playIndexQuery(env, parsePlayIndexQueryInput(await readPlayJsonBody(request))));
      }
      if (request.method === "POST" && url.pathname === "/api/play/table-scan") {
        return runPlayOp(async () => playTableScan(env, parsePlayTableScanInput(await readPlayJsonBody(request))));
      }
      if (request.method === "POST" && url.pathname === "/api/play/sql") {
        return runPlayOp(async () => playSql(env, parsePlaySqlInput(await readPlayJsonBody(request))));
      }
      if (request.method === "POST" && url.pathname === "/api/play/scatter") {
        return runPlayOp(async () => playScatter(env, parsePlayScatterInput(await readPlayJsonBody(request))));
      }
      // POST /api/play/route-inspect: the Playground's routing inspector
      // (READ-ONLY — see src/play.ts's playRouteInspect doc comment). Reuses
      // this same runPlayOp calm-error contract as every other Playground
      // route above; a catalog/vbucket the live map doesn't (yet) recognize
      // surfaces as PlayValidationError -> 400, exactly like a bad
      // warehouseId/table would.
      if (request.method === "POST" && url.pathname === "/api/play/route-inspect") {
        return runPlayOp(async () => playRouteInspect(env, parsePlayRouteInspectInput(await readPlayJsonBody(request))));
      }
    }

    // /api/build/*: the "Build on it" panel (App room) — generates a REAL,
    // runnable CloudflareShard starter repo (a small multi-tenant
    // "inventory" app, service-bound to cloudflare-shard-mvp's
    // CloudflareShardRpc entrypoint, mirroring examples/rpc-consumer) so a
    // developer who watched the demo can leave with something they can
    // actually run. See src/build.ts's header comment for the full
    // accuracy/security contract (every generated line cross-checked
    // against the real RPC surface; no secret ever appears in a generated
    // file — generateScaffoldFiles() is a pure function with no Env access,
    // so it's structurally incapable of embedding one). GATING
    // CONFIRMATION: both routes below are /api/* routes, so they already
    // passed the isGateAuthorized check at the top of this function (this
    // code is unreachable otherwise) — same as every other /api/* family in
    // this file.
    if (url.pathname.startsWith("/api/build/")) {
      // GET /api/build/manifest: the same file set as the zip below, as
      // JSON — backs the frontend's inline browsable preview (no need to
      // parse a zip client-side just to show file contents).
      if (request.method === "GET" && url.pathname === "/api/build/manifest") {
        return json({ files: generateScaffoldFiles() });
      }
      // GET /api/build/scaffold: the same file set, packaged as a
      // dependency-free "stored" zip (src/build.ts's buildZip) and served
      // as a real download.
      if (request.method === "GET" && url.pathname === "/api/build/scaffold") {
        const zipBytes = buildZip(generateScaffoldFiles());
        return new Response(zipBytes, {
          status: 200,
          headers: {
            "content-type": "application/zip",
            "content-disposition": 'attachment; filename="cloudflareshard-inventory-starter.zip"',
          },
        });
      }
    }

    return json({ error: `Unknown route: ${url.pathname}` }, 404);
  },
};
