/** Shardscope's Edge room (Phase 3, T11) — GET /api/edge server-side helper.
 *
 * ============================================================================
 * HONESTY CONTRACT (read before touching this file)
 * ============================================================================
 * This endpoint returns ONLY real data Cloudflare's edge already knows about
 * the incoming request — `request.cf` (IncomingRequestCfProperties) — nothing
 * computed, estimated, or invented. Shardscope has no live multi-region probe
 * network today, so this is deliberately narrow: "which Cloudflare colo
 * actually served THIS request, and what does Cloudflare know about where it
 * came from." The Edge room's client (../public/app.js) fires several real
 * round trips to THIS endpoint to measure the viewer's own real browser ->
 * Worker latency; it must not, and does not, fabricate latency numbers for
 * any other region — see app.js's Edge room section and ../DESIGN.md for the
 * full rationale.
 *
 * `request.cf` is `undefined` for a Worker not actually running behind a real
 * Cloudflare edge (`wrangler dev` without `--remote`, vitest-pool-workers'
 * default Miniflare runtime, etc.) — there is no real colo to report in that
 * case. That is NOT an error condition: it's the expected local-dev shape,
 * surfaced as `{ local: true, edge: null }` so the UI can say so honestly
 * ("running locally — no Cloudflare edge data") instead of inventing a colo.
 *
 * This route intentionally exposes only the CALLER'S OWN edge/geo — nothing
 * about ADMIN_TOKEN, SHARDSCOPE_GATE_TOKEN, or the cluster's topology — so it
 * carries none of the sensitivity index.ts's header comment describes for
 * other /api/* routes. It still sits behind the same SHARDSCOPE_GATE_TOKEN
 * gate as every other /api/* route (see index.ts's routing block), simply
 * because every /api/* route does, not because this one needs it.
 * ============================================================================
 */

export interface EdgeInfoResponse {
  /** true when this Worker is NOT running behind a real Cloudflare edge
   * (request.cf was undefined/incomplete) — local dev, miniflare,
   * vitest-pool-workers. The UI must render an explicit "running locally"
   * state whenever this is true, never a fabricated colo. */
  local: boolean;
  /** Real `request.cf` fields for the datacenter that served THIS request,
   * or null when `local` is true. Every field here is exactly what
   * Cloudflare's edge reported for this request — nothing derived, geocoded,
   * or guessed. */
  edge: {
    /** IATA-style datacenter code, e.g. "AMS", "NRT". Always present
     * whenever `edge` is non-null (see buildEdgeInfo's local-check below). */
    colo: string;
    country: string | null;
    city: string | null;
    region: string | null;
    /** String-typed lat/long straight from request.cf, when Cloudflare
     * reports them — used only to place the viewer's real position on the
     * Edge room's reference map. Left null (never guessed) when Cloudflare
     * doesn't report them for this request. */
    latitude: string | null;
    longitude: string | null;
  } | null;
  /** Server-side receive timestamp (epoch ms). The Edge room's actual RTT
   * math (see app.js's fetchEdgeOnce/runEdgeMeasurement) is done entirely
   * from the browser's own fetch start/end timestamps and needs no server
   * clock at all; this field is included only as an honest "when the server
   * saw this request" data point, not something the client currently
   * depends on for its measurement. */
  serverReceivedAt: number;
}

/** Pure, directly-testable core of the GET /api/edge route (see index.ts's
 * routing block for the one line that calls this with `request.cf`). Kept
 * side-effect-free and dependency-free (no `Env`, no fetch) specifically so
 * the cf-present vs. cf-undefined branches can be unit tested without
 * standing up a Worker request at all — see edge.test.ts. */
export function buildEdgeInfo(cf: IncomingRequestCfProperties | undefined, now: number = Date.now()): EdgeInfoResponse {
  if (!cf || typeof cf.colo !== "string" || cf.colo.length === 0) {
    return { local: true, edge: null, serverReceivedAt: now };
  }
  return {
    local: false,
    edge: {
      colo: cf.colo,
      country: typeof cf.country === "string" ? cf.country : null,
      city: typeof cf.city === "string" ? cf.city : null,
      region: typeof cf.region === "string" ? cf.region : null,
      latitude: typeof cf.latitude === "string" ? cf.latitude : null,
      longitude: typeof cf.longitude === "string" ? cf.longitude : null,
    },
    serverReceivedAt: now,
  };
}
