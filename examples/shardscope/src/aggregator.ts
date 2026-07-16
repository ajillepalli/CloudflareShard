/** TopologyAggregator — the single shared topology poller/fan-out Durable
 * Object behind Shardscope's /api/stream.
 *
 * Exactly one instance of this DO exists per Shardscope deployment (always
 * addressed via idFromName("singleton") from src/index.ts). On a repeating
 * alarm() tick, it makes ONE round of admin API calls against
 * cloudflare-shard-mvp (via env.SHARD_API, the RPC service binding — see
 * wrangler.toml), merges the results into a single topology snapshot, and
 * pushes that snapshot to every currently-open SSE subscriber. Browser tabs
 * never cause their own poll — they just register as a subscriber and wait
 * for the next tick's fan-out. This keeps admin API load O(1) in the number
 * of watching operators, not O(subscribers).
 */
import type { Env, ShardApiBinding } from "./env";

// ----------------------------------------------------------------------------
// Response shapes for the three admin calls this DO makes over env.SHARD_API.
// These are cast-targets for the `unknown` ShardApiBinding returns (see
// env.d.ts) — they mirror the actual JSON bodies produced by
// adminStatusCore / adminVbucketMapCore (src/index.ts) and
// ShardDO.handleStats (src/shard.ts). Kept local to this file because
// nothing else in Shardscope needs them yet.
// ----------------------------------------------------------------------------

interface AdminStatusResponse {
  initialized: boolean;
  catalogShardCount: number;
  shards: { total: number; active: number; draining: number };
  catalogs: Array<{
    catalogShardId: string;
    initialized: boolean;
    shards?: { total: number; active: number; draining: number };
  }>;
}

interface VbucketMapRow {
  vbucket: number;
  shardId: string;
  migrationStatus: string;
  targetShardId: string | null;
  cutoverStartedAt: string | null;
}

interface AdminVbucketMapResponse {
  catalogShardCount: number;
  totalVBuckets: number;
  catalogs: Array<{ catalogShardId: string; totalVBuckets: number; map: VbucketMapRow[] }>;
}

// adminShardStatsCore just forwards ShardDO's /stats body verbatim (see
// ShardDO.handleStats in src/shard.ts) — typed loosely since the aggregator
// only needs to attach it to a shardId, not interpret its fields.
type AdminShardStatsResponse = Record<string, unknown>;

/** Cast-target for the JSON body ./load/load-driver.ts's LoadDriver DO
 * returns from GET /api/load/status (see that file's toStatusJson) — only
 * the two fields this aggregator actually reads (`running` and
 * `correctness`) are declared here, mirroring this file's existing
 * loosely-typed-response convention for the other two admin calls above. */
interface LoadDriverStatusResponse {
  running: boolean;
  correctness: {
    writesAcked: number;
    writesRetriedIdempotent: number;
    txAbortedExpected: number;
    lost: number;
    meterState: "green" | "red";
  };
}

// ----------------------------------------------------------------------------
// Shardscope T4 — the correctness scoreboard's checksum label. See this
// file's deriveChecksumStatus for the full reasoning; the short version:
// CloudflareShard's migration checksum is a CUTOVER EVENT (computed once,
// inside cutover; a mismatch aborts the migration) — it is NOT a
// continuously-readable invariant the way "lost" is. Rendering a permanent
// "checksum OK" would be fabricating a signal that was never actually
// (re)checked. This label is instead derived, tick to tick, from the live
// vbucket map's own migrationStatus field plus a small amount of in-memory
// cross-tick memory (see ChecksumTrackingState) needed to tell "just
// finished a clean cutover" apart from "just got aborted" — both of which
// collapse to migrationStatus: "none" on the CURRENT tick alone.
// ----------------------------------------------------------------------------

type MigrationPhase = "none" | "backfilling" | "cutover" | "aborting";

export type ChecksumState = "idle" | "backfilling" | "verifying" | "stalled" | "aborting" | "verified" | "aborted";

export interface ChecksumStatus {
  label: string;
  state: ChecksumState;
}

/** In-memory, cross-tick bookkeeping ONLY (like lastSnapshot below) — safe to
 * lose on a DO eviction/restart: the worst case is the label resets to
 * "idle" tracking (never a false "verified"/"aborted", since those require
 * having actually OBSERVED a cutover/abort phase first; a freshly-reset
 * tracker just starts from "hasn't seen one yet this instance's lifetime",
 * which degrades to the honest "idle" case below, not a fabricated one). */
// Exported (type + initializer) solely so aggregator.test.ts can drive
// deriveChecksumStatus directly across multiple simulated ticks without
// reaching into TopologyAggregator's private instance field.
export interface ChecksumTrackingState {
  lastPhase: MigrationPhase;
  sawCutoverSinceIdle: boolean;
  sawAbortingSinceIdle: boolean;
}

export function initialChecksumTrackingState(): ChecksumTrackingState {
  return { lastPhase: "none", sawCutoverSinceIdle: false, sawAbortingSinceIdle: false };
}

// A real cutover (the checksum computation + ownership flip) is expected to
// land within one or two aggregator ticks (TICK_INTERVAL_MS below) — not a
// measured value, a generous placeholder threshold past which a vbucket
// stuck in migrationStatus "cutover" reads as genuinely stalled rather than
// "still verifying" (see catalog.ts's own cutover_stall_reason mechanism,
// which this label can't read directly — see deriveChecksumStatus's comment
// on why it uses cutoverStartedAt age as a proxy instead).
const CUTOVER_STALL_THRESHOLD_MS = 15_000;

/** Pure derivation of the checksum label from ONE tick's live vbucket map
 * (every catalog, every row) plus the previous tick's ChecksumTrackingState.
 * Exported (and unit-tested directly in aggregator.test.ts) since this is
 * exactly the kind of "must not silently fabricate green" logic that needs
 * to be provably correct, independent of the DO/SSE plumbing around it.
 *
 * Reasoning per state:
 *   - "none" everywhere, and no migration has been observed yet this
 *     tracking lifetime -> "idle". This is the honest default: no cutover
 *     has ever happened, so there is nothing to report as verified.
 *   - any row "backfilling" (data copying, pre-cutover) -> "backfilling…".
 *     Not "idle" (a migration genuinely is active) and NOT "verifying…"
 *     either (the checksum computation hasn't started — see this file's
 *     header comment: it happens INSIDE cutover, not during backfill).
 *   - any row "cutover" -> "verifying…", UNLESS that row's cutoverStartedAt
 *     is older than CUTOVER_STALL_THRESHOLD_MS, in which case "stalled — ...".
 *     The vbucket-map row doesn't expose catalog.ts's own
 *     cutover_stall_reason column (only the more detailed per-vbucket
 *     migrate-status endpoint does, and this scoreboard intentionally stays
 *     within its documented "one extra call per tick" budget — see
 *     pollSnapshot's own O(1)-in-viewers comment), so cutoverStartedAt age
 *     is used as an honest proxy: a cutover that hasn't landed in
 *     CUTOVER_STALL_THRESHOLD_MS is presented as stalled rather than
 *     silently kept as "verifying…" forever.
 *   - any row "aborting" -> "aborting…" (a migration cleanup in flight).
 *   - everything "none" again, but this tracking lifetime previously
 *     observed "aborting" since the last idle period -> "aborted"
 *     (last-known outcome).
 *   - everything "none" again, having previously observed "cutover" (and
 *     NOT "aborting") since the last idle period -> "cutover verified"
 *     (last-known outcome — the ONE state that reports a checksum actually
 *     passed, and only because a cutover genuinely reached "none" without
 *     ever having been seen in "aborting").
 * "aborting" strictly wins over "cutover"/"backfilling" when multiple rows
 * disagree in one tick (an aborting migration is the most consequential
 * state to surface), and "cutover" wins over "backfilling" for the same
 * reason — in practice the cluster-wide topology lock means at most one
 * vbucket is actively migrating at a time, so this priority rarely matters,
 * but it's defined so the function is total or the read of vbucket rows
 * would depend on the input map's ORDER, that its output cannot depend on. */
export function deriveChecksumStatus(
  catalogs: Array<{ vbuckets: Array<Pick<VbucketMapRow, "migrationStatus" | "cutoverStartedAt">> }>,
  tracking: ChecksumTrackingState,
  now: number,
): { status: ChecksumStatus; nextTracking: ChecksumTrackingState } {
  let sawAborting = false;
  let sawCutover = false;
  let sawBackfilling = false;
  let oldestCutoverStartedAtMs: number | null = null;

  for (const catalog of catalogs) {
    for (const row of catalog.vbuckets) {
      if (row.migrationStatus === "aborting") {
        sawAborting = true;
      } else if (row.migrationStatus === "cutover") {
        sawCutover = true;
        if (row.cutoverStartedAt) {
          const t = Date.parse(row.cutoverStartedAt);
          if (!Number.isNaN(t) && (oldestCutoverStartedAtMs === null || t < oldestCutoverStartedAtMs)) oldestCutoverStartedAtMs = t;
        }
      } else if (row.migrationStatus === "backfilling") {
        sawBackfilling = true;
      }
    }
  }

  const phase: MigrationPhase = sawAborting ? "aborting" : sawCutover ? "cutover" : sawBackfilling ? "backfilling" : "none";

  let sawCutoverSinceIdle = tracking.sawCutoverSinceIdle;
  let sawAbortingSinceIdle = tracking.sawAbortingSinceIdle;
  if (phase !== "none" && tracking.lastPhase === "none") {
    // A fresh migration cycle just started — reset the per-cycle flags so a
    // PREVIOUS cycle's abort/cutover doesn't leak into this new one's
    // eventual last-known outcome.
    sawCutoverSinceIdle = false;
    sawAbortingSinceIdle = false;
  }
  if (phase === "cutover") sawCutoverSinceIdle = true;
  if (phase === "aborting") sawAbortingSinceIdle = true;

  let status: ChecksumStatus;
  if (phase === "aborting") {
    status = { label: "aborting…", state: "aborting" };
  } else if (phase === "cutover") {
    const stalled = oldestCutoverStartedAtMs !== null && now - oldestCutoverStartedAtMs > CUTOVER_STALL_THRESHOLD_MS;
    status = stalled ? { label: "stalled — cutover not advancing", state: "stalled" } : { label: "verifying…", state: "verifying" };
  } else if (phase === "backfilling") {
    status = { label: "backfilling…", state: "backfilling" };
  } else if (sawAbortingSinceIdle) {
    status = { label: "aborted", state: "aborted" };
  } else if (sawCutoverSinceIdle) {
    status = { label: "cutover verified", state: "verified" };
  } else {
    status = { label: "idle", state: "idle" };
  }

  return { status, nextTracking: { lastPhase: phase, sawCutoverSinceIdle, sawAbortingSinceIdle } };
}

/** The scoreboard merged into every TopologySnapshot — Shardscope T4. See
 * DESIGN.md's invariant scoreboard ("writes N · lost 0 · checksum OK") and
 * this file's header comment on deriveChecksumStatus for why `checksum` is
 * a derived label, not a boolean. When no load is running, `writesAcked`/
 * `writesRetriedIdempotent`/`txAbortedExpected`/`lost` are all forced to 0
 * (meterState "green") rather than showing a PREVIOUS run's stale totals —
 * showing an old "lost 0" as if it were live would itself be a kind of
 * fake-green. `checksum` is deliberately NOT forced to "idle" alongside
 * them: it reflects the cluster's REAL migration state (a topology-op
 * doesn't require Shardscope's own load engine to be running), so it stays
 * truthful even when an operator drives a migration from the Reshard console
 * with no load traffic at all — see deriveChecksumStatus. */
export interface Scoreboard {
  writesAcked: number;
  writesRetriedIdempotent: number;
  txAbortedExpected: number;
  lost: number;
  meterState: "green" | "red";
  loadRunning: boolean;
  checksum: ChecksumStatus;
}

// ----------------------------------------------------------------------------
// Merged snapshot shape fanned out over SSE. Small and JSON-serializable by
// design — one SSE frame per tick, per subscriber. Lets the Topology room
// render heat-ramp coloring per shard (DESIGN.md's "Heat ramp" palette) and
// in-flight migration paths (DESIGN.md's "--migration" cyan) off one payload,
// no second round trip.
// ----------------------------------------------------------------------------

interface TopologySnapshot {
  ts: number;
  cluster: {
    initialized: boolean;
    catalogShardCount: number;
    shards: { total: number; active: number; draining: number };
  };
  catalogs: Array<{
    catalogShardId: string;
    totalVBuckets: number;
    vbuckets: VbucketMapRow[];
  }>;
  // Per-shard stats. `stats` is null (with a short `error`) when that one
  // shard's adminShardStats call failed this tick — a shard mid-drain, a
  // chaos "blip shard" attack, or a briefly-unreachable node. This is
  // deliberately non-fatal: the rest of the topology (the map + every
  // healthy shard) still broadcasts, so the dashboard stays composed and
  // renders the affected shard distinctly instead of the whole view going
  // dark. That "calm under chaos" contrast is the product thesis (DESIGN.md).
  shards: Array<{ shardId: string; stats: AdminShardStatsResponse | null; error?: string }>;
  // Shardscope T4 — see the Scoreboard interface above.
  scoreboard: Scoreboard;
}

// Tick cadence: how often the singleton polls cloudflare-shard-mvp's admin
// API while >=1 subscriber is connected. Not a measured value — a
// placeholder guess at "fast enough to feel live, slow enough not to hammer
// the control plane it's filming." Revisit once real load numbers exist.
const TICK_INTERVAL_MS = 900;

// Caps how many /admin/shard-stats calls run concurrently per tick, so a
// cluster with a large shard count doesn't fan out hundreds of simultaneous
// RPC calls from a single alarm tick.
const MAX_CONCURRENT_SHARD_STATS_CALLS = 8;

export class TopologyAggregator {
  private state: DurableObjectState;
  private env: Env;

  // Open SSE subscribers, one WritableStreamDefaultWriter per connected
  // browser tab. Each tick writes the merged snapshot to every entry in this
  // set; handleStream() adds an entry when a new /api/stream connection
  // comes in, and entries are dropped as soon as a write to them fails
  // (client gone) or the request's AbortSignal fires (client disconnected
  // cleanly).
  //
  // NOTE: this is in-memory, not persisted — correct, because subscriber
  // connections don't survive a DO eviction/restart anyway (the browser's
  // EventSource will auto-reconnect and re-register). Do not try to persist
  // this set to state.storage.
  private subscribers: Set<WritableStreamDefaultWriter<Uint8Array>> = new Set();

  // Guards against overlapping ticks (e.g. an alarm firing while the
  // "kick off the first tick immediately" path from handleStream() is still
  // in flight for the same instance).
  private polling = false;

  // Last successfully merged snapshot, sent immediately to any new
  // subscriber so they don't have to wait a full tick interval for their
  // first frame.
  private lastSnapshot: TopologySnapshot | null = null;

  // Shardscope T4 — cross-tick memory ONLY for deriveChecksumStatus's
  // "last-known cutover verified / aborted" outcome (see that function's doc
  // comment and ChecksumTrackingState's own comment on why this is safe to
  // lose on eviction). In-memory, not persisted, same reasoning as
  // lastSnapshot above.
  private checksumTracking: ChecksumTrackingState = initialChecksumTrackingState();

  constructor(state: DurableObjectState, env: Env) {
    this.state = state;
    this.env = env;
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);

    if (url.pathname === "/api/stream") {
      return this.handleStream(request);
    }

    return new Response("Not found", { status: 404 });
  }

  private async handleStream(request: Request): Promise<Response> {
    const { readable, writable } = new TransformStream<Uint8Array, Uint8Array>();
    const writer = writable.getWriter();
    const isFirstSubscriber = this.subscribers.size === 0;
    this.subscribers.add(writer);

    const cleanup = () => {
      this.subscribers.delete(writer);
    };
    // Cloudflare Workers aborts a Request's signal when the client
    // disconnects — drop the subscriber immediately rather than waiting for
    // the next tick's write() to discover it's gone.
    request.signal.addEventListener("abort", cleanup);

    await writer.write(sseEvent("hello", { message: "shardscope aggregator: connected, waiting for first tick" }));

    // Don't make a brand-new subscriber wait a full tick interval for data
    // that's already sitting in memory.
    if (this.lastSnapshot) {
      await this.safeWrite(writer, sseEvent("snapshot", this.lastSnapshot));
    }

    if (isFirstSubscriber) {
      // Fire-and-forget: kick the shared poll loop off right away instead of
      // waiting for the first alarm to fire, so the first viewer of an idle
      // dashboard doesn't stare at nothing for a whole TICK_INTERVAL_MS.
      // runTick() reschedules itself via scheduleNextTick() when it's done,
      // so this single call is enough to keep the loop alive.
      void this.runTick();
    }

    return new Response(readable, {
      status: 200,
      headers: {
        "content-type": "text/event-stream",
        "cache-control": "no-cache",
        connection: "keep-alive",
      },
    });
  }

  /** Schedules the next shared poll tick. */
  private async scheduleNextTick(): Promise<void> {
    await this.state.storage.setAlarm(Date.now() + TICK_INTERVAL_MS);
  }

  /** alarm() — fired by the platform per scheduleNextTick(). Just runs one
   * tick; all the real logic (overlap guard, empty-subscriber guard,
   * rescheduling) lives in runTick() so the same path can also be triggered
   * synchronously the moment the first subscriber connects. */
  async alarm(): Promise<void> {
    await this.runTick();
  }

  /** Runs (at most) one poll+fan-out cycle, then reschedules the next tick
   * if there's still someone to serve. Safe to call both from alarm() and
   * directly from handleStream() — the in-flight guard makes overlapping
   * calls a no-op. */
  private async runTick(): Promise<void> {
    if (this.polling) return; // a tick is already in flight; let it finish
    if (this.subscribers.size === 0) return; // nobody watching; don't poll into the void

    this.polling = true;
    try {
      const snapshot = await this.pollSnapshot();
      this.lastSnapshot = snapshot;
      await this.broadcast(sseEvent("snapshot", snapshot));
    } catch (err) {
      await this.broadcast(sseEvent("error", { message: this.safeErrorMessage(err) }));
    } finally {
      this.polling = false;
    }

    if (this.subscribers.size > 0) {
      await this.scheduleNextTick();
    }
    // else: last subscriber disconnected while this tick was running — go
    // idle. The next handleStream() call is responsible for restarting the
    // loop.
  }

  /** One round of admin calls, merged into a single TopologySnapshot.
   * Throws on any failure (cluster not initialized, RPC error, etc.) —
   * callers are responsible for turning that into an SSE "error" event.
   *
   * Shardscope T4: this now makes exactly ONE extra call beyond the
   * pre-existing two (adminStatus, adminVbucketMap) — a GET
   * /api/load/status against the LOAD_DRIVER singleton DO (see
   * ./load/load-driver.ts), fetched inside the SAME Promise.all as the other
   * two so it doesn't add a serial round trip. This preserves the
   * O(1)-in-viewers property the whole aggregator exists for (see this
   * file's header comment): the call happens once per TICK_INTERVAL_MS while
   * >=1 subscriber is connected, never once per subscriber. A failure here
   * is treated exactly like a per-shard adminShardStats failure below —
   * non-fatal, degrading the scoreboard to its honest "no load running"
   * shape (see mergeScoreboard) rather than blanking the whole snapshot. */
  private async pollSnapshot(): Promise<TopologySnapshot> {
    const shardApi: ShardApiBinding = this.env.SHARD_API;
    const adminToken = this.env.ADMIN_TOKEN;

    const [statusRaw, vbucketMapRaw, loadStatus] = await Promise.all([
      shardApi.adminStatus(adminToken),
      shardApi.adminVbucketMap(adminToken),
      this.fetchLoadDriverStatus(),
    ]);
    const status = statusRaw as AdminStatusResponse;
    const vbucketMap = vbucketMapRaw as AdminVbucketMapResponse;

    // Authoritative shard-id set comes from the vbucket map (the union of
    // every row's current shardId AND non-null targetShardId across every
    // catalog) — NOT from /admin/status, which only reports shard counts,
    // not ids.
    const shardIds = new Set<string>();
    for (const catalog of vbucketMap.catalogs) {
      for (const row of catalog.map) {
        shardIds.add(row.shardId);
        if (row.targetShardId) shardIds.add(row.targetShardId);
      }
    }

    // Per-shard stats are fetched independently and a single shard's failure
    // is NON-FATAL: a shard mid-drain / under a chaos "blip" / briefly
    // unreachable must not blank the whole topology. Each call gets its own
    // try/catch here; a failure becomes a `{ stats: null, error }` marker the
    // UI can render distinctly, while every healthy shard and the vbucket map
    // still broadcast. Only adminStatus/adminVbucketMap failing (above) is
    // snapshot-fatal — without those there's no topology to draw at all.
    const shards = await mapWithConcurrencyLimit(
      [...shardIds],
      MAX_CONCURRENT_SHARD_STATS_CALLS,
      async (shardId): Promise<{ shardId: string; stats: AdminShardStatsResponse | null; error?: string }> => {
        try {
          const stats = (await shardApi.adminShardStats(adminToken, { shardId })) as AdminShardStatsResponse;
          return { shardId, stats };
        } catch (err) {
          return { shardId, stats: null, error: this.safeErrorMessage(err) };
        }
      },
    );

    const catalogs = vbucketMap.catalogs.map((c) => ({
      catalogShardId: c.catalogShardId,
      totalVBuckets: c.totalVBuckets,
      vbuckets: c.map,
    }));

    return {
      ts: Date.now(),
      cluster: {
        initialized: status.initialized,
        catalogShardCount: status.catalogShardCount,
        shards: status.shards,
      },
      catalogs,
      shards,
      scoreboard: this.mergeScoreboard(catalogs, loadStatus),
    };
  }

  /** The one extra RPC this file's T4 work adds — GET /api/load/status
   * against the LOAD_DRIVER singleton, same fetch()-over-DO-binding pattern
   * src/index.ts's own forwardToLoadDriver uses. Never throws: any failure
   * (LoadDriver DO unreachable, a cold start, a non-2xx status, an
   * unparsable body) resolves to null, same non-fatal-per-call contract as
   * this file's existing adminShardStats try/catch below. */
  private async fetchLoadDriverStatus(): Promise<LoadDriverStatusResponse | null> {
    try {
      const id = this.env.LOAD_DRIVER.idFromName("singleton");
      const stub = this.env.LOAD_DRIVER.get(id);
      const res = await stub.fetch("https://load-driver.internal/api/load/status");
      if (!res.ok) return null;
      return (await res.json()) as LoadDriverStatusResponse;
    } catch {
      return null;
    }
  }

  /** Merges this tick's LoadDriver status + live vbucket map into one
   * Scoreboard — see the Scoreboard interface's own doc comment for exactly
   * which fields zero out when no load is running, and why `checksum` does
   * NOT zero out alongside them. Also advances this.checksumTracking (the
   * one piece of cross-tick memory this merge needs — see
   * deriveChecksumStatus). */
  private mergeScoreboard(catalogs: TopologySnapshot["catalogs"], loadStatus: LoadDriverStatusResponse | null): Scoreboard {
    const { status: checksum, nextTracking } = deriveChecksumStatus(catalogs, this.checksumTracking, Date.now());
    this.checksumTracking = nextTracking;

    const loadRunning = !!loadStatus?.running;
    if (!loadRunning) {
      // Honest, not fake-green: a PREVIOUS run's totals would still be
      // sitting in LoadDriver's durable state (see load-driver.ts's own
      // comment on why it persists `correctness` across ticks/evictions),
      // but rendering those as if they were live right now — while nothing
      // is actually being verified — is exactly the kind of stale-as-live
      // theater this scoreboard exists to avoid. Zero/idle is the honest
      // "nothing is running" state.
      return { writesAcked: 0, writesRetriedIdempotent: 0, txAbortedExpected: 0, lost: 0, meterState: "green", loadRunning: false, checksum };
    }
    const c = loadStatus.correctness;
    return {
      writesAcked: c.writesAcked,
      writesRetriedIdempotent: c.writesRetriedIdempotent,
      txAbortedExpected: c.txAbortedExpected,
      lost: c.lost,
      meterState: c.meterState,
      loadRunning: true,
      checksum,
    };
  }

  /** Writes `payload` to every open subscriber, dropping any writer whose
   * write() throws (the tab disconnected without a clean abort signal). */
  private async broadcast(payload: Uint8Array): Promise<void> {
    await Promise.all([...this.subscribers].map((writer) => this.safeWrite(writer, payload)));
  }

  private async safeWrite(writer: WritableStreamDefaultWriter<Uint8Array>, payload: Uint8Array): Promise<void> {
    try {
      await writer.write(payload);
    } catch {
      this.subscribers.delete(writer);
    }
  }

  /** Strips the admin token out of an error message before it's allowed
   * anywhere near an SSE frame a browser will read — defense in depth on top
   * of the fact that nothing in the admin API is expected to echo it back. */
  private safeErrorMessage(err: unknown): string {
    const raw = err instanceof Error ? err.message : String(err);
    const token = this.env.ADMIN_TOKEN;
    return token ? raw.split(token).join("[redacted]") : raw;
  }
}

function sseEvent(event: string, data: unknown): Uint8Array {
  const payload = `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
  return new TextEncoder().encode(payload);
}

/** Runs `fn` over `items` with at most `limit` calls in flight at once,
 * preserving input order in the result array. Used to bound how many
 * /admin/shard-stats RPC calls a single tick fans out concurrently. */
async function mapWithConcurrencyLimit<T, R>(items: T[], limit: number, fn: (item: T) => Promise<R>): Promise<R[]> {
  const results: R[] = new Array(items.length);
  let nextIndex = 0;

  async function worker(): Promise<void> {
    while (true) {
      const i = nextIndex++;
      if (i >= items.length) return;
      results[i] = await fn(items[i]);
    }
  }

  const workerCount = Math.min(limit, items.length);
  await Promise.all(Array.from({ length: workerCount }, () => worker()));
  return results;
}
