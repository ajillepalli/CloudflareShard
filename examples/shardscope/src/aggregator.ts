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
   * callers are responsible for turning that into an SSE "error" event. */
  private async pollSnapshot(): Promise<TopologySnapshot> {
    const shardApi: ShardApiBinding = this.env.SHARD_API;
    const adminToken = this.env.ADMIN_TOKEN;

    const [statusRaw, vbucketMapRaw] = await Promise.all([
      shardApi.adminStatus(adminToken),
      shardApi.adminVbucketMap(adminToken),
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

    return {
      ts: Date.now(),
      cluster: {
        initialized: status.initialized,
        catalogShardCount: status.catalogShardCount,
        shards: status.shards,
      },
      catalogs: vbucketMap.catalogs.map((c) => ({
        catalogShardId: c.catalogShardId,
        totalVBuckets: c.totalVBuckets,
        vbuckets: c.map,
      })),
      shards,
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
