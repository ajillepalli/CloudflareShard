/** TopologyAggregator — the single shared topology poller/fan-out Durable
 * Object behind Shardscope's /api/stream.
 *
 * Target architecture (Phase 2+, NOT implemented yet — see the TODOs below):
 * exactly one instance of this DO exists per Shardscope deployment (always
 * addressed via idFromName("singleton") from src/index.ts). On a repeating
 * alarm() tick, it makes ONE round of admin API calls against
 * cloudflare-shard-mvp (via env.SHARD_API, the RPC service binding — see
 * wrangler.toml), merges the results into a single topology snapshot, and
 * pushes that snapshot to every currently-open SSE subscriber. Browser tabs
 * never cause their own poll — they just register as a subscriber and wait
 * for the next tick's fan-out. This keeps admin API load O(1) in the number
 * of watching operators, not O(subscribers).
 *
 * Phase 1 (this file, right now): no real polling. alarm() is a stub, and
 * the one thing /api/stream actually does is send a single "hello" SSE event
 * so the wiring between src/index.ts, this DO, and a browser EventSource can
 * be verified end to end before the real data plane exists.
 */
import type { Env } from "./env";

// TODO(shardscope): this is a placeholder shape. The real snapshot needs to
// merge three admin sources into one payload:
//   - GET /admin/status            (cluster-level counters: writes, lost,
//                                    checksum, quorum — the invariant
//                                    scoreboard per DESIGN.md's layout
//                                    section)
//   - GET /admin/vbucket-map       (vBucket -> shard ownership + any
//                                    in-flight migration state — this is the
//                                    endpoint the in-flight companion task is
//                                    adding; it does not exist yet as of this
//                                    skeleton, neither as an HTTP route nor
//                                    an adminXxx RPC method. Do not build
//                                    against it until that task lands.)
//   - GET /admin/shard-stats       (per-shard load, fanned out across every
//                                    shard — this DOES exist today, both as
//                                    an HTTP route (handleAdminShardStats)
//                                    and should get an RPC-equivalent method
//                                    the same way adminListTables /
//                                    adminTopologyLockStatus do; verify
//                                    before wiring)
// The merged shape should let the Topology room render heat-ramp coloring
// per shard (DESIGN.md's "Heat ramp" palette) and in-flight migration paths
// (DESIGN.md's "--migration" cyan) without a second round trip.
interface TopologySnapshot {
  // TODO(shardscope): replace with the real merged shape once
  // /admin/vbucket-map exists. Left minimal on purpose.
  tick: number;
  generatedAt: string;
}

export class TopologyAggregator {
  private state: DurableObjectState;
  private env: Env;

  // Open SSE subscribers, one WritableStreamDefaultWriter per connected
  // browser tab. alarm() (once real) writes the merged snapshot to every
  // entry in this set on each tick; fetch() adds an entry when a new
  // /api/stream connection comes in and removes it when the connection
  // closes/errors.
  //
  // NOTE: this is in-memory, not persisted — correct, because subscriber
  // connections don't survive a DO eviction/restart anyway (the browser's
  // EventSource will auto-reconnect and re-register). Do not try to persist
  // this set to state.storage.
  private subscribers: Set<WritableStreamDefaultWriter<Uint8Array>> = new Set();

  constructor(state: DurableObjectState, env: Env) {
    this.state = state;
    this.env = env;
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);

    if (url.pathname === "/api/stream") {
      return this.handleStream();
    }

    return new Response("Not found", { status: 404 });
  }

  private async handleStream(): Promise<Response> {
    const { readable, writable } = new TransformStream<Uint8Array, Uint8Array>();
    const writer = writable.getWriter();
    this.subscribers.add(writer);

    // TODO(shardscope): once alarm()-driven polling is real, kick off the
    // first tick here if one isn't already scheduled, so the very first
    // subscriber doesn't wait a full alarm period for data. For now, send a
    // single stub event so the SSE wiring (Worker -> DO -> browser
    // EventSource) is provably connected end to end, then leave the
    // connection open with no further events.
    await writer.write(sseEvent("hello", { message: "shardscope aggregator: skeleton online, no real polling yet" }));

    // TODO(shardscope): call scheduleNextTick() here once alarm() actually
    // does something — right now there is nothing useful for an alarm to
    // produce, so no alarm is scheduled and this subscriber will just see
    // the one hello event until this Phase 1 stub is replaced.

    return new Response(readable, {
      status: 200,
      headers: {
        "content-type": "text/event-stream",
        "cache-control": "no-cache",
        connection: "keep-alive",
      },
    });
  }

  /** Schedules the next shared poll tick. Call this once real polling exists
   * (from the constructor / first subscriber, and again at the end of each
   * alarm() run) to keep the tick loop going as long as at least one
   * subscriber cares. Left as a standalone method now so alarm() can call it
   * without this file needing to change shape later. */
  private async scheduleNextTick(intervalMs = 2000): Promise<void> {
    // TODO(shardscope): pick a real tick interval once we know the cost of
    // an /admin/status + /admin/vbucket-map + fanned /admin/shard-stats
    // round trip. 2s is a placeholder guess, not a measured value.
    await this.state.storage.setAlarm(Date.now() + intervalMs);
  }

  /** alarm() — the one place the real shared poll + fan-out will happen.
   *
   * TODO(shardscope): implement the real body once /admin/vbucket-map
   * exists:
   *   1. If this.subscribers is empty, do nothing (don't poll for no one)
   *      and don't reschedule — let the alarm loop go idle. The next
   *      handleStream() call is responsible for restarting it.
   *   2. Otherwise, call env.SHARD_API for /admin/status,
   *      /admin/vbucket-map, and fanned /admin/shard-stats (however many
   *      calls "fanned" turns out to require — see CloudflareShardRpc's
   *      admin method surface in the main Worker's src/index.ts for what's
   *      actually callable over the RPC binding vs. still HTTP-only today).
   *   3. Merge the three results into one TopologySnapshot.
   *   4. Write one SSE "snapshot" event, JSON-encoded, to every writer in
   *      this.subscribers. Drop/remove any writer whose write() throws
   *      (the tab disconnected).
   *   5. Call scheduleNextTick() again to keep the loop going.
   */
  async alarm(): Promise<void> {
    // Intentionally empty in Phase 1. See TODO block above.
  }
}

function sseEvent(event: string, data: unknown): Uint8Array {
  const payload = `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
  return new TextEncoder().encode(payload);
}
