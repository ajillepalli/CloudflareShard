import { bootApp, type Harness, type HarnessOptions } from "./domHarness";

/** Minimal live TopologySnapshot (see login-live-mode.spa.test.ts's identical
 * fixture) — just enough shape for render()/refreshReshardPickers() to seed the
 * catalog-0 / vbucket 0 / shard-0 defaults the Reshard/Chaos tests rely on. */
export function liveSnapshotFixture() {
  return {
    ts: Date.now(),
    cluster: { initialized: true, catalogShardCount: 1, shards: { total: 2, active: 2, draining: 0 } },
    catalogs: [
      {
        catalogShardId: "catalog-0",
        totalVBuckets: 2,
        vbuckets: [
          { vbucket: 0, shardId: "shard-0", migrationStatus: "none", targetShardId: null, cutoverStartedAt: null },
          { vbucket: 1, shardId: "shard-1", migrationStatus: "none", targetShardId: null, cutoverStartedAt: null },
        ],
      },
    ],
    shards: [
      { shardId: "shard-0", stats: { ok: true, tables: [], idempotencyTableSize: 0, pendingIntentCount: 0, indexPendingJobCount: 0, indexEntryCount: 0, rowOwnerCount: 0 } },
      { shardId: "shard-1", stats: { ok: true, tables: [], idempotencyTableSize: 0, pendingIntentCount: 0, indexPendingJobCount: 0, indexEntryCount: 0, rowOwnerCount: 0 } },
    ],
    scoreboard: {
      writesAcked: 1,
      writesRetriedIdempotent: 0,
      txAbortedExpected: 0,
      lost: 0,
      trackedKeyCount: 1,
      meterState: "green",
      verified: true,
      loadRunning: true,
      checksum: { label: "verified", state: "verified" },
    },
  };
}

/** Boots the app in authenticated LIVE mode (not ?demo=1) and enters the Reshard
 * room (the Chaos "Break It" panel lives inside it too). reshardFetch() — the
 * single choke point every reshard/chaos call goes through — short-circuits
 * with an honest "demo mode" error while mode==="demo" (see its own doc
 * comment in app.js), so exercising real reshard/chaos wire behavior needs
 * mode==="live" instead of the harness's convenient ?demo=1 default: an
 * authorized gate preflight, then one pushed "snapshot" SSE frame (mode only
 * flips to "live" once a snapshot lands — see connectLive()'s snapshot
 * listener), before clicking into the room. refreshReshardPickers() (called
 * synchronously on entry, off that pushed snapshot) is what seeds the
 * catalog/vbucket/shard <select> defaults tests rely on. */
export async function enterReshardRoomLive(extraRoutes: HarnessOptions["routes"] = {}): Promise<Harness> {
  const harness = bootApp({
    search: "",
    routes: { "/api/load/status": { status: 200 }, ...extraRoutes },
  });
  await harness.flush();
  harness.dispatchServerEvent("snapshot", liveSnapshotFixture());
  await harness.flush();
  harness.hook("rail-reshard")!.click();
  await harness.flush();
  return harness;
}

/** Leaves the Reshard room so its setInterval poll (app.js's
 * startReshardPolling, RESHARD_POLL_INTERVAL_MS = 1500) is cleared before the
 * test ends, rather than relying on window.close() to sweep it up. */
export function leaveReshardRoom(harness: Harness) {
  harness.hook("rail-topology")!.click();
}
