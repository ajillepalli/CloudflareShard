import { afterEach, describe, expect, it } from "vitest";
import { bootApp, type Harness } from "./helpers/domHarness";

/** Minimal live TopologySnapshot (see login-live-mode.spa.test.ts's identical fixture
 * shape) with `loadRunning` overridable per test — that's the field
 * renderScenarioControls() branches on. */
function liveSnapshotFixture(loadRunning: boolean) {
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
      loadRunning,
      checksum: { label: "verified", state: "verified" },
    },
  };
}

/** Boots in authenticated live mode and pushes one snapshot with the given
 * `loadRunning` value — everything below asserts off this settled state. */
async function bootLive(loadRunning: boolean, extraRoutes: Record<string, unknown> = {}): Promise<Harness> {
  const harness = bootApp({
    search: "",
    routes: { "/api/load/status": { status: 200 }, ...extraRoutes },
  });
  await harness.flush();
  harness.dispatchServerEvent("snapshot", liveSnapshotFixture(loadRunning));
  await harness.flush();
  return harness;
}

describe("Shardscope SPA — self-service scenario controls (Topology room)", () => {
  let harness: Harness | null = null;

  afterEach(() => {
    harness?.cleanup();
    harness = null;
  });

  it("?demo=1 shows the Start banner (not the running pill) on boot, ignoring the embedded sample's own loadRunning:true", async () => {
    // Default harness boot takes the ?demo=1 path. buildSampleSnapshot()'s
    // scoreboard.loadRunning is true for illustrative purposes only (see
    // runDemoScenarioTick's own comment) — renderScenarioControls's demo
    // branch ignores that field entirely and derives `running` from the
    // independent demoScenarioRunning flag instead, which starts false.
    harness = bootApp();
    await harness.flush();

    expect((harness.hook("scenario-start-banner") as HTMLElement).hidden).toBe(false);
    expect((harness.hook("scenario-running-pill") as HTMLElement).hidden).toBe(true);
  });

  it("live + loadRunning:false shows the Start banner (enabled) and hides the running pill", async () => {
    harness = await bootLive(false);

    expect((harness.hook("scenario-start-banner") as HTMLElement).hidden).toBe(false);
    expect((harness.hook("scenario-running-pill") as HTMLElement).hidden).toBe(true);
    expect((harness.hook("scenario-start-btn") as HTMLButtonElement).disabled).toBe(false);
  });

  it("live + loadRunning:true shows the running pill and hides the Start banner", async () => {
    harness = await bootLive(true);

    expect((harness.hook("scenario-start-banner") as HTMLElement).hidden).toBe(true);
    expect((harness.hook("scenario-running-pill") as HTMLElement).hidden).toBe(false);
  });

  it("Start: posts /api/load/start with {mode:'skew'} and no targetShardId, disables the button while in flight, then logs the resolved target on success", async () => {
    harness = await bootLive(false, {
      "/api/load/start": { status: 200, body: { config: { targetShardId: "shard-7" } } },
    });

    const startBtn = harness.hook("scenario-start-btn") as HTMLButtonElement;
    startBtn.click();

    // Synchronous UI feedback happens before the fetch promise settles.
    expect(startBtn.disabled).toBe(true);
    expect(startBtn.textContent).toBe("starting…");

    await harness.flush();

    const call = harness.calls.find((c) => c.pathname === "/api/load/start");
    expect(call).toBeDefined();
    expect(call!.method).toBe("POST");
    expect(call!.body).toEqual({ mode: "skew" });

    expect(startBtn.disabled).toBe(false);
    expect(startBtn.textContent).toBe("Start the scenario");
    expect(harness.hook("event-log")!.textContent).toContain("scenario started — real writes now landing on shard-7");
  });

  it("Start: a second click while already in flight is a no-op (scenarioActionInFlight guard)", async () => {
    harness = await bootLive(false, {
      "/api/load/start": { status: 200, body: { config: { targetShardId: "shard-7" } } },
    });

    const startBtn = harness.hook("scenario-start-btn") as HTMLButtonElement;
    startBtn.click();
    startBtn.click();
    startBtn.click();
    await harness.flush();

    expect(harness.calls.filter((c) => c.pathname === "/api/load/start")).toHaveLength(1);
  });

  it("Start: a failed request (e.g. self-seed budget rejection) shows a warn banner, logs the failure, and re-enables the button", async () => {
    harness = await bootLive(false, {
      "/api/load/start": { status: 400, body: { error: "projected self-seed subrequests (1228) exceed the 850 budget" } },
    });

    const startBtn = harness.hook("scenario-start-btn") as HTMLButtonElement;
    startBtn.click();
    await harness.flush();

    expect(startBtn.disabled).toBe(false);
    expect(startBtn.textContent).toBe("Start the scenario");
    const banner = harness.hook("status-banner") as HTMLElement;
    expect(banner.hidden).toBe(false);
    expect(banner.className).toContain("warn");
    expect(banner.textContent).toContain("couldn't start the scenario");
    expect(banner.textContent).toContain("exceed the 850 budget");
    expect(harness.hook("event-log")!.textContent).toContain("scenario start failed");
  });

  it("Start: a 401 mid-flight logs the session out (login panel reappears, scenario controls hidden) instead of just showing a generic error", async () => {
    harness = await bootLive(false, {
      "/api/load/start": { status: 401 },
    });

    const startBtn = harness.hook("scenario-start-btn") as HTMLButtonElement;
    startBtn.click();
    await harness.flush();

    expect((harness.hook("login-panel") as HTMLElement).hidden).toBe(false);
    expect((harness.hook("scenario-start-banner") as HTMLElement).hidden).toBe(true);
    expect((harness.hook("scenario-running-pill") as HTMLElement).hidden).toBe(true);
    expect(harness.hook("event-log")!.textContent).toContain("logged out");
  });

  it("Stop: posts /api/load/stop, disables the button while in flight, and logs on success", async () => {
    harness = await bootLive(true, {
      "/api/load/stop": { status: 200, body: {} },
    });

    const stopBtn = harness.hook("scenario-stop-btn") as HTMLButtonElement;
    stopBtn.click();
    expect(stopBtn.disabled).toBe(true);

    await harness.flush();

    const call = harness.calls.find((c) => c.pathname === "/api/load/stop");
    expect(call).toBeDefined();
    expect(call!.method).toBe("POST");
    expect(stopBtn.disabled).toBe(false);
    expect(harness.hook("event-log")!.textContent).toContain("scenario stopped");
  });

  it("Stop: a failed request shows a warn banner and re-enables the button", async () => {
    harness = await bootLive(true, {
      "/api/load/stop": { status: 500, body: { error: "internal error" } },
    });

    const stopBtn = harness.hook("scenario-stop-btn") as HTMLButtonElement;
    stopBtn.click();
    await harness.flush();

    expect(stopBtn.disabled).toBe(false);
    const banner = harness.hook("status-banner") as HTMLElement;
    expect(banner.hidden).toBe(false);
    expect(banner.className).toContain("warn");
    expect(banner.textContent).toContain("couldn't stop the scenario");
  });

  it("a cluster-not-initialized snapshot hides the banner/pill even if a prior snapshot had left one visible", async () => {
    harness = await bootLive(false); // Start banner now visible (loadRunning:false, live).
    expect((harness.hook("scenario-start-banner") as HTMLElement).hidden).toBe(false);

    const uninitialized = liveSnapshotFixture(false);
    uninitialized.cluster.initialized = false;
    harness.dispatchServerEvent("snapshot", uninitialized);
    await harness.flush();

    expect((harness.hook("scenario-start-banner") as HTMLElement).hidden).toBe(true);
    expect((harness.hook("scenario-running-pill") as HTMLElement).hidden).toBe(true);
  });
});

describe("Shardscope SPA — demo-mode scenario simulation (?demo=1, client-side only)", () => {
  let harness: Harness | null = null;
  const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
  const FAST_TICK_MS = 5;

  afterEach(() => {
    harness?.cleanup();
    harness = null;
  });

  it("Start: shows the running pill, logs a sample-data-labeled start line, and never fires a real fetch", async () => {
    harness = bootApp({ windowOverrides: { __SHARDSCOPE_DEMO_SCENARIO_TICK_MS_OVERRIDE__: FAST_TICK_MS } });
    await harness.flush();

    (harness.hook("scenario-start-btn") as HTMLButtonElement).click();
    await harness.flush();

    expect((harness.hook("scenario-start-banner") as HTMLElement).hidden).toBe(true);
    expect((harness.hook("scenario-running-pill") as HTMLElement).hidden).toBe(false);
    const logText = harness.hook("event-log")!.textContent;
    expect(logText).toContain("scenario started");
    expect(logText).toContain("sample data");
    expect(logText).toContain("simulated");
    // The whole point of ?demo=1: this must NEVER become a real network call.
    expect(harness.calls).toHaveLength(0);
  });

  it("Codex-found: the banner subtext is swapped to honest demo copy — never claims 'real' writes or a live cluster", async () => {
    // index.html's static copy ("Starts a real write load against one
    // shard...") describes the LIVE feature — before renderScenarioControls's
    // demo branch overwrites it, a demo visitor would be told the button
    // touches a real cluster when it's actually a local-only simulation.
    harness = bootApp({ windowOverrides: { __SHARDSCOPE_DEMO_SCENARIO_TICK_MS_OVERRIDE__: FAST_TICK_MS } });
    await harness.flush();

    const subText = harness.hook("scenario-start-sub")!.textContent!;
    expect(subText).not.toContain("real write load");
    expect(subText.toLowerCase()).toContain("simulat");
    expect(subText.toLowerCase()).toContain("no live cluster");
  });

  it("ticks climb writesAcked and keep lost at 0, without ever calling fetch", async () => {
    harness = bootApp({ windowOverrides: { __SHARDSCOPE_DEMO_SCENARIO_TICK_MS_OVERRIDE__: FAST_TICK_MS } });
    await harness.flush();
    const baselineWrites = harness.hook("sb-writes")!.textContent;

    (harness.hook("scenario-start-btn") as HTMLButtonElement).click();
    await harness.flush();
    await sleep(FAST_TICK_MS * 6);
    await harness.flush();

    expect(harness.hook("sb-writes")!.textContent).not.toBe(baselineWrites);
    expect(harness.hook("sb-lost")!.textContent).toContain("lost 0");
    expect(harness.calls).toHaveLength(0);
  });

  it("Stop: reverts to the Start banner, logs 'scenario stopped', and resets the scoreboard to the static baseline", async () => {
    harness = bootApp({ windowOverrides: { __SHARDSCOPE_DEMO_SCENARIO_TICK_MS_OVERRIDE__: FAST_TICK_MS } });
    await harness.flush();
    const baselineWrites = harness.hook("sb-writes")!.textContent;

    (harness.hook("scenario-start-btn") as HTMLButtonElement).click();
    await harness.flush();
    await sleep(FAST_TICK_MS * 6);
    await harness.flush();
    expect(harness.hook("sb-writes")!.textContent).not.toBe(baselineWrites);

    (harness.hook("scenario-stop-btn") as HTMLButtonElement).click();
    await harness.flush();

    expect((harness.hook("scenario-start-banner") as HTMLElement).hidden).toBe(false);
    expect((harness.hook("scenario-running-pill") as HTMLElement).hidden).toBe(true);
    expect(harness.hook("event-log")!.textContent).toContain("scenario stopped");
    expect(harness.hook("sb-writes")!.textContent).toBe(baselineWrites);
    expect(harness.calls).toHaveLength(0);
  });

  it("a second Start click while already running does not restart the simulation or duplicate the start log line", async () => {
    harness = bootApp({ windowOverrides: { __SHARDSCOPE_DEMO_SCENARIO_TICK_MS_OVERRIDE__: FAST_TICK_MS } });
    await harness.flush();

    const startBtn = harness.hook("scenario-start-btn") as HTMLButtonElement;
    startBtn.click();
    await harness.flush();
    startBtn.click();
    startBtn.click();
    await harness.flush();

    const startCount = (harness.hook("event-log")!.textContent!.match(/scenario started/g) ?? []).length;
    expect(startCount).toBe(1);
  });
});
