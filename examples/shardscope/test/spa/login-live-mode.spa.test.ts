import { afterEach, describe, expect, it } from "vitest";
import { bootApp, type Harness } from "./helpers/domHarness";

/** A full TopologySnapshot (see app.js's own field-mapping doc comment at the
 * top of the file, and buildSampleSnapshot() for the canonical shape) with a
 * writesAcked value ?demo=1's embedded sample never produces (48213) — proof
 * that any render asserted below came from THIS pushed snapshot, not a
 * leftover/fallback one. */
function liveSnapshotFixture() {
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
      writesAcked: 999001,
      writesRetriedIdempotent: 0,
      txAbortedExpected: 0,
      lost: 0,
      trackedKeyCount: 77,
      meterState: "green",
      verified: true,
      loadRunning: true,
      checksum: { label: "verified", state: "verified" },
    },
  };
}

describe("Shardscope SPA — login / live-mode gate (non-?demo=1)", () => {
  let harness: Harness | null = null;

  afterEach(() => {
    harness?.cleanup();
    harness = null;
  });

  it("unauthorized gate preflight (401) shows the login panel and never opens a live connection", async () => {
    // Boot WITHOUT ?demo=1 -> init() takes startLiveFlow(), which preflights
    // GET /api/load/status before ever opening the SSE stream (see
    // authPreflight()'s and startLiveFlow()'s own doc comments).
    harness = bootApp({
      search: "",
      routes: {
        "/api/load/status": { status: 401 },
      },
    });
    await harness.flush();

    expect(harness.calls.some((c) => c.pathname === "/api/load/status" && c.method === "GET")).toBe(true);
    expect((harness.hook("login-panel") as HTMLElement).hidden).toBe(false);
    expect((harness.hook("logout-btn") as HTMLElement).hidden).toBe(true);
    expect((harness.hook("sample-badge") as HTMLElement).hidden).toBe(true); // not a demo/sample state — an explicit gate
    // authPreflight()'s 401 branch (showLoginPanel()) never reaches connectLive() -> no EventSource opened.
    expect(harness.eventSources).toHaveLength(0);
    expect(harness.hook("canvas-status")!.textContent).toBe("Topology — login required");
  });

  it("a login form submit with a valid token hides the login panel and opens the live stream", async () => {
    harness = bootApp({
      search: "",
      routes: {
        "/api/load/status": { status: 401 },
        "/login": { status: 200 },
      },
    });
    await harness.flush();
    expect((harness.hook("login-panel") as HTMLElement).hidden).toBe(false);

    const form = harness.hook("login-form") as HTMLFormElement;
    const tokenInput = harness.hook("login-token-input") as HTMLInputElement;
    tokenInput.value = "correct-token";
    form.dispatchEvent(new harness.window.Event("submit", { bubbles: true, cancelable: true }));
    await harness.flush();

    const loginCall = harness.calls.find((c) => c.pathname === "/login");
    expect(loginCall).toBeDefined();
    expect(loginCall!.method).toBe("POST");
    expect(loginCall!.body).toEqual({ token: "correct-token" });

    // handleLoginSubmit's 200 branch -> hideLoginPanel() + connectLive().
    expect((harness.hook("login-panel") as HTMLElement).hidden).toBe(true);
    expect((harness.hook("logout-btn") as HTMLElement).hidden).toBe(false);
    expect(harness.eventSources).toHaveLength(1);
    expect(harness.eventSources[0]!.url).toContain("/api/stream");

    // connectLive() armed a 6s "no live cluster detected" fallback timer
    // (FALLBACK_TIMEOUT_MS); a real "hello" frame doesn't clear it, but the
    // first "snapshot" frame does (see connectLive()'s own snapshot
    // listener) — push one so no timer is left ticking past this test, per
    // this suite's "leave no timers running" discipline (no wall-clock
    // sleep needed: the snapshot listener clears it synchronously).
    harness.dispatchServerEvent("snapshot", liveSnapshotFixture());
    await harness.flush();
    expect(harness.hook("canvas-status")!.textContent).toBe("Topology — live");
  });

  it("an authorized preflight opens the live stream, and a pushed 'snapshot' SSE frame renders it as live (not sample/demo)", async () => {
    harness = bootApp({
      search: "",
      routes: {
        "/api/load/status": { status: 200 },
      },
    });
    await harness.flush();

    // authorized -> hideLoginPanel() + connectLive() -> one EventSource opened
    // against /api/stream, before any snapshot has arrived.
    expect((harness.hook("login-panel") as HTMLElement).hidden).toBe(true);
    expect(harness.eventSources).toHaveLength(1);
    expect(harness.eventSources[0]!.url).toContain("/api/stream");
    expect((harness.hook("sample-badge") as HTMLElement).hidden).toBe(true); // still connecting — not sample data

    // Drive the stub's recorded "snapshot" listener directly (connectLive()'s
    // es.addEventListener("snapshot", ...)) — this is the extension point
    // domHarness.ts's dispatchServerEvent adds for exactly this scenario.
    harness.dispatchServerEvent("snapshot", liveSnapshotFixture());
    await harness.flush();

    // render() ran off a genuinely live-pushed snapshot: sample badge stays
    // hidden, canvas status flips to "live", and the scoreboard shows this
    // exact snapshot's writesAcked (999,001 — a value ?demo=1's embedded
    // sample never produces).
    expect((harness.hook("sample-badge") as HTMLElement).hidden).toBe(true);
    expect(harness.hook("canvas-status")!.textContent).toBe("Topology — live");
    expect(harness.hook("sb-writes")!.textContent).toContain("999,001");
    expect(harness.hook("sb-lost")!.textContent).toContain("lost 0");
  });

  it("a network/unreachable preflight falls back to the embedded sample, never showing the login panel", async () => {
    harness = bootApp({
      search: "",
      routes: {
        "/api/load/status": { status: 500 },
      },
    });
    await harness.flush();

    expect((harness.hook("login-panel") as HTMLElement).hidden).toBe(true);
    expect((harness.hook("sample-badge") as HTMLElement).hidden).toBe(false);
    expect(harness.eventSources).toHaveLength(0); // fallbackToSample() never opens a live connection
  });
});
