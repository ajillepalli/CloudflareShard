import { afterEach, describe, expect, it } from "vitest";
import { bootApp, type Harness } from "./helpers/domHarness";

/** VISIBILITY_PAUSE_GRACE_MS's real-world value (30s) would make this a slow,
 * flaky wall-clock test — booted with the window-override seam app.js reads
 * (see its own doc comment) shrunk to a few ms instead, mirroring
 * TopologyAggregator's constructor-injected writeTimeoutMs on the server side.
 */
const FAST_GRACE_MS = 20;

/** jsdom's `document.hidden` has no native setter — flips it via
 * Object.defineProperty (the standard way to fake Page Visibility state in a
 * DOM test environment) and dispatches the same event app.js listens for. */
function setHidden(harness: Harness, hidden: boolean) {
  Object.defineProperty(harness.window.document, "hidden", { value: hidden, configurable: true });
  harness.window.document.dispatchEvent(new harness.window.Event("visibilitychange"));
}

/** Real wall-clock wait (not harness.flush()'s 0ms macrotask tick, which
 * isn't guaranteed to be >= FAST_GRACE_MS) so the grace-period setTimeout
 * genuinely has time to fire before assertions run. */
function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

describe("Shardscope SPA — visibility-based live-connection pause", () => {
  let harness: Harness | null = null;

  afterEach(() => {
    harness?.cleanup();
    harness = null;
  });

  it("hiding the tab past the grace period closes the EventSource and shows a paused state", async () => {
    harness = bootApp({
      search: "",
      routes: { "/api/load/status": { status: 200 } },
      windowOverrides: { __SHARDSCOPE_VISIBILITY_PAUSE_GRACE_MS_OVERRIDE__: FAST_GRACE_MS },
    });
    await harness.flush();
    expect(harness.eventSources).toHaveLength(1);

    setHidden(harness, true);
    await sleep(FAST_GRACE_MS * 4);
    await harness.flush();

    expect(harness.hook("live-chip-label")!.textContent).toBe("paused (tab hidden)");
    const logText = harness.hook("event-log")!.textContent ?? "";
    expect(logText).toContain("live connection paused");
  });

  it("a quick tab-switch shorter than the grace period never pauses (no flapping on brief hides)", async () => {
    harness = bootApp({
      search: "",
      routes: { "/api/load/status": { status: 200 } },
      windowOverrides: { __SHARDSCOPE_VISIBILITY_PAUSE_GRACE_MS_OVERRIDE__: 1000 },
    });
    await harness.flush();
    expect(harness.eventSources).toHaveLength(1);

    setHidden(harness, true);
    await sleep(10); // well under the 1000ms grace period
    setHidden(harness, false);
    await sleep(50);
    await harness.flush();

    // Never actually paused: still exactly the one original EventSource, no
    // "paused" log line, and no reconnect (which only fires from the resume
    // branch — never entered here since pausedForVisibility was never set).
    expect(harness.eventSources).toHaveLength(1);
    const logText = harness.hook("event-log")!.textContent ?? "";
    expect(logText).not.toContain("live connection paused");
    expect(logText).not.toContain("reconnecting");
  });

  it("bringing the tab back visible after a pause reconnects the live stream", async () => {
    harness = bootApp({
      search: "",
      routes: { "/api/load/status": { status: 200 } },
      windowOverrides: { __SHARDSCOPE_VISIBILITY_PAUSE_GRACE_MS_OVERRIDE__: FAST_GRACE_MS },
    });
    await harness.flush();

    setHidden(harness, true);
    await sleep(FAST_GRACE_MS * 4);
    await harness.flush();
    expect(harness.hook("live-chip-label")!.textContent).toBe("paused (tab hidden)");

    setHidden(harness, false);
    await harness.flush();

    // connectLive() ran again: a SECOND EventSource opened (the paused one
    // was already closed and dropped, not reused).
    expect(harness.eventSources).toHaveLength(2);
    expect(harness.eventSources[1]!.url).toContain("/api/stream");
    const logText = harness.hook("event-log")!.textContent ?? "";
    expect(logText).toContain("tab visible again");
  });

  it("demo mode (?demo=1) never pauses — there is no live connection to pause", async () => {
    harness = bootApp({
      windowOverrides: { __SHARDSCOPE_VISIBILITY_PAUSE_GRACE_MS_OVERRIDE__: FAST_GRACE_MS },
    });
    await harness.flush();
    expect(harness.eventSources).toHaveLength(0); // ?demo=1 never opens one

    setHidden(harness, true);
    await sleep(FAST_GRACE_MS * 4);
    await harness.flush();

    expect(harness.eventSources).toHaveLength(0);
    const logText = harness.hook("event-log")!.textContent ?? "";
    expect(logText).not.toContain("live connection paused");
  });

  it("a tab that's ALREADY hidden when the live connection opens still arms the pause timer (Codex review round 1 gap)", async () => {
    // Regression guard: connectLive() used to only react to a FUTURE
    // visibilitychange event — a stream opened while the tab is already
    // backgrounded (opened in a background tab, or hidden while the gate
    // preflight/login was still pending) never fired that event, so no pause
    // timer was ever armed and the connection could stay open indefinitely.
    harness = bootApp({
      search: "",
      routes: { "/api/load/status": { status: 200 } },
      documentHiddenAtBoot: true,
      windowOverrides: { __SHARDSCOPE_VISIBILITY_PAUSE_GRACE_MS_OVERRIDE__: FAST_GRACE_MS },
    });
    await harness.flush();
    expect(harness.eventSources).toHaveLength(1); // connectLive() still opens one

    await sleep(FAST_GRACE_MS * 4);
    await harness.flush();

    expect(harness.hook("live-chip-label")!.textContent).toBe("paused (tab hidden)");
    const logText = harness.hook("event-log")!.textContent ?? "";
    expect(logText).toContain("live connection paused");
  });
});
