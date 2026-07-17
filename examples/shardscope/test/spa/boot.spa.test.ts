import { afterEach, describe, expect, it } from "vitest";
import { bootApp, type Harness } from "./helpers/domHarness";

describe("Shardscope SPA — boot", () => {
  let harness: Harness | null = null;

  afterEach(() => {
    harness?.cleanup();
    harness = null;
  });

  it("evaluates app.js against the real index.html DOM without throwing", () => {
    expect(() => {
      harness = bootApp();
    }).not.toThrow();
    expect(harness).not.toBeNull();
  });

  it("renders the default room (Topology) with the embedded sample data", () => {
    harness = bootApp(); // default ?demo=1 — no /api/* calls, renders buildSampleSnapshot() synchronously

    // Topology is the default active room — index.html already ships this as
    // the default state; app.js's init() (called at the bottom of the file)
    // shouldn't have touched room visibility since nothing calls
    // setActiveRoom("topology") when it's already active (setActiveRoom
    // early-returns on `room === activeRoom`).
    expect(harness.hook("rail-topology")!.classList.contains("active")).toBe(true);
    expect((harness.hook("canvas-wrap") as HTMLElement).hidden).toBe(false);
    expect((harness.hook("play-wrap") as HTMLElement).hidden).toBe(true);
    expect((harness.hook("app-wrap") as HTMLElement).hidden).toBe(true);
    expect((harness.hook("edge-wrap") as HTMLElement).hidden).toBe(true);

    // ?demo=1's whole point: sample badge visible, no live gate/login panel.
    expect((harness.hook("sample-badge") as HTMLElement).hidden).toBe(false);
    expect((harness.hook("login-panel") as HTMLElement).hidden).toBe(true);

    // buildSampleSnapshot() rendered synchronously — the scoreboard isn't
    // still showing its unrendered "—" placeholders.
    expect(harness.hook("sb-writes")!.textContent).toContain("writes");
    expect(harness.hook("sb-writes")!.textContent).not.toBe("writes —");
    expect(harness.hook("canvas-status")!.textContent).toBe("Topology — demo");
  });

  it("never opens a live connection or calls /api/* in demo mode", () => {
    harness = bootApp();
    expect(harness.calls).toEqual([]);
  });
});
