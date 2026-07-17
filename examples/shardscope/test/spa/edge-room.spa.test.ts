import { afterEach, describe, expect, it } from "vitest";
import { bootApp, type Harness } from "./helpers/domHarness";

describe("Shardscope SPA — Edge room (honesty contract)", () => {
  let harness: Harness | null = null;

  afterEach(() => {
    harness?.cleanup();
    harness = null;
  });

  it("demo mode: entering the room never calls /api/edge and renders the explicit 'no live measurement' state, not a fabricated latency", () => {
    harness = bootApp(); // default ?demo=1

    harness.hook("rail-edge")!.click();

    expect(harness.calls.some((c) => c.pathname === "/api/edge")).toBe(false);
    expect((harness.hook("edge-wrap") as HTMLElement).hidden).toBe(false);
    // renderEdgeUnmeasured never writes a number here — always the "—" placeholder.
    expect(harness.hook("edge-hero-value")!.textContent).toBe("—");
    expect(harness.hook("edge-hero-value")!.className).toContain("local");
    expect(harness.hook("edge-status")!.textContent).toContain("demo mode");
    expect(harness.hook("edge-hero-served")!.textContent).toContain("demo mode");
    expect(harness.hook("edge-hero-caption")!.textContent).toContain("illustrative reference points only");
  });

  it("demo mode: the map renders only illustrative reference dots (title-tagged), no 'you' dot, since nothing was measured", () => {
    harness = bootApp();
    harness.hook("rail-edge")!.click();

    const svg = harness.hook("edge-map-svg") as HTMLElement;
    // ILLUSTRATIVE_REGIONS has 6 hand-picked cities, each explicitly tagged
    // "illustrative reference point, not measured" via a <title> — see
    // renderEdgeMap()'s honesty contract.
    const illustrativeDots = svg.querySelectorAll("circle.edge-dot-illustrative");
    expect(illustrativeDots.length).toBe(6);
    illustrativeDots.forEach((dot) => {
      expect(dot.querySelector("title")!.textContent).toContain("illustrative reference point, not measured");
    });
    // No real "you" dot rendered — renderEdgeMap(null) was called (youPoint is null).
    expect(svg.querySelector("circle.edge-dot-you")).toBeNull();
    expect(svg.querySelector(".you-label")).toBeNull();
  });

  it("live path, no edge data available (worker running locally): fetches /api/edge and renders the honest 'no edge data' state, never a fabricated number", async () => {
    // Boot WITHOUT ?demo=1 so mode never becomes "demo" (the only mode
    // startEdgeRoom() special-cases to skip the fetch). Mock the gate
    // preflight to an unreachable/error status so startLiveFlow() falls back
    // to the embedded sample (mode = "sample-fallback") WITHOUT ever showing
    // the login panel or opening a live EventSource/timer — the cleanest way
    // to reach the Edge room's live fetch path deterministically. See
    // login-live-mode.spa.test.ts for the login-panel-shown and
    // authorized+SSE branches of this same gate.
    harness = bootApp({
      search: "",
      routes: {
        "/api/load/status": { status: 500 },
        // buildEdgeInfo's { local: true } shape: no request.cf on this
        // Worker instance (e.g. local dev/miniflare) -> never a real colo/geo.
        "/api/edge": { status: 200, body: { local: true } },
      },
    });
    await harness.flush(); // settles startLiveFlow()'s authPreflight().then(...) fallback

    expect((harness.hook("login-panel") as HTMLElement).hidden).toBe(true); // never gated — fell back to sample, not unauthorized

    harness.hook("rail-edge")!.click();
    await harness.flush();

    expect(harness.calls.some((c) => c.pathname === "/api/edge" && c.method === "GET")).toBe(true);
    expect(harness.hook("edge-hero-value")!.textContent).toBe("—");
    expect(harness.hook("edge-hero-value")!.className).toContain("local");
    expect(harness.hook("edge-status")!.textContent).toContain("running locally");
    expect(harness.hook("edge-hero-caption")!.textContent).toContain("illustrative only");
  });

  it("live path, /api/edge unreachable: renders the honest error state, never a fabricated number", async () => {
    harness = bootApp({
      search: "",
      routes: {
        "/api/load/status": { status: 500 },
        "/api/edge": { status: 503 },
      },
    });
    await harness.flush();

    harness.hook("rail-edge")!.click();
    await harness.flush();

    expect(harness.calls.some((c) => c.pathname === "/api/edge")).toBe(true);
    expect(harness.hook("edge-hero-value")!.textContent).toBe("—");
    expect(harness.hook("edge-status")!.textContent).toContain("unreachable");
    expect(harness.hook("edge-hero-served")!.textContent).toContain("couldn't measure");
  });
});
