import { afterEach, describe, expect, it } from "vitest";
import { bootApp, type Harness } from "./helpers/domHarness";

/** Mirrors room-switching.spa.test.ts's table — which room-wrap hook should
 * be visible for a given active room (Reshard is the one exception:
 * canvas-wrap stays visible behind the Reshard console). */
const ROOM_WRAP_HOOKS = {
  topology: "canvas-wrap",
  reshard: "reshard-panel",
  edge: "edge-wrap",
  play: "play-wrap",
  app: "app-wrap",
} as const;

type Room = keyof typeof ROOM_WRAP_HOOKS;

function assertOnlyRoomVisible(harness: Harness, activeRoom: Room) {
  for (const [room, hookName] of Object.entries(ROOM_WRAP_HOOKS) as Array<[Room, string]>) {
    const node = harness.hook(hookName) as HTMLElement;
    if (room === activeRoom) {
      expect(node.hidden, `${hookName} should be visible while ${activeRoom} is active`).toBe(false);
    } else if (room === "topology" && activeRoom === "reshard") {
      expect(node.hidden, "canvas-wrap should stay visible under the Reshard console").toBe(false);
    } else {
      expect(node.hidden, `${hookName} should be hidden while ${activeRoom} is active`).toBe(true);
    }
  }
}

describe("Shardscope SPA — shareable deep-links", () => {
  let harness: Harness | null = null;

  afterEach(() => {
    harness?.cleanup();
    harness = null;
  });

  it("?room=reshard boots straight into the Reshard room", async () => {
    harness = bootApp({
      search: "?demo=1&room=reshard",
      routes: { "/api/reshard/lock-status": { status: 200, body: { held: false } } },
    });
    await harness.flush();

    assertOnlyRoomVisible(harness, "reshard");
    expect(harness.hook("rail-reshard")!.classList.contains("active")).toBe(true);
  });

  it("?demo=1&room=edge boots into Edge and stays demo-safe (never calls /api/edge)", async () => {
    harness = bootApp({ search: "?demo=1&room=edge" });
    await harness.flush();

    assertOnlyRoomVisible(harness, "edge");
    expect(harness.hook("rail-edge")!.classList.contains("active")).toBe(true);
    // The whole point of applying the deep-link room AFTER `mode` is set to
    // "demo": startEdgeRoom() must take the illustrative-only branch, not
    // fire a real measurement.
    expect(harness.calls.some((c) => c.pathname === "/api/edge")).toBe(false);
    expect(harness.hook("edge-status")!.textContent).toContain("demo mode");
  });

  it("an unrecognized ?room falls back to the default (Topology) without crashing", () => {
    expect(() => {
      harness = bootApp({ search: "?demo=1&room=not-a-real-room" });
    }).not.toThrow();

    assertOnlyRoomVisible(harness!, "topology");
    expect(harness!.hook("rail-topology")!.classList.contains("active")).toBe(true);
  });

  it("an absent ?room also falls back to the default (Topology) — plain ?demo=1 is unaffected", () => {
    harness = bootApp(); // default search = "?demo=1", no room param
    assertOnlyRoomVisible(harness, "topology");
    expect(harness.hook("sample-badge")!.hidden).toBe(false);
    expect(harness.calls).toEqual([]);
  });

  it("the address bar reflects the current room after a room switch (history.replaceState, no reload)", () => {
    harness = bootApp(); // ?demo=1

    harness.hook("rail-app")!.click();
    expect(harness.window.location.search).toContain("room=app");
    expect(harness.window.location.search).toContain("demo=1"); // existing param preserved

    harness.hook("rail-play")!.click();
    expect(harness.window.location.search).toContain("room=play");
    expect(harness.window.location.search).not.toContain("room=app");
  });
});

describe("Shardscope SPA — Share this view", () => {
  let harness: Harness | null = null;

  afterEach(() => {
    harness?.cleanup();
    harness = null;
  });

  it("clicking 'Share this view' copies a deep-link to the CURRENT room via navigator.clipboard.writeText", async () => {
    harness = bootApp(); // ?demo=1, default room = topology

    harness.hook("share-btn")!.click();
    await harness.flush();

    expect(harness.clipboardWrites.length).toBe(1);
    const url = new URL(harness.clipboardWrites[0]);
    expect(url.searchParams.get("room")).toBe("topology");
    expect(url.searchParams.get("demo")).toBe("1");
  });

  it("switching rooms first changes what 'Share this view' copies", async () => {
    harness = bootApp();
    harness.hook("rail-play")!.click();

    harness.hook("share-btn")!.click();
    await harness.flush();

    const url = new URL(harness.clipboardWrites[0]);
    expect(url.searchParams.get("room")).toBe("play");
  });

  it("shows a brief 'copied' confirmation on the button", async () => {
    harness = bootApp();
    const btn = harness.hook("share-btn") as HTMLButtonElement;
    const originalLabel = btn.textContent;

    btn.click();
    await harness.flush();

    expect(btn.textContent).not.toBe(originalLabel);
    expect(btn.textContent).toContain("Copied");
  });
});

describe("Shardscope SPA — guided tour", () => {
  let harness: Harness | null = null;

  afterEach(() => {
    harness?.cleanup();
    harness = null;
  });

  it("&tour=1 auto-starts the tour after boot (overlay visible, step 1)", () => {
    harness = bootApp({ search: "?demo=1&tour=1" });

    expect((harness.hook("tour-overlay") as HTMLElement).hidden).toBe(false);
    expect(harness.hook("tour-step-indicator")!.textContent).toMatch(/^1 \/ \d+$/);
    expect(harness.hook("tour-title")!.textContent!.length).toBeGreaterThan(0);
    expect(harness.hook("tour-caption")!.textContent!.length).toBeGreaterThan(0);
  });

  it("bare ?tour (no =1) also auto-starts the tour", () => {
    harness = bootApp({ search: "?demo=1&tour" });
    expect((harness.hook("tour-overlay") as HTMLElement).hidden).toBe(false);
  });

  it("without the flag, the tour never auto-starts", () => {
    harness = bootApp(); // ?demo=1, no tour flag
    expect((harness.hook("tour-overlay") as HTMLElement).hidden).toBe(true);
  });

  it("Next advances the step and switches rooms accordingly; Back retreats", async () => {
    harness = bootApp({
      search: "?demo=1&tour=1",
      routes: { "/api/reshard/lock-status": { status: 200, body: { held: false } } },
    });

    // Step 1: welcome, no room switch — still on the default Topology room.
    expect(harness.hook("tour-step-indicator")!.textContent).toBe("1 / 7");
    assertOnlyRoomVisible(harness, "topology");

    // Step 2: Topology (explicit).
    harness.hook("tour-next-btn")!.click();
    expect(harness.hook("tour-step-indicator")!.textContent).toBe("2 / 7");
    assertOnlyRoomVisible(harness, "topology");

    // Step 3: Reshard — the hero step.
    harness.hook("tour-next-btn")!.click();
    await harness.flush();
    expect(harness.hook("tour-step-indicator")!.textContent).toBe("3 / 7");
    assertOnlyRoomVisible(harness, "reshard");

    // Step 4: App.
    harness.hook("tour-next-btn")!.click();
    expect(harness.hook("tour-step-indicator")!.textContent).toBe("4 / 7");
    assertOnlyRoomVisible(harness, "app");

    // Back to step 3 (Reshard) again.
    harness.hook("tour-back-btn")!.click();
    expect(harness.hook("tour-step-indicator")!.textContent).toBe("3 / 7");
    assertOnlyRoomVisible(harness, "reshard");

    // Back button is disabled exactly at step 1.
    harness.hook("tour-back-btn")!.click();
    harness.hook("tour-back-btn")!.click();
    expect(harness.hook("tour-step-indicator")!.textContent).toBe("1 / 7");
    expect((harness.hook("tour-back-btn") as HTMLButtonElement).disabled).toBe(true);
  });

  it("Skip/dismiss hides the overlay and nothing keeps advancing afterward (no lingering timer)", async () => {
    harness = bootApp({ search: "?demo=1&tour=1" });
    harness.hook("tour-next-btn")!.click(); // step 2
    const stepBeforeSkip = harness.hook("tour-step-indicator")!.textContent;

    harness.hook("tour-skip-btn")!.click();
    expect((harness.hook("tour-overlay") as HTMLElement).hidden).toBe(true);

    // Wait a few ticks (long enough for any auto-advance timer to have
    // fired, if one existed) and confirm the step indicator is untouched —
    // the tour has no auto-advance timer at all, so this holds trivially,
    // but it's the behavior the "leak-free" requirement demands either way.
    await harness.flush();
    await new Promise((resolve) => harness!.window.setTimeout(resolve, 50));
    expect(harness.hook("tour-step-indicator")!.textContent).toBe(stepBeforeSkip);
    expect((harness.hook("tour-overlay") as HTMLElement).hidden).toBe(true);
  });

  it("Esc dismisses the tour", () => {
    harness = bootApp({ search: "?demo=1&tour=1" });
    expect((harness.hook("tour-overlay") as HTMLElement).hidden).toBe(false);

    const evt = new harness.window.KeyboardEvent("keydown", { key: "Escape" });
    harness.window.document.dispatchEvent(evt);

    expect((harness.hook("tour-overlay") as HTMLElement).hidden).toBe(true);
  });

  it("'Take the tour' button starts the tour manually", () => {
    harness = bootApp(); // ?demo=1, no auto-start
    expect((harness.hook("tour-overlay") as HTMLElement).hidden).toBe(true);

    harness.hook("tour-start-btn")!.click();
    expect((harness.hook("tour-overlay") as HTMLElement).hidden).toBe(false);
    expect(harness.hook("tour-step-indicator")!.textContent).toBe("1 / 7");
  });

  it("never fires a mutating /api/reshard/* or /api/chaos/* POST just from running the whole tour", async () => {
    harness = bootApp({
      search: "?demo=1&tour=1",
      routes: { "/api/reshard/lock-status": { status: 200, body: { held: false } } },
    });

    // Walk every step to the end — click Next until the overlay closes
    // itself (tourNext() ends the tour once Next is clicked from the final
    // step), bounded so a bug that stops it auto-ending can't hang the test.
    for (let i = 0; i < 12 && !(harness.hook("tour-overlay") as HTMLElement).hidden; i++) {
      harness.hook("tour-next-btn")!.click();
      await harness.flush();
    }

    expect((harness.hook("tour-overlay") as HTMLElement).hidden).toBe(true); // tour finished itself

    const mutatingCalls = harness.calls.filter(
      (c) => c.method === "POST" && (c.pathname.startsWith("/api/reshard/") || c.pathname.startsWith("/api/chaos/")),
    );
    expect(mutatingCalls).toEqual([]);
  });

  it("the tour composes with a mid-run 'Share this view' click — copies the current room + tour=1", async () => {
    harness = bootApp({ search: "?demo=1&tour=1" });
    harness.hook("tour-next-btn")!.click(); // step 2: Topology

    harness.hook("share-btn")!.click();
    await harness.flush();

    const url = new URL(harness.clipboardWrites[0]);
    expect(url.searchParams.get("room")).toBe("topology");
    expect(url.searchParams.get("tour")).toBe("1");
  });

  it("finishing (or skipping) the tour drops the &tour=1 flag from a subsequent share", async () => {
    harness = bootApp({ search: "?demo=1&tour=1" });
    harness.hook("tour-skip-btn")!.click();

    harness.hook("share-btn")!.click();
    await harness.flush();

    const url = new URL(harness.clipboardWrites[0]);
    expect(url.searchParams.has("tour")).toBe(false);
  });
});
