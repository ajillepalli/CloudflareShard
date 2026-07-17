import { afterEach, describe, expect, it } from "vitest";
import { bootApp, type Harness } from "./helpers/domHarness";

/** Every room-wrap hook this suite asserts on, and which room each is
 * expected to be visible for. Kept as one table so each assertion below can
 * check "this room's wrap is visible AND every other room's wrap is
 * hidden" — the bug this guards against is a room switch that shows the new
 * room without properly hiding a previous one. */
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
    const el = harness.hook(hookName) as HTMLElement;
    if (room === activeRoom) {
      expect(el.hidden, `${hookName} should be visible while ${activeRoom} is active`).toBe(false);
    } else if (room === "topology" && activeRoom === "reshard") {
      // Reshard is the one exception: the Reshard console is the RIGHT
      // panel, and canvas-wrap (the topology hero) stays visible behind it
      // — see app.js's setActiveRoom, which only hides canvas-wrap for
      // room === "edge" | "play" | "app".
      expect(el.hidden, "canvas-wrap should stay visible under the Reshard console").toBe(false);
    } else {
      expect(el.hidden, `${hookName} should be hidden while ${activeRoom} is active`).toBe(true);
    }
  }
}

describe("Shardscope SPA — room switching", () => {
  let harness: Harness | null = null;

  afterEach(() => {
    harness?.cleanup();
    harness = null;
  });

  it("clicking each rail item shows only that room and hides the rest", async () => {
    harness = bootApp({
      routes: {
        "/api/reshard/lock-status": { status: 200, body: { held: false } },
      },
    });

    // Starts on Topology (index.html's shipped default).
    assertOnlyRoomVisible(harness, "topology");
    expect(harness.hook("rail-topology")!.classList.contains("active")).toBe(true);

    // App
    harness.hook("rail-app")!.click();
    assertOnlyRoomVisible(harness, "app");
    expect(harness.hook("rail-app")!.classList.contains("active")).toBe(true);
    expect(harness.hook("rail-topology")!.classList.contains("active")).toBe(false);

    // Topology
    harness.hook("rail-topology")!.click();
    assertOnlyRoomVisible(harness, "topology");
    expect(harness.hook("rail-topology")!.classList.contains("active")).toBe(true);
    expect(harness.hook("rail-app")!.classList.contains("active")).toBe(false);

    // Reshard — entering it fires a real /api/reshard/lock-status poll
    // (app.js's setActiveRoom -> pollLockStatus()); flush so that settles
    // before asserting, and so its setInterval poll is the only thing
    // ticking (cleared automatically the moment we navigate away below).
    harness.hook("rail-reshard")!.click();
    await harness.flush();
    assertOnlyRoomVisible(harness, "reshard");
    expect(harness.hook("rail-reshard")!.classList.contains("active")).toBe(true);
    expect(harness.calls.some((c) => c.pathname === "/api/reshard/lock-status")).toBe(true);

    // Edge
    harness.hook("rail-edge")!.click();
    assertOnlyRoomVisible(harness, "edge");
    expect(harness.hook("rail-edge")!.classList.contains("active")).toBe(true);
    expect(harness.hook("rail-reshard")!.classList.contains("active")).toBe(false);

    // Play — leaving Reshard for Edge above already cleared Reshard's poll
    // interval (setActiveRoom's else-branch calls stopReshardPolling()), so
    // no timer is left running by the time this test ends here.
    harness.hook("rail-play")!.click();
    assertOnlyRoomVisible(harness, "play");
    expect(harness.hook("rail-play")!.classList.contains("active")).toBe(true);
    expect(harness.hook("rail-edge")!.classList.contains("active")).toBe(false);
  });
});
