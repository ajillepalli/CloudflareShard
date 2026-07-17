import { afterEach, describe, expect, it } from "vitest";
import { bootApp, type Harness } from "./helpers/domHarness";

/** Enters the Reshard room (the chaos panel lives inside it) and lets its
 * entry-triggered /api/reshard/lock-status poll settle, matching the same
 * sequencing room-switching.spa.test.ts already exercises. */
async function enterReshardRoom(harness: Harness) {
  harness.hook("rail-reshard")!.click();
  await harness.flush();
}

/** Leaves the Reshard room so its setInterval poll (app.js's
 * startReshardPolling, RESHARD_POLL_INTERVAL_MS = 1500) is cleared before the
 * test ends — the same "navigate away rather than rely on window.close()"
 * discipline room-switching.spa.test.ts documents. */
function leaveReshardRoom(harness: Harness) {
  harness.hook("rail-topology")!.click();
}

describe("Shardscope SPA — Chaos 'Break It' panel", () => {
  let harness: Harness | null = null;

  afterEach(() => {
    harness?.cleanup();
    harness = null;
  });

  it("clicking an attack button fires the matching POST /api/chaos/<attack> and renders the verdict via textContent", async () => {
    harness = bootApp({
      routes: {
        "/api/reshard/lock-status": { status: 200, body: { held: false } },
        "/api/chaos/double-submit": {
          status: 200,
          body: {
            attack: "double-submit",
            did: "fired the SAME requestId twice, concurrently, as a tpcc_stock update",
            expected: "requestId-based idempotency dedupes the second submission",
            observed: "exactly one decrement landed; the duplicate was deduped",
            survived: true,
            note: "the T4 scoreboard's lost stayed 0 throughout",
          },
        },
      },
    });

    await enterReshardRoom(harness);
    harness.hook("chaos-btn-double-submit")!.click();
    await harness.flush();

    const call = harness.calls.find((c) => c.pathname === "/api/chaos/double-submit");
    expect(call).toBeDefined();
    expect(call!.method).toBe("POST");
    expect(call!.body).toEqual({}); // handleChaosAttackClick always sends body: "{}"

    const resultEl = harness.hook("chaos-result") as HTMLElement;
    expect(resultEl.hidden).toBe(false);
    expect(harness.hook("chaos-result-verdict")!.textContent).toBe("✓ survived");
    expect(harness.hook("chaos-result-verdict")!.className).toContain("survived");
    expect(harness.hook("chaos-result-attack")!.textContent).toBe("double-submit");
    expect(harness.hook("chaos-result-observed")!.textContent).toBe("exactly one decrement landed; the duplicate was deduped");
    // No error banner alongside a real result.
    expect((harness.hook("chaos-error") as HTMLElement).hidden).toBe(true);

    leaveReshardRoom(harness);
  });

  it("fires the correct route for every attack button, including the admin-gated 'blip shard offline'", async () => {
    const attacks = [
      "double-submit",
      "mismatched-replay",
      "drain-hot-node",
      "split-hot-vbucket",
      "migrate-hot-vbucket",
      "abort-migration",
      "blip-shard-offline",
    ];
    harness = bootApp({
      routes: {
        "/api/reshard/lock-status": { status: 200, body: { held: false } },
        // Every /api/chaos/* attack in this test returns the same generic
        // "survived" outcome — this test only cares which route each button
        // wires to, not the render, which the previous test already covers.
        ...Object.fromEntries(
          attacks.map((attack) => [
            `/api/chaos/${attack}`,
            { status: 200, body: { attack, did: "x", expected: "y", observed: "z", survived: true, note: "" } },
          ]),
        ),
      },
    });

    await enterReshardRoom(harness);
    for (const attack of attacks) {
      harness.hook(`chaos-btn-${attack}`)!.click();
      await harness.flush();
    }

    for (const attack of attacks) {
      const call = harness.calls.find((c) => c.pathname === `/api/chaos/${attack}`);
      expect(call, `expected a POST to /api/chaos/${attack}`).toBeDefined();
      expect(call!.method).toBe("POST");
    }

    leaveReshardRoom(harness);
  });

  it("renders a survived:false outcome as a real '✗ broke' verdict, not as inconclusive (a real finding must never be softened)", async () => {
    harness = bootApp({
      routes: {
        "/api/reshard/lock-status": { status: 200, body: { held: false } },
        "/api/chaos/mismatched-replay": {
          status: 200,
          body: {
            attack: "mismatched-replay",
            did: "reused requestId for a second, different update",
            expected: "the gateway rejects the replay with 409",
            observed: "the replay was silently accepted and applied",
            survived: false,
            note: "a real bug, not a precondition failure",
          },
        },
      },
    });

    await enterReshardRoom(harness);
    harness.hook("chaos-btn-mismatched-replay")!.click();
    await harness.flush();

    const resultEl = harness.hook("chaos-result") as HTMLElement;
    expect(resultEl.hidden).toBe(false);
    expect(harness.hook("chaos-result-verdict")!.textContent).toBe("✗ broke");
    expect(harness.hook("chaos-result-verdict")!.className).toContain("broke");
    expect((harness.hook("chaos-error") as HTMLElement).hidden).toBe(true);

    leaveReshardRoom(harness);
  });

  it("XSS guard: HTML in the outcome's fields renders as literal text, never as parsed markup", async () => {
    const maliciousNote = '<img src=x onerror="window.__xss_fired = true">';
    const maliciousObserved = "<script>window.__xss_fired = true;</script>";
    harness = bootApp({
      routes: {
        "/api/reshard/lock-status": { status: 200, body: { held: false } },
        "/api/chaos/drain-hot-node": {
          status: 200,
          body: {
            attack: "drain-hot-node",
            did: "called reshard drain on the hot shard",
            expected: "the cluster evacuates every vBucket off this shard",
            observed: maliciousObserved,
            survived: true,
            note: maliciousNote,
          },
        },
      },
    });

    await enterReshardRoom(harness);
    harness.hook("chaos-btn-drain-hot-node")!.click();
    await harness.flush();

    const resultEl = harness.hook("chaos-result") as HTMLElement;
    expect(resultEl.hidden).toBe(false);
    // Every ChaosOutcome field is set via .textContent (renderChaosOutcome) —
    // literal text present...
    expect(harness.hook("chaos-result-observed")!.textContent).toBe(maliciousObserved);
    expect(harness.hook("chaos-result-note")!.textContent).toBe(maliciousNote);
    // ...but never parsed as markup anywhere under the result panel.
    expect(resultEl.querySelector("script")).toBeNull();
    expect(resultEl.querySelector("img")).toBeNull();

    leaveReshardRoom(harness);
  });

  it("an inconclusive (precondition-failed) attack renders as inconclusive via chaos-error — NEVER as a fabricated survived/broke verdict", async () => {
    // src/chaos.ts throws a ChaosPreconditionError (e.g. "no skew load
    // running to derive a hot shard from") when an attack can't even be
    // attempted; src/index.ts's runOperatorOp maps that to a calm HTTP 400
    // with `{ error: "<message>" }` (a plain string, not an object) — see
    // reshardFetch's own unwrap logic in app.js, which turns that into a
    // rejected Error carrying the message verbatim.
    const preconditionMessage = "no skew load running to derive a hot shard from — start load first";
    harness = bootApp({
      routes: {
        "/api/reshard/lock-status": { status: 200, body: { held: false } },
        "/api/chaos/split-hot-vbucket": { status: 400, body: { error: preconditionMessage } },
      },
    });

    await enterReshardRoom(harness);
    harness.hook("chaos-btn-split-hot-vbucket")!.click();
    await harness.flush();

    // The honesty-critical assertion: this NEVER reaches renderChaosOutcome
    // (handleChaosAttackClick's .catch branch hides chaos-result outright),
    // so there is no ✓/✗ verdict badge at all for this fire — only the calm
    // precondition message in chaos-error.
    const resultEl = harness.hook("chaos-result") as HTMLElement;
    expect(resultEl.hidden).toBe(true);

    const errorEl = harness.hook("chaos-error") as HTMLElement;
    expect(errorEl.hidden).toBe(false);
    expect(errorEl.textContent).toBe(preconditionMessage);

    leaveReshardRoom(harness);
  });

  it("an inconclusive result after a real verdict hides the STALE verdict, so it never reads as this fire's outcome", async () => {
    // Regression guard for the exact race app.js's handleChaosAttackClick
    // comments call out: a precondition message must never sit next to a
    // leftover ✓/✗ badge from an earlier, different fire.
    harness = bootApp({
      routes: {
        "/api/reshard/lock-status": { status: 200, body: { held: false } },
        "/api/chaos/double-submit": {
          status: 200,
          body: { attack: "double-submit", did: "x", expected: "y", observed: "z", survived: true, note: "" },
        },
      },
    });

    await enterReshardRoom(harness);

    // First fire: a real, rendered "survived" verdict.
    harness.hook("chaos-btn-double-submit")!.click();
    await harness.flush();
    expect((harness.hook("chaos-result") as HTMLElement).hidden).toBe(false);

    // Second fire, a DIFFERENT attack, this time inconclusive.
    harness.setRoute("/api/chaos/migrate-hot-vbucket", { status: 400, body: { error: "topology lock busy (op op-1)" } });
    harness.hook("chaos-btn-migrate-hot-vbucket")!.click();
    await harness.flush();

    // The stale "✓ survived" badge from the first fire must not still be
    // showing next to (or instead of) the new precondition message.
    expect((harness.hook("chaos-result") as HTMLElement).hidden).toBe(true);
    expect((harness.hook("chaos-error") as HTMLElement).hidden).toBe(false);
    expect(harness.hook("chaos-error")!.textContent).toBe("topology lock busy (op op-1)");

    leaveReshardRoom(harness);
  });
});
