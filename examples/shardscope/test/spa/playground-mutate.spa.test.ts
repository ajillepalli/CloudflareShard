import { afterEach, describe, expect, it } from "vitest";
import { bootApp, type Harness } from "./helpers/domHarness";
import { liveSnapshotFixture } from "./helpers/liveReshardBoot";

describe("Shardscope SPA — Playground Mutate panel", () => {
  let harness: Harness | null = null;

  afterEach(() => {
    harness?.cleanup();
    harness = null;
  });

  it("submits /api/play/mutate and renders the response via textContent, escaping any HTML in it (XSS regression guard)", async () => {
    // A value containing a <script> tag, as if a cluster/user-controlled
    // field somehow round-tripped through the mutate response — app.js's
    // renderPlayResult must never let this parse as markup (see app.js's
    // own header comment on this panel: "every value below... reaches the
    // DOM via createElement/textContent only").
    const maliciousNote = '<script>window.__xss_fired = true;</script>';
    // GitHub #53: playFetch (every /api/play/* call, including this one) is
    // now demo-mode-gated, same as reshardFetch — the Playground room is
    // live-only, so this test must boot live, not the harness's default
    // ?demo=1, or the mutate call would short-circuit before ever hitting
    // the mocked route below.
    harness = bootApp({
      search: "",
      routes: {
        "/api/load/status": { status: 200 },
        "/api/play/mutate": {
          status: 200,
          body: { ok: true, requestId: "req-123", note: maliciousNote },
        },
      },
    });
    await harness.flush();
    harness.dispatchServerEvent("snapshot", liveSnapshotFixture());
    await harness.flush();

    harness.hook("rail-play")!.click(); // enters the Playground room, wires the Mutate form's submit listener

    const form = harness.hook("play-mutate-form") as HTMLFormElement;
    const key = harness.hook("play-mutate-key") as HTMLInputElement;
    const values = harness.hook("play-mutate-values") as HTMLTextAreaElement;
    key.value = "tpcc-w0001-i0042";
    values.value = JSON.stringify({ i_price: 12.5 });

    // handlePlayMutateSubmit is wired via form.addEventListener("submit", ...)
    // and calls evt.preventDefault() — dispatch a real, cancelable submit
    // event (jsdom's HTMLFormElement.requestSubmit() logs an "unimplemented"
    // warning; a synthetic dispatch is the reliable path here).
    form.dispatchEvent(new harness.window.Event("submit", { bubbles: true, cancelable: true }));
    await harness.flush();

    const mutateCall = harness.calls.find((c) => c.pathname === "/api/play/mutate");
    expect(mutateCall).toBeDefined();
    expect(mutateCall!.method).toBe("POST");
    expect(mutateCall!.body).toMatchObject({
      table: "tpcc_warehouse", // first PLAYGROUND_TABLES entry — the form's default selection
      op: "insert", // first PLAYGROUND_MUTATE_OPS entry
      partitionKey: "tpcc-w0001-i0042",
    });

    const resultEl = harness.hook("play-mutate-result") as HTMLElement;
    expect(resultEl.hidden).toBe(false);
    expect(resultEl.className).toContain("ok");

    // The response body (including the malicious note) is JSON.stringify'd
    // into a <pre> via textContent — assert it shows up as literal text...
    expect(resultEl.textContent).toContain(maliciousNote);
    // ...and, critically, that it was never parsed as HTML: no actual
    // <script> element exists anywhere under the result panel.
    expect(resultEl.querySelector("script")).toBeNull();
  });

  it("GitHub #53 regression: submitting in ?demo=1 never fires a real fetch and never triggers a spurious logout", async () => {
    // Default harness boot is ?demo=1. Before the #53 fix, playFetch had no
    // demo-mode guard, so submitting this form would fire a real
    // /api/play/mutate call, get a 401 (no session exists in demo mode),
    // and handleLogout() would show the login panel — breaking the
    // login-free demo-mode promise entirely.
    harness = bootApp();
    await harness.flush();

    harness.hook("rail-play")!.click();
    const form = harness.hook("play-mutate-form") as HTMLFormElement;
    form.dispatchEvent(new harness.window.Event("submit", { bubbles: true, cancelable: true }));
    await harness.flush();

    expect(harness.calls.some((c) => c.pathname === "/api/play/mutate")).toBe(false);
    expect((harness.hook("login-panel") as HTMLElement).hidden).toBe(true);

    const resultEl = harness.hook("play-mutate-result") as HTMLElement;
    expect(resultEl.hidden).toBe(false);
    expect(resultEl.textContent).toContain("demo mode");
  });
});
