import { afterEach, describe, expect, it } from "vitest";
import { bootApp, type Harness } from "./helpers/domHarness";

describe("Shardscope SPA — Build on it panel (App room)", () => {
  let harness: Harness | null = null;

  afterEach(() => {
    harness?.cleanup();
    harness = null;
  });

  it("entering the App room loads GET /api/build/manifest and renders the file list + first file's content via textContent", async () => {
    harness = bootApp({
      search: "", // live path (not ?demo=1) — see this file's other test for the demo-mode behavior
      routes: {
        // authPreflight() checks this before anything else on the live path.
        "/api/load/status": { status: 200, body: {} },
        "/api/build/manifest": {
          status: 200,
          body: {
            files: [
              { path: "wrangler.toml", content: 'binding = "SHARD_API"' },
              { path: "src/index.ts", content: "export default { fetch() {} };" },
            ],
          },
        },
        "/api/play/table-scan": { status: 200, body: { rows: [] } },
      },
    });

    harness.hook("rail-app")!.click();
    await harness.flush();

    const manifestCalls = harness.calls.filter((c) => c.pathname === "/api/build/manifest");
    expect(manifestCalls).toHaveLength(1);
    expect(manifestCalls[0].method).toBe("GET");

    const fileList = harness.hook("build-file-list") as HTMLElement;
    const items = Array.from(fileList.querySelectorAll(".build-file-item"));
    expect(items.map((el) => el.textContent)).toEqual(["wrangler.toml", "src/index.ts"]);
    expect(items[0].classList.contains("active")).toBe(true);

    expect(harness.hook("build-file-preview")!.textContent).toBe('binding = "SHARD_API"');
    expect(harness.hook("build-file-preview-name")!.textContent).toBe("wrangler.toml");
    expect(harness.hook("build-status")!.textContent).toBe("2 files");

    // Clicking the second file switches the preview and the active item —
    // via textContent only (asserted for real by the XSS test below).
    items[1].dispatchEvent(new harness.window.Event("click", { bubbles: true }));
    await harness.flush();
    expect(harness.hook("build-file-preview")!.textContent).toBe("export default { fetch() {} };");
    expect(harness.hook("build-file-preview-name")!.textContent).toBe("src/index.ts");
  });

  it("a malicious file content string from the manifest renders inert (XSS regression guard)", async () => {
    const payload = '<img src=x onerror="window.__xss_fired = true">';
    harness = bootApp({
      search: "",
      routes: {
        "/api/load/status": { status: 200, body: {} },
        "/api/build/manifest": { status: 200, body: { files: [{ path: "README.md", content: payload }] } },
        "/api/play/table-scan": { status: 200, body: { rows: [] } },
      },
    });

    harness.hook("rail-app")!.click();
    await harness.flush();

    const preview = harness.hook("build-file-preview") as HTMLElement;
    // The payload shows up as literal text...
    expect(preview.textContent).toBe(payload);
    // ...and was never parsed as markup: no <img> element materialized,
    // and the "attack" never ran.
    expect(preview.querySelector("img")).toBeNull();
    expect((harness.window as unknown as { __xss_fired?: boolean }).__xss_fired).toBeUndefined();

    const fileList = harness.hook("build-file-list") as HTMLElement;
    expect(fileList.querySelector("img")).toBeNull();
  });

  it("clicking Download starter repo fires GET /api/build/scaffold", async () => {
    harness = bootApp({
      search: "",
      routes: {
        "/api/load/status": { status: 200, body: {} },
        "/api/build/manifest": { status: 200, body: { files: [{ path: "README.md", content: "hello" }] } },
        "/api/build/scaffold": { status: 200, body: {} },
        "/api/play/table-scan": { status: 200, body: { rows: [] } },
      },
    });

    harness.hook("rail-app")!.click();
    await harness.flush();

    harness.hook("build-download-btn")!.click();
    await harness.flush();

    const scaffoldCalls = harness.calls.filter((c) => c.pathname === "/api/build/scaffold");
    expect(scaffoldCalls).toHaveLength(1);
    expect(scaffoldCalls[0].method).toBe("GET");
    expect(harness.hook("build-error")!.hidden).toBe(true);
  });

  it("?demo=1 never calls /api/build/* and shows an honest demo-mode message instead", async () => {
    harness = bootApp(); // default search is "?demo=1"

    harness.hook("rail-app")!.click();
    await harness.flush();

    expect(harness.calls.some((c) => c.pathname.startsWith("/api/build/"))).toBe(false);
    expect(harness.hook("build-status")!.textContent).toBe("demo mode — no live preview");
    expect((harness.hook("build-download-btn") as HTMLButtonElement).disabled).toBe(true);
  });
});
