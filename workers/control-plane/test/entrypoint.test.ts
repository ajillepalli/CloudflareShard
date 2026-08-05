import { SELF, createExecutionContext, env } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import ControlPlaneWorker from "../src/index.js";

describe("route-less ControlPlaneWorker", () => {
  it("exposes no public HTTP product surface", async () => {
    const response = await SELF.fetch("https://control-plane.invalid/anything");
    expect(response.status).toBe(404);
    expect(await response.text()).toBe("Not Found");
  });

  it("rejects malformed untrusted registration RPC values without throwing", async () => {
    const worker = new ControlPlaneWorker(createExecutionContext(), env);
    const malformed: unknown[] = [null, {}, { record: null }, { record: { tx_id: "untrusted" } }];

    for (const value of malformed) {
      await expect(worker.registerManifest(value)).resolves.toMatchObject({
        ok: false,
        status: "rejected",
        http_status: 400,
        error: { code: "TX_ENVELOPE_INVALID" },
      });
    }
  });
});
