import { SELF, env } from "cloudflare:test";
import { describe, expect, it } from "vitest";

describe("restore public contract boundary", () => {
  it("returns the typed invalid-request contract for authenticated malformed JSON", async () => {
    const response = await SELF.fetch("https://worker.internal/admin/restore-preview", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        authorization: `Bearer ${env.ADMIN_TOKEN}`,
      },
      body: "{not-json",
    });

    expect(response.status).toBe(400);
    expect(await response.json()).toEqual({
      ok: false,
      status: "rejected",
      error: {
        schema_version: 1,
        code: "RESTORE_INVALID_REQUEST",
        message: "Request body must be valid JSON.",
        http_status: 400,
        retryable: false,
      },
    });
  });
});
