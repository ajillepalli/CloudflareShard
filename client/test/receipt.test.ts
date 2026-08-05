import { mkdtemp, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { stableStringify, writeReceipt } from "../src/receipt.js";
import type { VerifyResult } from "../src/onboarding.js";
import { credentialUrl } from "./redaction-fixtures.js";

const result: VerifyResult = {
  schemaVersion: "cloudflareshard.onboarding-result.v1",
  command: "verify",
  status: "FAILED",
  runId: "run1",
  startedAt: "2026-08-05T12:00:00.000Z",
  finishedAt: "2026-08-05T12:00:01.000Z",
  checks: [{ id: "failure", label: "Failure", status: "FAIL", message: "Bearer tenant-secret and admin-secret must not survive; https://worker.example.test must not survive either" }],
  summary: { distinctPlacements: 0, rowsVerified: 0, replayMatched: false, resourcesRetained: true },
};

describe("versioned receipts", () => {
  it("writes a redacted checksum-bearing receipt without the target URL", async () => {
    const directory = await mkdtemp(path.join(tmpdir(), "cfs-receipt-"));
    const unsafeResult = {
      ...result,
      diagnostic: { password: "nested-password", metadata: { client_secret: "nested-client-secret" } },
    } as VerifyResult;
    const written = await writeReceipt(
      unsafeResult,
      credentialUrl("alice", "target-password", "worker.example.test", "/path?token=admin-secret#fragment-secret"),
      { directory, secrets: ["admin-secret", "tenant-secret"] },
    );
    const text = await readFile(written.path, "utf8");
    const parsed = JSON.parse(text) as Record<string, unknown>;
    expect(text).not.toContain("admin-secret");
    expect(text).not.toContain("tenant-secret");
    expect(text).not.toContain("target-password");
    expect(text).not.toContain("nested-password");
    expect(text).not.toContain("nested-client-secret");
    expect(text).not.toContain("fragment-secret");
    expect(text).not.toContain("worker.example.test");
    expect(parsed.schemaVersion).toBe("cloudflareshard.receipt.v1");
    expect((parsed.checksum as { value: string }).value).toBe(written.checksum);
  });

  it("canonicalizes object keys for stable checksums", () => {
    expect(stableStringify({ b: 1, a: { d: 2, c: 3 } })).toBe('{"a":{"c":3,"d":2},"b":1}');
  });
});
