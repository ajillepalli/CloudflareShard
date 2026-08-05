import { describe, expect, it } from "vitest";
import { exitCodeFor, renderHuman, renderJson, terminalWidth, type OnboardingOutput } from "../src/onboarding-output.js";
import type { DoctorResult, VerifyResult } from "../src/onboarding.js";

const base = {
  schemaVersion: "cloudflareshard.onboarding-result.v1" as const,
  startedAt: "2026-08-05T12:00:00.000Z",
  finishedAt: "2026-08-05T12:00:01.000Z",
  checks: [{ id: "x", label: "A long check", status: "WARN" as const, message: "This message is deliberately long enough to demonstrate deterministic wrapping at the supported terminal widths without depending on the host terminal." }],
};

function output(result: DoctorResult | VerifyResult): OnboardingOutput {
  return { schemaVersion: "cloudflareshard.cli-output.v1", result, receipt: { path: `C:/a-very-long-workspace-name/${"nested/".repeat(12)}receipt.json`, checksum: "a".repeat(64) } };
}

describe("onboarding renderers", () => {
  it("uses exactly the supported 80/120-column layouts", () => {
    expect(terminalWidth(undefined)).toBe(80);
    expect(terminalWidth(119)).toBe(80);
    expect(terminalWidth(120)).toBe(120);
    const doctor: DoctorResult = { ...base, command: "doctor", status: "READY_WITH_WARNINGS" };
    expect(Math.max(...renderHuman(output(doctor), { width: 80 }).trimEnd().split("\n").map((line) => line.length))).toBeLessThanOrEqual(80);
    expect(Math.max(...renderHuman(output(doctor), { width: 120 }).trimEnd().split("\n").map((line) => line.length))).toBeLessThanOrEqual(120);
  });

  it("adds ANSI status color only when the caller explicitly enables it", () => {
    const doctor: DoctorResult = { ...base, command: "doctor", status: "READY_WITH_WARNINGS" };
    expect(renderHuman(output(doctor), { color: false })).not.toContain("\u001b[");
    expect(renderHuman(output(doctor), { color: true })).toContain("\u001b[");
  });

  it("emits stable one-line JSON with no ANSI escapes", () => {
    const doctor: DoctorResult = { ...base, command: "doctor", status: "READY_WITH_WARNINGS" };
    const text = renderJson(output(doctor));
    expect(text.trim().split("\n")).toHaveLength(1);
    expect(text).not.toContain("\u001b[");
    expect(JSON.parse(text).schemaVersion).toBe("cloudflareshard.cli-output.v1");
  });

  it("redacts unsafe result fields at both renderer boundaries", () => {
    const doctor: DoctorResult = {
      ...base,
      command: "doctor",
      status: "NOT_READY",
      checks: [{
        id: "failure",
        label: "password: label-secret",
        status: "FAIL",
        message: "https://alice:supersecret@worker.example.test/path?token=query-secret#fragment-secret",
      }],
    };
    const unsafeOutput = output(doctor);
    if (!unsafeOutput.receipt) throw new Error("test fixture requires a receipt");
    unsafeOutput.receipt.path = "https://alice:receipt-secret@receipts.example.test/result.json?token=path-secret";

    for (const rendered of [renderHuman(unsafeOutput), renderJson(unsafeOutput)]) {
      for (const secret of ["label-secret", "supersecret", "worker.example.test", "query-secret", "receipt-secret", "receipts.example.test", "path-secret"]) {
        expect(rendered).not.toContain(secret);
      }
    }
  });

  it("renders a typed receipt prerequisite failure in JSON and human modes", () => {
    const doctor: DoctorResult = { ...base, command: "doctor", status: "READY_WITH_WARNINGS" };
    const failed: OnboardingOutput = {
      schemaVersion: "cloudflareshard.cli-output.v1",
      result: doctor,
      receipt: null,
      failure: { code: "RECEIPT_WRITE_FAILED", category: "PREREQUISITE", message: "Could not write token=secret-value" },
    };

    const json = JSON.parse(renderJson(failed)) as OnboardingOutput;
    expect(json.receipt).toBeNull();
    expect(json.failure?.code).toBe("RECEIPT_WRITE_FAILED");
    expect(JSON.stringify(json)).not.toContain("secret-value");
    expect(renderHuman(failed)).toContain("PREREQUISITE_FAILED");
  });

  it("uses documented exit codes, including pending reconciliation", () => {
    const doctor: DoctorResult = { ...base, command: "doctor", status: "NOT_READY" };
    const verify: VerifyResult = { ...base, command: "verify", status: "PENDING_RECONCILIATION", runId: "r", summary: { distinctPlacements: 2, rowsVerified: 2, replayMatched: true, resourcesRetained: true } };
    const ready: DoctorResult = { ...doctor, status: "READY" };
    const verified: VerifyResult = { ...verify, status: "VERIFIED" };
    const failed: VerifyResult = { ...verify, status: "FAILED" };
    expect(exitCodeFor(ready)).toBe(0);
    expect(exitCodeFor(verified)).toBe(0);
    expect(exitCodeFor(doctor)).toBe(3);
    expect(exitCodeFor(failed)).toBe(4);
    expect(exitCodeFor(verify)).toBe(5);
  });
});
