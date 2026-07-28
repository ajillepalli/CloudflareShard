import { describe, expect, it } from "vitest";
import { hasLoadRunExceededMaxDuration, MAX_LOAD_RUN_DURATION_MS, resolveLoadDriverBaseUrl } from "./load-driver";

describe("load-driver.ts — resolveLoadDriverBaseUrl", () => {
  it("an explicit, non-empty body value always wins over the env default", () => {
    expect(resolveLoadDriverBaseUrl("https://explicit.example", "https://env-default.example")).toBe("https://explicit.example");
  });

  it("falls back to the env value when the body doesn't supply one", () => {
    expect(resolveLoadDriverBaseUrl(undefined, "https://env-default.example")).toBe("https://env-default.example");
  });

  it("an empty-string body value is treated as absent, not a real override", () => {
    expect(resolveLoadDriverBaseUrl("", "https://env-default.example")).toBe("https://env-default.example");
  });

  it("a non-string body value (e.g. a stray number from a malformed request) is ignored, not coerced", () => {
    expect(resolveLoadDriverBaseUrl(12345, "https://env-default.example")).toBe("https://env-default.example");
  });

  it("returns null when neither the body nor the env has a usable value — matches handleStart's pre-existing 'no base URL configured' contract", () => {
    expect(resolveLoadDriverBaseUrl(undefined, undefined)).toBeNull();
    expect(resolveLoadDriverBaseUrl("", "")).toBeNull();
  });
});

describe("load-driver.ts — hasLoadRunExceededMaxDuration", () => {
  const NOW = Date.parse("2026-07-28T12:00:00.000Z");

  it("a run that never started (startedAt: null) is never over the cap", () => {
    expect(hasLoadRunExceededMaxDuration(null, NOW)).toBe(false);
  });

  it("a run well within the cap is not flagged", () => {
    expect(hasLoadRunExceededMaxDuration(NOW - 60_000, NOW)).toBe(false);
  });

  it("a run exactly at the cap is not yet flagged (strictly greater-than, not greater-or-equal)", () => {
    expect(hasLoadRunExceededMaxDuration(NOW - MAX_LOAD_RUN_DURATION_MS, NOW)).toBe(false);
  });

  it("a run one tick past the cap is flagged", () => {
    expect(hasLoadRunExceededMaxDuration(NOW - MAX_LOAD_RUN_DURATION_MS - 1, NOW)).toBe(true);
  });

  it("a run left running for hours is flagged — the actual scenario this cap exists for", () => {
    expect(hasLoadRunExceededMaxDuration(NOW - 3 * 60 * 60_000, NOW)).toBe(true);
  });
});
