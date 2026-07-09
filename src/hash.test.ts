import { describe, expect, it } from "vitest";
import { hashKey } from "./hash";

describe("hashKey", () => {
  it("is deterministic for the same input", () => {
    expect(hashKey("tenant-1:events:user-1")).toBe(hashKey("tenant-1:events:user-1"));
  });

  it("returns a non-negative 32-bit unsigned integer", () => {
    const h = hashKey("some-key");
    expect(Number.isInteger(h)).toBe(true);
    expect(h).toBeGreaterThanOrEqual(0);
    expect(h).toBeLessThanOrEqual(0xffffffff);
  });

  it("handles the empty string without throwing", () => {
    expect(() => hashKey("")).not.toThrow();
    expect(Number.isInteger(hashKey(""))).toBe(true);
  });

  it("produces different hashes for different inputs (no trivial collisions)", () => {
    const hashes = new Set(["a", "b", "c", "tenant-1", "tenant-2", "tenant-3"].map(hashKey));
    expect(hashes.size).toBe(6);
  });

  it("handles unicode input without throwing", () => {
    expect(() => hashKey("tenant-éè日本語")).not.toThrow();
  });
});
