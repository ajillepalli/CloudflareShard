import { describe, expect, it } from "vitest";
import { isValidBearerToken, timingSafeEqual } from "./auth";

describe("timingSafeEqual", () => {
  it("returns true for identical strings", () => {
    expect(timingSafeEqual("secret-token", "secret-token")).toBe(true);
  });

  it("returns false for different strings of the same length", () => {
    expect(timingSafeEqual("secret-tokenA", "secret-tokenB")).toBe(false);
  });

  it("returns false for different-length strings", () => {
    expect(timingSafeEqual("short", "a-much-longer-string")).toBe(false);
  });

  it("returns false when compared against an empty string", () => {
    expect(timingSafeEqual("nonempty", "")).toBe(false);
  });

  it("returns true for two empty strings", () => {
    expect(timingSafeEqual("", "")).toBe(true);
  });
});

describe("isValidBearerToken", () => {
  it("accepts the correctly formatted bearer header", () => {
    expect(isValidBearerToken("Bearer abc123", "abc123")).toBe(true);
  });

  it("rejects a missing authorization header", () => {
    expect(isValidBearerToken(null, "abc123")).toBe(false);
  });

  it("rejects a mismatched token", () => {
    expect(isValidBearerToken("Bearer wrong", "abc123")).toBe(false);
  });

  it("rejects a header missing the Bearer prefix", () => {
    expect(isValidBearerToken("abc123", "abc123")).toBe(false);
  });
});
