import { describe, expect, it } from "vitest";
import { CloudflareShardError } from "../src/errors.js";

describe("CloudflareShardError", () => {
  it("parses the structured { error: { code, message, fix } } shape", () => {
    const err = new CloudflareShardError(409, { error: { code: "TABLE_ALREADY_REGISTERED", message: "events is already registered.", fix: "Use a different table name." } });
    expect(err.status).toBe(409);
    expect(err.code).toBe("TABLE_ALREADY_REGISTERED");
    expect(err.message).toBe("events is already registered.");
    expect(err.fix).toBe("Use a different table name.");
  });

  it("parses the plain { error: \"string\" } shape, leaving code/fix undefined", () => {
    const err = new CloudflareShardError(400, { error: "Missing shardId" });
    expect(err.status).toBe(400);
    expect(err.message).toBe("Missing shardId");
    expect(err.code).toBeUndefined();
    expect(err.fix).toBeUndefined();
  });

  it("falls back to a generic message for an unrecognized body shape", () => {
    const err = new CloudflareShardError(500, "not json");
    expect(err.message).toBe("CloudflareShard request failed.");
  });

  it("preserves the raw body for callers that need more detail", () => {
    const body = { error: { code: "X", message: "y" } };
    const err = new CloudflareShardError(400, body);
    expect(err.body).toBe(body);
  });

  it("preserves retry, overload, delay, and typed detail metadata", () => {
    const err = new CloudflareShardError(503, {
      error: {
        code: "RESTORE_UNAVAILABLE",
        message: "Participant is temporarily unavailable.",
        retryable: true,
        overloaded: true,
        retry_after_ms: 30_000,
        details: { participant_id: "shard-a" },
      },
    });
    expect(err).toMatchObject({
      retryable: true,
      overloaded: true,
      retryAfterMs: 30_000,
      details: { participant_id: "shard-a" },
    });
  });
});
