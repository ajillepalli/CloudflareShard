/**
 * CloudflareShard's HTTP API uses two error body shapes across its routes:
 * the newer `{ error: { code, message, fix? } }` and an older plain
 * `{ error: "message string" }` (see src/index.ts). This class normalizes
 * both into one typed shape so SDK callers never have to branch on which
 * one a given route happens to use.
 */
export class CloudflareShardError extends Error {
  readonly status: number;
  readonly code: string | undefined;
  readonly fix: string | undefined;
  readonly retryable: boolean | undefined;
  readonly overloaded: boolean | undefined;
  readonly retryAfterMs: number | undefined;
  readonly details: Readonly<Record<string, unknown>> | undefined;
  readonly body: unknown;

  constructor(status: number, body: unknown) {
    const { message, code, fix, retryable, overloaded, retryAfterMs, details } = CloudflareShardError.parseBody(body);
    super(message);
    this.name = "CloudflareShardError";
    this.status = status;
    this.code = code;
    this.fix = fix;
    this.retryable = retryable;
    this.overloaded = overloaded;
    this.retryAfterMs = retryAfterMs;
    this.details = details;
    this.body = body;
  }

  private static parseBody(body: unknown): {
    message: string;
    code?: string;
    fix?: string;
    retryable?: boolean;
    overloaded?: boolean;
    retryAfterMs?: number;
    details?: Readonly<Record<string, unknown>>;
  } {
    if (body && typeof body === "object" && "error" in body) {
      const err = (body as { error: unknown }).error;
      if (typeof err === "string") return { message: err };
      if (err && typeof err === "object") {
        const e = err as {
          code?: unknown;
          message?: unknown;
          fix?: unknown;
          retryable?: unknown;
          overloaded?: unknown;
          retry_after_ms?: unknown;
          details?: unknown;
        };
        return {
          message: typeof e.message === "string" ? e.message : "CloudflareShard request failed.",
          code: typeof e.code === "string" ? e.code : undefined,
          fix: typeof e.fix === "string" ? e.fix : undefined,
          retryable: typeof e.retryable === "boolean" ? e.retryable : undefined,
          overloaded: typeof e.overloaded === "boolean" ? e.overloaded : undefined,
          retryAfterMs: typeof e.retry_after_ms === "number" && Number.isFinite(e.retry_after_ms)
            ? e.retry_after_ms
            : undefined,
          details: e.details !== null && typeof e.details === "object" && !Array.isArray(e.details)
            ? e.details as Readonly<Record<string, unknown>>
            : undefined,
        };
      }
    }
    return { message: "CloudflareShard request failed." };
  }
}
