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
  readonly body: unknown;

  constructor(status: number, body: unknown) {
    const { message, code, fix } = CloudflareShardError.parseBody(body);
    super(message);
    this.name = "CloudflareShardError";
    this.status = status;
    this.code = code;
    this.fix = fix;
    this.body = body;
  }

  private static parseBody(body: unknown): { message: string; code?: string; fix?: string } {
    if (body && typeof body === "object" && "error" in body) {
      const err = (body as { error: unknown }).error;
      if (typeof err === "string") return { message: err };
      if (err && typeof err === "object") {
        const e = err as { code?: unknown; message?: unknown; fix?: unknown };
        return {
          message: typeof e.message === "string" ? e.message : "CloudflareShard request failed.",
          code: typeof e.code === "string" ? e.code : undefined,
          fix: typeof e.fix === "string" ? e.fix : undefined,
        };
      }
    }
    return { message: "CloudflareShard request failed." };
  }
}
