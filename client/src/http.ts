import { CloudflareShardError } from "./errors.js";

export interface ClientOptions {
  /** e.g. "https://cloudflare-shard-mvp.<account>.workers.dev" or "http://127.0.0.1:8787" for wrangler dev. No trailing slash required. */
  baseUrl: string;
  /** Bearer token -- ADMIN_TOKEN for CloudflareShardAdminClient, a tenant token (from registerTenant) for CloudflareShardClient. */
  token: string;
  /** Override fetch (e.g. for tests). Defaults to the global fetch. */
  fetchImpl?: typeof fetch;
}

/** Shared HTTP mechanics for both the tenant and admin clients: every
 * CloudflareShard route is POST + JSON body + bearer auth, so there's no
 * per-route variance to configure beyond the path and body. Throws
 * CloudflareShardError (normalizing both error body shapes the API uses)
 * for any non-2xx response, so callers never have to check res.ok
 * themselves. */
export class HttpClient {
  protected readonly baseUrl: string;
  protected readonly token: string;
  private readonly fetchImpl: typeof fetch;

  constructor(options: ClientOptions) {
    this.baseUrl = options.baseUrl.replace(/\/+$/, "");
    this.token = options.token;
    this.fetchImpl = options.fetchImpl ?? fetch;
  }

  protected async post<T>(path: string, body: unknown): Promise<T> {
    const res = await this.fetchImpl(`${this.baseUrl}${path}`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        authorization: `Bearer ${this.token}`,
      },
      body: JSON.stringify(body ?? {}),
    });
    const text = await res.text();
    // Codex review: a non-JSON body (e.g. a Cloudflare/proxy 5xx HTML error
    // page, or any malformed response) must never surface as a raw
    // SyntaxError -- every failure mode this method can hit should come
    // back as CloudflareShardError, with the real HTTP status attached,
    // whether or not the body happened to be valid JSON.
    let parsed: unknown;
    try {
      parsed = text.length > 0 ? JSON.parse(text) : {};
    } catch {
      throw new CloudflareShardError(res.status, text);
    }
    if (!res.ok) {
      throw new CloudflareShardError(res.status, parsed);
    }
    return parsed as T;
  }
}
