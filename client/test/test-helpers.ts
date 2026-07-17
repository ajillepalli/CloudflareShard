import { expect, vi } from "vitest";

export interface RecordedCall {
  url: string;
  method: string | undefined;
  headers: Record<string, string>;
  body: unknown;
}

/** A fetch stand-in that records every call and returns a scripted
 * response, so client/admin-client tests can assert on exactly what was
 * sent without a real network call or a live wrangler dev instance. */
export function mockFetch(status: number, body: unknown): { fetchImpl: typeof fetch; calls: RecordedCall[] } {
  const calls: RecordedCall[] = [];
  const fetchImpl = vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
    const url = typeof input === "string" ? input : input.toString();
    const headers = Object.fromEntries(new Headers(init?.headers).entries());
    calls.push({ url, method: init?.method, headers, body: init?.body ? JSON.parse(init.body as string) : undefined });
    return new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
  }) as unknown as typeof fetch;
  return { fetchImpl, calls };
}

/** A fetch stand-in that returns a different scripted response on each
 * successive call, in order -- for tests that need to script a sequence
 * (e.g. waitForIndexReady's poll-until-ready loop). */
export function mockFetchSequence(responses: Array<{ status: number; body: unknown }>): { fetchImpl: typeof fetch; calls: RecordedCall[] } {
  const calls: RecordedCall[] = [];
  let i = 0;
  const fetchImpl = vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
    const url = typeof input === "string" ? input : input.toString();
    const headers = Object.fromEntries(new Headers(init?.headers).entries());
    calls.push({ url, method: init?.method, headers, body: init?.body ? JSON.parse(init.body as string) : undefined });
    const next = responses[Math.min(i, responses.length - 1)];
    i += 1;
    return new Response(JSON.stringify(next.body), { status: next.status, headers: { "content-type": "application/json" } });
  }) as unknown as typeof fetch;
  return { fetchImpl, calls };
}

export function expectBearerToken(call: RecordedCall, token: string): void {
  expect(call.headers.authorization).toBe(`Bearer ${token}`);
}
