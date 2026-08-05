export class ApiRequestError extends Error {
  constructor(kind, code, status = null) {
    super(code);
    this.name = "ApiRequestError";
    this.kind = kind;
    this.code = code;
    this.status = status;
  }
}

const SAFE_SERVER_CODES = new Set([
  "DO_OVERLOADED",
  "RATE_LIMITED",
  "ROW_LOCK_CONFLICT",
  "TX_ABORTED",
  "VBUCKET_FENCED",
]);

function safeHttpCode(status, body) {
  const candidate = body && typeof body === "object" && body.error && typeof body.error === "object" ? body.error.code : undefined;
  return typeof candidate === "string" && SAFE_SERVER_CODES.has(candidate) ? candidate : `HTTP_${status}`;
}

export class BenchmarkApiClient {
  constructor({ baseUrl, fetchImpl = globalThis.fetch, now = Date.now }) {
    this.baseUrl = baseUrl.replace(/\/+$/, "");
    this.fetchImpl = fetchImpl;
    this.now = now;
  }

  async post(path, body, token, { requestTimeoutMs, absoluteDeadlineMs }) {
    const remainingMs = absoluteDeadlineMs - this.now();
    if (remainingMs <= 0) throw new ApiRequestError("timeout", "RUN_GRACE_EXPIRED");

    const effectiveTimeoutMs = Math.min(requestTimeoutMs, remainingMs);
    const timeoutCode = remainingMs <= requestTimeoutMs ? "RUN_GRACE_EXPIRED" : "REQUEST_TIMEOUT";
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), effectiveTimeoutMs);
    try {
      const response = await this.fetchImpl(`${this.baseUrl}${path}`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${token}`,
        },
        body: JSON.stringify(body ?? {}),
        signal: controller.signal,
      });
      const responseBody = await response.json().catch(() => ({}));
      if (!response.ok) throw new ApiRequestError("failure", safeHttpCode(response.status, responseBody), response.status);
      return responseBody;
    } catch (error) {
      if (error instanceof ApiRequestError) throw error;
      if (controller.signal.aborted || (error && typeof error === "object" && error.name === "AbortError")) {
        throw new ApiRequestError("timeout", timeoutCode);
      }
      throw new ApiRequestError("failure", "NETWORK_ERROR");
    } finally {
      clearTimeout(timer);
    }
  }
}
