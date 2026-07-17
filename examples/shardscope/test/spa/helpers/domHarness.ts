/** Test harness for Shardscope's frontend SPA (examples/shardscope/public/app.js).
 *
 * app.js is a no-build-step, import-free classic script (see its own header
 * comment) that expects a real browser DOM plus `fetch` and `EventSource` as
 * ambient globals. It has zero test coverage today. This harness:
 *
 *   1. Loads the real index.html into a jsdom Document (same file the
 *      Worker actually serves — no test-only HTML fixture).
 *   2. Stubs the browser APIs jsdom doesn't provide (fetch, EventSource) or
 *      that app.js doesn't currently call but could reach in the future
 *      (requestAnimationFrame, matchMedia) — cheap insurance, per the task
 *      brief, even though a grep of app.js today shows neither is used.
 *   3. Evaluates the real app.js source inside the jsdom window's realm via
 *      `window.eval` (jsdom's `runScripts: "outside-only"` mode is built
 *      exactly for this: it lets test code inject/run scripts without jsdom
 *      itself trying to fetch and execute the `<script type="module"
 *      src="./app.js">` tag already sitting in index.html).
 *
 * app.js has no imports/exports (verified: no `import`/`export` statements
 * anywhere in the file) — it's safe to eval as a classic (non-module)
 * script. It calls `init()` itself at the bottom of the file, synchronously,
 * so by the time `bootApp()` returns, the app has already wired its DOM
 * listeners and rendered its default room.
 *
 * Tests interact with the running app exclusively through the DOM (clicking
 * elements, reading data-hook nodes, inspecting fetch calls) — never by
 * reaching into app.js's internal function/variable bindings. That keeps
 * this harness honest about what it's proving: the same app.js a browser
 * would load, driven the way a browser would drive it.
 */
import { readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { JSDOM, type DOMWindow } from "jsdom";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PUBLIC_DIR = path.resolve(__dirname, "../../../public");

const INDEX_HTML = readFileSync(path.join(PUBLIC_DIR, "index.html"), "utf-8");
const APP_JS = readFileSync(path.join(PUBLIC_DIR, "app.js"), "utf-8");

export interface FetchCall {
  /** pathname only (e.g. "/api/play/mutate") — query string stripped, matching how routes are registered. */
  pathname: string;
  method: string;
  /** Parsed JSON request body, or the raw string if it wasn't JSON, or undefined if there was none. */
  body: unknown;
}

export interface RouteResponse {
  status?: number;
  body?: unknown;
}

export type RouteHandler = (call: FetchCall) => RouteResponse | Promise<RouteResponse>;

export interface HarnessOptions {
  /** Query string (including leading "?") appended to the boot URL. Defaults to "?demo=1" so init() takes the
   * embedded-sample-data path and never touches /api/* or opens a live EventSource — the cleanest, most
   * deterministic boot for jsdom (no dangling gate-preflight fetch, no fallback timer, no live SSE stream). */
  search?: string;
  /** Canned responses per exact pathname (e.g. "/api/play/mutate"). Anything not listed here gets a default
   * `{status: 200, body: {}}` — permissive so an incidental/background call (e.g. the Reshard room's lock-status
   * poll) never throws or logs an unhandled rejection just because a test didn't anticipate it. */
  routes?: Record<string, RouteResponse | RouteHandler>;
}

export interface Harness {
  window: DOMWindow;
  document: Document;
  /** `document.querySelector('[data-hook="name"]')` shorthand, matching app.js's own `hook()` helper. */
  hook: (name: string) => HTMLElement | null;
  /** Every fetch() call observed so far, in order. */
  calls: FetchCall[];
  /** Register (or replace) a route's canned response after boot. */
  setRoute: (pathname: string, handler: RouteResponse | RouteHandler) => void;
  /** Waits a few microtask/macrotask turns so in-flight fetch().then() chains settle before assertions. */
  flush: () => Promise<void>;
  /** Stops the Reshard room's poll interval (if one is running) and closes the jsdom window. Call in afterEach. */
  cleanup: () => void;
}

function defaultRespond(r: RouteResponse | undefined) {
  const status = r?.status ?? 200;
  const ok = status >= 200 && status < 300;
  const body = r?.body ?? {};
  return {
    status,
    ok,
    json: () => Promise.resolve(body),
  };
}

export function bootApp(options: HarnessOptions = {}): Harness {
  const search = options.search ?? "?demo=1";
  const dom = new JSDOM(INDEX_HTML, {
    url: `https://shardscope.test/${search}`,
    runScripts: "outside-only",
  });
  const { window } = dom;

  const calls: FetchCall[] = [];
  const routes = new Map<string, RouteResponse | RouteHandler>(Object.entries(options.routes ?? {}));

  function setRoute(pathname: string, handler: RouteResponse | RouteHandler) {
    routes.set(pathname, handler);
  }

  // ---- fetch stub -----------------------------------------------------------
  // app.js's callers (playFetch, reshardFetch, authPreflight, fetchEdgeOnce)
  // only ever touch res.status / res.ok / res.json() — a plain object with
  // those three is sufficient; jsdom doesn't ship a real Response/fetch
  // implementation to build on top of anyway.
  (window as unknown as { fetch: typeof fetch }).fetch = ((input: unknown, init?: { method?: string; body?: unknown }) => {
    const rawUrl = typeof input === "string" ? input : (input as { url: string }).url;
    const pathname = new window.URL(rawUrl, window.location.href).pathname;
    const method = (init && init.method) || "GET";
    let body: unknown;
    if (init && typeof init.body === "string") {
      try {
        body = JSON.parse(init.body);
      } catch {
        body = init.body;
      }
    }
    calls.push({ pathname, method, body });

    const handler = routes.get(pathname);
    if (!handler) return Promise.resolve(defaultRespond({ status: 200, body: {} }));
    if (typeof handler === "function") {
      return Promise.resolve(handler({ pathname, method, body })).then(defaultRespond);
    }
    return Promise.resolve(defaultRespond(handler));
  }) as unknown as typeof fetch;

  // ---- EventSource stub -------------------------------------------------
  // connectLive() is only reached on the live (non-?demo=1) path, but is
  // stubbed unconditionally per the task brief — cheap insurance against
  // any test that boots in live mode. Records the last URL opened; never
  // opens a real connection, never fires events (tests that need live
  // "snapshot"/"error" events can reach in via `lastEventSource` and call
  // the recorded listeners directly).
  class StubEventSource {
    static instances: StubEventSource[] = [];
    url: string;
    listeners = new Map<string, Array<(ev: unknown) => void>>();
    constructor(url: string) {
      this.url = url;
      StubEventSource.instances.push(this);
    }
    addEventListener(type: string, fn: (ev: unknown) => void) {
      const list = this.listeners.get(type) ?? [];
      list.push(fn);
      this.listeners.set(type, list);
    }
    removeEventListener() {}
    close() {}
  }
  (window as unknown as { EventSource: unknown }).EventSource = StubEventSource;

  // ---- other browser APIs app.js doesn't call today, stubbed defensively ----
  if (!("requestAnimationFrame" in window)) {
    (window as unknown as { requestAnimationFrame: (cb: FrameRequestCallback) => number }).requestAnimationFrame = (cb) =>
      window.setTimeout(() => cb(Date.now()), 0) as unknown as number;
    (window as unknown as { cancelAnimationFrame: (id: number) => void }).cancelAnimationFrame = (id) => window.clearTimeout(id);
  }
  if (!("matchMedia" in window)) {
    (window as unknown as { matchMedia: (q: string) => MediaQueryList }).matchMedia = ((query: string) =>
      ({
        matches: false,
        media: query,
        onchange: null,
        addListener() {},
        removeListener() {},
        addEventListener() {},
        removeEventListener() {},
        dispatchEvent: () => false,
      }) as unknown as MediaQueryList) as unknown as (q: string) => MediaQueryList;
  }

  // ---- evaluate the real app.js in the jsdom window's realm ----------------
  // app.js calls init() itself at the bottom of the file — by the time this
  // returns, the app has already booted (rendered its default room, wired
  // every listener).
  window.eval(APP_JS);

  function hook(name: string): HTMLElement | null {
    return window.document.querySelector(`[data-hook="${name}"]`);
  }

  async function flush(): Promise<void> {
    // A handful of microtask + macrotask turns — enough for a chain like
    // fetch().then(res => res.json()).then(render) to fully settle, since
    // our stub fetch is already synchronous-ish (Promise.resolve-based) but
    // still goes through several .then() hops.
    for (let i = 0; i < 5; i++) {
      await Promise.resolve();
    }
    await new Promise((resolve) => window.setTimeout(resolve, 0));
  }

  function cleanup(): void {
    window.close();
  }

  return { window, document: window.document, hook, calls, setRoute, flush, cleanup };
}
