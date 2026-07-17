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

/** A single stubbed `EventSource` instance app.js opened via `new EventSource(url)`
 * (only ever reached on the live, non-`?demo=1` path — see connectLive()). Exposed
 * read-only so tests can inspect what URL was opened; use `dispatchServerEvent`
 * below to drive its listeners rather than reaching into `listeners` directly. */
export interface StubEventSourceHandle {
  url: string;
  listeners: Map<string, Array<(ev: unknown) => void>>;
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
  /** Every EventSource the app has opened so far, in order (only ever populated on
   * the live path — connectLive() opens exactly one, against "/api/stream", after a
   * successful gate check). Empty in ?demo=1 mode, which never opens one. */
  eventSources: StubEventSourceHandle[];
  /** Drives a server-sent event on the most recently opened EventSource, calling
   * every listener app.js registered for `type` via `addEventListener`. Pass `data`
   * as a string to become `ev.data` verbatim (e.g. a pre-serialized JSON snapshot
   * payload), or any other value to have it JSON.stringify'd first; omit `data`
   * entirely to simulate a native, data-less browser Event (e.g. EventSource's own
   * connection-level "error", as opposed to the server's named "error" SSE frame —
   * see connectLive()'s own doc comment on telling the two apart). Throws if no
   * EventSource has been opened yet. */
  dispatchServerEvent: (type: string, data?: unknown) => void;
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
    // The Build panel's download flow (app.js's handleBuildDownloadClick)
    // calls res.blob() against GET /api/build/scaffold — a real zip binary
    // in production, but tests only need SOMETHING blob-shaped to exercise
    // the download click through to a synthetic <a download> without
    // throwing; the bytes themselves are never asserted on here (that's
    // build.test.ts's job, against the real server-side zip bytes).
    blob: () => Promise.resolve(new Blob([JSON.stringify(body)])),
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
  // any test that boots in live mode. Records every instance opened; never
  // opens a real connection, never fires events on its own — tests that need
  // live "snapshot"/"error" events reach in via the harness's own
  // `eventSources` / `dispatchServerEvent` below, which are just a thin,
  // documented wrapper over this class's `instances` + `listeners`.
  class StubEventSource implements StubEventSourceHandle {
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

  function dispatchServerEvent(type: string, data?: unknown): void {
    const instance = StubEventSource.instances[StubEventSource.instances.length - 1];
    if (!instance) {
      throw new Error(
        "dispatchServerEvent: no EventSource has been opened yet — app.js only opens one on the live (non-?demo=1) path, via connectLive(), after a successful gate check (see startLiveFlow()).",
      );
    }
    const listeners = instance.listeners.get(type) ?? [];
    const event = data === undefined ? {} : { data: typeof data === "string" ? data : JSON.stringify(data) };
    listeners.forEach((fn) => fn(event));
  }

  // ---- other browser APIs app.js doesn't call today, stubbed defensively ----
  if (!("requestAnimationFrame" in window)) {
    (window as unknown as { requestAnimationFrame: (cb: FrameRequestCallback) => number }).requestAnimationFrame = (cb) =>
      window.setTimeout(() => cb(Date.now()), 0) as unknown as number;
    (window as unknown as { cancelAnimationFrame: (id: number) => void }).cancelAnimationFrame = (id) => window.clearTimeout(id);
  }
  // jsdom doesn't implement URL.createObjectURL/revokeObjectURL (throws
  // "Not implemented" if called) — stubbed defensively so the Build panel's
  // handleBuildDownloadClick (URL.createObjectURL(blob) -> synthetic <a
  // download> click -> revokeObjectURL) can run end to end in tests without
  // that unrelated jsdom gap surfacing as a false failure.
  if (!("createObjectURL" in window.URL)) {
    (window.URL as unknown as { createObjectURL: (b: unknown) => string }).createObjectURL = () => "blob:stub-url";
    (window.URL as unknown as { revokeObjectURL: (u: string) => void }).revokeObjectURL = () => {};
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

  return {
    window,
    document: window.document,
    hook,
    calls,
    setRoute,
    flush,
    eventSources: StubEventSource.instances,
    dispatchServerEvent,
    cleanup,
  };
}
