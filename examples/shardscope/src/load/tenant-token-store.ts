/** tenant-token-store.ts — T5's durable tenant-token store: the real
 * TokenProvider (see ./token-provider.ts) that replaces the "pending T5"
 * EnvTokenProvider stub as ./load-driver.ts's default.
 *
 * STORAGE CHOICE: a Durable Object (TenantTokenStore, below — singleton,
 * same pattern as ./aggregator.ts's TopologyAggregator and
 * ./load-driver.ts's LoadDriver), NOT a Workers KV namespace. Reason:
 * get-or-create has a genuine atomicity requirement — never issue two
 * concurrent /admin/register-tenant calls for the same missing tenant, and
 * never overwrite an existing stored token — that KV's eventually-consistent,
 * no-compare-and-swap put() can't satisfy without bolting on extra locking
 * machinery of its own (e.g. a second coordination primitive just to guard
 * the first). A Durable Object instance's JS only ever runs one turn at a
 * time (true parallelism doesn't exist inside one isolate, only interleaving
 * at explicit await points), so a plain in-memory
 * `Map<tenantId, Promise<string>>` inside the instance (see `inFlight`
 * below) is a correct, zero-dependency single-flight lock for concurrent
 * getOrCreateTenantToken() calls racing the same missing tenant — no
 * placeholder KV namespace id, no extra wrangler resource to provision.
 *
 * CRITICAL INVARIANT — read before changing anything here: a tenant's bearer
 * token is returned ONLY ONCE, at /register-tenant time; the catalog
 * (CatalogDO's tenant_auth table) stores just its SHA-256 hash from then on
 * (see the main repo's src/auth.ts's sha256Hex + src/catalog.ts's
 * handleRegisterTenant). This store therefore NEVER passes `rotate: true`
 * to env.SHARD_API.adminRegisterTenant — it only ever registers a tenantId
 * that has genuinely never been registered before (from THIS store's point
 * of view: it found no durable record for it). If the underlying tenant
 * turns out to already be registered by someone else (e.g. the Node
 * TPC-C harness caching a token in its own .tpcc-tenants.json, or a prior
 * Shardscope deployment whose durable record didn't carry over), rotating
 * here would silently invalidate whatever that other caller already cached
 * — so that case is refused outright (TenantAlreadyRegisteredError) rather
 * than "fixed" by rotating.
 *
 * getOrCreateTenantToken (below) is the pure, storage-agnostic core exercised
 * directly by tenant-token-store.test.ts (no DurableObjectState or Miniflare
 * needed — just a trivial in-memory TokenKv and a mock registerTenant
 * callback). TenantTokenStore (the actual Durable Object class) and
 * TenantTokenStoreTokenProvider (the TokenProvider ./load-driver.ts
 * constructs) are both thin adapters around it.
 */
import type { Env } from "../env";
import { tenantIdForWarehouse } from "./transactions";
import type { TokenProvider } from "./token-provider";

const TOKEN_KEY_PREFIX = "tenant-token:";

function tokenStorageKey(tenantId: string): string {
  return `${TOKEN_KEY_PREFIX}${tenantId}`;
}

/** Minimal storage shape getOrCreateTenantToken depends on — satisfied by
 * DurableObjectStorage (state.storage, see TenantTokenStore below) in
 * production, and a trivial in-memory Map in tests. */
export interface TokenKv {
  get(key: string): Promise<string | undefined>;
  put(key: string, value: string): Promise<void>;
}

/** Thrown when the underlying tenant is already registered on the cluster
 * (by some OTHER caller/store) but THIS store holds no token for it — see
 * this file's header comment for why that's a deliberate refusal to rotate,
 * not a bug to "fix" by rotating anyway. */
export class TenantAlreadyRegisteredError extends Error {
  constructor(readonly tenantId: string) {
    super(
      `TenantTokenStore: tenant ${tenantId} is already registered on the cluster, but this store holds no token ` +
        `for it (never registered here before, or its durable record was lost). Refusing to rotate — a tenant's ` +
        `token is returned only once at registration and the catalog stores only its hash, so rotating here could ` +
        `silently break another already-cached holder of the original token (e.g. the Node TPC-C harness's ` +
        `.tpcc-tenants.json).`,
    );
    this.name = "TenantAlreadyRegisteredError";
  }
}

/** register-tenant callback shape — mirrors the ONE call
 * TenantTokenStore's real registerTenant() below makes
 * (env.SHARD_API.adminRegisterTenant), abstracted so
 * getOrCreateTenantToken can be unit tested without a real SHARD_API
 * binding. MUST reject with TenantAlreadyRegisteredError specifically when
 * the tenant is already registered elsewhere (never rotate); any other
 * rejection is a genuine error and propagates as-is. On success, MUST
 * resolve with a freshly-issued, real bearer token (never empty). */
export type RegisterTenantFn = (tenantId: string) => Promise<string>;

/** get-or-create + single-flight + no-rotate-on-existing, all in one place
 * — see this file's header comment for the storage-agnostic design and why.
 * `inFlight` is caller-owned (one Map per store instance) so concurrent
 * calls for the SAME tenantId share one registration attempt instead of
 * racing two register-tenant calls (only one of which could ever actually
 * win — the loser would hit TENANT_ALREADY_REGISTERED with no way to
 * recover the winner's token). */
export async function getOrCreateTenantToken(
  warehouseId: number,
  kv: TokenKv,
  registerTenant: RegisterTenantFn,
  inFlight: Map<string, Promise<string>>,
): Promise<string> {
  const tenantId = tenantIdForWarehouse(warehouseId);
  const key = tokenStorageKey(tenantId);

  const existing = await kv.get(key);
  if (existing) return existing;

  const pending = inFlight.get(tenantId);
  if (pending) return pending;

  const attempt = (async () => {
    // Re-check storage: another call may have already persisted a token for
    // this tenant between our first kv.get above and now (e.g. a prior
    // in-flight registration for this same tenantId that just completed and
    // was removed from `inFlight` immediately before this one started).
    const already = await kv.get(key);
    if (already) return already;

    const token = await registerTenant(tenantId);
    await kv.put(key, token);
    return token;
  })();

  inFlight.set(tenantId, attempt);
  try {
    return await attempt;
  } finally {
    // Only ever remove OUR OWN attempt — a defensive guard in case some
    // caller ever reused an inFlight Map across overlapping stores; today
    // each TenantTokenStore instance owns exactly one.
    if (inFlight.get(tenantId) === attempt) inFlight.delete(tenantId);
  }
}

// ----------------------------------------------------------------------------
// TenantTokenStore — the actual Durable Object. One route (POST
// /get-or-create), fetch()-routed exactly like ./aggregator.ts's
// TopologyAggregator and ./load-driver.ts's LoadDriver. A thin adapter over
// getOrCreateTenantToken: state.storage as TokenKv, and
// env.SHARD_API.adminRegisterTenant as the registerTenant callback.
// ----------------------------------------------------------------------------

export class TenantTokenStore {
  private readonly state: DurableObjectState;
  private readonly env: Env;

  // In-memory single-flight lock — see this file's header comment for why
  // this is safe (a DO instance's JS never truly runs two turns at once)
  // and what it does and doesn't protect against (it dedupes concurrent
  // calls WITHIN one instance's lifetime; it does not need to dedupe across
  // instances, because durable storage — checked first, and re-checked right
  // before registering — is the source of truth that survives eviction).
  private readonly inFlight: Map<string, Promise<string>> = new Map();

  constructor(state: DurableObjectState, env: Env) {
    this.state = state;
    this.env = env;
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    if (request.method === "POST" && url.pathname === "/get-or-create") {
      return this.handleGetOrCreate(request);
    }
    return json({ error: `Unknown tenant-token-store route: ${url.pathname}` }, 404);
  }

  private async handleGetOrCreate(request: Request): Promise<Response> {
    let body: { warehouseId?: number };
    try {
      body = (await request.json()) as { warehouseId?: number };
    } catch {
      return json({ error: "Invalid JSON body." }, 400);
    }
    if (typeof body.warehouseId !== "number" || !Number.isFinite(body.warehouseId)) {
      return json({ error: "Missing or invalid 'warehouseId'." }, 400);
    }

    try {
      const token = await getOrCreateTenantToken(body.warehouseId, this.storageKv(), (tenantId) => this.registerTenant(tenantId), this.inFlight);
      return json({ token });
    } catch (err) {
      const status = err instanceof TenantAlreadyRegisteredError ? 409 : 502;
      return json({ error: err instanceof Error ? err.message : String(err) }, status);
    }
  }

  private storageKv(): TokenKv {
    return {
      get: (key: string) => this.state.storage.get<string>(key),
      put: (key: string, value: string) => this.state.storage.put(key, value),
    };
  }

  /** The one real network call this store makes: registers `tenantId` via
   * cloudflare-shard-mvp's admin RPC surface, authorized with
   * env.ADMIN_TOKEN. CRITICAL: never passes `rotate: true` — see this file's
   * header comment. CloudflareShardRpc.adminRegisterTenant's own unwrapForRpc
   * throws (rather than returning an error body) on any non-2xx response, so
   * a 409 TENANT_ALREADY_REGISTERED response surfaces here as a thrown Error
   * whose message contains that code — detected below and translated into
   * TenantAlreadyRegisteredError so callers can tell "already registered
   * elsewhere, refusing to rotate" apart from any other failure. */
  private async registerTenant(tenantId: string): Promise<string> {
    let result: { ok?: boolean; tenantId?: string; token?: string } | undefined;
    try {
      result = (await this.env.SHARD_API.adminRegisterTenant(this.env.ADMIN_TOKEN, { tenantId })) as typeof result;
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      if (message.includes("TENANT_ALREADY_REGISTERED")) {
        throw new TenantAlreadyRegisteredError(tenantId);
      }
      throw new Error(`TenantTokenStore: /admin/register-tenant RPC failed for tenant ${tenantId}: ${message}`);
    }
    if (!result || typeof result.token !== "string" || result.token.length === 0) {
      throw new Error(`TenantTokenStore: /admin/register-tenant RPC returned no token for tenant ${tenantId}: ${JSON.stringify(result)}`);
    }
    return result.token;
  }
}

const json = (data: unknown, status = 200): Response =>
  new Response(JSON.stringify(data, null, 2), { status, headers: { "content-type": "application/json" } });

// ----------------------------------------------------------------------------
// TenantTokenStoreTokenProvider — the real TokenProvider ./load-driver.ts now
// constructs by default (replacing EnvTokenProvider). Talks to the
// TenantTokenStore DO over env.TENANT_TOKEN_STORE, the same fetch()-based
// binding pattern LoadDriver itself is addressed by from src/index.ts.
// ----------------------------------------------------------------------------

export class TenantTokenStoreTokenProvider implements TokenProvider {
  constructor(private readonly env: Env) {}

  async getTenantToken(warehouseId: number): Promise<string> {
    const id = this.env.TENANT_TOKEN_STORE.idFromName("singleton");
    const stub = this.env.TENANT_TOKEN_STORE.get(id);
    const res = await stub.fetch("https://tenant-token-store.internal/get-or-create", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ warehouseId }),
    });
    const body = (await res.json().catch(() => ({}))) as { token?: string; error?: string };
    if (!res.ok || typeof body.token !== "string" || body.token.length === 0) {
      throw new Error(`TenantTokenStoreTokenProvider: failed to resolve a tenant token for warehouse ${warehouseId}: ${body.error ?? `HTTP ${res.status}`}`);
    }
    return body.token;
  }
}
