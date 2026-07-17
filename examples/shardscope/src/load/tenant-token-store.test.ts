import { describe, expect, it, vi } from "vitest";
import { getOrCreateTenantToken, TenantAlreadyRegisteredError, type TokenKv } from "./tenant-token-store";
import { tenantIdForWarehouse } from "./transactions";

/** Trivial in-memory TokenKv — exercises getOrCreateTenantToken's own logic
 * directly, no DurableObjectState/Miniflare needed (see
 * tenant-token-store.ts's header comment for why this pure core exists
 * separately from the actual Durable Object). */
function memoryKv(): TokenKv {
  const store = new Map<string, string>();
  return {
    get: async (key) => store.get(key),
    put: async (key, value) => {
      store.set(key, value);
    },
  };
}

describe("tenant-token-store.ts — getOrCreateTenantToken", () => {
  it("registers a brand-new warehouse's tenant and persists the returned token", async () => {
    const kv = memoryKv();
    const registerTenant = vi.fn(async (tenantId: string) => `token-for-${tenantId}`);
    const inFlight = new Map<string, Promise<string>>();

    const token = await getOrCreateTenantToken(7, kv, registerTenant, inFlight);

    expect(token).toBe(`token-for-${tenantIdForWarehouse(7)}`);
    expect(registerTenant).toHaveBeenCalledTimes(1);
    expect(registerTenant).toHaveBeenCalledWith(tenantIdForWarehouse(7));
  });

  it("get-or-create: returns the STORED token on a second call and never re-registers (no rotation)", async () => {
    const kv = memoryKv();
    const registerTenant = vi.fn(async (tenantId: string) => `token-for-${tenantId}`);
    const inFlight = new Map<string, Promise<string>>();

    const first = await getOrCreateTenantToken(3, kv, registerTenant, inFlight);
    const second = await getOrCreateTenantToken(3, kv, registerTenant, inFlight);
    const third = await getOrCreateTenantToken(3, kv, registerTenant, inFlight);

    expect(second).toBe(first);
    expect(third).toBe(first);
    // The one and only invariant this store exists to enforce: a warehouse
    // that already has a stored token must NEVER trigger a second
    // register-tenant call, which in the real system would mean rotating a
    // token some other caller (e.g. the Node TPC-C harness) may have already
    // cached.
    expect(registerTenant).toHaveBeenCalledTimes(1);
  });

  it("different warehouses map to different tenants and each registers independently", async () => {
    const kv = memoryKv();
    const registerTenant = vi.fn(async (tenantId: string) => `token-for-${tenantId}`);
    const inFlight = new Map<string, Promise<string>>();

    const tokenA = await getOrCreateTenantToken(1, kv, registerTenant, inFlight);
    const tokenB = await getOrCreateTenantToken(2, kv, registerTenant, inFlight);

    expect(tokenA).not.toBe(tokenB);
    expect(registerTenant).toHaveBeenCalledTimes(2);
  });

  it("single-flight: concurrent get-or-create calls for the SAME missing warehouse share one registration", async () => {
    const kv = memoryKv();
    let resolveRegister!: (token: string) => void;
    let registerCallCount = 0;
    const registerTenant = vi.fn(
      () =>
        new Promise<string>((resolve) => {
          registerCallCount += 1;
          resolveRegister = resolve;
        }),
    );
    const inFlight = new Map<string, Promise<string>>();

    // Fire two concurrent get-or-create calls for the same warehouse before
    // either registration attempt resolves -- this is exactly the race two
    // LoadDriver alarm ticks racing the same newly-added warehouse would
    // produce (see load-driver.ts's runBoundedBatch, which runs multiple
    // transaction attempts concurrently via Promise.all).
    const p1 = getOrCreateTenantToken(9, kv, registerTenant, inFlight);
    const p2 = getOrCreateTenantToken(9, kv, registerTenant, inFlight);

    // Give both calls a chance to reach (and dedupe via) the in-flight map
    // before the registration resolves.
    await Promise.resolve();
    await Promise.resolve();

    expect(registerCallCount).toBe(1);

    resolveRegister("shared-token");
    const [token1, token2] = await Promise.all([p1, p2]);

    expect(token1).toBe("shared-token");
    expect(token2).toBe("shared-token");
    expect(registerTenant).toHaveBeenCalledTimes(1);

    // The in-flight entry must be cleaned up after settling, so a later,
    // genuinely new request for the same tenant isn't stuck waiting on a
    // stale promise.
    expect(inFlight.size).toBe(0);
  });

  it("propagates a registration failure to every concurrent caller and clears the in-flight entry so a later retry can succeed", async () => {
    const kv = memoryKv();
    let attempt = 0;
    const registerTenant = vi.fn(async (tenantId: string) => {
      attempt += 1;
      if (attempt === 1) throw new Error("simulated registration failure");
      return `token-for-${tenantId}`;
    });
    const inFlight = new Map<string, Promise<string>>();

    const p1 = getOrCreateTenantToken(5, kv, registerTenant, inFlight);
    const p2 = getOrCreateTenantToken(5, kv, registerTenant, inFlight);

    await expect(p1).rejects.toThrow("simulated registration failure");
    await expect(p2).rejects.toThrow("simulated registration failure");
    expect(registerTenant).toHaveBeenCalledTimes(1);
    expect(inFlight.size).toBe(0);

    // A subsequent call (e.g. the next alarm tick) must be able to retry --
    // not be permanently wedged behind the first attempt's failure.
    const retried = await getOrCreateTenantToken(5, kv, registerTenant, inFlight);
    expect(retried).toBe(`token-for-${tenantIdForWarehouse(5)}`);
    expect(registerTenant).toHaveBeenCalledTimes(2);
  });

  it("propagates TenantAlreadyRegisteredError as-is (never silently rotates or swallows it)", async () => {
    const kv = memoryKv();
    const registerTenant = vi.fn(async (tenantId: string) => {
      throw new TenantAlreadyRegisteredError(tenantId);
    });
    const inFlight = new Map<string, Promise<string>>();

    await expect(getOrCreateTenantToken(11, kv, registerTenant, inFlight)).rejects.toBeInstanceOf(TenantAlreadyRegisteredError);
    expect(registerTenant).toHaveBeenCalledTimes(1);

    // And it must NOT have stored a placeholder/empty value that a later
    // call would treat as "already resolved".
    expect(await kv.get(`tenant-token:${tenantIdForWarehouse(11)}`)).toBeUndefined();
  });

  it("re-checks storage right before registering: a token that lands between the initial miss and the registration attempt starting is honored, and registerTenant is never called", async () => {
    // getOrCreateTenantToken reads storage twice on the fresh-registration
    // path: once up front (the initial miss check) and once again inside
    // attempt(), immediately before deciding whether to call
    // registerTenant. This models the defensive purpose of that second
    // check -- it's what would catch a token that showed up in durable
    // storage in the gap between those two reads (e.g. across a DO eviction
    // + restart) -- deterministically, via a fake kv whose second get()
    // call injects the token, rather than relying on real timing.
    let getCallCount = 0;
    const store = new Map<string, string>();
    const kv: TokenKv = {
      get: async (key) => {
        getCallCount += 1;
        if (getCallCount === 2) {
          store.set(key, "token-written-between-checks");
        }
        return store.get(key);
      },
      put: async (key, value) => {
        store.set(key, value);
      },
    };
    const registerTenant = vi.fn(async (tenantId: string) => `should-never-be-used-${tenantId}`);
    const inFlight = new Map<string, Promise<string>>();

    const token = await getOrCreateTenantToken(6, kv, registerTenant, inFlight);

    expect(token).toBe("token-written-between-checks");
    expect(registerTenant).not.toHaveBeenCalled();
  });
});
