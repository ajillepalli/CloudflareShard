/** token-provider.ts — the seam T5 (durable tenant-token storage) plugs
 * into.
 *
 * ./transactions.ts and ./gateway-client.ts don't know or care HOW a
 * warehouse's tenant bearer token is obtained — they only depend on this
 * interface. Today (T3, this file) there is no durable per-tenant token
 * store in Shardscope yet, so `EnvTokenProvider` is a stub that always
 * throws a clear "pending T5" error the moment a real transaction tries to
 * issue against the gateway. That's a deliberate, documented gap, not a bug:
 * T3's job is the load-mix/skew/batch machinery, not tenant-token storage.
 *
 * When T5 lands, it should add a new TokenProvider implementation (backed by
 * whatever durable store T5 builds — a DO, KV, wherever tenant tokens
 * actually get persisted) and swap it in wherever LoadDriver constructs its
 * TokenProvider (see ./load-driver.ts's constructor) — no other file in
 * ./load/ needs to change, since everything upstream only depends on this
 * interface.
 */

export interface TokenProvider {
  /** Resolves the bearer token for the tenant that owns `warehouseId`'s
   * data (one tenant per warehouse — see ./transactions.ts's
   * tenantIdForWarehouse). Must reject/throw, never return an empty or
   * placeholder token, when no real token is available. */
  getTenantToken(warehouseId: number): Promise<string>;
}

/** Stub TokenProvider. Reads an optional JSON map of `{ [warehouseId]: token
 * }` from a caller-supplied string (intended to eventually be an env var or
 * secret, once such a thing exists) — but since no durable or even
 * env-configured tenant-token source is wired up yet (that's T5's job), the
 * realistic path through this class today is: no source configured ->
 * always throws. Passing a real tokensJson map (e.g. in a test, or once a
 * future env var carries one) makes it work as a genuine — if flat-file,
 * non-durable — lookup, which is useful for exercising the rest of the load
 * pipeline before T5 lands. */
export class EnvTokenProvider implements TokenProvider {
  constructor(private readonly tokensJson: string | undefined | null) {}

  async getTenantToken(warehouseId: number): Promise<string> {
    if (!this.tokensJson) {
      throw new Error(
        `EnvTokenProvider: tenant tokens not wired yet — pending T5 (durable tenant-token storage). ` +
          `No token source is configured for warehouse ${warehouseId}. LoadDriver only depends on the ` +
          `TokenProvider interface (see token-provider.ts), so once T5 lands, swap EnvTokenProvider for a ` +
          `TokenProvider backed by real durable per-tenant token storage — no other load/ file needs to change.`,
      );
    }

    let parsed: unknown;
    try {
      parsed = JSON.parse(this.tokensJson);
    } catch {
      throw new Error(`EnvTokenProvider: configured tenant-tokens JSON is malformed; cannot resolve a token for warehouse ${warehouseId}.`);
    }
    if (typeof parsed !== "object" || parsed === null) {
      throw new Error(`EnvTokenProvider: configured tenant-tokens JSON must be an object of { [warehouseId]: token }.`);
    }
    const token = (parsed as Record<string, unknown>)[String(warehouseId)];
    if (typeof token !== "string" || token.length === 0) {
      throw new Error(
        `EnvTokenProvider: tenant tokens not wired yet — pending T5. No token found for warehouse ${warehouseId} ` +
          `in the configured tenant-tokens map.`,
      );
    }
    return token;
  }
}
