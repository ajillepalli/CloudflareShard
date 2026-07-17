/** token-provider.ts — the seam T5 (durable tenant-token storage) plugs
 * into.
 *
 * ./transactions.ts and ./gateway-client.ts don't know or care HOW a
 * warehouse's tenant bearer token is obtained — they only depend on this
 * interface. Originally (T3, this file) there was no durable per-tenant
 * token store in Shardscope yet, so `EnvTokenProvider` was a stub that
 * always threw a clear "pending T5" error the moment a real transaction
 * tried to issue against the gateway — a deliberate, documented gap, not a
 * bug: T3's job was the load-mix/skew/batch machinery, not tenant-token
 * storage.
 *
 * T5 has landed: ./tenant-token-store.ts's TenantTokenStoreTokenProvider is
 * now the real implementation ./load-driver.ts's constructor wires in by
 * default, backed by a durable get-or-create tenant-token store (a
 * singleton Durable Object — see that file's header comment for why a DO
 * was chosen over a KV namespace). `EnvTokenProvider` below remains — it's
 * still a valid, useful TokenProvider (e.g. for tests that want to inject
 * fixed tokens directly) — it's just no longer the default. No other file
 * in ./load/ needed to change to make this swap, exactly as designed:
 * everything upstream only depends on the TokenProvider interface below.
 */

export interface TokenProvider {
  /** Resolves the bearer token for the tenant that owns `warehouseId`'s
   * data (one tenant per warehouse — see ./transactions.ts's
   * tenantIdForWarehouse). Must reject/throw, never return an empty or
   * placeholder token, when no real token is available. */
  getTenantToken(warehouseId: number): Promise<string>;
}

/** Fallback/test TokenProvider — no longer LoadDriver's default (see this
 * file's header comment; ./tenant-token-store.ts's
 * TenantTokenStoreTokenProvider is). Reads an optional JSON map of
 * `{ [warehouseId]: token }` from a caller-supplied string. With no map
 * configured, every call throws a clear error rather than returning an
 * empty/placeholder token — useful for tests that want a fixed, predictable
 * token set without standing up a real TenantTokenStore DO. */
export class EnvTokenProvider implements TokenProvider {
  constructor(private readonly tokensJson: string | undefined | null) {}

  async getTenantToken(warehouseId: number): Promise<string> {
    if (!this.tokensJson) {
      throw new Error(
        `EnvTokenProvider: no tenant-tokens JSON configured, so no token is available for warehouse ${warehouseId}. ` +
          `EnvTokenProvider is a fallback/test TokenProvider — pass a real \`{ [warehouseId]: token }\` JSON map to ` +
          `use it, or use ./tenant-token-store.ts's TenantTokenStoreTokenProvider (LoadDriver's default) for real ` +
          `durable tenant tokens.`,
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
      throw new Error(`EnvTokenProvider: no token found for warehouse ${warehouseId} in the configured tenant-tokens map.`);
    }
    return token;
  }
}
