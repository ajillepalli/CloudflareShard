/** Constant-time comparison — avoids leaking token length/prefix match via response timing. */
export function timingSafeEqual(a: string, b: string): boolean {
  const enc = new TextEncoder();
  const aBytes = enc.encode(a);
  const bBytes = enc.encode(b);
  const maxLen = Math.max(aBytes.length, bBytes.length);
  let diff = aBytes.length === bBytes.length ? 0 : 1;
  for (let i = 0; i < maxLen; i += 1) {
    diff |= (aBytes[i] ?? 0) ^ (bBytes[i] ?? 0);
  }
  return diff === 0;
}

export function isValidBearerToken(authorizationHeader: string | null, expectedToken: string): boolean {
  if (!authorizationHeader) return false;
  return timingSafeEqual(authorizationHeader, `Bearer ${expectedToken}`);
}

/** Shared admin-token gate used by both the Worker and CatalogDO — a single
 * place to change the auth error shape or token scheme for every admin route. */
export function checkAdminAuth(
  adminToken: string | undefined,
  request: Request,
): { error: string; status: number } | null {
  if (!adminToken) {
    return { error: "ADMIN_TOKEN is not configured.", status: 500 };
  }
  if (!isValidBearerToken(request.headers.get("authorization"), adminToken)) {
    return { error: "Unauthorized.", status: 401 };
  }
  return null;
}
