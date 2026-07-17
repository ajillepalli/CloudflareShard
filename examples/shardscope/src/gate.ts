/** gate.ts — Shardscope's OWN auth gate (T5). Gates this Worker's /api/*
 * surface behind SHARDSCOPE_GATE_TOKEN — see src/index.ts's header comment
 * for the two-tier auth model this fits into (ADMIN_TOKEN vs
 * SHARDSCOPE_GATE_TOKEN are different secrets with different jobs).
 *
 * NOT hardened multi-user auth. This is a single shared secret
 * (SHARDSCOPE_GATE_TOKEN) that a browser either presents directly as a
 * `authorization: Bearer <token>` header, or — after a one-shot
 * `POST /login` with that same token — gets echoed back as a plain session
 * cookie holding the literal gate token (HttpOnly; no server-side session
 * store; no per-user identity; no expiry rotation beyond a fixed max-age).
 * That is exactly enough to keep Shardscope's /api/* surface (starting a
 * load run, reading live topology) from being wide open to anyone who can
 * reach the Worker's URL. It is a demo-grade viewer/operator gate, not a
 * real authentication system — do not mistake it for one, and do not build
 * multi-user features (roles, audit-by-identity, revocable sessions) on top
 * of it without replacing this first.
 *
 * ADMIN_TOKEN and any tenant token are NEVER sent to, or held by, the
 * browser under any circumstance — only this one gate artifact ever reaches
 * client-side code.
 */
import type { Env } from "./env";
import { timingSafeEqual } from "../../../src/auth";

export const GATE_COOKIE_NAME = "shardscope_gate";

// A demo session length, not a security-tuned value — see this file's
// header comment.
const SESSION_MAX_AGE_SECONDS = 60 * 60 * 12; // 12h

function readCookie(cookieHeader: string | null, name: string): string | null {
  if (!cookieHeader) return null;
  for (const part of cookieHeader.split(";")) {
    const eq = part.indexOf("=");
    if (eq === -1) continue;
    const key = part.slice(0, eq).trim();
    if (key !== name) continue;
    try {
      return decodeURIComponent(part.slice(eq + 1).trim());
    } catch {
      return null; // malformed percent-encoding — treat as "no cookie", never throw out of a gate check
    }
  }
  return null;
}

/** True iff `request` presents SHARDSCOPE_GATE_TOKEN either as
 * `authorization: Bearer <token>` or as the `shardscope_gate` cookie set by
 * POST /login below. Fails closed: a misconfigured (empty/unset)
 * SHARDSCOPE_GATE_TOKEN is never treated as "no gate needed" — every /api/*
 * request is rejected instead. */
export function isGateAuthorized(request: Request, env: Env): boolean {
  if (!env.SHARDSCOPE_GATE_TOKEN) return false;

  const authHeader = request.headers.get("authorization");
  if (authHeader) {
    const match = /^Bearer\s+(.+)$/i.exec(authHeader.trim());
    if (match && timingSafeEqual(match[1], env.SHARDSCOPE_GATE_TOKEN)) return true;
  }

  const cookieToken = readCookie(request.headers.get("cookie"), GATE_COOKIE_NAME);
  if (cookieToken && timingSafeEqual(cookieToken, env.SHARDSCOPE_GATE_TOKEN)) return true;

  return false;
}

const jsonHeaders = { "content-type": "application/json" };

/** POST /login handler — body `{ token: string }`. On a match, sets the
 * session cookie and returns 200; otherwise 401. This is the "simple login"
 * referenced in this file's header: it creates no server-side session
 * record, so there is nothing to revoke server-side short of rotating
 * SHARDSCOPE_GATE_TOKEN itself (which invalidates every existing cookie and
 * header holder at once). Deliberately NOT gated by isGateAuthorized — it's
 * how a browser obtains the gate artifact in the first place. */
export async function handleLogin(request: Request, env: Env): Promise<Response> {
  if (!env.SHARDSCOPE_GATE_TOKEN) {
    return new Response(JSON.stringify({ error: "SHARDSCOPE_GATE_TOKEN is not configured." }), { status: 500, headers: jsonHeaders });
  }

  let body: { token?: string };
  try {
    body = (await request.json()) as { token?: string };
  } catch {
    return new Response(JSON.stringify({ error: "Invalid JSON body." }), { status: 400, headers: jsonHeaders });
  }

  if (typeof body.token !== "string" || !timingSafeEqual(body.token, env.SHARDSCOPE_GATE_TOKEN)) {
    return new Response(JSON.stringify({ error: "Unauthorized." }), { status: 401, headers: jsonHeaders });
  }

  return new Response(JSON.stringify({ ok: true }), {
    status: 200,
    headers: {
      ...jsonHeaders,
      "set-cookie": `${GATE_COOKIE_NAME}=${encodeURIComponent(body.token)}; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age=${SESSION_MAX_AGE_SECONDS}`,
    },
  });
}

/** POST /logout — clears the session cookie. No server-side state to revoke
 * (see handleLogin's doc comment); this only affects the calling browser. */
export function handleLogout(): Response {
  return new Response(JSON.stringify({ ok: true }), {
    status: 200,
    headers: {
      ...jsonHeaders,
      "set-cookie": `${GATE_COOKIE_NAME}=; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age=0`,
    },
  });
}
