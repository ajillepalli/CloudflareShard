/** reshard.ts — Shardscope's Reshard console server-side layer (T8).
 *
 * Thin, validated wrappers around the topology-mutating admin RPC methods
 * CloudflareShardRpc already exposes (adminSplitVbucket, adminMigrateVbucket,
 * adminMigrateVbucketStatus, adminMigrateVbucketAbort, adminDrainShard,
 * adminDrainShardStatus, adminTopologyLockStatus,
 * adminForceReleaseTopologyLock) — see src/env.d.ts's ShardApiBinding for the
 * exact payload/response shapes these mirror (which themselves mirror the
 * main repo's *Core functions in src/index.ts / src/catalog.ts).
 *
 * This file's only job is:
 *   1. Validate/coerce the JSON body (or query params, for the two GET
 *      status routes) a Reshard console HTTP route in src/index.ts receives
 *      from the browser into the exact shape the RPC method expects. vBucket
 *      ids are catalog-local (see env.d.ts's header comment), so every
 *      mutating/status call here REQUIRES catalogShardId — unlike
 *      TopologyAggregator's read-only adminStatus/adminVbucketMap calls,
 *      which don't take one.
 *   2. Call env.SHARD_API.adminXxx(env.ADMIN_TOKEN, ...) so ADMIN_TOKEN never
 *      has to be threaded through (or even referenced by) src/index.ts's
 *      route handlers, keeping it strictly server-side (see this Worker's
 *      two-tier auth model, documented in src/index.ts's header comment).
 *
 * Every call() function here can reject: the underlying RPC's unwrapForRpc
 * (main repo's src/index.ts) throws on any non-2xx HTTP response from the
 * catalog (e.g. 409 MIGRATION_IN_PROGRESS, 409 RING_EVACUATION_NO_CANDIDATE).
 * Route handlers in src/index.ts are responsible for catching that and
 * turning it into a calm inline error for the browser — this file does not
 * swallow or reshape those errors itself.
 */
import type { Env } from "./env";

/** Thrown by the parse*() functions below on a malformed request body/query
 * — distinct from the errors env.SHARD_API's calls themselves can reject
 * with, so route handlers can tell "bad request from the browser" (400) apart
 * from "the cluster rejected this operation" (whatever status the underlying
 * RPC error carries, surfaced generically). */
export class ReshardValidationError extends Error {}

function asRecord(body: unknown): Record<string, unknown> {
  if (typeof body !== "object" || body === null || Array.isArray(body)) {
    throw new ReshardValidationError("Request body must be a JSON object.");
  }
  return body as Record<string, unknown>;
}

function requireNonEmptyString(value: unknown, field: string): string {
  if (typeof value !== "string" || value.trim() === "") {
    throw new ReshardValidationError(`Missing or invalid "${field}" (must be a non-empty string).`);
  }
  return value;
}

function optionalNonEmptyString(value: unknown, field: string): string | undefined {
  if (value === undefined || value === null || value === "") return undefined;
  if (typeof value !== "string" || value.trim() === "") {
    throw new ReshardValidationError(`"${field}", if provided, must be a non-empty string.`);
  }
  return value;
}

/** Accepts either a JSON number or a numeric string (GET query params arrive
 * as strings; POST JSON bodies typically carry a real number from
 * `<input type="number">`, but a string is coerced too rather than rejected —
 * defensive, since the browser is the one constructing these bodies). */
function requireVbucket(value: unknown, field = "vbucket"): number {
  const n = typeof value === "string" && value.trim() !== "" ? Number(value) : value;
  if (typeof n !== "number" || !Number.isInteger(n) || n < 0) {
    throw new ReshardValidationError(`Missing or invalid "${field}" (must be a non-negative integer).`);
  }
  return n;
}

// ============================================================================
// Split
// ============================================================================

export interface SplitVbucketInput {
  catalogShardId: string;
  vbucket: number;
  newShardId?: string;
}

export function parseSplitVbucketInput(body: unknown): SplitVbucketInput {
  const b = asRecord(body);
  return {
    catalogShardId: requireNonEmptyString(b.catalogShardId, "catalogShardId"),
    vbucket: requireVbucket(b.vbucket),
    newShardId: optionalNonEmptyString(b.newShardId, "newShardId"),
  };
}

export function splitVbucket(env: Env, input: SplitVbucketInput): Promise<unknown> {
  return env.SHARD_API.adminSplitVbucket(env.ADMIN_TOKEN, input);
}

// ============================================================================
// Migrate
// ============================================================================

export interface MigrateVbucketInput {
  catalogShardId: string;
  vbucket: number;
  targetShardId?: string;
}

export function parseMigrateVbucketInput(body: unknown): MigrateVbucketInput {
  const b = asRecord(body);
  return {
    catalogShardId: requireNonEmptyString(b.catalogShardId, "catalogShardId"),
    vbucket: requireVbucket(b.vbucket),
    targetShardId: optionalNonEmptyString(b.targetShardId, "targetShardId"),
  };
}

export function migrateVbucket(env: Env, input: MigrateVbucketInput): Promise<unknown> {
  return env.SHARD_API.adminMigrateVbucket(env.ADMIN_TOKEN, input);
}

// ============================================================================
// Migrate status / abort — share a (catalogShardId, vbucket) shape
// ============================================================================

export interface MigrateVbucketRefInput {
  catalogShardId: string;
  vbucket: number;
}

function parseMigrateVbucketRef(source: Record<string, unknown> | URLSearchParams): MigrateVbucketRefInput {
  const get = (key: string): unknown => (source instanceof URLSearchParams ? source.get(key) ?? undefined : source[key]);
  return {
    catalogShardId: requireNonEmptyString(get("catalogShardId"), "catalogShardId"),
    vbucket: requireVbucket(get("vbucket")),
  };
}

/** GET /api/reshard/migrate-status?catalogShardId=...&vbucket=... */
export function parseMigrateVbucketStatusQuery(params: URLSearchParams): MigrateVbucketRefInput {
  return parseMigrateVbucketRef(params);
}

export function migrateVbucketStatus(env: Env, input: MigrateVbucketRefInput): Promise<unknown> {
  return env.SHARD_API.adminMigrateVbucketStatus(env.ADMIN_TOKEN, input);
}

/** POST /api/reshard/migrate-abort — body `{ catalogShardId, vbucket }`. */
export function parseMigrateVbucketAbortInput(body: unknown): MigrateVbucketRefInput {
  return parseMigrateVbucketRef(asRecord(body));
}

export function migrateVbucketAbort(env: Env, input: MigrateVbucketRefInput): Promise<unknown> {
  return env.SHARD_API.adminMigrateVbucketAbort(env.ADMIN_TOKEN, input);
}

// ============================================================================
// Drain
// ============================================================================

export interface DrainShardInput {
  catalogShardId: string;
  shardId: string;
}

export function parseDrainShardInput(body: unknown): DrainShardInput {
  const b = asRecord(body);
  return {
    catalogShardId: requireNonEmptyString(b.catalogShardId, "catalogShardId"),
    shardId: requireNonEmptyString(b.shardId, "shardId"),
  };
}

export function drainShard(env: Env, input: DrainShardInput): Promise<unknown> {
  // adminDrainShardCore's payload puts shardId first, catalogShardId second,
  // but ShardApiBinding takes one object — key order is irrelevant to the
  // JSON body the RPC actually sends.
  return env.SHARD_API.adminDrainShard(env.ADMIN_TOKEN, input);
}

/** GET /api/reshard/drain-status?catalogShardId=...&shardId=... */
export function parseDrainShardStatusQuery(params: URLSearchParams): DrainShardInput {
  return {
    catalogShardId: requireNonEmptyString(params.get("catalogShardId") ?? undefined, "catalogShardId"),
    shardId: requireNonEmptyString(params.get("shardId") ?? undefined, "shardId"),
  };
}

export function drainShardStatus(env: Env, input: DrainShardInput): Promise<unknown> {
  return env.SHARD_API.adminDrainShardStatus(env.ADMIN_TOKEN, input);
}

// ============================================================================
// Topology lock status / force-release — cluster-wide, no catalogShardId
// ============================================================================

export function topologyLockStatus(env: Env): Promise<unknown> {
  return env.SHARD_API.adminTopologyLockStatus(env.ADMIN_TOKEN);
}

export interface ForceReleaseTopologyLockInput {
  operationId: string;
}

export function parseForceReleaseTopologyLockInput(body: unknown): ForceReleaseTopologyLockInput {
  const b = asRecord(body);
  return { operationId: requireNonEmptyString(b.operationId, "operationId") };
}

export function forceReleaseTopologyLock(env: Env, input: ForceReleaseTopologyLockInput): Promise<unknown> {
  return env.SHARD_API.adminForceReleaseTopologyLock(env.ADMIN_TOKEN, input);
}
