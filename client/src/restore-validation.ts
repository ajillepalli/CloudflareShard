import { CloudflareShardError } from "./errors.js";
import type { RestoreAcceptedResponse, RestorePlan, RestorePreviewResponse, RestoreStatusResponse } from "./types.js";

const HASH = /^[a-f0-9]{64}$/;
const PHASES = new Set([
  "previewing", "previewed", "fencing", "restoring", "reconciliation_pending", "reconciling",
  "verifying", "rolling_back", "parked_lease_lost", "complete", "rolled_back",
  "manual_repair_required", "failed",
]);
const ERROR_CODES = new Set([
  "RESTORE_INVALID_REQUEST", "RESTORE_VERSION_UNSUPPORTED", "RESTORE_CUTOFF_IN_FUTURE",
  "RESTORE_CUTOFF_OUTSIDE_PITR_WINDOW", "RESTORE_BOOKMARK_MISSING", "RESTORE_ENUMERATION_INCOMPLETE",
  "RESTORE_PLAN_HASH_MISMATCH", "RESTORE_PLAN_STALE", "RESTORE_CONFLICT", "RESTORE_INTERRUPTED",
  "RESTORE_MANIFEST_GAP", "RESTORE_HASH_CONTRADICTION", "RESTORE_INVARIANT_FAILED", "RESTORE_UNAVAILABLE",
]);

function invalid(message: string): never {
  throw new CloudflareShardError(502, {
    error: { code: "INVALID_RESTORE_RESPONSE", message: `Invalid restore response: ${message}` },
  });
}

function object(value: unknown, field: string): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) invalid(`${field} must be an object.`);
  const prototype = Object.getPrototypeOf(value);
  if (prototype !== Object.prototype && prototype !== null) invalid(`${field} must be a plain object.`);
  return value as Record<string, unknown>;
}

function exact(value: Record<string, unknown>, field: string, keys: readonly string[]): void {
  const expected = new Set(keys);
  if (Object.keys(value).some((key) => !expected.has(key)) || keys.some((key) => !(key in value))) {
    invalid(`${field} fields do not match the V1 contract.`);
  }
}

function string(value: unknown, field: string): string {
  if (typeof value !== "string" || value.length === 0) invalid(`${field} must be a non-empty string.`);
  return value;
}

function hash(value: unknown, field: string): string {
  if (typeof value !== "string" || !HASH.test(value)) invalid(`${field} must be a lowercase SHA-256 digest.`);
  return value;
}

function integer(value: unknown, field: string, minimum = 0): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < minimum) {
    invalid(`${field} must be a safe integer greater than or equal to ${minimum}.`);
  }
  return value;
}

function timestamp(value: unknown, field: string): string {
  const text = string(value, field);
  const parsed = new Date(text);
  if (!Number.isFinite(parsed.getTime()) || parsed.toISOString() !== text) invalid(`${field} must be a canonical UTC timestamp.`);
  return text;
}

function nullableTimestamp(value: unknown, field: string): string | null {
  return value === null ? null : timestamp(value, field);
}

function nullableString(value: unknown, field: string): string | null {
  return value === null ? null : string(value, field);
}

function validateRestorePlan(value: unknown): RestorePlan {
  const plan = object(value, "plan");
  exact(plan, "plan", [
    "protocol_version", "format_version", "restore_id", "fleet_id", "cutoff", "previewed_at", "execute_before",
    "parameter_hash", "topology", "manifest", "participants", "impact", "rollback", "plan_hash",
  ]);
  if (plan.protocol_version !== 1 || plan.format_version !== 1) invalid("plan version is unsupported.");
  string(plan.restore_id, "plan.restore_id");
  string(plan.fleet_id, "plan.fleet_id");
  const cutoff = timestamp(plan.cutoff, "plan.cutoff");
  const previewedAt = timestamp(plan.previewed_at, "plan.previewed_at");
  const executeBefore = timestamp(plan.execute_before, "plan.execute_before");
  if (Date.parse(cutoff) > Date.parse(previewedAt) || Date.parse(previewedAt) >= Date.parse(executeBefore)) {
    invalid("plan timestamps are inconsistent.");
  }
  hash(plan.parameter_hash, "plan.parameter_hash");
  hash(plan.plan_hash, "plan.plan_hash");

  const topology = object(plan.topology, "plan.topology");
  exact(topology, "plan.topology", ["topology_epoch", "topology_hash"]);
  integer(topology.topology_epoch, "plan.topology.topology_epoch", 1);
  hash(topology.topology_hash, "plan.topology.topology_hash");

  const manifest = object(plan.manifest, "plan.manifest");
  exact(manifest, "plan.manifest", [
    "coverage_start", "catalog_close_key", "catalog_generation", "catalog_snapshot_hash",
    "fleet_root_hash", "partition_config_hash", "record_count",
  ]);
  const coverageStart = timestamp(manifest.coverage_start, "plan.manifest.coverage_start");
  if (Date.parse(coverageStart) > Date.parse(cutoff)) invalid("manifest coverage starts after the cutoff.");
  hash(manifest.catalog_close_key, "plan.manifest.catalog_close_key");
  integer(manifest.catalog_generation, "plan.manifest.catalog_generation", 1);
  hash(manifest.catalog_snapshot_hash, "plan.manifest.catalog_snapshot_hash");
  hash(manifest.fleet_root_hash, "plan.manifest.fleet_root_hash");
  hash(manifest.partition_config_hash, "plan.manifest.partition_config_hash");
  const recordCount = integer(manifest.record_count, "plan.manifest.record_count");

  if (!Array.isArray(plan.participants) || plan.participants.length === 0) invalid("plan.participants must be non-empty.");
  let previousParticipant = "";
  for (const candidate of plan.participants) {
    const participant = object(candidate, "plan.participant");
    exact(participant, "plan.participant", ["participant_id", "target_bookmark", "preview_bookmark"]);
    const participantId = string(participant.participant_id, "plan.participant.participant_id");
    if (participantId <= previousParticipant) invalid("plan participants must be unique and sorted.");
    previousParticipant = participantId;
    string(participant.target_bookmark, "plan.participant.target_bookmark");
    string(participant.preview_bookmark, "plan.participant.preview_bookmark");
  }

  const impact = object(plan.impact, "plan.impact");
  exact(impact, "plan.impact", ["participant_count", "transaction_count", "intentional_loss_from", "intentional_loss_through"]);
  if (integer(impact.participant_count, "plan.impact.participant_count", 1) !== plan.participants.length) invalid("participant count is inconsistent.");
  if (integer(impact.transaction_count, "plan.impact.transaction_count") !== recordCount) invalid("transaction count is inconsistent.");
  if (timestamp(impact.intentional_loss_from, "plan.impact.intentional_loss_from") !== cutoff
      || timestamp(impact.intentional_loss_through, "plan.impact.intentional_loss_through") !== executeBefore) {
    invalid("intentional loss bounds are inconsistent.");
  }

  const rollback = object(plan.rollback, "plan.rollback");
  exact(rollback, "plan.rollback", ["undo_supported", "undo_expires_at", "limitations"]);
  if (typeof rollback.undo_supported !== "boolean") invalid("plan.rollback.undo_supported must be boolean.");
  nullableTimestamp(rollback.undo_expires_at, "plan.rollback.undo_expires_at");
  if (!Array.isArray(rollback.limitations)) invalid("plan.rollback.limitations must be an array.");
  let previousLimitation = "";
  for (const candidate of rollback.limitations) {
    const limitation = string(candidate, "plan.rollback.limitation");
    if (limitation <= previousLimitation) invalid("rollback limitations must be unique and sorted.");
    previousLimitation = limitation;
  }
  return plan as unknown as RestorePlan;
}

export function validateRestorePreviewResponse(value: unknown): RestorePreviewResponse {
  const response = object(value, "preview response");
  if (response.status === "previewing") {
    exact(response, "preview response", ["ok", "status", "restore_id", "retry_after_ms"]);
    if (response.ok !== true) invalid("preview response must be successful.");
    string(response.restore_id, "preview response.restore_id");
    integer(response.retry_after_ms, "preview response.retry_after_ms");
    return response as unknown as RestorePreviewResponse;
  }
  if (response.status === "previewed") {
    exact(response, "preview response", ["ok", "status", "plan"]);
    if (response.ok !== true) invalid("preview response must be successful.");
    validateRestorePlan(response.plan);
    return response as unknown as RestorePreviewResponse;
  }
  invalid("preview status is unsupported.");
}

export function validateRestoreAcceptedResponse(value: unknown): RestoreAcceptedResponse {
  const response = object(value, "accepted response");
  exact(response, "accepted response", ["ok", "status", "restore_id", "plan_hash"]);
  if (response.ok !== true || (response.status !== "accepted" && response.status !== "already_started")) {
    invalid("accepted response status is unsupported.");
  }
  string(response.restore_id, "accepted response.restore_id");
  hash(response.plan_hash, "accepted response.plan_hash");
  return response as unknown as RestoreAcceptedResponse;
}

export function validateRestoreStatusResponse(value: unknown): RestoreStatusResponse {
  const response = object(value, "status response");
  exact(response, "status response", [
    "protocol_version", "format_version", "restore_id", "plan_hash", "fleet_id", "cutoff", "phase",
    "started_at", "updated_at", "completed_at", "progress", "blockers", "report",
  ]);
  if (response.protocol_version !== 1 || response.format_version !== 1) invalid("status response version is unsupported.");
  string(response.restore_id, "status response.restore_id");
  string(response.fleet_id, "status response.fleet_id");
  timestamp(response.cutoff, "status response.cutoff");
  if (typeof response.phase !== "string" || !PHASES.has(response.phase)) invalid("status response phase is unsupported.");
  if (response.plan_hash === null) {
    if (response.phase !== "previewing") invalid("status response plan hash may be null only while previewing.");
  } else hash(response.plan_hash, "status response.plan_hash");
  nullableTimestamp(response.started_at, "status response.started_at");
  timestamp(response.updated_at, "status response.updated_at");
  const completedAt = nullableTimestamp(response.completed_at, "status response.completed_at");

  const progress = object(response.progress, "status response.progress");
  exact(progress, "status response.progress", ["participants_total", "participants_restored", "transactions_total", "transactions_reconciled"]);
  const participantsTotal = integer(progress.participants_total, "status response.progress.participants_total");
  const participantsRestored = integer(progress.participants_restored, "status response.progress.participants_restored");
  const transactionsTotal = integer(progress.transactions_total, "status response.progress.transactions_total");
  const transactionsReconciled = integer(progress.transactions_reconciled, "status response.progress.transactions_reconciled");
  if (participantsRestored > participantsTotal || transactionsReconciled > transactionsTotal) invalid("status response progress exceeds totals.");

  if (!Array.isArray(response.blockers)) invalid("status response blockers must be an array.");
  for (const candidate of response.blockers) {
    const blocker = object(candidate, "status response blocker");
    exact(blocker, "status response blocker", ["code", "message", "participant_id", "tx_id"]);
    if (typeof blocker.code !== "string" || !ERROR_CODES.has(blocker.code)) invalid("status response blocker code is unsupported.");
    string(blocker.message, "status response blocker.message");
    nullableString(blocker.participant_id, "status response blocker.participant_id");
    nullableString(blocker.tx_id, "status response blocker.tx_id");
  }

  let reportComplete = false;
  if (response.report !== null) {
    const report = object(response.report, "status response report");
    exact(report, "status response report", [
      "discarded_write_count", "discarded_write_report_hash", "discarded_write_report_complete",
      "measured_rpo_ms", "measured_rto_ms", "verified_at",
    ]);
    integer(report.discarded_write_count, "status response report.discarded_write_count");
    hash(report.discarded_write_report_hash, "status response report.discarded_write_report_hash");
    if (typeof report.discarded_write_report_complete !== "boolean") invalid("status response report completeness must be boolean.");
    reportComplete = report.discarded_write_report_complete;
    integer(report.measured_rpo_ms, "status response report.measured_rpo_ms");
    integer(report.measured_rto_ms, "status response report.measured_rto_ms");
    timestamp(report.verified_at, "status response report.verified_at");
  }
  if (response.phase === "complete" && (
    completedAt === null || response.report === null || !reportComplete
    || participantsRestored !== participantsTotal || transactionsReconciled !== transactionsTotal
    || response.blockers.length !== 0
  )) invalid("complete status response lacks complete evidence.");
  if (response.phase === "rolled_back" && (completedAt === null || response.blockers.length !== 0)) {
    invalid("rolled-back status response is incomplete.");
  }
  return response as unknown as RestoreStatusResponse;
}
