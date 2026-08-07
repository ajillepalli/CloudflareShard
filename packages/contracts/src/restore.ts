/**
 * Binding-free fleet restore contracts. Runtime adapters own Durable Object
 * storage and orchestration; this module owns immutable wire formats, exact
 * validation, hashing, status states, and typed fail-closed errors.
 */

import { hashCanonicalJson, type JsonValue } from "./transaction.js";

export const RESTORE_PROTOCOL_VERSION = 1 as const;
export const RESTORE_REQUEST_FORMAT_VERSION = 1 as const;
export const RESTORE_PLAN_FORMAT_VERSION = 1 as const;
export const MIN_READABLE_RESTORE_PLAN_FORMAT_VERSION = 1 as const;
export const RESTORE_STATUS_FORMAT_VERSION = 1 as const;
export const RESTORE_ERROR_SCHEMA_VERSION = 1 as const;

export const RESTORE_ERROR_CODES = [
  "RESTORE_INVALID_REQUEST",
  "RESTORE_VERSION_UNSUPPORTED",
  "RESTORE_CUTOFF_IN_FUTURE",
  "RESTORE_CUTOFF_OUTSIDE_PITR_WINDOW",
  "RESTORE_BOOKMARK_MISSING",
  "RESTORE_ENUMERATION_INCOMPLETE",
  "RESTORE_PLAN_HASH_MISMATCH",
  "RESTORE_PLAN_STALE",
  "RESTORE_CONFLICT",
  "RESTORE_INTERRUPTED",
  "RESTORE_MANIFEST_GAP",
  "RESTORE_HASH_CONTRADICTION",
  "RESTORE_INVARIANT_FAILED",
  "RESTORE_UNAVAILABLE",
] as const;

export type RestoreErrorCode = (typeof RESTORE_ERROR_CODES)[number];

const RESTORE_ERROR_HTTP_STATUS: Readonly<Record<RestoreErrorCode, number>> = {
  RESTORE_INVALID_REQUEST: 400,
  RESTORE_VERSION_UNSUPPORTED: 503,
  RESTORE_CUTOFF_IN_FUTURE: 409,
  RESTORE_CUTOFF_OUTSIDE_PITR_WINDOW: 410,
  RESTORE_BOOKMARK_MISSING: 409,
  RESTORE_ENUMERATION_INCOMPLETE: 409,
  RESTORE_PLAN_HASH_MISMATCH: 409,
  RESTORE_PLAN_STALE: 409,
  RESTORE_CONFLICT: 409,
  RESTORE_INTERRUPTED: 503,
  RESTORE_MANIFEST_GAP: 409,
  RESTORE_HASH_CONTRADICTION: 409,
  RESTORE_INVARIANT_FAILED: 409,
  RESTORE_UNAVAILABLE: 503,
};

const RETRYABLE_RESTORE_ERRORS: ReadonlySet<RestoreErrorCode> = new Set([
  "RESTORE_INTERRUPTED",
  "RESTORE_UNAVAILABLE",
]);

export interface RestoreProtocolError {
  readonly schema_version: typeof RESTORE_ERROR_SCHEMA_VERSION;
  readonly code: RestoreErrorCode;
  readonly message: string;
  readonly http_status: number;
  readonly retryable: boolean;
  readonly overloaded?: true;
  readonly retry_after_ms?: number;
  readonly details?: Readonly<Record<string, JsonValue>>;
}

export class RestoreContractViolation extends Error {
  readonly protocolError: RestoreProtocolError;

  constructor(protocolError: RestoreProtocolError) {
    super(protocolError.message);
    this.name = "RestoreContractViolation";
    this.protocolError = protocolError;
  }
}

export function restoreError(
  code: RestoreErrorCode,
  message: string,
  details?: Readonly<Record<string, JsonValue>>,
): RestoreProtocolError {
  return {
    schema_version: RESTORE_ERROR_SCHEMA_VERSION,
    code,
    message,
    http_status: RESTORE_ERROR_HTTP_STATUS[code],
    retryable: RETRYABLE_RESTORE_ERRORS.has(code),
    ...(details === undefined ? {} : { details }),
  };
}

function failRestore(
  code: RestoreErrorCode,
  message: string,
  details?: Readonly<Record<string, JsonValue>>,
): never {
  throw new RestoreContractViolation(restoreError(code, message, details));
}

export interface RestorePreviewRequestV1 {
  readonly protocol_version: typeof RESTORE_PROTOCOL_VERSION;
  readonly format_version: typeof RESTORE_REQUEST_FORMAT_VERSION;
  readonly fleet_id: string;
  readonly cutoff: string;
  readonly idempotency_key: string;
}

export interface RestoreExecuteRequestV1 {
  readonly protocol_version: typeof RESTORE_PROTOCOL_VERSION;
  readonly format_version: typeof RESTORE_REQUEST_FORMAT_VERSION;
  readonly restore_id: string;
  readonly plan_hash: string;
}

export interface RestoreStatusRequestV1 {
  readonly protocol_version: typeof RESTORE_PROTOCOL_VERSION;
  readonly format_version: typeof RESTORE_REQUEST_FORMAT_VERSION;
  readonly restore_id: string;
}

export type RestoreReconcileRequestV1 = RestoreExecuteRequestV1;
export type RestoreRollbackRequestV1 = RestoreExecuteRequestV1;

export interface RestoreTopologyPinV1 {
  readonly topology_epoch: number;
  readonly topology_hash: string;
}

export interface RestoreManifestPinV1 {
  readonly coverage_start: string;
  readonly catalog_close_key: string;
  readonly catalog_generation: number;
  readonly catalog_snapshot_hash: string;
  readonly fleet_root_hash: string;
  readonly partition_config_hash: string;
  readonly record_count: number;
}

export interface RestoreParticipantPlanV1 {
  readonly participant_id: string;
  readonly target_bookmark: string;
  readonly preview_bookmark: string;
}

export interface RestoreImpactV1 {
  readonly participant_count: number;
  readonly transaction_count: number;
  readonly intentional_loss_from: string;
  readonly intentional_loss_through: string;
}

export interface RestoreRollbackV1 {
  readonly undo_supported: boolean;
  readonly undo_expires_at: string | null;
  readonly limitations: readonly string[];
}

export interface RestorePlanBodyV1 {
  readonly protocol_version: typeof RESTORE_PROTOCOL_VERSION;
  readonly format_version: typeof RESTORE_PLAN_FORMAT_VERSION;
  readonly restore_id: string;
  readonly fleet_id: string;
  readonly cutoff: string;
  readonly previewed_at: string;
  readonly execute_before: string;
  readonly parameter_hash: string;
  readonly topology: RestoreTopologyPinV1;
  readonly manifest: RestoreManifestPinV1;
  readonly participants: readonly RestoreParticipantPlanV1[];
  readonly impact: RestoreImpactV1;
  readonly rollback: RestoreRollbackV1;
}

export interface RestorePlanV1 extends RestorePlanBodyV1 {
  readonly plan_hash: string;
}

export type ReadableRestorePlan = RestorePlanV1;

export const RESTORE_PHASES = [
  "previewing",
  "previewed",
  "fencing",
  "restoring",
  "reconciliation_pending",
  "reconciling",
  "verifying",
  "rolling_back",
  "parked_lease_lost",
  "complete",
  "rolled_back",
  "manual_repair_required",
  "failed",
] as const;

export type RestorePhase = (typeof RESTORE_PHASES)[number];

export interface RestoreProgressV1 {
  readonly participants_total: number;
  readonly participants_restored: number;
  readonly transactions_total: number;
  readonly transactions_reconciled: number;
}

export interface RestoreBlockerV1 {
  readonly code: RestoreErrorCode;
  readonly message: string;
  readonly participant_id: string | null;
  readonly tx_id: string | null;
}

export interface RestoreReportV1 {
  readonly discarded_write_count: number;
  readonly discarded_write_report_hash: string;
  readonly discarded_write_report_complete: boolean;
  readonly measured_rpo_ms: number;
  readonly measured_rto_ms: number;
  readonly verified_at: string;
}

export interface RestoreStatusV1 {
  readonly protocol_version: typeof RESTORE_PROTOCOL_VERSION;
  readonly format_version: typeof RESTORE_STATUS_FORMAT_VERSION;
  readonly restore_id: string;
  readonly plan_hash: string | null;
  readonly fleet_id: string;
  readonly cutoff: string;
  readonly phase: RestorePhase;
  readonly started_at: string | null;
  readonly updated_at: string;
  readonly completed_at: string | null;
  readonly progress: RestoreProgressV1;
  readonly blockers: readonly RestoreBlockerV1[];
  readonly report: RestoreReportV1 | null;
}

export interface RestorePreviewCompleteResultV1 {
  readonly ok: true;
  readonly status: "previewed";
  readonly plan: RestorePlanV1;
}

export interface RestorePreviewPendingResultV1 {
  readonly ok: true;
  readonly status: "previewing";
  readonly restore_id: string;
  readonly retry_after_ms: number;
}

export type RestorePreviewResultV1 = RestorePreviewCompleteResultV1 | RestorePreviewPendingResultV1;

export interface RestoreAcceptedResultV1 {
  readonly ok: true;
  readonly status: "accepted" | "already_started";
  readonly restore_id: string;
  readonly plan_hash: string;
}

function assertPlainObject(value: unknown, field: string): asserts value is Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    failRestore("RESTORE_INVALID_REQUEST", `${field} must be a plain object.`, { field });
  }
  const prototype = Object.getPrototypeOf(value);
  if (prototype !== Object.prototype && prototype !== null) {
    failRestore("RESTORE_INVALID_REQUEST", `${field} must be a plain object.`, { field });
  }
}

function assertExactKeys(value: Record<string, unknown>, field: string, keys: readonly string[]): void {
  const allowed = new Set(keys);
  const unknown = Object.keys(value).filter((key) => !allowed.has(key));
  if (unknown.length > 0) {
    failRestore("RESTORE_INVALID_REQUEST", `${field} contains unknown fields.`, { field, unknown_fields: unknown });
  }
  const missing = keys.filter((key) => !(key in value));
  if (missing.length > 0) {
    failRestore("RESTORE_INVALID_REQUEST", `${field} is missing required fields.`, { field, missing_fields: missing });
  }
}

function assertVersion(value: Record<string, unknown>, expectedFormat: number, field: string): void {
  if (value.protocol_version !== RESTORE_PROTOCOL_VERSION || value.format_version !== expectedFormat) {
    failRestore("RESTORE_VERSION_UNSUPPORTED", `${field} uses an unsupported protocol or format version.`, {
      protocol_version: String(value.protocol_version),
      format_version: String(value.format_version),
    });
  }
}

function assertString(value: unknown, field: string): asserts value is string {
  if (typeof value !== "string" || value.length === 0) {
    failRestore("RESTORE_INVALID_REQUEST", `${field} must be a non-empty string.`, { field });
  }
}

function assertHash(value: unknown, field: string): asserts value is string {
  if (typeof value !== "string" || !/^[a-f0-9]{64}$/.test(value)) {
    failRestore("RESTORE_INVALID_REQUEST", `${field} must be a lowercase SHA-256 digest.`, { field });
  }
}

function assertInteger(value: unknown, field: string, minimum = 0): asserts value is number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < minimum) {
    failRestore("RESTORE_INVALID_REQUEST", `${field} must be a safe integer greater than or equal to ${minimum}.`, { field });
  }
}

function parseTimestamp(value: unknown, field: string): Date {
  assertString(value, field);
  const parsed = new Date(value);
  if (!Number.isFinite(parsed.getTime()) || parsed.toISOString() !== value) {
    failRestore("RESTORE_INVALID_REQUEST", `${field} must be a canonical UTC timestamp with millisecond precision.`, { field });
  }
  return parsed;
}

function validateRequestBase(value: unknown, field: string, keys: readonly string[]): Record<string, unknown> {
  assertPlainObject(value, field);
  assertExactKeys(value, field, keys);
  assertVersion(value, RESTORE_REQUEST_FORMAT_VERSION, field);
  return value;
}

export function validateRestorePreviewRequest(value: unknown): asserts value is RestorePreviewRequestV1 {
  const request = validateRequestBase(value, "restore_preview_request", [
    "protocol_version", "format_version", "fleet_id", "cutoff", "idempotency_key",
  ]);
  assertString(request.fleet_id, "fleet_id");
  parseTimestamp(request.cutoff, "cutoff");
  assertString(request.idempotency_key, "idempotency_key");
}

export function validateRestoreExecuteRequest(value: unknown): asserts value is RestoreExecuteRequestV1 {
  const request = validateRequestBase(value, "restore_execute_request", [
    "protocol_version", "format_version", "restore_id", "plan_hash",
  ]);
  assertString(request.restore_id, "restore_id");
  assertHash(request.plan_hash, "plan_hash");
}

export function validateRestoreStatusRequest(value: unknown): asserts value is RestoreStatusRequestV1 {
  const request = validateRequestBase(value, "restore_status_request", [
    "protocol_version", "format_version", "restore_id",
  ]);
  assertString(request.restore_id, "restore_id");
}

export function validateRestoreReconcileRequest(value: unknown): asserts value is RestoreReconcileRequestV1 {
  const request = validateRequestBase(value, "restore_reconcile_request", [
    "protocol_version", "format_version", "restore_id", "plan_hash",
  ]);
  assertString(request.restore_id, "restore_id");
  assertHash(request.plan_hash, "plan_hash");
}

export function validateRestoreRollbackRequest(value: unknown): asserts value is RestoreRollbackRequestV1 {
  const request = validateRequestBase(value, "restore_rollback_request", [
    "protocol_version", "format_version", "restore_id", "plan_hash",
  ]);
  assertString(request.restore_id, "restore_id");
  assertHash(request.plan_hash, "plan_hash");
}

function validateTopology(value: unknown): asserts value is RestoreTopologyPinV1 {
  assertPlainObject(value, "restore_topology");
  assertExactKeys(value, "restore_topology", ["topology_epoch", "topology_hash"]);
  assertInteger(value.topology_epoch, "topology_epoch", 1);
  assertHash(value.topology_hash, "topology_hash");
}

function validateManifest(value: unknown, cutoff: Date): asserts value is RestoreManifestPinV1 {
  assertPlainObject(value, "restore_manifest");
  assertExactKeys(value, "restore_manifest", [
    "coverage_start", "catalog_close_key", "catalog_generation", "catalog_snapshot_hash",
    "fleet_root_hash", "partition_config_hash", "record_count",
  ]);
  const coverageStart = parseTimestamp(value.coverage_start, "coverage_start");
  if (coverageStart.getTime() > cutoff.getTime()) {
    failRestore("RESTORE_INVALID_REQUEST", "coverage_start must not exceed cutoff.");
  }
  assertHash(value.catalog_close_key, "catalog_close_key");
  assertInteger(value.catalog_generation, "catalog_generation", 1);
  assertHash(value.catalog_snapshot_hash, "catalog_snapshot_hash");
  assertHash(value.fleet_root_hash, "fleet_root_hash");
  assertHash(value.partition_config_hash, "partition_config_hash");
  assertInteger(value.record_count, "record_count");
}

function validateParticipants(value: unknown): asserts value is readonly RestoreParticipantPlanV1[] {
  if (!Array.isArray(value) || value.length === 0) {
    failRestore("RESTORE_INVALID_REQUEST", "participants must be a non-empty array.");
  }
  let previous = "";
  for (const participant of value) {
    assertPlainObject(participant, "restore_participant");
    assertExactKeys(participant, "restore_participant", ["participant_id", "target_bookmark", "preview_bookmark"]);
    assertString(participant.participant_id, "participant_id");
    if (participant.participant_id <= previous) {
      failRestore("RESTORE_INVALID_REQUEST", "participants must be unique and sorted by participant_id.");
    }
    previous = participant.participant_id;
    assertString(participant.target_bookmark, "target_bookmark");
    assertString(participant.preview_bookmark, "preview_bookmark");
  }
}

function validateImpact(
  value: unknown,
  participantCount: number,
  transactionCount: number,
  cutoff: string,
  executeBefore: string,
): asserts value is RestoreImpactV1 {
  assertPlainObject(value, "restore_impact");
  assertExactKeys(value, "restore_impact", [
    "participant_count", "transaction_count", "intentional_loss_from", "intentional_loss_through",
  ]);
  assertInteger(value.participant_count, "participant_count", 1);
  assertInteger(value.transaction_count, "transaction_count");
  parseTimestamp(value.intentional_loss_from, "intentional_loss_from");
  parseTimestamp(value.intentional_loss_through, "intentional_loss_through");
  if (value.participant_count !== participantCount || value.transaction_count !== transactionCount) {
    failRestore("RESTORE_INVALID_REQUEST", "Restore impact counts do not match the immutable resource/evidence sets.");
  }
  if (value.intentional_loss_from !== cutoff || value.intentional_loss_through !== executeBefore) {
    failRestore("RESTORE_INVALID_REQUEST", "Restore impact loss bounds must equal cutoff and execute_before.");
  }
}

function validateRollback(value: unknown): asserts value is RestoreRollbackV1 {
  assertPlainObject(value, "restore_rollback");
  assertExactKeys(value, "restore_rollback", ["undo_supported", "undo_expires_at", "limitations"]);
  if (typeof value.undo_supported !== "boolean") {
    failRestore("RESTORE_INVALID_REQUEST", "undo_supported must be a boolean.");
  }
  if (value.undo_expires_at !== null) parseTimestamp(value.undo_expires_at, "undo_expires_at");
  if (!Array.isArray(value.limitations)) {
    failRestore("RESTORE_INVALID_REQUEST", "limitations must be an array.");
  }
  let previous = "";
  for (const limitation of value.limitations) {
    assertString(limitation, "limitation");
    if (limitation <= previous) {
      failRestore("RESTORE_INVALID_REQUEST", "limitations must be unique and sorted.");
    }
    previous = limitation;
  }
}

export function validateRestorePlanBody(value: unknown): asserts value is RestorePlanBodyV1 {
  assertPlainObject(value, "restore_plan_body");
  assertExactKeys(value, "restore_plan_body", [
    "protocol_version", "format_version", "restore_id", "fleet_id", "cutoff", "previewed_at", "execute_before",
    "parameter_hash", "topology", "manifest", "participants", "impact", "rollback",
  ]);
  assertVersion(value, RESTORE_PLAN_FORMAT_VERSION, "restore_plan_body");
  assertString(value.restore_id, "restore_id");
  assertString(value.fleet_id, "fleet_id");
  const cutoff = parseTimestamp(value.cutoff, "cutoff");
  const previewedAt = parseTimestamp(value.previewed_at, "previewed_at");
  const executeBefore = parseTimestamp(value.execute_before, "execute_before");
  if (cutoff.getTime() > previewedAt.getTime() || previewedAt.getTime() >= executeBefore.getTime()) {
    failRestore("RESTORE_INVALID_REQUEST", "Restore timestamps must satisfy cutoff <= previewed_at < execute_before.");
  }
  assertHash(value.parameter_hash, "parameter_hash");
  validateTopology(value.topology);
  validateManifest(value.manifest, cutoff);
  validateParticipants(value.participants);
  validateImpact(
    value.impact,
    value.participants.length,
    value.manifest.record_count,
    cutoff.toISOString(),
    executeBefore.toISOString(),
  );
  validateRollback(value.rollback);
}

export function validateRestorePlanStructure(value: unknown): asserts value is RestorePlanV1 {
  assertPlainObject(value, "restore_plan");
  assertExactKeys(value, "restore_plan", [
    "protocol_version", "format_version", "restore_id", "fleet_id", "cutoff", "previewed_at", "execute_before",
    "parameter_hash", "topology", "manifest", "participants", "impact", "rollback", "plan_hash",
  ]);
  const { plan_hash: planHash, ...body } = value;
  validateRestorePlanBody(body);
  assertHash(planHash, "plan_hash");
}

export function validateRestorePreviewResult(value: unknown): asserts value is RestorePreviewResultV1 {
  assertPlainObject(value, "restore_preview_result");
  if (value.status === "previewing") {
    assertExactKeys(value, "restore_preview_result", ["ok", "status", "restore_id", "retry_after_ms"]);
    if (value.ok !== true) failRestore("RESTORE_INVALID_REQUEST", "Restore preview result must be successful.");
    assertString(value.restore_id, "restore_id");
    assertInteger(value.retry_after_ms, "retry_after_ms");
    return;
  }
  if (value.status === "previewed") {
    assertExactKeys(value, "restore_preview_result", ["ok", "status", "plan"]);
    if (value.ok !== true) failRestore("RESTORE_INVALID_REQUEST", "Restore preview result must be successful.");
    validateRestorePlanStructure(value.plan);
    return;
  }
  failRestore("RESTORE_INVALID_REQUEST", "Restore preview result status is unsupported.");
}

export function validateRestoreAcceptedResult(value: unknown): asserts value is RestoreAcceptedResultV1 {
  assertPlainObject(value, "restore_accepted_result");
  assertExactKeys(value, "restore_accepted_result", ["ok", "status", "restore_id", "plan_hash"]);
  if (value.ok !== true) failRestore("RESTORE_INVALID_REQUEST", "Restore accepted result must be successful.");
  if (value.status !== "accepted" && value.status !== "already_started") {
    failRestore("RESTORE_INVALID_REQUEST", "Restore accepted result status is unsupported.");
  }
  assertString(value.restore_id, "restore_id");
  assertHash(value.plan_hash, "plan_hash");
}

export async function hashRestorePreviewParameters(value: RestorePreviewRequestV1): Promise<string> {
  validateRestorePreviewRequest(value);
  return hashCanonicalJson({
    protocol_version: value.protocol_version,
    format_version: value.format_version,
    fleet_id: value.fleet_id,
    cutoff: value.cutoff,
  });
}

export async function hashRestorePlanBody(value: RestorePlanBodyV1): Promise<string> {
  validateRestorePlanBody(value);
  return hashCanonicalJson(value);
}

export async function validateRestorePlan(value: unknown): Promise<void> {
  validateRestorePlanStructure(value);
  const { plan_hash: planHash, ...body } = value;
  const expected = await hashRestorePlanBody(body);
  if (planHash !== expected) {
    failRestore("RESTORE_PLAN_HASH_MISMATCH", "plan_hash does not match the exact immutable restore plan body.", {
      expected_plan_hash: expected,
      received_plan_hash: planHash,
    });
  }
}

function validateProgress(value: unknown): asserts value is RestoreProgressV1 {
  assertPlainObject(value, "restore_progress");
  assertExactKeys(value, "restore_progress", [
    "participants_total", "participants_restored", "transactions_total", "transactions_reconciled",
  ]);
  assertInteger(value.participants_total, "participants_total");
  assertInteger(value.participants_restored, "participants_restored");
  assertInteger(value.transactions_total, "transactions_total");
  assertInteger(value.transactions_reconciled, "transactions_reconciled");
  if (value.participants_restored > value.participants_total || value.transactions_reconciled > value.transactions_total) {
    failRestore("RESTORE_INVALID_REQUEST", "Restore progress cannot exceed its totals.");
  }
}

function validateBlocker(value: unknown): asserts value is RestoreBlockerV1 {
  assertPlainObject(value, "restore_blocker");
  assertExactKeys(value, "restore_blocker", ["code", "message", "participant_id", "tx_id"]);
  if (typeof value.code !== "string" || !(RESTORE_ERROR_CODES as readonly string[]).includes(value.code)) {
    failRestore("RESTORE_INVALID_REQUEST", "Restore blocker code is unsupported.");
  }
  assertString(value.message, "message");
  if (value.participant_id !== null) assertString(value.participant_id, "participant_id");
  if (value.tx_id !== null) assertString(value.tx_id, "tx_id");
}

function validateReport(value: unknown): asserts value is RestoreReportV1 {
  assertPlainObject(value, "restore_report");
  assertExactKeys(value, "restore_report", [
    "discarded_write_count", "discarded_write_report_hash", "discarded_write_report_complete",
    "measured_rpo_ms", "measured_rto_ms", "verified_at",
  ]);
  assertInteger(value.discarded_write_count, "discarded_write_count");
  assertHash(value.discarded_write_report_hash, "discarded_write_report_hash");
  if (typeof value.discarded_write_report_complete !== "boolean") {
    failRestore("RESTORE_INVALID_REQUEST", "discarded_write_report_complete must be a boolean.");
  }
  assertInteger(value.measured_rpo_ms, "measured_rpo_ms");
  assertInteger(value.measured_rto_ms, "measured_rto_ms");
  parseTimestamp(value.verified_at, "verified_at");
}

export function validateRestoreStatus(value: unknown): asserts value is RestoreStatusV1 {
  assertPlainObject(value, "restore_status");
  assertExactKeys(value, "restore_status", [
    "protocol_version", "format_version", "restore_id", "plan_hash", "fleet_id", "cutoff", "phase",
    "started_at", "updated_at", "completed_at", "progress", "blockers", "report",
  ]);
  assertVersion(value, RESTORE_STATUS_FORMAT_VERSION, "restore_status");
  assertString(value.restore_id, "restore_id");
  assertString(value.fleet_id, "fleet_id");
  parseTimestamp(value.cutoff, "cutoff");
  if (typeof value.phase !== "string" || !(RESTORE_PHASES as readonly string[]).includes(value.phase)) {
    failRestore("RESTORE_INVALID_REQUEST", "Restore phase is unsupported.");
  }
  if (value.plan_hash === null) {
    if (value.phase !== "previewing") {
      failRestore("RESTORE_INVALID_REQUEST", "plan_hash may be null only while a preview is still being built.");
    }
  } else {
    assertHash(value.plan_hash, "plan_hash");
  }
  if (value.started_at !== null) parseTimestamp(value.started_at, "started_at");
  parseTimestamp(value.updated_at, "updated_at");
  if (value.completed_at !== null) parseTimestamp(value.completed_at, "completed_at");
  validateProgress(value.progress);
  if (!Array.isArray(value.blockers)) failRestore("RESTORE_INVALID_REQUEST", "blockers must be an array.");
  value.blockers.forEach(validateBlocker);
  if (value.report !== null) validateReport(value.report);
  if (value.phase === "complete") {
    if (value.completed_at === null || value.report === null || value.report.discarded_write_report_complete !== true) {
      failRestore("RESTORE_INVALID_REQUEST", "A complete restore requires completion time and complete verified report.");
    }
    if (
      value.progress.participants_restored !== value.progress.participants_total
      || value.progress.transactions_reconciled !== value.progress.transactions_total
      || value.blockers.length !== 0
    ) {
      failRestore("RESTORE_INVALID_REQUEST", "A complete restore requires complete progress and no blockers.");
    }
  }
  if (value.phase === "rolled_back" && (value.completed_at === null || value.blockers.length !== 0)) {
    failRestore("RESTORE_INVALID_REQUEST", "A rolled-back restore requires completion time and no blockers.");
  }
}
