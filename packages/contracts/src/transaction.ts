/**
 * Binding-free transaction protocol shared by the core Worker and the
 * control-plane Worker. Runtime adapters own storage and RPC; this package owns
 * durable/wire formats, legal transitions, hashing, routing, and typed errors.
 */

export const CURRENT_PROTOCOL_VERSION = 1 as const;
export const MIN_READABLE_PROTOCOL_VERSION = 1 as const;
export const TRANSACTION_STATE_MODEL_VERSION = 2 as const;
export const MIN_READABLE_TRANSACTION_STATE_MODEL_VERSION = 1 as const;
export const REDO_ENVELOPE_FORMAT_VERSION = 1 as const;
export const MANIFEST_RECORD_FORMAT_VERSION = 1 as const;
export const MANIFEST_RESERVATION_FORMAT_VERSION = 1 as const;
export const MANIFEST_TERMINAL_INTENT_FORMAT_VERSION = 1 as const;
export const MANIFEST_RECORD_V2_FORMAT_VERSION = 2 as const;
export const MANIFEST_CATALOG_FORMAT_VERSION = 1 as const;
export const MANIFEST_SEAL_FORMAT_VERSION = 1 as const;
export const MANIFEST_PAGE_FORMAT_VERSION = 1 as const;
export const MANIFEST_ENUMERATION_FORMAT_VERSION = 1 as const;
export const MANIFEST_CURSOR_FORMAT_VERSION = 1 as const;
export const PARTICIPANT_TOMBSTONE_FORMAT_VERSION = 1 as const;
export const TRANSACTION_ERROR_SCHEMA_VERSION = 1 as const;

export const MAX_REDO_ENVELOPE_BYTES = 256 * 1024;
/** Caller-supplied row keys are capped separately at the HTTP boundary. A
 * redo envelope groups those rows plus system-generated index intents by
 * physical shard, so its participant ceiling must cover the documented
 * maximum cluster topology. The byte ceiling remains the tighter payload
 * bound for realistic transactions. */
export const MAX_REDO_PARTICIPANTS = 256;
export const MAX_PARTICIPANT_KEYS = 8;
export const MAX_VBUCKET = 65_535;
export const MANIFEST_PARTITION_COUNT = 16 as const;
export const COORDINATOR_RETENTION_DAYS = 35;
export const PARTICIPANT_TOMBSTONE_RETENTION_DAYS = 35;
export const DEFAULT_IDEMPOTENCY_DAYS = 7;
export const MIN_IDEMPOTENCY_DAYS = 1;
export const MAX_IDEMPOTENCY_DAYS = 30;
export const DEFAULT_MANIFEST_PAGE_LIMIT = 100;
export const MAX_MANIFEST_PAGE_LIMIT = 500;

export type JsonPrimitive = string | number | boolean | null;
export type JsonValue = JsonPrimitive | readonly JsonValue[] | { readonly [key: string]: JsonValue };

export const TRANSACTION_STATES = [
  "new",
  "manifest_reserving",
  "preparing",
  "abort_decided",
  "aborting",
  "aborted",
  "aborted_pending_manifest_cancel",
  "prepared",
  "commit_deciding",
  "commit_decided",
  "commit_pending_manifest",
  "manifest_registered",
  "committing",
  "committed_pending_ack",
  "committed",
  "quarantined",
] as const;

export type TransactionState = (typeof TRANSACTION_STATES)[number];
export type TransactionDecision = "undecided" | "abort" | "commit" | "quarantined";
export type TransactionResponseStatus =
  | "commit_pending_manifest"
  | "committed_pending_ack"
  | "committed"
  | "aborted"
  | "quarantined";

const DIRECT_TRANSITIONS: Readonly<Record<TransactionState, readonly TransactionState[]>> = {
  new: ["manifest_reserving", "preparing", "quarantined"],
  manifest_reserving: ["preparing", "abort_decided", "aborted", "aborted_pending_manifest_cancel", "quarantined"],
  preparing: ["abort_decided", "prepared", "quarantined"],
  abort_decided: ["aborting", "quarantined"],
  aborting: ["aborted", "aborted_pending_manifest_cancel", "quarantined"],
  aborted: ["quarantined"],
  aborted_pending_manifest_cancel: ["aborted", "quarantined"],
  prepared: ["abort_decided", "commit_deciding", "commit_decided", "quarantined"],
  commit_deciding: ["commit_pending_manifest", "manifest_registered", "quarantined"],
  commit_decided: ["commit_pending_manifest", "manifest_registered", "quarantined"],
  commit_pending_manifest: ["manifest_registered", "quarantined"],
  manifest_registered: ["committing", "quarantined"],
  committing: ["committed_pending_ack", "committed", "quarantined"],
  committed_pending_ack: ["committed", "quarantined"],
  committed: ["quarantined"],
  // These exits are available only to the audited quarantine-repair path,
  // which carries the canonical terminal intent and operator attestation.
  quarantined: ["manifest_registered", "aborted"],
};

export const TRANSACTION_ERROR_CODES = [
  "TX_INVALID_TRANSITION",
  "TX_COMMIT_ALREADY_DECIDED",
  "TX_ABORT_ALREADY_DECIDED",
  "TX_EPOCH_STALE",
  "TX_EPOCH_CONFLICT",
  "TX_DECISION_CONFLICT",
  "TX_ID_REQUEST_MISMATCH",
  "TX_ENVELOPE_HASH_MISMATCH",
  "TX_ENVELOPE_INVALID",
  "TX_ENVELOPE_TOO_LARGE",
  "TX_MANIFEST_CONFLICT",
  "TX_DECISION_UNAVAILABLE",
  "TX_MANIFEST_UNAVAILABLE",
  "TX_VERSION_UNSUPPORTED",
  "TX_QUARANTINED",
  "TX_ABORTED",
  "MANIFEST_INVALID_REQUEST",
  "MANIFEST_FUTURE_CUTOFF",
  "MANIFEST_NONMONOTONIC_CUTOFF",
  "MANIFEST_SEAL_IN_PROGRESS",
  "MANIFEST_RESERVATION_CONFLICT",
  "MANIFEST_TERMINAL_CONFLICT",
  "MANIFEST_CURSOR_MISMATCH",
  "MANIFEST_COVERAGE_GAP",
  "MANIFEST_VERSION_UNSUPPORTED",
  "MANIFEST_TEMPORARILY_UNAVAILABLE",
  "MANIFEST_QUARANTINED",
  "MANIFEST_RETENTION_EXPIRED",
  "MANIFEST_UNPROVEN_LEGACY_WINDOW",
  "LEGACY_CERTIFICATION_UNAVAILABLE",
  "V1_CLOSED",
] as const;

export type TransactionErrorCode = (typeof TRANSACTION_ERROR_CODES)[number];

const ERROR_HTTP_STATUS: Readonly<Record<TransactionErrorCode, number>> = {
  TX_INVALID_TRANSITION: 409,
  TX_COMMIT_ALREADY_DECIDED: 409,
  TX_ABORT_ALREADY_DECIDED: 409,
  TX_EPOCH_STALE: 409,
  TX_EPOCH_CONFLICT: 409,
  TX_DECISION_CONFLICT: 409,
  TX_ID_REQUEST_MISMATCH: 409,
  TX_ENVELOPE_HASH_MISMATCH: 409,
  TX_ENVELOPE_INVALID: 400,
  TX_ENVELOPE_TOO_LARGE: 413,
  TX_MANIFEST_CONFLICT: 409,
  TX_DECISION_UNAVAILABLE: 503,
  TX_MANIFEST_UNAVAILABLE: 503,
  TX_VERSION_UNSUPPORTED: 503,
  TX_QUARANTINED: 409,
  TX_ABORTED: 409,
  MANIFEST_INVALID_REQUEST: 400,
  MANIFEST_FUTURE_CUTOFF: 409,
  MANIFEST_NONMONOTONIC_CUTOFF: 409,
  MANIFEST_SEAL_IN_PROGRESS: 409,
  MANIFEST_RESERVATION_CONFLICT: 409,
  MANIFEST_TERMINAL_CONFLICT: 409,
  MANIFEST_CURSOR_MISMATCH: 400,
  MANIFEST_COVERAGE_GAP: 409,
  MANIFEST_VERSION_UNSUPPORTED: 503,
  MANIFEST_TEMPORARILY_UNAVAILABLE: 503,
  MANIFEST_QUARANTINED: 409,
  MANIFEST_RETENTION_EXPIRED: 410,
  MANIFEST_UNPROVEN_LEGACY_WINDOW: 409,
  LEGACY_CERTIFICATION_UNAVAILABLE: 503,
  V1_CLOSED: 409,
};

const RETRYABLE_ERRORS: ReadonlySet<TransactionErrorCode> = new Set([
  "TX_DECISION_UNAVAILABLE",
  "TX_MANIFEST_UNAVAILABLE",
  "MANIFEST_SEAL_IN_PROGRESS",
  "MANIFEST_TEMPORARILY_UNAVAILABLE",
  "LEGACY_CERTIFICATION_UNAVAILABLE",
]);

export interface TransactionProtocolError {
  readonly schema_version: typeof TRANSACTION_ERROR_SCHEMA_VERSION;
  readonly code: TransactionErrorCode;
  readonly message: string;
  readonly http_status: number;
  readonly retryable: boolean;
  readonly details?: Readonly<Record<string, JsonValue>>;
}

export class TransactionContractViolation extends Error {
  readonly protocolError: TransactionProtocolError;

  constructor(protocolError: TransactionProtocolError) {
    super(protocolError.message);
    this.name = "TransactionContractViolation";
    this.protocolError = protocolError;
  }
}

export function transactionError(
  code: TransactionErrorCode,
  message: string,
  details?: Readonly<Record<string, JsonValue>>,
): TransactionProtocolError {
  return {
    schema_version: TRANSACTION_ERROR_SCHEMA_VERSION,
    code,
    message,
    http_status: ERROR_HTTP_STATUS[code],
    retryable: RETRYABLE_ERRORS.has(code),
    ...(details === undefined ? {} : { details }),
  };
}

function fail(
  code: TransactionErrorCode,
  message: string,
  details?: Readonly<Record<string, JsonValue>>,
): never {
  throw new TransactionContractViolation(transactionError(code, message, details));
}

export function isTransactionState(value: unknown): value is TransactionState {
  return typeof value === "string" && (TRANSACTION_STATES as readonly string[]).includes(value);
}

export function isCommitDecidedOrLater(state: TransactionState): boolean {
  return (
    state === "commit_decided" ||
    state === "commit_deciding" ||
    state === "commit_pending_manifest" ||
    state === "manifest_registered" ||
    state === "committing" ||
    state === "committed_pending_ack" ||
    state === "committed"
  );
}

export function isAbortDecidedOrLater(state: TransactionState): boolean {
  return state === "abort_decided" || state === "aborting" || state === "aborted_pending_manifest_cancel" || state === "aborted";
}

export function decisionForState(state: TransactionState): TransactionDecision {
  if (isCommitDecidedOrLater(state)) return "commit";
  if (isAbortDecidedOrLater(state)) return "abort";
  if (state === "quarantined") return "quarantined";
  return "undecided";
}

/** Same-state replay is an idempotent no-op, not a new durable transition. */
export function isTransactionTransitionAllowed(from: TransactionState, to: TransactionState): boolean {
  return from === to || DIRECT_TRANSITIONS[from].includes(to);
}

export function assertTransactionTransition(from: TransactionState, to: TransactionState): void {
  if (isTransactionTransitionAllowed(from, to)) return;

  if (isCommitDecidedOrLater(from) && isAbortDecidedOrLater(to)) {
    fail(
      "TX_COMMIT_ALREADY_DECIDED",
      `Transaction cannot transition from ${from} to ${to} after commit was decided.`,
      { from, to },
    );
  }
  if (isAbortDecidedOrLater(from) && isCommitDecidedOrLater(to)) {
    fail(
      "TX_ABORT_ALREADY_DECIDED",
      `Transaction cannot transition from ${from} to ${to} after abort was decided.`,
      { from, to },
    );
  }
  fail("TX_INVALID_TRANSITION", `Illegal transaction transition from ${from} to ${to}.`, { from, to });
}

export function isReadableProtocolVersion(version: unknown): version is number {
  return (
    typeof version === "number" &&
    Number.isSafeInteger(version) &&
    version >= MIN_READABLE_PROTOCOL_VERSION &&
    version <= CURRENT_PROTOCOL_VERSION
  );
}

export function assertReadableProtocolVersion(version: unknown): asserts version is number {
  if (!isReadableProtocolVersion(version)) {
    fail(
      "TX_VERSION_UNSUPPORTED",
      `Protocol version ${String(version)} is not readable; supported versions are ${MIN_READABLE_PROTOCOL_VERSION}-${CURRENT_PROTOCOL_VERSION}.`,
      { received_version: String(version), min_readable_version: MIN_READABLE_PROTOCOL_VERSION, current_version: CURRENT_PROTOCOL_VERSION },
    );
  }
}

export function assertWritableProtocolVersion(version: unknown): asserts version is typeof CURRENT_PROTOCOL_VERSION {
  if (version !== CURRENT_PROTOCOL_VERSION) {
    fail(
      "TX_VERSION_UNSUPPORTED",
      `Protocol version ${String(version)} is read-only or unsupported; new writes require version ${CURRENT_PROTOCOL_VERSION}.`,
      { received_version: String(version), current_version: CURRENT_PROTOCOL_VERSION },
    );
  }
}

export function validateIdempotencyDays(days: unknown): number {
  if (typeof days !== "number" || !Number.isSafeInteger(days) || days < MIN_IDEMPOTENCY_DAYS || days > MAX_IDEMPOTENCY_DAYS) {
    fail(
      "TX_ENVELOPE_INVALID",
      `Idempotency retention must be an integer from ${MIN_IDEMPOTENCY_DAYS} through ${MAX_IDEMPOTENCY_DAYS} days.`,
      { received_days: String(days), minimum_days: MIN_IDEMPOTENCY_DAYS, maximum_days: MAX_IDEMPOTENCY_DAYS },
    );
  }
  return days;
}

export type StructuredMutationKind = "insert" | "update" | "delete" | "upsert";
export const STRUCTURED_MUTATION_KINDS = ["insert", "update", "delete", "upsert"] as const;

export interface RedoIntentV1 {
  readonly intent_seq: number;
  readonly sql: string;
  readonly params: readonly JsonValue[];
  readonly tenant_id: string;
  readonly table_name: string;
  readonly partition_key: string;
  readonly vbucket: number | null;
  readonly operation: StructuredMutationKind | null;
  readonly mirror_target_participant_id: string | null;
}

export interface RedoParticipantV1 {
  readonly participant_id: string;
  readonly epoch: number;
  readonly intents: readonly RedoIntentV1[];
}

export interface RedoEnvelopeV1 {
  readonly protocol_version: typeof CURRENT_PROTOCOL_VERSION;
  readonly format_version: typeof REDO_ENVELOPE_FORMAT_VERSION;
  readonly tx_id: string;
  readonly fleet_id: string;
  readonly coordinator_id: string;
  readonly decision: "commit";
  readonly decision_epoch: number;
  readonly commit_decided_at: string;
  readonly retention_deadline: string;
  readonly operation_hash: string;
  readonly participants: readonly RedoParticipantV1[];
}

export interface ManifestRecordV1 {
  readonly protocol_version: typeof CURRENT_PROTOCOL_VERSION;
  readonly format_version: typeof MANIFEST_RECORD_FORMAT_VERSION;
  readonly fleet_id: string;
  readonly utc_day: string;
  readonly partition: number;
  readonly partition_count: typeof MANIFEST_PARTITION_COUNT;
  readonly routing_key: string;
  readonly tx_id: string;
  readonly coordinator_id: string;
  readonly commit_decided_at: string;
  readonly decision_epoch: number;
  readonly envelope_hash: string;
  readonly retention_deadline: string;
}

export interface ManifestRegistrationV1 {
  readonly record: ManifestRecordV1;
  readonly record_hash: string;
}

export type ParticipantDecision = "abort" | "commit";
export type ParticipantPhase = "prepare" | "commit" | "abort" | "status" | "recover";
export const PARTICIPANT_DECISIONS = ["abort", "commit"] as const;
export const PARTICIPANT_PHASES = ["prepare", "commit", "abort", "status", "recover"] as const;

export interface ParticipantDecisionTombstoneV1 {
  readonly protocol_version: typeof CURRENT_PROTOCOL_VERSION;
  readonly format_version: typeof PARTICIPANT_TOMBSTONE_FORMAT_VERSION;
  readonly tx_id: string;
  readonly highest_epoch: number;
  readonly decision: ParticipantDecision;
  readonly operation_hash: string;
  readonly decided_at: string;
  readonly retention_deadline: string;
}

export interface ParticipantPhaseMessageV1 {
  readonly protocol_version: typeof CURRENT_PROTOCOL_VERSION;
  readonly tx_id: string;
  readonly epoch: number;
  readonly phase: ParticipantPhase;
  readonly operation_hash: string;
}

function assertNonEmptyString(value: unknown, field: string): asserts value is string {
  if (typeof value !== "string" || value.length === 0) {
    fail("TX_ENVELOPE_INVALID", `${field} must be a non-empty string.`, { field });
  }
}

function assertPlainObject(value: unknown, field: string): asserts value is Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    fail("TX_ENVELOPE_INVALID", `${field} must be a plain object.`, { field });
  }
  const prototype = Object.getPrototypeOf(value);
  if (prototype !== Object.prototype && prototype !== null) {
    fail("TX_ENVELOPE_INVALID", `${field} must be a plain object.`, { field });
  }
}

function assertNullableNonEmptyString(value: unknown, field: string): asserts value is string | null {
  if (value === null) return;
  assertNonEmptyString(value, field);
}

function assertVbucket(value: unknown): asserts value is number | null {
  if (value === null) return;
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0 || value > MAX_VBUCKET) {
    fail("TX_ENVELOPE_INVALID", `vbucket must be null or an integer from 0 through ${MAX_VBUCKET}.`);
  }
}

function assertStructuredMutationKind(value: unknown): asserts value is StructuredMutationKind | null {
  if (value === null) return;
  if (typeof value !== "string" || !(STRUCTURED_MUTATION_KINDS as readonly string[]).includes(value)) {
    fail("TX_ENVELOPE_INVALID", "operation must be null or an exact structured mutation kind.");
  }
}

function assertParticipantDecision(value: unknown): asserts value is ParticipantDecision {
  if (typeof value !== "string" || !(PARTICIPANT_DECISIONS as readonly string[]).includes(value)) {
    fail("TX_ENVELOPE_INVALID", "decision must be an exact participant decision.");
  }
}

function assertParticipantPhase(value: unknown): asserts value is ParticipantPhase {
  if (typeof value !== "string" || !(PARTICIPANT_PHASES as readonly string[]).includes(value)) {
    fail("TX_ENVELOPE_INVALID", "phase must be an exact participant phase.");
  }
}

function assertEpoch(value: unknown, field = "epoch"): asserts value is number {
  if (!Number.isSafeInteger(value) || (value as number) < 1) {
    fail("TX_ENVELOPE_INVALID", `${field} must be a positive safe integer.`, { field });
  }
}

function assertSha256Hex(value: unknown, field: string): asserts value is string {
  if (typeof value !== "string" || !/^[a-f0-9]{64}$/.test(value)) {
    fail("TX_ENVELOPE_INVALID", `${field} must be a lowercase SHA-256 hexadecimal digest.`, { field });
  }
}

function parseCanonicalUtcTimestamp(value: unknown, field: string): Date {
  assertNonEmptyString(value, field);
  const parsed = new Date(value);
  if (!Number.isFinite(parsed.getTime()) || parsed.toISOString() !== value) {
    fail("TX_ENVELOPE_INVALID", `${field} must be a canonical UTC timestamp with millisecond precision.`, { field });
  }
  return parsed;
}

function assertRetentionWindow(decidedAt: unknown, deadline: unknown, minimumDays: number): void {
  const decided = parseCanonicalUtcTimestamp(decidedAt, "decided_at");
  const retention = parseCanonicalUtcTimestamp(deadline, "retention_deadline");
  const minimumMs = minimumDays * 24 * 60 * 60 * 1000;
  if (retention.getTime() - decided.getTime() < minimumMs) {
    fail(
      "TX_ENVELOPE_INVALID",
      `retention_deadline must be at least ${minimumDays} days after the decision.`,
      { minimum_days: minimumDays },
    );
  }
}

/** RFC 8785-style properties needed here: sorted object keys and stable JSON values. */
export function canonicalJson(value: unknown): string {
  if (value === null) return "null";
  if (typeof value === "string" || typeof value === "boolean") return JSON.stringify(value);
  if (typeof value === "number") {
    if (!Number.isFinite(value)) fail("TX_ENVELOPE_INVALID", "Canonical JSON cannot contain a non-finite number.");
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) return `[${value.map((entry) => canonicalJson(entry)).join(",")}]`;
  if (typeof value === "object") {
    const prototype = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null) {
      fail("TX_ENVELOPE_INVALID", "Canonical JSON accepts only plain objects, arrays, and JSON primitives.");
    }
    const record = value as Record<string, unknown>;
    return `{${Object.keys(record)
      .sort()
      .map((key) => `${JSON.stringify(key)}:${canonicalJson(record[key])}`)
      .join(",")}}`;
  }
  fail("TX_ENVELOPE_INVALID", `Canonical JSON cannot encode ${typeof value}.`);
}

export function canonicalByteLength(value: unknown): number {
  return new TextEncoder().encode(canonicalJson(value)).byteLength;
}

async function sha256Bytes(value: string): Promise<Uint8Array> {
  return new Uint8Array(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(value)));
}

export async function sha256Hex(value: string): Promise<string> {
  const digest = await sha256Bytes(value);
  return Array.from(digest, (byte) => byte.toString(16).padStart(2, "0")).join("");
}

export async function hashCanonicalJson(value: unknown): Promise<string> {
  return sha256Hex(canonicalJson(value));
}

export async function hashRedoEnvelope(envelope: RedoEnvelopeV1): Promise<string> {
  return hashCanonicalJson(envelope);
}

export async function hashParticipantOperations(participants: readonly RedoParticipantV1[]): Promise<string> {
  return hashCanonicalJson(participants);
}

export function validateRedoEnvelopeStructure(value: unknown): asserts value is RedoEnvelopeV1 {
  assertPlainObject(value, "redo_envelope");
  assertWritableProtocolVersion(value.protocol_version);
  if (value.format_version !== REDO_ENVELOPE_FORMAT_VERSION) {
    fail("TX_VERSION_UNSUPPORTED", `Redo envelope format ${String(value.format_version)} is unsupported.`);
  }
  assertNonEmptyString(value.tx_id, "tx_id");
  assertNonEmptyString(value.fleet_id, "fleet_id");
  assertNonEmptyString(value.coordinator_id, "coordinator_id");
  if (value.decision !== "commit") fail("TX_ENVELOPE_INVALID", "A redo envelope must contain the immutable commit decision.");
  assertEpoch(value.decision_epoch, "decision_epoch");
  assertSha256Hex(value.operation_hash, "operation_hash");
  assertRetentionWindow(value.commit_decided_at, value.retention_deadline, COORDINATOR_RETENTION_DAYS);

  if (!Array.isArray(value.participants)) fail("TX_ENVELOPE_INVALID", "participants must be an array.");
  if (value.participants.length < 1 || value.participants.length > MAX_REDO_PARTICIPANTS) {
    fail(
      "TX_ENVELOPE_INVALID",
      `Redo envelope must contain 1-${MAX_REDO_PARTICIPANTS} physical participants.`,
      { participant_count: value.participants.length, maximum_participants: MAX_REDO_PARTICIPANTS },
    );
  }

  let priorParticipant = "";
  for (const participant of value.participants) {
    assertPlainObject(participant, "participant");
    assertNonEmptyString(participant.participant_id, "participant_id");
    if (participant.participant_id <= priorParticipant) {
      fail("TX_ENVELOPE_INVALID", "Participants must be unique and sorted by participant_id.");
    }
    priorParticipant = participant.participant_id;
    assertEpoch(participant.epoch, "participant_epoch");
    if (participant.epoch !== value.decision_epoch) {
      fail("TX_EPOCH_CONFLICT", "Every participant epoch must equal the envelope decision_epoch.");
    }
    if (!Array.isArray(participant.intents) || participant.intents.length === 0) {
      fail("TX_ENVELOPE_INVALID", "Every participant must contain a non-empty intents array.");
    }
    participant.intents.forEach((intent, index) => {
      assertPlainObject(intent, "intent");
      if (intent.intent_seq !== index) {
        fail("TX_ENVELOPE_INVALID", "Redo intents must use contiguous zero-based intent_seq values.");
      }
      assertNonEmptyString(intent.sql, "sql");
      assertNonEmptyString(intent.tenant_id, "tenant_id");
      assertNonEmptyString(intent.table_name, "table_name");
      assertNonEmptyString(intent.partition_key, "partition_key");
      if (!Array.isArray(intent.params)) fail("TX_ENVELOPE_INVALID", "params must be an array of JSON values.");
      canonicalJson(intent.params);
      assertVbucket(intent.vbucket);
      assertStructuredMutationKind(intent.operation);
      assertNullableNonEmptyString(intent.mirror_target_participant_id, "mirror_target_participant_id");
    });
  }

  const bytes = canonicalByteLength(value);
  if (bytes > MAX_REDO_ENVELOPE_BYTES) {
    fail(
      "TX_ENVELOPE_TOO_LARGE",
      `Serialized redo envelope is ${bytes} bytes; the qualification ceiling is ${MAX_REDO_ENVELOPE_BYTES} bytes.`,
      { actual_bytes: bytes, maximum_bytes: MAX_REDO_ENVELOPE_BYTES },
    );
  }
}

export async function validateRedoEnvelope(value: unknown): Promise<void> {
  validateRedoEnvelopeStructure(value);
  const expectedOperationHash = await hashParticipantOperations(value.participants);
  if (value.operation_hash !== expectedOperationHash) {
    fail("TX_ENVELOPE_HASH_MISMATCH", "operation_hash does not match the canonical participant operations.");
  }
}

export async function manifestRoute(
  txId: string,
  commitDecidedAt: string,
): Promise<{ utc_day: string; partition: number; partition_count: typeof MANIFEST_PARTITION_COUNT; routing_key: string }> {
  assertNonEmptyString(txId, "tx_id");
  const timestamp = parseCanonicalUtcTimestamp(commitDecidedAt, "commit_decided_at");
  const utcDay = timestamp.toISOString().slice(0, 10);
  const digest = await sha256Bytes(txId);
  const partition = digest[digest.length - 1] % MANIFEST_PARTITION_COUNT;
  return {
    utc_day: utcDay,
    partition,
    partition_count: MANIFEST_PARTITION_COUNT,
    routing_key: `${utcDay}:${partition.toString().padStart(2, "0")}`,
  };
}

export async function createManifestRegistration(envelope: RedoEnvelopeV1): Promise<ManifestRegistrationV1> {
  await validateRedoEnvelope(envelope);
  const route = await manifestRoute(envelope.tx_id, envelope.commit_decided_at);
  const record: ManifestRecordV1 = {
    protocol_version: CURRENT_PROTOCOL_VERSION,
    format_version: MANIFEST_RECORD_FORMAT_VERSION,
    fleet_id: envelope.fleet_id,
    ...route,
    tx_id: envelope.tx_id,
    coordinator_id: envelope.coordinator_id,
    commit_decided_at: envelope.commit_decided_at,
    decision_epoch: envelope.decision_epoch,
    envelope_hash: await hashRedoEnvelope(envelope),
    retention_deadline: envelope.retention_deadline,
  };
  return { record, record_hash: await hashCanonicalJson(record) };
}

export async function validateManifestRegistration(
  registration: unknown,
  envelope?: unknown,
): Promise<void> {
  assertPlainObject(registration, "manifest_registration");
  assertPlainObject(registration.record, "manifest_record");
  const record = registration.record;

  assertWritableProtocolVersion(record.protocol_version);
  if (record.format_version !== MANIFEST_RECORD_FORMAT_VERSION) {
    fail("TX_VERSION_UNSUPPORTED", `Manifest record format ${String(record.format_version)} is unsupported.`);
  }
  assertNonEmptyString(record.fleet_id, "fleet_id");
  assertNonEmptyString(record.utc_day, "utc_day");
  if (typeof record.partition !== "number" || !Number.isSafeInteger(record.partition)) {
    fail("TX_ENVELOPE_INVALID", "partition must be a safe integer.");
  }
  if (record.partition_count !== MANIFEST_PARTITION_COUNT) {
    fail("TX_MANIFEST_CONFLICT", `partition_count must equal ${MANIFEST_PARTITION_COUNT}.`);
  }
  assertNonEmptyString(record.routing_key, "routing_key");
  assertNonEmptyString(record.tx_id, "tx_id");
  assertNonEmptyString(record.coordinator_id, "coordinator_id");
  assertNonEmptyString(record.commit_decided_at, "commit_decided_at");
  assertNonEmptyString(record.retention_deadline, "retention_deadline");
  assertSha256Hex(record.envelope_hash, "envelope_hash");
  assertSha256Hex(registration.record_hash, "record_hash");
  assertEpoch(record.decision_epoch, "decision_epoch");
  assertRetentionWindow(
    record.commit_decided_at,
    record.retention_deadline,
    COORDINATOR_RETENTION_DAYS,
  );
  const expectedRoute = await manifestRoute(record.tx_id, record.commit_decided_at);
  if (
    record.utc_day !== expectedRoute.utc_day ||
    record.partition !== expectedRoute.partition ||
    record.partition_count !== expectedRoute.partition_count ||
    record.routing_key !== expectedRoute.routing_key
  ) {
    fail("TX_MANIFEST_CONFLICT", "Manifest record does not match deterministic UTC-day/partition routing.");
  }
  const expectedRecordHash = await hashCanonicalJson(record);
  if (registration.record_hash !== expectedRecordHash) {
    fail("TX_MANIFEST_CONFLICT", "Manifest record_hash does not match the immutable record content.");
  }
  if (envelope !== undefined) {
    validateRedoEnvelopeStructure(envelope);
    await validateRedoEnvelope(envelope);
    const expectedEnvelopeHash = await hashRedoEnvelope(envelope);
    if (
      record.tx_id !== envelope.tx_id ||
      record.fleet_id !== envelope.fleet_id ||
      record.coordinator_id !== envelope.coordinator_id ||
      record.commit_decided_at !== envelope.commit_decided_at ||
      record.decision_epoch !== envelope.decision_epoch ||
      record.retention_deadline !== envelope.retention_deadline ||
      record.envelope_hash !== expectedEnvelopeHash
    ) {
      fail("TX_MANIFEST_CONFLICT", "Manifest record does not identify the supplied redo envelope.");
    }
  }
}

export function validateParticipantPhaseMessage(
  message: unknown,
): asserts message is ParticipantPhaseMessageV1 {
  assertPlainObject(message, "participant_phase_message");
  assertWritableProtocolVersion(message.protocol_version);
  assertNonEmptyString(message.tx_id, "tx_id");
  assertEpoch(message.epoch);
  assertParticipantPhase(message.phase);
  assertSha256Hex(message.operation_hash, "operation_hash");
}

export function validateParticipantMessageAgainstTombstone(
  message: unknown,
  tombstone: unknown,
): void {
  validateParticipantPhaseMessage(message);
  validateParticipantTombstone(tombstone);
  if (message.tx_id !== tombstone.tx_id) fail("TX_ID_REQUEST_MISMATCH", "Participant message and tombstone tx_id differ.");
  if (message.epoch < tombstone.highest_epoch) {
    fail("TX_EPOCH_STALE", "Participant message epoch is older than the durable decision tombstone.", {
      message_epoch: message.epoch,
      highest_epoch: tombstone.highest_epoch,
    });
  }
  if (message.epoch > tombstone.highest_epoch) {
    fail("TX_EPOCH_CONFLICT", "A later epoch cannot replace an existing durable decision tombstone.", {
      message_epoch: message.epoch,
      highest_epoch: tombstone.highest_epoch,
    });
  }
  if (message.operation_hash !== tombstone.operation_hash) {
    fail("TX_ENVELOPE_HASH_MISMATCH", "Participant message operation_hash conflicts with the durable tombstone.");
  }

  const contradictsCommit = tombstone.decision === "commit" && (message.phase === "prepare" || message.phase === "abort");
  const contradictsAbort = tombstone.decision === "abort" && (message.phase === "prepare" || message.phase === "commit");
  if (contradictsCommit || contradictsAbort) {
    fail("TX_DECISION_CONFLICT", `Participant phase ${message.phase} contradicts durable ${tombstone.decision}.`);
  }
}

export function validateParticipantTombstone(
  tombstone: unknown,
): asserts tombstone is ParticipantDecisionTombstoneV1 {
  assertPlainObject(tombstone, "participant_tombstone");
  assertReadableProtocolVersion(tombstone.protocol_version);
  if (tombstone.format_version !== PARTICIPANT_TOMBSTONE_FORMAT_VERSION) {
    fail("TX_VERSION_UNSUPPORTED", `Participant tombstone format ${String(tombstone.format_version)} is unsupported.`);
  }
  assertNonEmptyString(tombstone.tx_id, "tx_id");
  assertEpoch(tombstone.highest_epoch, "highest_epoch");
  assertParticipantDecision(tombstone.decision);
  assertSha256Hex(tombstone.operation_hash, "operation_hash");
  assertRetentionWindow(tombstone.decided_at, tombstone.retention_deadline, PARTICIPANT_TOMBSTONE_RETENTION_DAYS);
}

// ---------------------------------------------------------------------------
// Manifest V2 reservation, sealing, and enumeration contracts
// ---------------------------------------------------------------------------

export interface ManifestRouteV2 {
  readonly reservation_utc_day: string;
  readonly partition: number;
  readonly partition_count: typeof MANIFEST_PARTITION_COUNT;
  readonly routing_key: string;
  readonly partition_config_hash: string;
}

export interface ManifestReservationV1 extends ManifestRouteV2 {
  readonly protocol_version: typeof CURRENT_PROTOCOL_VERSION;
  readonly format_version: typeof MANIFEST_RESERVATION_FORMAT_VERSION;
  readonly fleet_id: string;
  readonly tx_id: string;
  readonly coordinator_id: string;
  readonly operation_hash: string;
  readonly decision_epoch: number;
  readonly reserved_at: string;
}

export interface CreateManifestReservationInput {
  readonly fleet_id: string;
  readonly tx_id: string;
  readonly coordinator_id: string;
  readonly operation_hash: string;
  readonly decision_epoch: number;
  readonly partition_config_hash: string;
}

export interface ManifestFinalizeIntentV1 {
  readonly protocol_version: typeof CURRENT_PROTOCOL_VERSION;
  readonly format_version: typeof MANIFEST_TERMINAL_INTENT_FORMAT_VERSION;
  readonly tx_id: string;
  readonly reservation_hash: string;
  readonly redo_envelope_hash: string;
  readonly operation_hash: string;
  readonly decision_epoch: number;
  readonly idempotency_key: string;
}

export interface ManifestCancelIntentV1 {
  readonly protocol_version: typeof CURRENT_PROTOCOL_VERSION;
  readonly format_version: typeof MANIFEST_TERMINAL_INTENT_FORMAT_VERSION;
  readonly tx_id: string;
  readonly reservation_hash: string;
  readonly operation_hash: string;
  readonly decision_epoch: number;
  readonly idempotency_key: string;
}

export interface ManifestRecordV2 extends ManifestRouteV2 {
  readonly protocol_version: typeof CURRENT_PROTOCOL_VERSION;
  readonly format_version: typeof MANIFEST_RECORD_V2_FORMAT_VERSION;
  readonly fleet_id: string;
  readonly tx_id: string;
  readonly coordinator_id: string;
  readonly operation_hash: string;
  readonly decision_epoch: number;
  readonly reserved_at: string;
  readonly reservation_hash: string;
  readonly envelope_hash: string;
  readonly commit_decided_at: string;
  readonly commit_decided_at_ms: number;
  readonly decision_sequence: number;
  readonly retention_deadline: string;
}

export interface PartitionConfigV1 {
  readonly protocol_version: typeof CURRENT_PROTOCOL_VERSION;
  readonly format_version: typeof MANIFEST_CATALOG_FORMAT_VERSION;
  readonly effective_from_day: string;
  readonly partition_count: typeof MANIFEST_PARTITION_COUNT;
  readonly prior_config_hash: string | null;
  readonly config_hash: string;
}

export interface ManifestCatalogActivateRequestV1 extends ManifestRouteV2 {
  readonly protocol_version: typeof CURRENT_PROTOCOL_VERSION;
  readonly format_version: typeof MANIFEST_CATALOG_FORMAT_VERSION;
  readonly fleet_id: string;
  readonly activation_key: string;
}

export interface ManifestCatalogSnapshotRequestV1 {
  readonly protocol_version: typeof CURRENT_PROTOCOL_VERSION;
  readonly format_version: typeof MANIFEST_CATALOG_FORMAT_VERSION;
  readonly fleet_id: string;
  readonly cutoff: string;
  readonly partition_config_hash: string;
  readonly idempotency_key: string;
}

export interface ManifestCatalogRetireRequestV1 extends ManifestRouteV2 {
  readonly protocol_version: typeof CURRENT_PROTOCOL_VERSION;
  readonly format_version: typeof MANIFEST_CATALOG_FORMAT_VERSION;
  readonly fleet_id: string;
  readonly safety_certificate_hash: string;
  readonly idempotency_key: string;
}

export interface ManifestSealRequestV1 extends ManifestRouteV2 {
  readonly protocol_version: typeof CURRENT_PROTOCOL_VERSION;
  readonly format_version: typeof MANIFEST_SEAL_FORMAT_VERSION;
  readonly fleet_id: string;
  readonly cutoff: string;
  readonly idempotency_key: string;
}

export interface ManifestSealReceiptV1 extends ManifestRouteV2 {
  readonly protocol_version: typeof CURRENT_PROTOCOL_VERSION;
  readonly format_version: typeof MANIFEST_SEAL_FORMAT_VERSION;
  readonly fleet_id: string;
  readonly cutoff: string;
  readonly generation: number;
  readonly decision_floor_ms: number;
  readonly sealed_through_ms: number;
  readonly record_count: number;
  readonly maximum_decision_sequence: number | null;
  readonly records_deleted_through_ms: number | null;
  readonly retention_epoch: number;
  readonly records_root: string;
  readonly conflict_resolution_root: string;
  readonly local_legacy_certificate_hash: string;
  readonly prior_receipt_hash: string | null;
  readonly receipt_hash: string;
}

export interface ManifestLocalPageCursorV1 {
  readonly protocol_version: typeof CURRENT_PROTOCOL_VERSION;
  readonly format_version: typeof MANIFEST_CURSOR_FORMAT_VERSION;
  readonly request_hash: string;
  readonly retention_epoch: number;
  readonly seal_generation: number;
  readonly seal_receipt_hash: string;
  readonly last_commit_decided_at_ms: number;
  readonly last_decision_sequence: number;
  readonly last_tx_id: string;
}

export interface ManifestLocalPageRequestV1 extends ManifestRouteV2 {
  readonly protocol_version: typeof CURRENT_PROTOCOL_VERSION;
  readonly format_version: typeof MANIFEST_PAGE_FORMAT_VERSION;
  readonly fleet_id: string;
  readonly coverage_start: string;
  readonly cutoff: string;
  readonly expected_retention_epoch: number;
  readonly seal_generation: number;
  readonly seal_receipt_hash: string;
  readonly limit: number;
  readonly cursor: ManifestLocalPageCursorV1 | null;
}

export interface ManifestEnumerationCursorV1 {
  readonly protocol_version: typeof CURRENT_PROTOCOL_VERSION;
  readonly format_version: typeof MANIFEST_CURSOR_FORMAT_VERSION;
  readonly request_hash: string;
  readonly catalog_generation: number;
  readonly catalog_snapshot_hash: string;
  readonly reservation_utc_day: string;
  readonly partition: number;
  readonly local_cursor: ManifestLocalPageCursorV1 | null;
}

export interface ManifestEnumerationRequestV1 {
  readonly protocol_version: typeof CURRENT_PROTOCOL_VERSION;
  readonly format_version: typeof MANIFEST_ENUMERATION_FORMAT_VERSION;
  readonly fleet_id: string;
  readonly coverage_start: string;
  readonly cutoff: string;
  readonly partition_config_hash: string;
  readonly catalog_generation: number;
  readonly catalog_snapshot_hash: string;
  readonly limit: number;
  readonly cursor: ManifestEnumerationCursorV1 | null;
}

export interface ManifestCoverageEvidenceV1 extends ManifestRouteV2 {
  readonly protocol_version: typeof CURRENT_PROTOCOL_VERSION;
  readonly format_version: typeof MANIFEST_ENUMERATION_FORMAT_VERSION;
  readonly cutoff: string;
  readonly seal_generation: number;
  readonly seal_receipt_hash: string;
  readonly retention_epoch: number;
  readonly records_deleted_through_ms: number | null;
  readonly local_legacy_certificate_hash: string;
}

export const MANIFEST_COVERAGE_STATUSES = [
  "complete",
  "incomplete",
  "quarantined",
  "unproven_legacy_window",
  "retention_expired",
] as const;
export type ManifestCoverageStatus = (typeof MANIFEST_COVERAGE_STATUSES)[number];

export interface ManifestEnumerationDiagnosticsV1 {
  readonly inspected_buckets: number;
  readonly incomplete_buckets: number;
  readonly returned_records: number;
}

export interface ManifestEnumerationResultV1 {
  readonly protocol_version: typeof CURRENT_PROTOCOL_VERSION;
  readonly format_version: typeof MANIFEST_ENUMERATION_FORMAT_VERSION;
  readonly request_hash: string;
  readonly coverage: ManifestCoverageStatus;
  readonly complete: boolean;
  readonly records: readonly ManifestRecordV2[];
  readonly evidence: readonly ManifestCoverageEvidenceV1[];
  readonly next_cursor: ManifestEnumerationCursorV1 | null;
  readonly diagnostics: ManifestEnumerationDiagnosticsV1;
}

export type ManifestMemberCandidate = Readonly<{
  state: string;
  commit_decided_at_ms: number | null;
}>;

/** The only V2 PITR membership predicate used by sealing, counting, and paging. */
export function manifestMemberAt(candidate: ManifestMemberCandidate, cutoffMs: number): boolean {
  return (
    Number.isSafeInteger(cutoffMs) &&
    candidate.state === "FINALIZED" &&
    candidate.commit_decided_at_ms !== null &&
    Number.isSafeInteger(candidate.commit_decided_at_ms) &&
    candidate.commit_decided_at_ms <= cutoffMs
  );
}

function assertExactKeys(value: Record<string, unknown>, field: string, keys: readonly string[]): void {
  const allowed = new Set(keys);
  const unknown = Object.keys(value).filter((key) => !allowed.has(key));
  if (unknown.length > 0) {
    fail("MANIFEST_INVALID_REQUEST", `${field} contains unknown fields.`, { field, unknown_fields: unknown });
  }
  const missing = keys.filter((key) => !(key in value));
  if (missing.length > 0) {
    fail("MANIFEST_INVALID_REQUEST", `${field} is missing required fields.`, { field, missing_fields: missing });
  }
}

function assertManifestVersion(value: Record<string, unknown>, expectedFormat: number, field: string): void {
  if (value.protocol_version !== CURRENT_PROTOCOL_VERSION || value.format_version !== expectedFormat) {
    fail("MANIFEST_VERSION_UNSUPPORTED", `${field} uses an unsupported protocol or format version.`, {
      protocol_version: String(value.protocol_version),
      format_version: String(value.format_version),
    });
  }
}

function assertSafeInteger(value: unknown, field: string, minimum = 0): asserts value is number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < minimum) {
    fail("MANIFEST_INVALID_REQUEST", `${field} must be a safe integer greater than or equal to ${minimum}.`, { field });
  }
}

function assertManifestString(value: unknown, field: string): asserts value is string {
  if (typeof value !== "string" || value.length === 0) {
    fail("MANIFEST_INVALID_REQUEST", `${field} must be a non-empty string.`, { field });
  }
}

function assertManifestHash(value: unknown, field: string): asserts value is string {
  if (typeof value !== "string" || !/^[a-f0-9]{64}$/.test(value)) {
    fail("MANIFEST_INVALID_REQUEST", `${field} must be a lowercase SHA-256 hexadecimal digest.`, { field });
  }
}

function parseManifestTimestamp(value: unknown, field: string): Date {
  assertManifestString(value, field);
  const timestamp = new Date(value);
  if (!Number.isFinite(timestamp.getTime()) || timestamp.toISOString() !== value) {
    fail("MANIFEST_INVALID_REQUEST", `${field} must be a canonical UTC timestamp with millisecond precision.`, { field });
  }
  return timestamp;
}

function assertUtcDay(value: unknown, field: string): asserts value is string {
  assertManifestString(value, field);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value) || new Date(`${value}T00:00:00.000Z`).toISOString().slice(0, 10) !== value) {
    fail("MANIFEST_INVALID_REQUEST", `${field} must be a canonical UTC calendar day.`, { field });
  }
}

function assertNullableManifestHash(value: unknown, field: string): asserts value is string | null {
  if (value !== null) assertManifestHash(value, field);
}

function validateManifestRouteFields(value: Record<string, unknown>): void {
  assertUtcDay(value.reservation_utc_day, "reservation_utc_day");
  assertSafeInteger(value.partition, "partition");
  if (value.partition_count !== MANIFEST_PARTITION_COUNT) {
    fail("MANIFEST_INVALID_REQUEST", `partition_count must equal ${MANIFEST_PARTITION_COUNT}.`);
  }
  if ((value.partition as number) >= MANIFEST_PARTITION_COUNT) {
    fail("MANIFEST_INVALID_REQUEST", `partition must be less than ${MANIFEST_PARTITION_COUNT}.`);
  }
  assertManifestString(value.routing_key, "routing_key");
  const expectedRoutingKey = `${String(value.reservation_utc_day)}:${(value.partition as number).toString().padStart(2, "0")}`;
  if (value.routing_key !== expectedRoutingKey) {
    fail("MANIFEST_INVALID_REQUEST", "routing_key does not match reservation_utc_day and partition.");
  }
  assertManifestHash(value.partition_config_hash, "partition_config_hash");
}

const ROUTE_KEYS = ["reservation_utc_day", "partition", "partition_count", "routing_key", "partition_config_hash"] as const;

export async function manifestReservationRoute(
  txId: string,
  reservedAt: string,
  partitionConfigHash: string,
): Promise<ManifestRouteV2> {
  assertManifestString(txId, "tx_id");
  const timestamp = parseManifestTimestamp(reservedAt, "reserved_at");
  assertManifestHash(partitionConfigHash, "partition_config_hash");
  const digest = await sha256Bytes(txId);
  const partition = digest[digest.length - 1] % MANIFEST_PARTITION_COUNT;
  const reservationUtcDay = timestamp.toISOString().slice(0, 10);
  return {
    reservation_utc_day: reservationUtcDay,
    partition,
    partition_count: MANIFEST_PARTITION_COUNT,
    routing_key: `${reservationUtcDay}:${partition.toString().padStart(2, "0")}`,
    partition_config_hash: partitionConfigHash,
  };
}

export async function createManifestReservation(
  input: CreateManifestReservationInput,
  trustedReservedAt: string,
): Promise<ManifestReservationV1> {
  const candidate: ManifestReservationV1 = {
    protocol_version: CURRENT_PROTOCOL_VERSION,
    format_version: MANIFEST_RESERVATION_FORMAT_VERSION,
    fleet_id: input.fleet_id,
    ...(await manifestReservationRoute(input.tx_id, trustedReservedAt, input.partition_config_hash)),
    tx_id: input.tx_id,
    coordinator_id: input.coordinator_id,
    operation_hash: input.operation_hash,
    decision_epoch: input.decision_epoch,
    reserved_at: trustedReservedAt,
  };
  validateManifestReservation(candidate);
  return candidate;
}

export function validateManifestReservation(value: unknown): asserts value is ManifestReservationV1 {
  assertPlainObject(value, "manifest_reservation");
  assertExactKeys(value, "manifest_reservation", [
    "protocol_version", "format_version", "fleet_id", ...ROUTE_KEYS, "tx_id", "coordinator_id",
    "operation_hash", "decision_epoch", "reserved_at",
  ]);
  assertManifestVersion(value, MANIFEST_RESERVATION_FORMAT_VERSION, "manifest_reservation");
  assertManifestString(value.fleet_id, "fleet_id");
  validateManifestRouteFields(value);
  assertManifestString(value.tx_id, "tx_id");
  assertManifestString(value.coordinator_id, "coordinator_id");
  assertManifestHash(value.operation_hash, "operation_hash");
  assertSafeInteger(value.decision_epoch, "decision_epoch", 1);
  const reservedAt = parseManifestTimestamp(value.reserved_at, "reserved_at");
  if (reservedAt.toISOString().slice(0, 10) !== value.reservation_utc_day) {
    fail("MANIFEST_INVALID_REQUEST", "reserved_at does not fall within reservation_utc_day.");
  }
}

export async function hashManifestReservation(value: unknown): Promise<string> {
  validateManifestReservation(value);
  const expectedRoute = await manifestReservationRoute(value.tx_id, value.reserved_at, value.partition_config_hash);
  if (
    value.reservation_utc_day !== expectedRoute.reservation_utc_day ||
    value.partition !== expectedRoute.partition ||
    value.partition_count !== expectedRoute.partition_count ||
    value.routing_key !== expectedRoute.routing_key
  ) {
    fail("MANIFEST_INVALID_REQUEST", "Manifest reservation does not match its deterministic frozen route.");
  }
  return hashCanonicalJson(value);
}

export function validateManifestFinalizeIntent(value: unknown): asserts value is ManifestFinalizeIntentV1 {
  assertPlainObject(value, "manifest_finalize_intent");
  assertExactKeys(value, "manifest_finalize_intent", [
    "protocol_version", "format_version", "tx_id", "reservation_hash", "redo_envelope_hash",
    "operation_hash", "decision_epoch", "idempotency_key",
  ]);
  assertManifestVersion(value, MANIFEST_TERMINAL_INTENT_FORMAT_VERSION, "manifest_finalize_intent");
  assertManifestString(value.tx_id, "tx_id");
  assertManifestHash(value.reservation_hash, "reservation_hash");
  assertManifestHash(value.redo_envelope_hash, "redo_envelope_hash");
  assertManifestHash(value.operation_hash, "operation_hash");
  assertSafeInteger(value.decision_epoch, "decision_epoch", 1);
  assertManifestString(value.idempotency_key, "idempotency_key");
}

export async function hashManifestFinalizeIntent(value: unknown): Promise<string> {
  validateManifestFinalizeIntent(value);
  return hashCanonicalJson(value);
}

export function validateManifestCancelIntent(value: unknown): asserts value is ManifestCancelIntentV1 {
  assertPlainObject(value, "manifest_cancel_intent");
  assertExactKeys(value, "manifest_cancel_intent", [
    "protocol_version", "format_version", "tx_id", "reservation_hash", "operation_hash", "decision_epoch", "idempotency_key",
  ]);
  assertManifestVersion(value, MANIFEST_TERMINAL_INTENT_FORMAT_VERSION, "manifest_cancel_intent");
  assertManifestString(value.tx_id, "tx_id");
  assertManifestHash(value.reservation_hash, "reservation_hash");
  assertManifestHash(value.operation_hash, "operation_hash");
  assertSafeInteger(value.decision_epoch, "decision_epoch", 1);
  assertManifestString(value.idempotency_key, "idempotency_key");
}

export function validateManifestRecordV2(value: unknown): asserts value is ManifestRecordV2 {
  assertPlainObject(value, "manifest_record_v2");
  assertExactKeys(value, "manifest_record_v2", [
    "protocol_version", "format_version", "fleet_id", ...ROUTE_KEYS, "tx_id", "coordinator_id", "operation_hash",
    "decision_epoch", "reserved_at", "reservation_hash", "envelope_hash", "commit_decided_at",
    "commit_decided_at_ms", "decision_sequence", "retention_deadline",
  ]);
  assertManifestVersion(value, MANIFEST_RECORD_V2_FORMAT_VERSION, "manifest_record_v2");
  assertManifestString(value.fleet_id, "fleet_id");
  validateManifestRouteFields(value);
  assertManifestString(value.tx_id, "tx_id");
  assertManifestString(value.coordinator_id, "coordinator_id");
  assertManifestHash(value.operation_hash, "operation_hash");
  assertSafeInteger(value.decision_epoch, "decision_epoch", 1);
  parseManifestTimestamp(value.reserved_at, "reserved_at");
  assertManifestHash(value.reservation_hash, "reservation_hash");
  assertManifestHash(value.envelope_hash, "envelope_hash");
  const decidedAt = parseManifestTimestamp(value.commit_decided_at, "commit_decided_at");
  assertSafeInteger(value.commit_decided_at_ms, "commit_decided_at_ms");
  if (decidedAt.getTime() !== value.commit_decided_at_ms) {
    fail("MANIFEST_INVALID_REQUEST", "commit_decided_at_ms does not match commit_decided_at.");
  }
  assertSafeInteger(value.decision_sequence, "decision_sequence", 1);
  const deadline = parseManifestTimestamp(value.retention_deadline, "retention_deadline");
  if (deadline.getTime() - decidedAt.getTime() < COORDINATOR_RETENTION_DAYS * 24 * 60 * 60 * 1000) {
    fail("MANIFEST_INVALID_REQUEST", `retention_deadline must be at least ${COORDINATOR_RETENTION_DAYS} days after commit_decided_at.`);
  }
}

export async function hashManifestRecordV2(value: unknown): Promise<string> {
  validateManifestRecordV2(value);
  const reservation: ManifestReservationV1 = {
    protocol_version: value.protocol_version,
    format_version: MANIFEST_RESERVATION_FORMAT_VERSION,
    fleet_id: value.fleet_id,
    reservation_utc_day: value.reservation_utc_day,
    partition: value.partition,
    partition_count: value.partition_count,
    routing_key: value.routing_key,
    partition_config_hash: value.partition_config_hash,
    tx_id: value.tx_id,
    coordinator_id: value.coordinator_id,
    operation_hash: value.operation_hash,
    decision_epoch: value.decision_epoch,
    reserved_at: value.reserved_at,
  };
  const expectedReservationHash = await hashManifestReservation(reservation);
  if (value.reservation_hash !== expectedReservationHash) {
    fail("MANIFEST_RESERVATION_CONFLICT", "Manifest record reservation_hash does not match its frozen reservation fields.");
  }
  return hashCanonicalJson(value);
}

export function validatePartitionConfig(value: unknown): asserts value is PartitionConfigV1 {
  assertPlainObject(value, "partition_config");
  assertExactKeys(value, "partition_config", [
    "protocol_version", "format_version", "effective_from_day", "partition_count", "prior_config_hash", "config_hash",
  ]);
  assertManifestVersion(value, MANIFEST_CATALOG_FORMAT_VERSION, "partition_config");
  assertUtcDay(value.effective_from_day, "effective_from_day");
  if (value.partition_count !== MANIFEST_PARTITION_COUNT) fail("MANIFEST_INVALID_REQUEST", `partition_count must equal ${MANIFEST_PARTITION_COUNT}.`);
  assertNullableManifestHash(value.prior_config_hash, "prior_config_hash");
  assertManifestHash(value.config_hash, "config_hash");
}

export function validateManifestCatalogActivateRequest(value: unknown): asserts value is ManifestCatalogActivateRequestV1 {
  assertPlainObject(value, "catalog_activate_request");
  assertExactKeys(value, "catalog_activate_request", [
    "protocol_version", "format_version", "fleet_id", ...ROUTE_KEYS, "activation_key",
  ]);
  assertManifestVersion(value, MANIFEST_CATALOG_FORMAT_VERSION, "catalog_activate_request");
  assertManifestString(value.fleet_id, "fleet_id");
  validateManifestRouteFields(value);
  assertManifestString(value.activation_key, "activation_key");
}

export function validateManifestCatalogSnapshotRequest(value: unknown): asserts value is ManifestCatalogSnapshotRequestV1 {
  assertPlainObject(value, "catalog_snapshot_request");
  assertExactKeys(value, "catalog_snapshot_request", [
    "protocol_version", "format_version", "fleet_id", "cutoff", "partition_config_hash", "idempotency_key",
  ]);
  assertManifestVersion(value, MANIFEST_CATALOG_FORMAT_VERSION, "catalog_snapshot_request");
  assertManifestString(value.fleet_id, "fleet_id");
  parseManifestTimestamp(value.cutoff, "cutoff");
  assertManifestHash(value.partition_config_hash, "partition_config_hash");
  assertManifestString(value.idempotency_key, "idempotency_key");
}

export function validateManifestCatalogRetireRequest(value: unknown): asserts value is ManifestCatalogRetireRequestV1 {
  assertPlainObject(value, "catalog_retire_request");
  assertExactKeys(value, "catalog_retire_request", [
    "protocol_version", "format_version", "fleet_id", ...ROUTE_KEYS, "safety_certificate_hash", "idempotency_key",
  ]);
  assertManifestVersion(value, MANIFEST_CATALOG_FORMAT_VERSION, "catalog_retire_request");
  assertManifestString(value.fleet_id, "fleet_id");
  validateManifestRouteFields(value);
  assertManifestHash(value.safety_certificate_hash, "safety_certificate_hash");
  assertManifestString(value.idempotency_key, "idempotency_key");
}

export function validateManifestSealRequest(value: unknown): asserts value is ManifestSealRequestV1 {
  assertPlainObject(value, "manifest_seal_request");
  assertExactKeys(value, "manifest_seal_request", [
    "protocol_version", "format_version", "fleet_id", ...ROUTE_KEYS, "cutoff", "idempotency_key",
  ]);
  assertManifestVersion(value, MANIFEST_SEAL_FORMAT_VERSION, "manifest_seal_request");
  assertManifestString(value.fleet_id, "fleet_id");
  validateManifestRouteFields(value);
  parseManifestTimestamp(value.cutoff, "cutoff");
  assertManifestString(value.idempotency_key, "idempotency_key");
}

export function validateManifestSealReceipt(value: unknown): asserts value is ManifestSealReceiptV1 {
  assertPlainObject(value, "manifest_seal_receipt");
  assertExactKeys(value, "manifest_seal_receipt", [
    "protocol_version", "format_version", "fleet_id", ...ROUTE_KEYS, "cutoff", "generation", "decision_floor_ms",
    "sealed_through_ms", "record_count", "maximum_decision_sequence", "records_deleted_through_ms", "retention_epoch",
    "records_root", "conflict_resolution_root", "local_legacy_certificate_hash", "prior_receipt_hash", "receipt_hash",
  ]);
  assertManifestVersion(value, MANIFEST_SEAL_FORMAT_VERSION, "manifest_seal_receipt");
  assertManifestString(value.fleet_id, "fleet_id");
  validateManifestRouteFields(value);
  const cutoff = parseManifestTimestamp(value.cutoff, "cutoff");
  assertSafeInteger(value.generation, "generation", 1);
  assertSafeInteger(value.decision_floor_ms, "decision_floor_ms");
  assertSafeInteger(value.sealed_through_ms, "sealed_through_ms");
  if (value.sealed_through_ms !== cutoff.getTime() || value.sealed_through_ms > (value.decision_floor_ms as number)) {
    fail("MANIFEST_INVALID_REQUEST", "seal watermarks do not prove the exact cutoff.");
  }
  assertSafeInteger(value.record_count, "record_count");
  if (value.maximum_decision_sequence !== null) assertSafeInteger(value.maximum_decision_sequence, "maximum_decision_sequence", 1);
  if (value.records_deleted_through_ms !== null) assertSafeInteger(value.records_deleted_through_ms, "records_deleted_through_ms");
  assertSafeInteger(value.retention_epoch, "retention_epoch");
  assertManifestHash(value.records_root, "records_root");
  assertManifestHash(value.conflict_resolution_root, "conflict_resolution_root");
  assertManifestHash(value.local_legacy_certificate_hash, "local_legacy_certificate_hash");
  assertNullableManifestHash(value.prior_receipt_hash, "prior_receipt_hash");
  assertManifestHash(value.receipt_hash, "receipt_hash");
}

export function validateManifestLocalPageCursor(value: unknown): asserts value is ManifestLocalPageCursorV1 {
  assertPlainObject(value, "manifest_local_page_cursor");
  assertExactKeys(value, "manifest_local_page_cursor", [
    "protocol_version", "format_version", "request_hash", "retention_epoch", "seal_generation", "seal_receipt_hash",
    "last_commit_decided_at_ms", "last_decision_sequence", "last_tx_id",
  ]);
  assertManifestVersion(value, MANIFEST_CURSOR_FORMAT_VERSION, "manifest_local_page_cursor");
  assertManifestHash(value.request_hash, "request_hash");
  assertSafeInteger(value.retention_epoch, "retention_epoch");
  assertSafeInteger(value.seal_generation, "seal_generation", 1);
  assertManifestHash(value.seal_receipt_hash, "seal_receipt_hash");
  assertSafeInteger(value.last_commit_decided_at_ms, "last_commit_decided_at_ms");
  assertSafeInteger(value.last_decision_sequence, "last_decision_sequence", 1);
  assertManifestString(value.last_tx_id, "last_tx_id");
}

function assertManifestPageLimit(value: unknown): asserts value is number {
  assertSafeInteger(value, "limit", 1);
  if (value > MAX_MANIFEST_PAGE_LIMIT) {
    fail("MANIFEST_INVALID_REQUEST", `limit must be between 1 and ${MAX_MANIFEST_PAGE_LIMIT}.`);
  }
}

export function validateManifestLocalPageRequest(value: unknown): asserts value is ManifestLocalPageRequestV1 {
  assertPlainObject(value, "manifest_local_page_request");
  assertExactKeys(value, "manifest_local_page_request", [
    "protocol_version", "format_version", "fleet_id", ...ROUTE_KEYS, "coverage_start", "cutoff",
    "expected_retention_epoch", "seal_generation", "seal_receipt_hash", "limit", "cursor",
  ]);
  assertManifestVersion(value, MANIFEST_PAGE_FORMAT_VERSION, "manifest_local_page_request");
  assertManifestString(value.fleet_id, "fleet_id");
  validateManifestRouteFields(value);
  const coverageStart = parseManifestTimestamp(value.coverage_start, "coverage_start");
  const cutoff = parseManifestTimestamp(value.cutoff, "cutoff");
  if (coverageStart.getTime() > cutoff.getTime()) fail("MANIFEST_INVALID_REQUEST", "coverage_start must not exceed cutoff.");
  assertSafeInteger(value.expected_retention_epoch, "expected_retention_epoch");
  assertSafeInteger(value.seal_generation, "seal_generation", 1);
  assertManifestHash(value.seal_receipt_hash, "seal_receipt_hash");
  assertManifestPageLimit(value.limit);
  if (value.cursor !== null) validateManifestLocalPageCursor(value.cursor);
}

export function validateManifestEnumerationCursor(value: unknown): asserts value is ManifestEnumerationCursorV1 {
  assertPlainObject(value, "manifest_enumeration_cursor");
  assertExactKeys(value, "manifest_enumeration_cursor", [
    "protocol_version", "format_version", "request_hash", "catalog_generation", "catalog_snapshot_hash",
    "reservation_utc_day", "partition", "local_cursor",
  ]);
  assertManifestVersion(value, MANIFEST_CURSOR_FORMAT_VERSION, "manifest_enumeration_cursor");
  assertManifestHash(value.request_hash, "request_hash");
  assertSafeInteger(value.catalog_generation, "catalog_generation", 1);
  assertManifestHash(value.catalog_snapshot_hash, "catalog_snapshot_hash");
  assertUtcDay(value.reservation_utc_day, "reservation_utc_day");
  assertSafeInteger(value.partition, "partition");
  if (value.partition >= MANIFEST_PARTITION_COUNT) fail("MANIFEST_INVALID_REQUEST", `partition must be less than ${MANIFEST_PARTITION_COUNT}.`);
  if (value.local_cursor !== null) validateManifestLocalPageCursor(value.local_cursor);
}

export function validateManifestEnumerationRequest(value: unknown): asserts value is ManifestEnumerationRequestV1 {
  assertPlainObject(value, "manifest_enumeration_request");
  assertExactKeys(value, "manifest_enumeration_request", [
    "protocol_version", "format_version", "fleet_id", "coverage_start", "cutoff", "partition_config_hash",
    "catalog_generation", "catalog_snapshot_hash", "limit", "cursor",
  ]);
  assertManifestVersion(value, MANIFEST_ENUMERATION_FORMAT_VERSION, "manifest_enumeration_request");
  assertManifestString(value.fleet_id, "fleet_id");
  const coverageStart = parseManifestTimestamp(value.coverage_start, "coverage_start");
  const cutoff = parseManifestTimestamp(value.cutoff, "cutoff");
  if (coverageStart.getTime() > cutoff.getTime()) fail("MANIFEST_INVALID_REQUEST", "coverage_start must not exceed cutoff.");
  assertManifestHash(value.partition_config_hash, "partition_config_hash");
  assertSafeInteger(value.catalog_generation, "catalog_generation", 1);
  assertManifestHash(value.catalog_snapshot_hash, "catalog_snapshot_hash");
  assertManifestPageLimit(value.limit);
  if (value.cursor !== null) validateManifestEnumerationCursor(value.cursor);
}

export async function hashManifestRequest(value: ManifestLocalPageRequestV1 | ManifestEnumerationRequestV1): Promise<string> {
  if ("expected_retention_epoch" in value) validateManifestLocalPageRequest(value);
  else validateManifestEnumerationRequest(value);
  return hashCanonicalJson({ ...value, cursor: null });
}

export function assertManifestCursorMatchesRequest(
  cursor: ManifestLocalPageCursorV1 | ManifestEnumerationCursorV1,
  requestHash: string,
): void {
  assertManifestHash(requestHash, "request_hash");
  if (cursor.request_hash !== requestHash) {
    fail("MANIFEST_CURSOR_MISMATCH", "Cursor request_hash does not match the canonical request.");
  }
}

export function validateManifestCoverageEvidence(value: unknown): asserts value is ManifestCoverageEvidenceV1 {
  assertPlainObject(value, "manifest_coverage_evidence");
  assertExactKeys(value, "manifest_coverage_evidence", [
    "protocol_version", "format_version", ...ROUTE_KEYS, "cutoff", "seal_generation", "seal_receipt_hash",
    "retention_epoch", "records_deleted_through_ms", "local_legacy_certificate_hash",
  ]);
  assertManifestVersion(value, MANIFEST_ENUMERATION_FORMAT_VERSION, "manifest_coverage_evidence");
  validateManifestRouteFields(value);
  parseManifestTimestamp(value.cutoff, "cutoff");
  assertSafeInteger(value.seal_generation, "seal_generation", 1);
  assertManifestHash(value.seal_receipt_hash, "seal_receipt_hash");
  assertSafeInteger(value.retention_epoch, "retention_epoch");
  if (value.records_deleted_through_ms !== null) assertSafeInteger(value.records_deleted_through_ms, "records_deleted_through_ms");
  assertManifestHash(value.local_legacy_certificate_hash, "local_legacy_certificate_hash");
}

export function validateManifestEnumerationResult(value: unknown): asserts value is ManifestEnumerationResultV1 {
  assertPlainObject(value, "manifest_enumeration_result");
  assertExactKeys(value, "manifest_enumeration_result", [
    "protocol_version", "format_version", "request_hash", "coverage", "complete", "records", "evidence",
    "next_cursor", "diagnostics",
  ]);
  assertManifestVersion(value, MANIFEST_ENUMERATION_FORMAT_VERSION, "manifest_enumeration_result");
  assertManifestHash(value.request_hash, "request_hash");
  if (typeof value.coverage !== "string" || !(MANIFEST_COVERAGE_STATUSES as readonly string[]).includes(value.coverage)) {
    fail("MANIFEST_INVALID_REQUEST", "coverage must be a supported manifest coverage status.");
  }
  if (typeof value.complete !== "boolean" || value.complete !== (value.coverage === "complete")) {
    fail("MANIFEST_INVALID_REQUEST", "complete must be true exactly when coverage is complete.");
  }
  if (!Array.isArray(value.records)) fail("MANIFEST_INVALID_REQUEST", "records must be an array.");
  value.records.forEach(validateManifestRecordV2);
  if (!Array.isArray(value.evidence)) fail("MANIFEST_INVALID_REQUEST", "evidence must be an array.");
  value.evidence.forEach(validateManifestCoverageEvidence);
  if (value.next_cursor !== null) validateManifestEnumerationCursor(value.next_cursor);
  if (value.complete && value.next_cursor !== null) {
    fail("MANIFEST_INVALID_REQUEST", "A complete enumeration cannot include a continuation cursor.");
  }
  assertPlainObject(value.diagnostics, "manifest_enumeration_diagnostics");
  assertExactKeys(value.diagnostics, "manifest_enumeration_diagnostics", ["inspected_buckets", "incomplete_buckets", "returned_records"]);
  assertSafeInteger(value.diagnostics.inspected_buckets, "inspected_buckets");
  assertSafeInteger(value.diagnostics.incomplete_buckets, "incomplete_buckets");
  assertSafeInteger(value.diagnostics.returned_records, "returned_records");
  if (value.diagnostics.returned_records !== value.records.length) {
    fail("MANIFEST_INVALID_REQUEST", "diagnostics.returned_records must match records.length.");
  }
}

export function isReadableTransactionStateModelVersion(version: unknown): version is number {
  return typeof version === "number" && Number.isSafeInteger(version) &&
    version >= MIN_READABLE_TRANSACTION_STATE_MODEL_VERSION && version <= TRANSACTION_STATE_MODEL_VERSION;
}

export function assertReadableTransactionStateModelVersion(version: unknown): asserts version is number {
  if (!isReadableTransactionStateModelVersion(version)) {
    fail("TX_VERSION_UNSUPPORTED", `Transaction state-model version ${String(version)} is unsupported.`);
  }
}
