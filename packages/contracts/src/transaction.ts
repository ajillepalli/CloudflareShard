/**
 * Binding-free transaction protocol shared by the core Worker and the
 * control-plane Worker. Runtime adapters own storage and RPC; this package owns
 * durable/wire formats, legal transitions, hashing, routing, and typed errors.
 */

export const CURRENT_PROTOCOL_VERSION = 1 as const;
export const MIN_READABLE_PROTOCOL_VERSION = 1 as const;
export const TRANSACTION_STATE_MODEL_VERSION = 1 as const;
export const REDO_ENVELOPE_FORMAT_VERSION = 1 as const;
export const MANIFEST_RECORD_FORMAT_VERSION = 1 as const;
export const PARTICIPANT_TOMBSTONE_FORMAT_VERSION = 1 as const;
export const TRANSACTION_ERROR_SCHEMA_VERSION = 1 as const;

export const MAX_REDO_ENVELOPE_BYTES = 256 * 1024;
export const MAX_PARTICIPANT_KEYS = 8;
export const MAX_VBUCKET = 65_535;
export const MANIFEST_PARTITION_COUNT = 16 as const;
export const COORDINATOR_RETENTION_DAYS = 35;
export const PARTICIPANT_TOMBSTONE_RETENTION_DAYS = 35;
export const DEFAULT_IDEMPOTENCY_DAYS = 7;
export const MIN_IDEMPOTENCY_DAYS = 1;
export const MAX_IDEMPOTENCY_DAYS = 30;

export type JsonPrimitive = string | number | boolean | null;
export type JsonValue = JsonPrimitive | readonly JsonValue[] | { readonly [key: string]: JsonValue };

export const TRANSACTION_STATES = [
  "new",
  "preparing",
  "abort_decided",
  "aborting",
  "aborted",
  "prepared",
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
  new: ["preparing", "quarantined"],
  preparing: ["abort_decided", "prepared", "quarantined"],
  abort_decided: ["aborting", "quarantined"],
  aborting: ["aborted", "quarantined"],
  aborted: ["quarantined"],
  prepared: ["abort_decided", "commit_decided", "quarantined"],
  commit_decided: ["commit_pending_manifest", "manifest_registered", "quarantined"],
  commit_pending_manifest: ["manifest_registered", "quarantined"],
  manifest_registered: ["committing", "quarantined"],
  committing: ["committed_pending_ack", "committed", "quarantined"],
  committed_pending_ack: ["committed", "quarantined"],
  committed: ["quarantined"],
  quarantined: [],
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
};

const RETRYABLE_ERRORS: ReadonlySet<TransactionErrorCode> = new Set([
  "TX_DECISION_UNAVAILABLE",
  "TX_MANIFEST_UNAVAILABLE",
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
    state === "commit_pending_manifest" ||
    state === "manifest_registered" ||
    state === "committing" ||
    state === "committed_pending_ack" ||
    state === "committed"
  );
}

export function isAbortDecidedOrLater(state: TransactionState): boolean {
  return state === "abort_decided" || state === "aborting" || state === "aborted";
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
  if (value.participants.length < 1 || value.participants.length > MAX_PARTICIPANT_KEYS) {
    fail(
      "TX_ENVELOPE_INVALID",
      `Redo envelope must contain 1-${MAX_PARTICIPANT_KEYS} participants.`,
      { participant_count: value.participants.length, maximum_participants: MAX_PARTICIPANT_KEYS },
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
