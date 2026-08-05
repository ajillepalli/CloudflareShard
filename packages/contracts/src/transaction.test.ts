import { describe, expect, it } from "vitest";
import {
  COORDINATOR_RETENTION_DAYS,
  CURRENT_PROTOCOL_VERSION,
  MANIFEST_CATALOG_FORMAT_VERSION,
  MANIFEST_CURSOR_FORMAT_VERSION,
  MANIFEST_ENUMERATION_FORMAT_VERSION,
  MANIFEST_PARTITION_COUNT,
  MANIFEST_PAGE_FORMAT_VERSION,
  MANIFEST_RECORD_V2_FORMAT_VERSION,
  MANIFEST_RESERVATION_FORMAT_VERSION,
  MANIFEST_SEAL_FORMAT_VERSION,
  MANIFEST_TERMINAL_INTENT_FORMAT_VERSION,
  MAX_MANIFEST_PAGE_LIMIT,
  MAX_REDO_ENVELOPE_BYTES,
  MAX_REDO_PARTICIPANTS,
  PARTICIPANT_TOMBSTONE_FORMAT_VERSION,
  REDO_ENVELOPE_FORMAT_VERSION,
  TRANSACTION_STATES,
  TransactionContractViolation,
  assertReadableProtocolVersion,
  assertReadableTransactionStateModelVersion,
  assertManifestCursorMatchesRequest,
  assertTransactionTransition,
  assertWritableProtocolVersion,
  canonicalByteLength,
  canonicalJson,
  createManifestRegistration,
  createManifestReservation,
  decisionForState,
  hashCanonicalJson,
  hashManifestFinalizeIntent,
  hashManifestRecordV2,
  hashManifestRequest,
  hashManifestReservation,
  isCommitDecidedOrLater,
  isTransactionTransitionAllowed,
  manifestRoute,
  manifestMemberAt,
  manifestReservationRoute,
  transactionError,
  validateIdempotencyDays,
  validateManifestRegistration,
  validateManifestCancelIntent,
  validateManifestCatalogActivateRequest,
  validateManifestCatalogRetireRequest,
  validateManifestCatalogSnapshotRequest,
  validateManifestCoverageEvidence,
  validateManifestEnumerationCursor,
  validateManifestEnumerationRequest,
  validateManifestEnumerationResult,
  validateManifestFinalizeIntent,
  validateManifestLocalPageCursor,
  validateManifestLocalPageRequest,
  validateManifestRecordV2,
  validateManifestReservation,
  validateManifestSealReceipt,
  validateManifestSealRequest,
  validatePartitionConfig,
  validateParticipantMessageAgainstTombstone,
  validateParticipantPhaseMessage,
  validateParticipantTombstone,
  validateRedoEnvelope,
  validateRedoEnvelopeStructure,
  type ParticipantDecisionTombstoneV1,
  type ParticipantPhaseMessageV1,
  type RedoEnvelopeV1,
  type ManifestCancelIntentV1,
  type ManifestEnumerationCursorV1,
  type ManifestEnumerationRequestV1,
  type ManifestFinalizeIntentV1,
  type ManifestLocalPageCursorV1,
  type ManifestLocalPageRequestV1,
  type ManifestRecordV2,
  type ManifestReservationV1,
  type ManifestSealReceiptV1,
  type TransactionErrorCode,
  type TransactionState,
} from "./transaction.js";

const DAY_MS = 24 * 60 * 60 * 1000;
const HASH_A = "6064f4783f5bef257abe86fa26609db060641d925486ccba2952bb4598932a3b";

function afterDays(timestamp: string, days: number): string {
  return new Date(new Date(timestamp).getTime() + days * DAY_MS).toISOString();
}

function envelope(overrides: Partial<RedoEnvelopeV1> = {}): RedoEnvelopeV1 {
  const commitDecidedAt = "2026-08-05T12:34:56.000Z";
  return {
    protocol_version: CURRENT_PROTOCOL_VERSION,
    format_version: REDO_ENVELOPE_FORMAT_VERSION,
    tx_id: "tx-001",
    fleet_id: "fleet-test",
    coordinator_id: "coordinator-tx-001",
    decision: "commit",
    decision_epoch: 1,
    commit_decided_at: commitDecidedAt,
    retention_deadline: afterDays(commitDecidedAt, COORDINATOR_RETENTION_DAYS),
    operation_hash: HASH_A,
    participants: [
      {
        participant_id: "shard-a",
        epoch: 1,
        intents: [
          {
            intent_seq: 0,
            sql: "INSERT INTO t (id, value) VALUES (?, ?)",
            params: ["row-1", "value-1"],
            tenant_id: "tenant-a",
            table_name: "t",
            partition_key: "row-1",
            vbucket: 7,
            operation: "insert",
            mirror_target_participant_id: null,
          },
        ],
      },
    ],
    ...overrides,
  };
}

function tombstone(overrides: Partial<ParticipantDecisionTombstoneV1> = {}): ParticipantDecisionTombstoneV1 {
  const decidedAt = "2026-08-05T12:34:56.000Z";
  return {
    protocol_version: CURRENT_PROTOCOL_VERSION,
    format_version: PARTICIPANT_TOMBSTONE_FORMAT_VERSION,
    tx_id: "tx-001",
    highest_epoch: 3,
    decision: "commit",
    operation_hash: HASH_A,
    decided_at: decidedAt,
    retention_deadline: afterDays(decidedAt, COORDINATOR_RETENTION_DAYS),
    ...overrides,
  };
}

function message(overrides: Partial<ParticipantPhaseMessageV1> = {}): ParticipantPhaseMessageV1 {
  return {
    protocol_version: CURRENT_PROTOCOL_VERSION,
    tx_id: "tx-001",
    epoch: 3,
    phase: "commit",
    operation_hash: HASH_A,
    ...overrides,
  };
}

const RESERVED_AT = "2026-08-05T12:00:00.000Z";
const CUTOFF = "2026-08-05T13:00:00.000Z";

async function reservation(overrides: Partial<ManifestReservationV1> = {}): Promise<ManifestReservationV1> {
  const base = await createManifestReservation(
    {
      fleet_id: "fleet-test",
      tx_id: "tx-001",
      coordinator_id: "coordinator-tx-001",
      operation_hash: HASH_A,
      decision_epoch: 1,
      partition_config_hash: "a".repeat(64),
    },
    RESERVED_AT,
  );
  return { ...base, ...overrides };
}

async function finalizedRecord(overrides: Partial<ManifestRecordV2> = {}): Promise<ManifestRecordV2> {
  const reserved = await reservation();
  const commitDecidedAtMs = new Date(CUTOFF).getTime();
  return {
    protocol_version: CURRENT_PROTOCOL_VERSION,
    format_version: MANIFEST_RECORD_V2_FORMAT_VERSION,
    fleet_id: reserved.fleet_id,
    reservation_utc_day: reserved.reservation_utc_day,
    partition: reserved.partition,
    partition_count: reserved.partition_count,
    routing_key: reserved.routing_key,
    partition_config_hash: reserved.partition_config_hash,
    tx_id: reserved.tx_id,
    coordinator_id: reserved.coordinator_id,
    operation_hash: reserved.operation_hash,
    decision_epoch: reserved.decision_epoch,
    reserved_at: reserved.reserved_at,
    reservation_hash: await hashManifestReservation(reserved),
    envelope_hash: "b".repeat(64),
    commit_decided_at: CUTOFF,
    commit_decided_at_ms: commitDecidedAtMs,
    decision_sequence: 1,
    retention_deadline: afterDays(CUTOFF, COORDINATOR_RETENTION_DAYS),
    ...overrides,
  };
}

function finalizeIntent(overrides: Partial<ManifestFinalizeIntentV1> = {}): ManifestFinalizeIntentV1 {
  return {
    protocol_version: CURRENT_PROTOCOL_VERSION,
    format_version: MANIFEST_TERMINAL_INTENT_FORMAT_VERSION,
    tx_id: "tx-001",
    reservation_hash: "c".repeat(64),
    redo_envelope_hash: "d".repeat(64),
    operation_hash: HASH_A,
    decision_epoch: 1,
    idempotency_key: "finalize-tx-001",
    ...overrides,
  };
}

function cancelIntent(overrides: Partial<ManifestCancelIntentV1> = {}): ManifestCancelIntentV1 {
  return {
    protocol_version: CURRENT_PROTOCOL_VERSION,
    format_version: MANIFEST_TERMINAL_INTENT_FORMAT_VERSION,
    tx_id: "tx-001",
    reservation_hash: "c".repeat(64),
    operation_hash: HASH_A,
    decision_epoch: 1,
    idempotency_key: "cancel-tx-001",
    ...overrides,
  };
}

function errorCode(action: () => unknown): TransactionErrorCode {
  try {
    action();
    throw new Error("Expected a TransactionContractViolation");
  } catch (error) {
    if (!(error instanceof TransactionContractViolation)) throw error;
    return error.protocolError.code;
  }
}

async function asyncErrorCode(action: () => Promise<unknown>): Promise<TransactionErrorCode> {
  try {
    await action();
    throw new Error("Expected a TransactionContractViolation");
  } catch (error) {
    if (!(error instanceof TransactionContractViolation)) throw error;
    return error.protocolError.code;
  }
}

describe("transaction state model", () => {
  const expectedDirect: Readonly<Record<TransactionState, readonly TransactionState[]>> = {
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
    quarantined: [],
  };

  it("exhaustively accepts only same-state replay and the frozen transition table", () => {
    for (const from of TRANSACTION_STATES) {
      for (const to of TRANSACTION_STATES) {
        const expected = from === to || expectedDirect[from].includes(to);
        expect(isTransactionTransitionAllowed(from, to), `${from} -> ${to}`).toBe(expected);
        if (expected) expect(() => assertTransactionTransition(from, to), `${from} -> ${to}`).not.toThrow();
      }
    }
  });

  it("never permits a commit-decided state to reach an abort state", () => {
    const abortStates: TransactionState[] = ["abort_decided", "aborting", "aborted_pending_manifest_cancel", "aborted"];
    for (const from of TRANSACTION_STATES.filter(isCommitDecidedOrLater)) {
      expect(decisionForState(from)).toBe("commit");
      for (const to of abortStates) {
        expect(errorCode(() => assertTransactionTransition(from, to)), `${from} -> ${to}`).toBe("TX_COMMIT_ALREADY_DECIDED");
      }
    }
  });

  it("never permits an abort-decided state to reach a commit state", () => {
    const commitStates = TRANSACTION_STATES.filter(isCommitDecidedOrLater);
    for (const from of ["abort_decided", "aborting", "aborted_pending_manifest_cancel", "aborted"] as const) {
      for (const to of commitStates) {
        expect(errorCode(() => assertTransactionTransition(from, to)), `${from} -> ${to}`).toBe("TX_ABORT_ALREADY_DECIDED");
      }
    }
  });
});

describe("protocol compatibility and errors", () => {
  it("writes only current and reads only the current/N-1 declared window", () => {
    expect(() => assertReadableProtocolVersion(1)).not.toThrow();
    expect(() => assertWritableProtocolVersion(1)).not.toThrow();
    expect(errorCode(() => assertReadableProtocolVersion(0))).toBe("TX_VERSION_UNSUPPORTED");
    expect(errorCode(() => assertReadableProtocolVersion(2))).toBe("TX_VERSION_UNSUPPORTED");
    expect(errorCode(() => assertWritableProtocolVersion(0))).toBe("TX_VERSION_UNSUPPORTED");
  });

  it("enforces the frozen 1-30 day idempotency range and seven-day default vocabulary", () => {
    expect(validateIdempotencyDays(1)).toBe(1);
    expect(validateIdempotencyDays(7)).toBe(7);
    expect(validateIdempotencyDays(30)).toBe(30);
    expect(errorCode(() => validateIdempotencyDays(0))).toBe("TX_ENVELOPE_INVALID");
    expect(errorCode(() => validateIdempotencyDays(31))).toBe("TX_ENVELOPE_INVALID");
  });

  it("owns stable HTTP/retry metadata for every typed error code", () => {
    expect(transactionError("TX_ENVELOPE_TOO_LARGE", "too large")).toMatchObject({ http_status: 413, retryable: false });
    expect(transactionError("TX_DECISION_UNAVAILABLE", "retry")).toMatchObject({ http_status: 503, retryable: true });
    expect(transactionError("TX_MANIFEST_UNAVAILABLE", "retry")).toMatchObject({ http_status: 503, retryable: true });
  });
});

describe("canonical redo envelope", () => {
  it("canonicalizes object key order and hashes identical content identically", async () => {
    expect(canonicalJson({ z: 1, a: { d: 4, c: 3 } })).toBe('{"a":{"c":3,"d":4},"z":1}');
    await expect(hashCanonicalJson({ z: 1, a: 2 })).resolves.toBe(await hashCanonicalJson({ a: 2, z: 1 }));
  });

  it("accepts a complete, sorted, reconstructable envelope with a matching operation hash", async () => {
    await expect(validateRedoEnvelope(envelope())).resolves.toBeUndefined();
  });

  it("separates the physical-shard ceiling from the eight caller-key budget", () => {
    const base = envelope();
    const participants = Array.from({ length: 9 }, (_, index) => ({
      ...base.participants[0],
      participant_id: `shard-${String(index).padStart(3, "0")}`,
    }));
    expect(() => validateRedoEnvelopeStructure(envelope({ participants }))).not.toThrow();

    const overPhysicalCeiling = Array.from({ length: MAX_REDO_PARTICIPANTS + 1 }, (_, index) => ({
      ...base.participants[0],
      participant_id: `shard-${String(index).padStart(3, "0")}`,
    }));
    expect(errorCode(() => validateRedoEnvelopeStructure(envelope({ participants: overPhysicalCeiling })))).toBe(
      "TX_ENVELOPE_INVALID",
    );
  });

  it("rejects unsorted participants, epoch divergence, short retention, and oversize content", async () => {
    const base = envelope();
    const second = { ...base.participants[0], participant_id: "shard-b" };
    await expect(
      asyncErrorCode(() => validateRedoEnvelope(envelope({ participants: [second, base.participants[0]] }))),
    ).resolves.toBe("TX_ENVELOPE_INVALID");
    await expect(
      asyncErrorCode(() =>
        validateRedoEnvelope(
          envelope({ participants: [{ ...base.participants[0], epoch: base.decision_epoch + 1 }] }),
        ),
      ),
    ).resolves.toBe("TX_EPOCH_CONFLICT");
    await expect(
      asyncErrorCode(() => validateRedoEnvelope(envelope({ retention_deadline: afterDays(base.commit_decided_at, 34) }))),
    ).resolves.toBe("TX_ENVELOPE_INVALID");

    const oversizedIntent = {
      ...base.participants[0].intents[0],
      sql: "x".repeat(MAX_REDO_ENVELOPE_BYTES),
    };
    await expect(
      asyncErrorCode(() =>
        validateRedoEnvelope(envelope({ participants: [{ ...base.participants[0], intents: [oversizedIntent] }] })),
      ),
    ).resolves.toBe("TX_ENVELOPE_TOO_LARGE");
  });

  it("accepts exactly 256 KiB and rejects the first byte above the ceiling", () => {
    const base = envelope();
    const blankIntent = { ...base.participants[0].intents[0], sql: "" };
    const blankEnvelope = envelope({ participants: [{ ...base.participants[0], intents: [blankIntent] }] });
    const payloadBytes = MAX_REDO_ENVELOPE_BYTES - canonicalByteLength(blankEnvelope);
    const exactIntent = { ...blankIntent, sql: "x".repeat(payloadBytes) };
    const exactEnvelope = envelope({ participants: [{ ...base.participants[0], intents: [exactIntent] }] });
    expect(canonicalByteLength(exactEnvelope)).toBe(MAX_REDO_ENVELOPE_BYTES);
    expect(() => validateRedoEnvelopeStructure(exactEnvelope)).not.toThrow();

    const overIntent = { ...exactIntent, sql: `${exactIntent.sql}x` };
    const overEnvelope = envelope({ participants: [{ ...base.participants[0], intents: [overIntent] }] });
    expect(errorCode(() => validateRedoEnvelopeStructure(overEnvelope))).toBe("TX_ENVELOPE_TOO_LARGE");
  });

  it("rejects a structurally valid envelope whose operation hash does not match its redo content", async () => {
    await expect(asyncErrorCode(() => validateRedoEnvelope(envelope({ operation_hash: "b".repeat(64) })))).resolves.toBe(
      "TX_ENVELOPE_HASH_MISMATCH",
    );
  });

  it("rejects adversarial unknown container shapes before hashing", () => {
    const base = envelope();
    const participant = base.participants[0];
    const invalidEnvelopes: unknown[] = [
      null,
      [],
      { ...base, participants: {} },
      { ...base, participants: [null] },
      { ...base, participants: [{ ...participant, intents: {} }] },
      { ...base, participants: [{ ...participant, intents: [null] }] },
    ];

    for (const candidate of invalidEnvelopes) {
      expect(errorCode(() => validateRedoEnvelopeStructure(candidate))).toBe("TX_ENVELOPE_INVALID");
    }
  });

  it("rejects non-array params and invalid structured routing fields", () => {
    const base = envelope();
    const participant = base.participants[0];
    const intent = participant.intents[0];
    const invalidIntentFields: ReadonlyArray<readonly [string, unknown]> = [
      ["params", {}],
      ["params", "not-an-array"],
      ["params", [undefined]],
      ["params", [Number.POSITIVE_INFINITY]],
      ["vbucket", -1],
      ["vbucket", 65_536],
      ["vbucket", 1.5],
      ["vbucket", "7"],
      ["operation", "merge"],
      ["operation", 1],
      ["mirror_target_participant_id", ""],
      ["mirror_target_participant_id", 7],
    ];

    for (const [field, value] of invalidIntentFields) {
      const candidate = {
        ...base,
        participants: [{ ...participant, intents: [{ ...intent, [field]: value }] }],
      };
      expect(errorCode(() => validateRedoEnvelopeStructure(candidate)), `${field}=${String(value)}`).toBe(
        "TX_ENVELOPE_INVALID",
      );
    }
  });
});

describe("manifest protocol", () => {
  it("uses a stable UTC day plus SHA-256 modulo-16 route", async () => {
    await expect(manifestRoute("tx-001", "2026-08-05T23:59:59.999Z")).resolves.toEqual({
      utc_day: "2026-08-05",
      partition: 1,
      partition_count: MANIFEST_PARTITION_COUNT,
      routing_key: "2026-08-05:01",
    });
  });

  it("creates and validates a content-addressed registration", async () => {
    const redo = envelope();
    const registration = await createManifestRegistration(redo);
    await expect(validateManifestRegistration(registration, redo)).resolves.toBeUndefined();

    const conflicting = {
      ...registration,
      record: { ...registration.record, coordinator_id: "different-coordinator" },
    };
    await expect(validateManifestRegistration(conflicting, redo)).rejects.toMatchObject({
      protocolError: { code: "TX_MANIFEST_CONFLICT" },
    });
  });

  it("rejects malformed unknown registration fields before content addressing", async () => {
    const registration = await createManifestRegistration(envelope());
    const invalidRecords: unknown[] = [
      null,
      {},
      { ...registration, record: null },
      { ...registration, record: { ...registration.record, fleet_id: "" } },
      { ...registration, record: { ...registration.record, partition: "1" } },
      { ...registration, record: { ...registration.record, partition_count: 32 } },
      { ...registration, record: { ...registration.record, routing_key: "" } },
      { ...registration, record: { ...registration.record, coordinator_id: 7 } },
      { ...registration, record_hash: "not-a-hash" },
    ];

    for (const candidate of invalidRecords) {
      await expect(asyncErrorCode(() => validateManifestRegistration(candidate))).resolves.toMatch(
        /^TX_(ENVELOPE_INVALID|MANIFEST_CONFLICT)$/,
      );
    }
  });
});

describe("participant epoch tombstones", () => {
  it("accepts same-epoch, same-hash reconciliation for the durable decision", () => {
    const durable = tombstone();
    expect(() => validateParticipantTombstone(durable)).not.toThrow();
    expect(() => validateParticipantMessageAgainstTombstone(message({ phase: "commit" }), durable)).not.toThrow();
    expect(() => validateParticipantMessageAgainstTombstone(message({ phase: "recover" }), durable)).not.toThrow();
    expect(() => validateParticipantMessageAgainstTombstone(message({ phase: "status" }), durable)).not.toThrow();
  });

  it("rejects stale, future, hash-mismatched, and contradictory messages", () => {
    const durable = tombstone();
    expect(errorCode(() => validateParticipantMessageAgainstTombstone(message({ epoch: 2 }), durable))).toBe("TX_EPOCH_STALE");
    expect(errorCode(() => validateParticipantMessageAgainstTombstone(message({ epoch: 4 }), durable))).toBe("TX_EPOCH_CONFLICT");
    expect(
      errorCode(() => validateParticipantMessageAgainstTombstone(message({ operation_hash: "b".repeat(64) }), durable)),
    ).toBe("TX_ENVELOPE_HASH_MISMATCH");
    expect(errorCode(() => validateParticipantMessageAgainstTombstone(message({ phase: "abort" }), durable))).toBe(
      "TX_DECISION_CONFLICT",
    );
    expect(
      errorCode(() =>
        validateParticipantMessageAgainstTombstone(message({ phase: "commit" }), tombstone({ decision: "abort" })),
      ),
    ).toBe("TX_DECISION_CONFLICT");
  });

  it("rejects unknown, missing, and cast-only participant phases and decisions", () => {
    for (const phase of ["neither", 1, undefined]) {
      expect(errorCode(() => validateParticipantPhaseMessage({ ...message(), phase }))).toBe("TX_ENVELOPE_INVALID");
      expect(errorCode(() => validateParticipantMessageAgainstTombstone({ ...message(), phase }, tombstone()))).toBe(
        "TX_ENVELOPE_INVALID",
      );
    }

    for (const decision of ["neither", 1, undefined]) {
      expect(errorCode(() => validateParticipantTombstone({ ...tombstone(), decision }))).toBe("TX_ENVELOPE_INVALID");
      expect(
        errorCode(() => validateParticipantMessageAgainstTombstone(message(), { ...tombstone(), decision })),
      ).toBe("TX_ENVELOPE_INVALID");
    }
  });

  it("rejects malformed unknown participant messages and tombstones", () => {
    for (const candidate of [null, [], {}, { ...message(), tx_id: "" }, { ...message(), operation_hash: 7 }]) {
      expect(["TX_ENVELOPE_INVALID", "TX_VERSION_UNSUPPORTED"]).toContain(
        errorCode(() => validateParticipantPhaseMessage(candidate)),
      );
    }
    for (const candidate of [null, [], {}, { ...tombstone(), tx_id: "" }, { ...tombstone(), highest_epoch: "3" }]) {
      expect(["TX_ENVELOPE_INVALID", "TX_VERSION_UNSUPPORTED"]).toContain(
        errorCode(() => validateParticipantTombstone(candidate)),
      );
    }
  });
});

describe("manifest V2 state-model compatibility", () => {
  it("reads state-model v1 and v2 while writing the expanded v2 transition graph", () => {
    expect(() => assertReadableTransactionStateModelVersion(1)).not.toThrow();
    expect(() => assertReadableTransactionStateModelVersion(2)).not.toThrow();
    expect(errorCode(() => assertReadableTransactionStateModelVersion(0))).toBe("TX_VERSION_UNSUPPORTED");
    expect(errorCode(() => assertReadableTransactionStateModelVersion(3))).toBe("TX_VERSION_UNSUPPORTED");
    expect(isTransactionTransitionAllowed("new", "manifest_reserving")).toBe(true);
    expect(isTransactionTransitionAllowed("prepared", "commit_deciding")).toBe(true);
    expect(isTransactionTransitionAllowed("commit_deciding", "abort_decided")).toBe(false);
    expect(isTransactionTransitionAllowed("aborted_pending_manifest_cancel", "aborted")).toBe(true);
  });
});

describe("manifest V2 reservation and terminal contracts", () => {
  it("constructs the trusted timestamp route and content-addresses stable reservation bytes", async () => {
    const value = await reservation();
    expect(value).toMatchObject({
      protocol_version: CURRENT_PROTOCOL_VERSION,
      format_version: MANIFEST_RESERVATION_FORMAT_VERSION,
      reservation_utc_day: "2026-08-05",
      partition_count: MANIFEST_PARTITION_COUNT,
      reserved_at: RESERVED_AT,
    });
    await expect(manifestReservationRoute("tx-001", RESERVED_AT, "a".repeat(64))).resolves.toEqual({
      reservation_utc_day: value.reservation_utc_day,
      partition: value.partition,
      partition_count: value.partition_count,
      routing_key: value.routing_key,
      partition_config_hash: value.partition_config_hash,
    });
    expect(() => validateManifestReservation(value)).not.toThrow();
    await expect(hashManifestReservation(value)).resolves.toBe(await hashCanonicalJson(value));
  });

  it("rejects caller-forged routes, noncanonical time, unsafe integers, bad hashes, versions, and unknown fields", async () => {
    const base = await reservation();
    const malformed: unknown[] = [
      { ...base, partition: MANIFEST_PARTITION_COUNT },
      { ...base, routing_key: "2026-08-05:15" },
      { ...base, reservation_utc_day: "2026-08-04" },
      { ...base, reserved_at: "2026-08-05T12:00:00Z" },
      { ...base, decision_epoch: Number.MAX_SAFE_INTEGER + 1 },
      { ...base, operation_hash: "A".repeat(64) },
      { ...base, protocol_version: 2 },
      { ...base, unexpected: true },
    ];
    for (const value of malformed) {
      expect(["MANIFEST_INVALID_REQUEST", "MANIFEST_VERSION_UNSUPPORTED"]).toContain(
        errorCode(() => validateManifestReservation(value)),
      );
    }
    const otherPartition = (base.partition + 1) % MANIFEST_PARTITION_COUNT;
    const forgedButStructurallyValid = {
      ...base,
      partition: otherPartition,
      routing_key: `${base.reservation_utc_day}:${otherPartition.toString().padStart(2, "0")}`,
    };
    expect(() => validateManifestReservation(forgedButStructurallyValid)).not.toThrow();
    await expect(asyncErrorCode(() => hashManifestReservation(forgedButStructurallyValid))).resolves.toBe(
      "MANIFEST_INVALID_REQUEST",
    );
  });

  it("validates and canonically hashes finalize while validating cancel independently", async () => {
    expect(() => validateManifestFinalizeIntent(finalizeIntent())).not.toThrow();
    expect(() => validateManifestCancelIntent(cancelIntent())).not.toThrow();
    await expect(hashManifestFinalizeIntent(finalizeIntent())).resolves.toBe(await hashCanonicalJson(finalizeIntent()));
    expect(errorCode(() => validateManifestFinalizeIntent({ ...finalizeIntent(), unknown: 1 }))).toBe(
      "MANIFEST_INVALID_REQUEST",
    );
    expect(errorCode(() => validateManifestCancelIntent({ ...cancelIntent(), reservation_hash: "short" }))).toBe(
      "MANIFEST_INVALID_REQUEST",
    );
  });

  it("validates bucket-issued decision time, sequence, retention, and canonical record hash", async () => {
    const record = await finalizedRecord();
    expect(() => validateManifestRecordV2(record)).not.toThrow();
    await expect(hashManifestRecordV2(record)).resolves.toBe(await hashCanonicalJson(record));
    await expect(asyncErrorCode(() => hashManifestRecordV2({ ...record, reservation_hash: "f".repeat(64) }))).resolves.toBe(
      "MANIFEST_RESERVATION_CONFLICT",
    );
    expect(errorCode(() => validateManifestRecordV2({ ...record, commit_decided_at_ms: record.commit_decided_at_ms + 1 }))).toBe(
      "MANIFEST_INVALID_REQUEST",
    );
    expect(errorCode(() => validateManifestRecordV2({ ...record, decision_sequence: 0 }))).toBe("MANIFEST_INVALID_REQUEST");
    expect(errorCode(() => validateManifestRecordV2({ ...record, retention_deadline: afterDays(CUTOFF, 34) }))).toBe(
      "MANIFEST_INVALID_REQUEST",
    );
  });

  it("uses one exact membership predicate for finalization and the cutoff boundary", () => {
    const cutoff = new Date(CUTOFF).getTime();
    expect(manifestMemberAt({ state: "FINALIZED", commit_decided_at_ms: cutoff }, cutoff)).toBe(true);
    expect(manifestMemberAt({ state: "FINALIZED", commit_decided_at_ms: cutoff + 1 }, cutoff)).toBe(false);
    expect(manifestMemberAt({ state: "RESERVED", commit_decided_at_ms: cutoff - 1 }, cutoff)).toBe(false);
    expect(manifestMemberAt({ state: "CANCELLED", commit_decided_at_ms: null }, cutoff)).toBe(false);
  });
});

describe("manifest catalog and seal contracts", () => {
  it("validates partition config plus activation, snapshot, and retirement requests", async () => {
    const route = await manifestReservationRoute("tx-001", RESERVED_AT, "a".repeat(64));
    const config = {
      protocol_version: CURRENT_PROTOCOL_VERSION,
      format_version: MANIFEST_CATALOG_FORMAT_VERSION,
      effective_from_day: "2026-08-08",
      partition_count: MANIFEST_PARTITION_COUNT,
      prior_config_hash: null,
      config_hash: "a".repeat(64),
    };
    const activate = {
      protocol_version: CURRENT_PROTOCOL_VERSION,
      format_version: MANIFEST_CATALOG_FORMAT_VERSION,
      fleet_id: "fleet-test",
      ...route,
      activation_key: "activate-1",
    };
    const snapshot = {
      protocol_version: CURRENT_PROTOCOL_VERSION,
      format_version: MANIFEST_CATALOG_FORMAT_VERSION,
      fleet_id: "fleet-test",
      cutoff: CUTOFF,
      partition_config_hash: route.partition_config_hash,
      idempotency_key: "snapshot-1",
    };
    const retire = {
      protocol_version: CURRENT_PROTOCOL_VERSION,
      format_version: MANIFEST_CATALOG_FORMAT_VERSION,
      fleet_id: "fleet-test",
      ...route,
      safety_certificate_hash: "b".repeat(64),
      idempotency_key: "retire-1",
    };
    expect(() => validatePartitionConfig(config)).not.toThrow();
    expect(() => validateManifestCatalogActivateRequest(activate)).not.toThrow();
    expect(() => validateManifestCatalogSnapshotRequest(snapshot)).not.toThrow();
    expect(() => validateManifestCatalogRetireRequest(retire)).not.toThrow();
    expect(errorCode(() => validateManifestCatalogActivateRequest({ ...activate, extra: true }))).toBe(
      "MANIFEST_INVALID_REQUEST",
    );
    expect(errorCode(() => validateManifestCatalogSnapshotRequest({ ...snapshot, cutoff: "tomorrow" }))).toBe(
      "MANIFEST_INVALID_REQUEST",
    );
  });

  it("validates exact-cutoff receipts and rejects impossible watermark evidence", async () => {
    const route = await manifestReservationRoute("tx-001", RESERVED_AT, "a".repeat(64));
    const request = {
      protocol_version: CURRENT_PROTOCOL_VERSION,
      format_version: MANIFEST_SEAL_FORMAT_VERSION,
      fleet_id: "fleet-test",
      ...route,
      cutoff: CUTOFF,
      idempotency_key: "seal-1",
    };
    const receipt: ManifestSealReceiptV1 = {
      protocol_version: CURRENT_PROTOCOL_VERSION,
      format_version: MANIFEST_SEAL_FORMAT_VERSION,
      fleet_id: "fleet-test",
      ...route,
      cutoff: CUTOFF,
      generation: 1,
      decision_floor_ms: new Date(CUTOFF).getTime(),
      sealed_through_ms: new Date(CUTOFF).getTime(),
      record_count: 0,
      maximum_decision_sequence: null,
      records_deleted_through_ms: null,
      retention_epoch: 0,
      records_root: "c".repeat(64),
      conflict_resolution_root: "d".repeat(64),
      local_legacy_certificate_hash: "e".repeat(64),
      prior_receipt_hash: null,
      receipt_hash: "f".repeat(64),
    };
    expect(() => validateManifestSealRequest(request)).not.toThrow();
    expect(() => validateManifestSealReceipt(receipt)).not.toThrow();
    expect(errorCode(() => validateManifestSealReceipt({ ...receipt, decision_floor_ms: receipt.sealed_through_ms - 1 }))).toBe(
      "MANIFEST_INVALID_REQUEST",
    );
    expect(errorCode(() => validateManifestSealReceipt({ ...receipt, sealed_through_ms: receipt.sealed_through_ms - 1 }))).toBe(
      "MANIFEST_INVALID_REQUEST",
    );
  });
});

describe("manifest page, enumeration, cursor, and coverage contracts", () => {
  it("binds local cursors to a canonical cursor-free request hash and enforces page bounds", async () => {
    const route = await manifestReservationRoute("tx-001", RESERVED_AT, "a".repeat(64));
    const request: ManifestLocalPageRequestV1 = {
      protocol_version: CURRENT_PROTOCOL_VERSION,
      format_version: MANIFEST_PAGE_FORMAT_VERSION,
      fleet_id: "fleet-test",
      ...route,
      coverage_start: RESERVED_AT,
      cutoff: CUTOFF,
      expected_retention_epoch: 0,
      seal_generation: 1,
      seal_receipt_hash: "b".repeat(64),
      limit: MAX_MANIFEST_PAGE_LIMIT,
      cursor: null,
    };
    const requestHash = await hashManifestRequest(request);
    const cursor: ManifestLocalPageCursorV1 = {
      protocol_version: CURRENT_PROTOCOL_VERSION,
      format_version: MANIFEST_CURSOR_FORMAT_VERSION,
      request_hash: requestHash,
      retention_epoch: request.expected_retention_epoch,
      seal_generation: request.seal_generation,
      seal_receipt_hash: request.seal_receipt_hash,
      last_commit_decided_at_ms: new Date(CUTOFF).getTime(),
      last_decision_sequence: 1,
      last_tx_id: "tx-001",
    };
    expect(() => validateManifestLocalPageCursor(cursor)).not.toThrow();
    expect(() => validateManifestLocalPageRequest({ ...request, cursor })).not.toThrow();
    expect(() => assertManifestCursorMatchesRequest(cursor, requestHash)).not.toThrow();
    expect(errorCode(() => assertManifestCursorMatchesRequest(cursor, "f".repeat(64)))).toBe("MANIFEST_CURSOR_MISMATCH");
    expect(errorCode(() => validateManifestLocalPageRequest({ ...request, limit: MAX_MANIFEST_PAGE_LIMIT + 1 }))).toBe(
      "MANIFEST_INVALID_REQUEST",
    );
  });

  it("validates fleet cursors, enumeration requests, per-bucket evidence, and completeness consistency", async () => {
    const request: ManifestEnumerationRequestV1 = {
      protocol_version: CURRENT_PROTOCOL_VERSION,
      format_version: MANIFEST_ENUMERATION_FORMAT_VERSION,
      fleet_id: "fleet-test",
      coverage_start: RESERVED_AT,
      cutoff: CUTOFF,
      partition_config_hash: "a".repeat(64),
      catalog_generation: 1,
      catalog_snapshot_hash: "b".repeat(64),
      conflict_resolution_root: "c".repeat(64),
      limit: 100,
      cursor: null,
    };
    const requestHash = await hashManifestRequest(request);
    const cursor: ManifestEnumerationCursorV1 = {
      protocol_version: CURRENT_PROTOCOL_VERSION,
      format_version: MANIFEST_CURSOR_FORMAT_VERSION,
      request_hash: requestHash,
      catalog_generation: request.catalog_generation,
      catalog_snapshot_hash: request.catalog_snapshot_hash,
      conflict_resolution_root: request.conflict_resolution_root,
      reservation_utc_day: "2026-08-05",
      partition: 1,
      local_cursor: null,
    };
    expect(() => validateManifestEnumerationCursor(cursor)).not.toThrow();
    expect(() => validateManifestEnumerationRequest({ ...request, cursor })).not.toThrow();

    const route = await manifestReservationRoute("tx-001", RESERVED_AT, request.partition_config_hash);
    const evidence = {
      protocol_version: CURRENT_PROTOCOL_VERSION,
      format_version: MANIFEST_ENUMERATION_FORMAT_VERSION,
      ...route,
      cutoff: CUTOFF,
      seal_generation: 1,
      seal_receipt_hash: "d".repeat(64),
      retention_epoch: 0,
      records_deleted_through_ms: null,
      local_legacy_certificate_hash: "e".repeat(64),
    };
    expect(() => validateManifestCoverageEvidence(evidence)).not.toThrow();
    const record = await finalizedRecord();
    const result = {
      protocol_version: CURRENT_PROTOCOL_VERSION,
      format_version: MANIFEST_ENUMERATION_FORMAT_VERSION,
      request_hash: requestHash,
      coverage: "complete",
      complete: true,
      records: [record],
      evidence: [evidence],
      next_cursor: null,
      diagnostics: { inspected_buckets: 1, incomplete_buckets: 0, returned_records: 1 },
    };
    expect(() => validateManifestEnumerationResult(result)).not.toThrow();
    expect(errorCode(() => validateManifestEnumerationResult({ ...result, coverage: "incomplete", complete: true }))).toBe(
      "MANIFEST_INVALID_REQUEST",
    );
    expect(errorCode(() => validateManifestEnumerationResult({
      ...result,
      diagnostics: { ...result.diagnostics, returned_records: 0 },
    }))).toBe("MANIFEST_INVALID_REQUEST");
  });

  it("assigns stable HTTP and retry metadata to manifest failures", () => {
    expect(transactionError("MANIFEST_INVALID_REQUEST", "bad input")).toMatchObject({ http_status: 400, retryable: false });
    expect(transactionError("MANIFEST_RETENTION_EXPIRED", "gone")).toMatchObject({ http_status: 410, retryable: false });
    expect(transactionError("MANIFEST_SEAL_IN_PROGRESS", "adopt")).toMatchObject({ http_status: 409, retryable: true });
    expect(transactionError("LEGACY_CERTIFICATION_UNAVAILABLE", "retry")).toMatchObject({ http_status: 503, retryable: true });
  });
});
