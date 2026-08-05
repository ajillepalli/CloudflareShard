import { describe, expect, it } from "vitest";
import {
  COORDINATOR_RETENTION_DAYS,
  CURRENT_PROTOCOL_VERSION,
  MANIFEST_PARTITION_COUNT,
  MAX_REDO_ENVELOPE_BYTES,
  PARTICIPANT_TOMBSTONE_FORMAT_VERSION,
  REDO_ENVELOPE_FORMAT_VERSION,
  TRANSACTION_STATES,
  TransactionContractViolation,
  assertReadableProtocolVersion,
  assertTransactionTransition,
  assertWritableProtocolVersion,
  canonicalByteLength,
  canonicalJson,
  createManifestRegistration,
  decisionForState,
  hashCanonicalJson,
  isCommitDecidedOrLater,
  isTransactionTransitionAllowed,
  manifestRoute,
  transactionError,
  validateIdempotencyDays,
  validateManifestRegistration,
  validateParticipantMessageAgainstTombstone,
  validateParticipantPhaseMessage,
  validateParticipantTombstone,
  validateRedoEnvelope,
  validateRedoEnvelopeStructure,
  type ParticipantDecisionTombstoneV1,
  type ParticipantPhaseMessageV1,
  type RedoEnvelopeV1,
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
    const abortStates: TransactionState[] = ["abort_decided", "aborting", "aborted"];
    for (const from of TRANSACTION_STATES.filter(isCommitDecidedOrLater)) {
      expect(decisionForState(from)).toBe("commit");
      for (const to of abortStates) {
        expect(errorCode(() => assertTransactionTransition(from, to)), `${from} -> ${to}`).toBe("TX_COMMIT_ALREADY_DECIDED");
      }
    }
  });

  it("never permits an abort-decided state to reach a commit state", () => {
    const commitStates = TRANSACTION_STATES.filter(isCommitDecidedOrLater);
    for (const from of ["abort_decided", "aborting", "aborted"] as const) {
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
