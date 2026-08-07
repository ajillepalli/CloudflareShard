import { describe, expect, it } from "vitest";
import {
  RESTORE_PLAN_FORMAT_VERSION,
  RESTORE_PROTOCOL_VERSION,
  RESTORE_REQUEST_FORMAT_VERSION,
  RestoreContractViolation,
  hashRestorePlanBody,
  hashRestorePreviewParameters,
  restoreError,
  validateRestoreExecuteRequest,
  validateRestoreAcceptedResult,
  validateRestorePlan,
  validateRestorePlanBody,
  validateRestorePreviewRequest,
  validateRestorePreviewResult,
  validateRestoreReconcileRequest,
  validateRestoreRollbackRequest,
  validateRestoreStatus,
  validateRestoreStatusRequest,
  type RestorePlanBodyV1,
  type RestoreStatusV1,
} from "./restore.js";

const HASH_A = "a".repeat(64);
const HASH_B = "b".repeat(64);
const HASH_C = "c".repeat(64);
const HASH_D = "d".repeat(64);

function planBody(overrides: Partial<RestorePlanBodyV1> = {}): RestorePlanBodyV1 {
  return {
    protocol_version: RESTORE_PROTOCOL_VERSION,
    format_version: RESTORE_PLAN_FORMAT_VERSION,
    restore_id: "restore-001",
    fleet_id: "default",
    cutoff: "2026-08-05T12:00:00.000Z",
    previewed_at: "2026-08-05T12:05:00.000Z",
    execute_before: "2026-08-05T12:35:00.000Z",
    parameter_hash: HASH_A,
    topology: { topology_epoch: 7, topology_hash: HASH_B },
    manifest: {
      coverage_start: "2026-07-06T12:00:00.000Z",
      catalog_close_key: HASH_C,
      catalog_generation: 3,
      catalog_snapshot_hash: HASH_D,
      fleet_root_hash: "e".repeat(64),
      partition_config_hash: "f".repeat(64),
      record_count: 2,
    },
    participants: [
      { participant_id: "shard-a", target_bookmark: "bookmark-a-target", preview_bookmark: "bookmark-a-preview" },
      { participant_id: "shard-b", target_bookmark: "bookmark-b-target", preview_bookmark: "bookmark-b-preview" },
    ],
    impact: {
      participant_count: 2,
      transaction_count: 2,
      intentional_loss_from: "2026-08-05T12:00:00.000Z",
      intentional_loss_through: "2026-08-05T12:35:00.000Z",
    },
    rollback: {
      undo_supported: true,
      undo_expires_at: "2026-09-04T12:35:00.000Z",
      limitations: ["Cannot restore coordinator state.", "Undo remains participant-local."],
    },
    ...overrides,
  };
}

function code(fn: () => unknown): string | undefined {
  try {
    fn();
    return undefined;
  } catch (error) {
    return error instanceof RestoreContractViolation ? error.protocolError.code : undefined;
  }
}

async function asyncCode(fn: () => Promise<unknown>): Promise<string | undefined> {
  try {
    await fn();
    return undefined;
  } catch (error) {
    return error instanceof RestoreContractViolation ? error.protocolError.code : undefined;
  }
}

describe("restore request contracts", () => {
  const preview = {
    protocol_version: RESTORE_PROTOCOL_VERSION,
    format_version: RESTORE_REQUEST_FORMAT_VERSION,
    fleet_id: "default",
    cutoff: "2026-08-05T12:00:00.000Z",
    idempotency_key: "preview-default-2026-08-05",
  } as const;

  it("validates the five exact request shapes", () => {
    expect(() => validateRestorePreviewRequest(preview)).not.toThrow();
    expect(() => validateRestoreExecuteRequest({
      protocol_version: 1, format_version: 1, restore_id: "restore-001", plan_hash: HASH_A,
    })).not.toThrow();
    expect(() => validateRestoreStatusRequest({
      protocol_version: 1, format_version: 1, restore_id: "restore-001",
    })).not.toThrow();
    expect(() => validateRestoreReconcileRequest({
      protocol_version: 1, format_version: 1, restore_id: "restore-001", plan_hash: HASH_A,
    })).not.toThrow();
    expect(() => validateRestoreRollbackRequest({
      protocol_version: 1, format_version: 1, restore_id: "restore-001", plan_hash: HASH_A,
    })).not.toThrow();
    expect(code(() => validateRestoreRollbackRequest({
      protocol_version: 1, format_version: 1, restore_id: "restore-001", plan_hash: HASH_A, force: true,
    }))).toBe("RESTORE_INVALID_REQUEST");
  });

  it("fails closed on unknown keys and unknown versions", () => {
    expect(code(() => validateRestorePreviewRequest({ ...preview, extra: true }))).toBe("RESTORE_INVALID_REQUEST");
    expect(code(() => validateRestorePreviewRequest({ ...preview, format_version: 2 }))).toBe("RESTORE_VERSION_UNSUPPORTED");
    expect(code(() => validateRestorePreviewRequest({ ...preview, protocol_version: 2 }))).toBe("RESTORE_VERSION_UNSUPPORTED");
  });

  it("hashes semantic preview parameters without the idempotency key", async () => {
    await expect(hashRestorePreviewParameters(preview)).resolves.toBe(
      await hashRestorePreviewParameters({ ...preview, idempotency_key: "a-different-retry-key" }),
    );
  });
});

describe("immutable restore plan", () => {
  it("hashes the exact validated plan body and verifies plan_hash", async () => {
    const body = planBody();
    const planHash = await hashRestorePlanBody(body);
    await expect(validateRestorePlan({ ...body, plan_hash: planHash })).resolves.toBeUndefined();

    const changed = planBody({ execute_before: "2026-08-05T12:36:00.000Z", impact: {
      ...body.impact,
      intentional_loss_through: "2026-08-05T12:36:00.000Z",
    } });
    await expect(hashRestorePlanBody(changed)).resolves.not.toBe(planHash);
  });

  it("rejects a mismatched plan hash", async () => {
    await expect(asyncCode(() => validateRestorePlan({ ...planBody(), plan_hash: HASH_A }))).resolves.toBe(
      "RESTORE_PLAN_HASH_MISMATCH",
    );
  });

  it("requires a sorted exact participant set and count-consistent impact", () => {
    const body = planBody();
    expect(code(() => validateRestorePlanBody({ ...body, participants: [...body.participants].reverse() }))).toBe(
      "RESTORE_INVALID_REQUEST",
    );
    expect(code(() => validateRestorePlanBody({
      ...body,
      impact: { ...body.impact, participant_count: 1 },
    }))).toBe("RESTORE_INVALID_REQUEST");
  });

  it("reads only V1 today and rejects N+1 before hashing", async () => {
    const candidate = { ...planBody(), format_version: 2 };
    expect(code(() => validateRestorePlanBody(candidate))).toBe("RESTORE_VERSION_UNSUPPORTED");
    await expect(asyncCode(() => hashRestorePlanBody(candidate as RestorePlanBodyV1))).resolves.toBe(
      "RESTORE_VERSION_UNSUPPORTED",
    );
  });
});

describe("restore response contracts", () => {
  it("validates exact preview result variants", () => {
    expect(() => validateRestorePreviewResult({
      ok: true, status: "previewing", restore_id: "restore-001", retry_after_ms: 1_000,
    })).not.toThrow();
    expect(() => validateRestorePreviewResult({
      ok: true, status: "previewed", plan: { ...planBody(), plan_hash: HASH_A },
    })).not.toThrow();
    expect(code(() => validateRestorePreviewResult({
      ok: true, status: "queued", restore_id: "restore-001", retry_after_ms: 1_000,
    }))).toBe("RESTORE_INVALID_REQUEST");
    expect(code(() => validateRestorePreviewResult({
      ok: true, status: "previewing", restore_id: "restore-001", retry_after_ms: 1_000, extra: true,
    }))).toBe("RESTORE_INVALID_REQUEST");
  });

  it("validates the exact accepted result", () => {
    expect(() => validateRestoreAcceptedResult({
      ok: true, status: "accepted", restore_id: "restore-001", plan_hash: HASH_A,
    })).not.toThrow();
    expect(code(() => validateRestoreAcceptedResult({
      ok: true, status: "queued", restore_id: "restore-001", plan_hash: HASH_A,
    }))).toBe("RESTORE_INVALID_REQUEST");
    expect(code(() => validateRestoreAcceptedResult({
      ok: true, status: "accepted", restore_id: "restore-001", plan_hash: "not-a-hash",
    }))).toBe("RESTORE_INVALID_REQUEST");
  });
});

describe("restore errors and status", () => {
  it("assigns stable HTTP and retry metadata", () => {
    expect(restoreError("RESTORE_PLAN_STALE", "stale")).toMatchObject({ http_status: 409, retryable: false });
    expect(restoreError("RESTORE_CUTOFF_OUTSIDE_PITR_WINDOW", "gone")).toMatchObject({ http_status: 410, retryable: false });
    expect(restoreError("RESTORE_INTERRUPTED", "resume")).toMatchObject({ http_status: 503, retryable: true });
  });

  it("allows previewing but requires complete evidence before complete", () => {
    const status: RestoreStatusV1 = {
      protocol_version: 1,
      format_version: 1,
      restore_id: "restore-001",
      plan_hash: null,
      fleet_id: "default",
      cutoff: "2026-08-05T12:00:00.000Z",
      phase: "previewing",
      started_at: null,
      updated_at: "2026-08-05T12:01:00.000Z",
      completed_at: null,
      progress: { participants_total: 2, participants_restored: 0, transactions_total: 2, transactions_reconciled: 0 },
      blockers: [],
      report: null,
    };
    expect(() => validateRestoreStatus(status)).not.toThrow();
    expect(code(() => validateRestoreStatus({ ...status, phase: "complete" }))).toBe("RESTORE_INVALID_REQUEST");
    expect(() => validateRestoreStatus({
      ...status,
      plan_hash: HASH_A,
      phase: "rolling_back",
      started_at: "2026-08-05T12:02:00.000Z",
    })).not.toThrow();
    expect(() => validateRestoreStatus({
      ...status,
      plan_hash: HASH_A,
      phase: "rolled_back",
      started_at: "2026-08-05T12:02:00.000Z",
      completed_at: "2026-08-05T12:03:00.000Z",
    })).not.toThrow();
    expect(code(() => validateRestoreStatus({
      ...status,
      plan_hash: HASH_A,
      phase: "rolled_back",
      started_at: "2026-08-05T12:02:00.000Z",
    }))).toBe("RESTORE_INVALID_REQUEST");
  });
});
