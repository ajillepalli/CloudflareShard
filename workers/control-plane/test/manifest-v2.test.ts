import { createExecutionContext, env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it, vi } from "vitest";
import {
  CURRENT_PROTOCOL_VERSION,
  COORDINATOR_RETENTION_DAYS,
  REDO_ENVELOPE_FORMAT_VERSION,
  MANIFEST_SEAL_FORMAT_VERSION,
  MANIFEST_PAGE_FORMAT_VERSION,
  MANIFEST_ENUMERATION_FORMAT_VERSION,
  MANIFEST_TERMINAL_INTENT_FORMAT_VERSION,
  hashCanonicalJson,
  createManifestRegistration,
  hashParticipantOperations,
  hashManifestReservation,
  manifestReservationRoute,
  type ManifestCancelIntentV1,
  type ManifestFinalizeIntentV1,
  type RedoEnvelopeV1,
  type RedoParticipantV1,
} from "../../../packages/contracts/src/index.js";
import ControlPlaneWorker, { manifestObjectNameForReservation } from "../src/index.js";

async function assigned(txId: string) {
  const worker = new ControlPlaneWorker(createExecutionContext(), env);
  const operationHash = await hashCanonicalJson([{ txId, operation: "insert" }]);
  const result = await worker.assignManifestRoute({
    draft: {
      fleet_id: `fleet-${txId}`,
      tx_id: txId,
      coordinator_id: `coordinator-${txId}`,
      operation_hash: operationHash,
      decision_epoch: 1,
    },
    idempotency_key: `assign-${txId}`,
  });
  if (!result.ok) throw new Error(result.error.message);
  return { worker, operationHash, ...result };
}

describe("Manifest V2 reservation and terminal transitions", () => {
  it("durably assigns an idempotent frozen route before bucket reservation", async () => {
    const txId = `tx-route-${crypto.randomUUID()}`;
    const first = await assigned(txId);
    const replay = await first.worker.assignManifestRoute({
      draft: {
        fleet_id: `fleet-${txId}`,
        tx_id: txId,
        coordinator_id: `coordinator-${txId}`,
        operation_hash: first.operationHash,
        decision_epoch: 1,
      },
      idempotency_key: `assign-${txId}`,
    });

    expect(first.status).toBe("assigned");
    expect(replay).toEqual({
      ok: true,
      status: "already_assigned",
      reservation: first.reservation,
      reservation_hash: first.reservation_hash,
    });
  });

  it("activates the catalog, certifies legacy state, and reserves idempotently", async () => {
    const assignedRoute = await assigned(`tx-reserve-${crypto.randomUUID()}`);
    const request = {
      reservation: assignedRoute.reservation,
      reservation_hash: assignedRoute.reservation_hash,
    };
    const first = await assignedRoute.worker.reserveManifest(request);
    const replay = await assignedRoute.worker.reserveManifest(request);

    expect(first).toMatchObject({
      ok: true,
      status: "reserved",
      reservation_hash: assignedRoute.reservation_hash,
      required_decision_floor_ms: 0,
      local_legacy_certificate_hash: expect.stringMatching(/^[a-f0-9]{64}$/),
    });
    expect(replay).toMatchObject({ ok: true, status: "already_reserved" });
  });

  it("fences new V1 admissions fleet-wide after the first V2 route assignment", async () => {
    const route = await assigned(`tx-v1-fence-${crypto.randomUUID()}`);
    const participants: readonly RedoParticipantV1[] = [{
      participant_id: "shard-a",
      epoch: 1,
      intents: [{
        intent_seq: 0,
        sql: "INSERT INTO t (id) VALUES (?)",
        params: ["row-a"],
        tenant_id: "tenant-a",
        table_name: "t",
        partition_key: "row-a",
        vbucket: null,
        operation: "insert",
        mirror_target_participant_id: null,
      }],
    }];
    const decidedAt = new Date().toISOString();
    const envelope: RedoEnvelopeV1 = {
      protocol_version: CURRENT_PROTOCOL_VERSION,
      format_version: REDO_ENVELOPE_FORMAT_VERSION,
      tx_id: `legacy-after-v2-${crypto.randomUUID()}`,
      fleet_id: route.reservation.fleet_id,
      coordinator_id: "legacy-coordinator",
      decision: "commit",
      decision_epoch: 1,
      commit_decided_at: decidedAt,
      retention_deadline: new Date(
        new Date(decidedAt).getTime() + COORDINATOR_RETENTION_DAYS * 86_400_000,
      ).toISOString(),
      operation_hash: await hashParticipantOperations(participants),
      participants,
    };
    const registration = await createManifestRegistration(envelope);
    await expect(route.worker.registerManifest(registration)).resolves.toMatchObject({
      ok: false,
      status: "rejected",
      error: { code: "V1_CLOSED" },
    });

    const replayWorker = new ControlPlaneWorker(createExecutionContext(), env);
    const replayFleet = `fleet-v1-lost-ack-${crypto.randomUUID()}`;
    const replayEnvelope: RedoEnvelopeV1 = {
      ...envelope,
      tx_id: `legacy-before-v2-${crypto.randomUUID()}`,
      fleet_id: replayFleet,
      coordinator_id: "legacy-replay-coordinator",
    };
    const replayRegistration = await createManifestRegistration(replayEnvelope);
    await expect(replayWorker.registerManifest(replayRegistration)).resolves.toMatchObject({
      ok: true,
      status: "registered",
    });
    const fenceOperationHash = await hashCanonicalJson({ operation: "fence-v1-replay" });
    await expect(replayWorker.assignManifestRoute({
      draft: {
        fleet_id: replayFleet,
        tx_id: `v2-fence-${crypto.randomUUID()}`,
        coordinator_id: "v2-fence-coordinator",
        operation_hash: fenceOperationHash,
        decision_epoch: 1,
      },
      idempotency_key: `fence-${crypto.randomUUID()}`,
    })).resolves.toMatchObject({ ok: true, status: "assigned" });
    await expect(replayWorker.registerManifest(replayRegistration)).resolves.toMatchObject({
      ok: true,
      status: "already_registered",
      record_hash: replayRegistration.record_hash,
    });

    const futureDecidedAt = new Date(Date.now() + 60_000).toISOString();
    const futureRegistration = await createManifestRegistration({
      ...envelope,
      tx_id: `legacy-clock-skew-${crypto.randomUUID()}`,
      fleet_id: `fleet-clock-skew-${crypto.randomUUID()}`,
      commit_decided_at: futureDecidedAt,
      retention_deadline: new Date(new Date(futureDecidedAt).getTime() + COORDINATOR_RETENTION_DAYS * 86_400_000).toISOString(),
    });
    await expect(replayWorker.registerManifest(futureRegistration)).resolves.toMatchObject({
      ok: false,
      status: "unavailable",
      http_status: 503,
      error: { code: "TX_MANIFEST_UNAVAILABLE", retryable: true },
    });
  });

  it("assigns the canonical decision in the bucket and replays finalization exactly", async () => {
    const assignedRoute = await assigned(`tx-finalize-${crypto.randomUUID()}`);
    await assignedRoute.worker.reserveManifest({
      reservation: assignedRoute.reservation,
      reservation_hash: assignedRoute.reservation_hash,
    });
    const intent: ManifestFinalizeIntentV1 = {
      protocol_version: CURRENT_PROTOCOL_VERSION,
      format_version: MANIFEST_TERMINAL_INTENT_FORMAT_VERSION,
      tx_id: assignedRoute.reservation.tx_id,
      reservation_hash: assignedRoute.reservation_hash,
      redo_envelope_hash: await hashCanonicalJson({ redo: assignedRoute.reservation.tx_id }),
      operation_hash: assignedRoute.operationHash,
      decision_epoch: 1,
      idempotency_key: `finalize-${assignedRoute.reservation.tx_id}`,
    };
    const request = {
      reservation: assignedRoute.reservation,
      reservation_hash: assignedRoute.reservation_hash,
      intent,
    };
    const first = await assignedRoute.worker.finalizeManifest(request);
    const replay = await assignedRoute.worker.finalizeManifest(request);

    expect(first).toMatchObject({
      ok: true,
      status: "finalized",
      record: {
        tx_id: assignedRoute.reservation.tx_id,
        reservation_hash: assignedRoute.reservation_hash,
        decision_sequence: 1,
      },
      record_hash: expect.stringMatching(/^[a-f0-9]{64}$/),
    });
    expect(replay).toEqual({ ...first, status: "already_finalized" });
  });

  it("cancels a reservation and quarantines a conflicting later finalize", async () => {
    const assignedRoute = await assigned(`tx-cancel-${crypto.randomUUID()}`);
    await assignedRoute.worker.reserveManifest({
      reservation: assignedRoute.reservation,
      reservation_hash: assignedRoute.reservation_hash,
    });
    const cancelIntent: ManifestCancelIntentV1 = {
      protocol_version: CURRENT_PROTOCOL_VERSION,
      format_version: MANIFEST_TERMINAL_INTENT_FORMAT_VERSION,
      tx_id: assignedRoute.reservation.tx_id,
      reservation_hash: assignedRoute.reservation_hash,
      operation_hash: assignedRoute.operationHash,
      decision_epoch: 1,
      idempotency_key: `cancel-${assignedRoute.reservation.tx_id}`,
    };
    await expect(assignedRoute.worker.cancelManifest({
      reservation: assignedRoute.reservation,
      reservation_hash: assignedRoute.reservation_hash,
      intent: cancelIntent,
    })).resolves.toEqual({ ok: true, status: "cancelled" });
    const catalog = env.FLEET_MANIFEST_CATALOG.getByName(`fleet:${assignedRoute.reservation.fleet_id}`);
    await runInDurableObject(catalog, async (_instance, state) => {
      const release = state.storage.sql.exec<{ delete_after_ms: number }>(
        "SELECT delete_after_ms FROM manifest_route_assignments WHERE tx_id = ?",
        assignedRoute.reservation.tx_id,
      ).one();
      expect(release.delete_after_ms).toBeLessThanOrEqual(Date.now() + 60 * 60 * 1000);
    });

    const finalizeIntent: ManifestFinalizeIntentV1 = {
      protocol_version: CURRENT_PROTOCOL_VERSION,
      format_version: MANIFEST_TERMINAL_INTENT_FORMAT_VERSION,
      tx_id: assignedRoute.reservation.tx_id,
      reservation_hash: assignedRoute.reservation_hash,
      redo_envelope_hash: await hashCanonicalJson({ redo: assignedRoute.reservation.tx_id }),
      operation_hash: assignedRoute.operationHash,
      decision_epoch: 1,
      idempotency_key: `finalize-after-cancel-${assignedRoute.reservation.tx_id}`,
    };
    await expect(assignedRoute.worker.finalizeManifest({
      reservation: assignedRoute.reservation,
      reservation_hash: assignedRoute.reservation_hash,
      intent: finalizeIntent,
    })).resolves.toMatchObject({ ok: false, status: "conflict", error: { code: "MANIFEST_TERMINAL_CONFLICT" } });

    const bucket = env.JOURNAL_MANIFEST.getByName(await manifestObjectNameForReservation(assignedRoute.reservation));
    await expect(bucket.stats()).resolves.toMatchObject({ v2_reservations: 1, v2_quarantined: 1 });
    const evidenceBeforeReplay = await runInDurableObject(bucket, async (_instance, state) => ({
      reservation: state.storage.sql.exec<{ conflict_root: string }>(
        "SELECT conflict_root FROM manifest_reservations WHERE tx_id = ?",
        assignedRoute.reservation.tx_id,
      ).one(),
      bucket: state.storage.sql.exec<{ evidence_revision: number }>(
        "SELECT evidence_revision FROM manifest_bucket_state WHERE id = 1",
      ).one(),
    }));
    await expect(assignedRoute.worker.reserveManifest({
      reservation: assignedRoute.reservation,
      reservation_hash: assignedRoute.reservation_hash,
    })).resolves.toMatchObject({ ok: false, status: "quarantined" });
    await runInDurableObject(bucket, async (_instance, state) => {
      expect(state.storage.sql.exec<{ conflict_root: string }>(
        "SELECT conflict_root FROM manifest_reservations WHERE tx_id = ?",
        assignedRoute.reservation.tx_id,
      ).one()).toEqual(evidenceBeforeReplay.reservation);
      expect(state.storage.sql.exec<{ evidence_revision: number }>(
        "SELECT evidence_revision FROM manifest_bucket_state WHERE id = 1",
      ).one()).toEqual(evidenceBeforeReplay.bucket);
      expect(state.storage.sql.exec<{ count: number }>(
        "SELECT COUNT(*) AS count FROM manifest_reservation_conflicts WHERE tx_id = ? AND transition_kind = 'RESERVE'",
        assignedRoute.reservation.tx_id,
      ).one().count).toBe(0);
    });

    const authorizationSourceHash = await hashCanonicalJson(cancelIntent);
    const coordinatorState = {
      tx_id: assignedRoute.reservation.tx_id,
      coordinator_id: assignedRoute.reservation.coordinator_id,
      state: "quarantined",
      decision: "quarantined",
      epoch: assignedRoute.reservation.decision_epoch,
      operation_hash: assignedRoute.reservation.operation_hash,
      reservation_hash: assignedRoute.reservation_hash,
      authorization_source_hash: authorizationSourceHash,
    };
    const resolutionRequest = {
      reservation: assignedRoute.reservation,
      reservation_hash: assignedRoute.reservation_hash,
      resolution: "CANCELLED" as const,
      selected_hash: authorizationSourceHash,
      evidence_hash: await hashCanonicalJson({ ticket: "INC-42", outcome: "cancel" }),
      actor: "operator@example.com",
      reason: "Coordinator durable abort confirms the canonical cancellation.",
      terminal_intent: cancelIntent,
      coordinator_state: coordinatorState,
      coordinator_state_hash: await hashCanonicalJson(coordinatorState),
      idempotency_key: `resolve-${assignedRoute.reservation.tx_id}`,
    };
    const resolved = await assignedRoute.worker.resolveManifestQuarantine(resolutionRequest);
    expect(resolved).toMatchObject({
      ok: true,
      status: "resolved",
      resolution: "CANCELLED",
      resolution_attestation_hash: expect.stringMatching(/^[a-f0-9]{64}$/),
    });
    await expect(assignedRoute.worker.resolveManifestQuarantine(resolutionRequest)).resolves.toEqual({
      ...resolved,
      status: "already_resolved",
    });
    await expect(assignedRoute.worker.reserveManifest({
      reservation: assignedRoute.reservation,
      reservation_hash: assignedRoute.reservation_hash,
    })).resolves.toMatchObject({ ok: true, status: "already_reserved" });
    await expect(bucket.stats()).resolves.toMatchObject({ v2_quarantined: 0 });
  });

  it("materializes a canonical record when cancel races the finalize hash and permits audited repair", async () => {
    const route = await assigned(`tx-finalize-cancel-race-${crypto.randomUUID()}`);
    await route.worker.reserveManifest({ reservation: route.reservation, reservation_hash: route.reservation_hash });
    const finalizeIntent: ManifestFinalizeIntentV1 = {
      protocol_version: CURRENT_PROTOCOL_VERSION,
      format_version: MANIFEST_TERMINAL_INTENT_FORMAT_VERSION,
      tx_id: route.reservation.tx_id,
      reservation_hash: route.reservation_hash,
      redo_envelope_hash: await hashCanonicalJson({ redo: route.reservation.tx_id }),
      operation_hash: route.operationHash,
      decision_epoch: 1,
      idempotency_key: `finalize-${route.reservation.tx_id}`,
    };
    const cancelIntent: ManifestCancelIntentV1 = {
      protocol_version: CURRENT_PROTOCOL_VERSION,
      format_version: MANIFEST_TERMINAL_INTENT_FORMAT_VERSION,
      tx_id: route.reservation.tx_id,
      reservation_hash: route.reservation_hash,
      operation_hash: route.operationHash,
      decision_epoch: 1,
      idempotency_key: `cancel-${route.reservation.tx_id}`,
    };

    const originalDigest = crypto.subtle.digest.bind(crypto.subtle);
    let releaseRecordHash!: () => void;
    const recordHashGate = new Promise<void>((resolve) => { releaseRecordHash = resolve; });
    let signalRecordHash!: () => void;
    const recordHashStarted = new Promise<void>((resolve) => { signalRecordHash = resolve; });
    let intercepted = false;
    const digestSpy = vi.spyOn(crypto.subtle, "digest").mockImplementation(async (algorithm, data) => {
      const bytes = data instanceof ArrayBuffer
        ? new Uint8Array(data)
        : new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
      const canonicalInput = new TextDecoder().decode(bytes);
      if (!intercepted && canonicalInput.includes('"decision_sequence"') && canonicalInput.includes('"retention_deadline"')) {
        intercepted = true;
        signalRecordHash();
        await recordHashGate;
      }
      return await originalDigest(algorithm, data);
    });

    let finalizeResult;
    try {
      const finalizing = route.worker.finalizeManifest({
        reservation: route.reservation,
        reservation_hash: route.reservation_hash,
        intent: finalizeIntent,
      });
      await recordHashStarted;
      await expect(route.worker.cancelManifest({
        reservation: route.reservation,
        reservation_hash: route.reservation_hash,
        intent: cancelIntent,
      })).resolves.toMatchObject({ ok: false, status: "conflict" });
      releaseRecordHash();
      finalizeResult = await finalizing;
    } finally {
      releaseRecordHash();
      digestSpy.mockRestore();
    }
    expect(intercepted).toBe(true);
    expect(finalizeResult).toMatchObject({ ok: false, status: "quarantined" });

    const bucket = env.JOURNAL_MANIFEST.getByName(await manifestObjectNameForReservation(route.reservation));
    const canonical = await runInDurableObject(bucket, async (_instance, state) => state.storage.sql
      .exec<{ record_hash: string; record_json: string }>(
        "SELECT record_hash, record_json FROM manifest_reservations WHERE tx_id = ?",
        route.reservation.tx_id,
      )
      .one());
    expect(canonical.record_hash).toMatch(/^[a-f0-9]{64}$/);
    expect(JSON.parse(canonical.record_json)).toMatchObject({ tx_id: route.reservation.tx_id });

    const authorizationSourceHash = await hashCanonicalJson(finalizeIntent);
    const coordinatorState = {
      tx_id: route.reservation.tx_id,
      coordinator_id: route.reservation.coordinator_id,
      state: "quarantined",
      decision: "quarantined",
      epoch: route.reservation.decision_epoch,
      operation_hash: route.reservation.operation_hash,
      reservation_hash: route.reservation_hash,
      authorization_source_hash: authorizationSourceHash,
    };
    await expect(route.worker.resolveManifestQuarantine({
      reservation: route.reservation,
      reservation_hash: route.reservation_hash,
      resolution: "FINALIZED",
      selected_hash: canonical.record_hash,
      evidence_hash: await hashCanonicalJson({ ticket: "INC-finalize-cancel-race" }),
      actor: "operator@example.com",
      reason: "The assigned finalize decision remains canonical.",
      terminal_intent: finalizeIntent,
      coordinator_state: coordinatorState,
      coordinator_state_hash: await hashCanonicalJson(coordinatorState),
      idempotency_key: `resolve-${route.reservation.tx_id}`,
    })).resolves.toMatchObject({ ok: true, status: "resolved", resolution: "FINALIZED", record_hash: canonical.record_hash });
    await expect(bucket.stats()).resolves.toMatchObject({ v2_quarantined: 0 });
  });

  it("defers V2 retention deletion while a seal generation is draining", async () => {
    const route = await assigned(`tx-retention-seal-${crypto.randomUUID()}`);
    await route.worker.reserveManifest({ reservation: route.reservation, reservation_hash: route.reservation_hash });
    const intent: ManifestFinalizeIntentV1 = {
      protocol_version: CURRENT_PROTOCOL_VERSION,
      format_version: MANIFEST_TERMINAL_INTENT_FORMAT_VERSION,
      tx_id: route.reservation.tx_id,
      reservation_hash: route.reservation_hash,
      redo_envelope_hash: await hashCanonicalJson({ redo: route.reservation.tx_id }),
      operation_hash: route.operationHash,
      decision_epoch: 1,
      idempotency_key: `finalize-${route.reservation.tx_id}`,
    };
    const finalized = await route.worker.finalizeManifest({
      reservation: route.reservation,
      reservation_hash: route.reservation_hash,
      intent,
    });
    if (!finalized.ok) throw new Error(finalized.error.message);
    await route.worker.releaseManifestV2({
      reservation: route.reservation,
      reservation_hash: route.reservation_hash,
      record_hash: finalized.record_hash,
      retention_deadline_ms: new Date(finalized.record.retention_deadline).getTime(),
    });
    const bucket = env.JOURNAL_MANIFEST.getByName(await manifestObjectNameForReservation(route.reservation));
    await runInDurableObject(bucket, async (instance, state) => {
      state.storage.sql.exec("UPDATE manifest_reservations SET retention_deadline_ms = 0 WHERE tx_id = ?", route.reservation.tx_id);
      state.storage.sql.exec(
        `INSERT INTO manifest_seal_generations
          (generation, idempotency_key, cutoff_ms, mode, status, digest_count, digest_root, created_at_ms, updated_at_ms)
         VALUES (999, 'retention-race', 0, 'ADVANCE', 'DRAINING', 0, '', 0, 0)`,
      );
      state.storage.sql.exec(
        "INSERT OR REPLACE INTO manifest_alarm_schedule (purpose, fire_at_ms, generation, payload_hash) VALUES ('retention', 0, 0, '')",
      );
      await instance.alarm?.();
    });
    await expect(bucket.stats()).resolves.toMatchObject({ v2_reservations: 1 });

    await runInDurableObject(bucket, async (instance, state) => {
      state.storage.sql.exec("DELETE FROM manifest_seal_generations WHERE generation = 999");
      state.storage.sql.exec(
        `INSERT INTO manifest_page_cursors
          (cursor_json, request_hash, created_at_ms, lease_expires_at_ms, coverage_start_ms, cutoff_ms)
         VALUES ('non-overlapping-lease', ?, 0, ?, ?, ?)`,
        "a".repeat(64),
        Date.now() + 60_000,
        finalized.record.commit_decided_at_ms + 1,
        finalized.record.commit_decided_at_ms + 2,
      );
      state.storage.sql.exec(
        "INSERT OR REPLACE INTO manifest_alarm_schedule (purpose, fire_at_ms, generation, payload_hash) VALUES ('retention', 0, 0, '')",
      );
      await instance.alarm?.();
    });
    await expect(bucket.stats()).resolves.toMatchObject({ v2_reservations: 0 });
  });

  it("ignores an expired cursor backlog when sweeping V2 retention", async () => {
    const route = await assigned(`tx-retention-expired-cursors-${crypto.randomUUID()}`);
    await route.worker.reserveManifest({ reservation: route.reservation, reservation_hash: route.reservation_hash });
    const intent: ManifestFinalizeIntentV1 = {
      protocol_version: CURRENT_PROTOCOL_VERSION,
      format_version: MANIFEST_TERMINAL_INTENT_FORMAT_VERSION,
      tx_id: route.reservation.tx_id,
      reservation_hash: route.reservation_hash,
      redo_envelope_hash: await hashCanonicalJson({ redo: route.reservation.tx_id }),
      operation_hash: route.operationHash,
      decision_epoch: 1,
      idempotency_key: `finalize-${route.reservation.tx_id}`,
    };
    const finalized = await route.worker.finalizeManifest({
      reservation: route.reservation,
      reservation_hash: route.reservation_hash,
      intent,
    });
    if (!finalized.ok) throw new Error(finalized.error.message);
    await route.worker.releaseManifestV2({
      reservation: route.reservation,
      reservation_hash: route.reservation_hash,
      record_hash: finalized.record_hash,
      retention_deadline_ms: new Date(finalized.record.retention_deadline).getTime(),
    });
    const bucket = env.JOURNAL_MANIFEST.getByName(await manifestObjectNameForReservation(route.reservation));
    await runInDurableObject(bucket, async (instance, state) => {
      state.storage.sql.exec("UPDATE manifest_reservations SET retention_deadline_ms = 0 WHERE tx_id = ?", route.reservation.tx_id);
      const columns = state.storage.sql
        .exec<{ name: string }>("PRAGMA table_info(manifest_reservations)")
        .toArray()
        .map((column) => column.name);
      const original = state.storage.sql
        .exec<Record<string, SqlStorageValue>>("SELECT * FROM manifest_reservations WHERE tx_id = ?", route.reservation.tx_id)
        .one();
      const placeholders = columns.map(() => "?").join(", ");
      for (let index = 0; index < 129; index += 1) {
        const values = columns.map((column) => column === "tx_id" ? `retention-backlog-${index}` : original[column]);
        state.storage.sql.exec(
          `INSERT INTO manifest_reservations (${columns.join(", ")}) VALUES (${placeholders})`,
          ...values,
        );
      }
      for (let index = 0; index < 129; index += 1) {
        state.storage.sql.exec(
          `INSERT INTO manifest_page_cursors
            (cursor_json, request_hash, created_at_ms, lease_expires_at_ms, coverage_start_ms, cutoff_ms)
           VALUES (?, ?, 0, 0, 0, ?)`,
          `expired-cursor-${index}`,
          "b".repeat(64),
          finalized.record.commit_decided_at_ms,
        );
      }
      state.storage.sql.exec(
        "INSERT OR REPLACE INTO manifest_alarm_schedule (purpose, fire_at_ms, generation, payload_hash) VALUES ('retention', 0, 0, '')",
      );
      await instance.alarm?.();
      expect(state.storage.sql.exec<{ count: number }>(
        "SELECT COUNT(*) AS count FROM manifest_page_cursors",
      ).one().count).toBe(1);
      expect(state.storage.sql.exec<{ count: number }>(
        "SELECT COUNT(*) AS count FROM manifest_reservations",
      ).one().count).toBe(2);
      expect(await state.storage.getAlarm()).not.toBeNull();
      await instance.alarm?.();
      expect(state.storage.sql.exec<{ count: number }>(
        "SELECT COUNT(*) AS count FROM manifest_page_cursors",
      ).one().count).toBe(0);
      expect(state.storage.sql.exec<{ count: number }>(
        "SELECT COUNT(*) AS count FROM manifest_reservations",
      ).one().count).toBe(0);
    });
    await expect(bucket.stats()).resolves.toMatchObject({ v2_reservations: 0 });
  });

  it("garbage-collects old cancelled reservations and superseded seal generations", async () => {
    const route = await assigned(`tx-history-gc-${crypto.randomUUID()}`);
    await route.worker.reserveManifest({ reservation: route.reservation, reservation_hash: route.reservation_hash });
    const cancelIntent: ManifestCancelIntentV1 = {
      protocol_version: CURRENT_PROTOCOL_VERSION,
      format_version: MANIFEST_TERMINAL_INTENT_FORMAT_VERSION,
      tx_id: route.reservation.tx_id,
      reservation_hash: route.reservation_hash,
      operation_hash: route.operationHash,
      decision_epoch: 1,
      idempotency_key: `cancel-${route.reservation.tx_id}`,
    };
    await route.worker.cancelManifest({ reservation: route.reservation, reservation_hash: route.reservation_hash, intent: cancelIntent });
    const bucket = env.JOURNAL_MANIFEST.getByName(await manifestObjectNameForReservation(route.reservation));
    await runInDurableObject(bucket, async (instance, state) => {
      state.storage.sql.exec("UPDATE manifest_reservations SET updated_at_ms = 0 WHERE tx_id = ?", route.reservation.tx_id);
      state.storage.sql.exec(
        `INSERT INTO manifest_seal_generations
          (generation, idempotency_key, cutoff_ms, mode, status, digest_count, digest_root,
           evidence_revision, receipt_hash, receipt_json, created_at_ms, updated_at_ms)
         VALUES (1, 'old-complete-seal', 0, 'ADVANCE', 'COMPLETE', 0, '', 0, ?, '{}', 0, 0),
                (2, 'latest-complete-seal', 1, 'ADVANCE', 'COMPLETE', 0, '', 0, ?, '{}', 0, ?)`,
        "a".repeat(64),
        "b".repeat(64),
        Date.now(),
      );
      state.storage.sql.exec(
        "INSERT INTO manifest_seal_digest_entries (generation, commit_decided_at_ms, decision_sequence, tx_id, entry_hash) VALUES (1, 0, 0, 'old', ?)",
        "c".repeat(64),
      );
      state.storage.sql.exec(
        "INSERT OR REPLACE INTO manifest_alarm_schedule (purpose, fire_at_ms, generation, payload_hash) VALUES ('retention', 0, 0, '')",
      );
      await instance.alarm?.();
      expect(state.storage.sql.exec<{ count: number }>("SELECT COUNT(*) AS count FROM manifest_seal_generations WHERE generation = 1").one().count).toBe(0);
      expect(state.storage.sql.exec<{ count: number }>("SELECT COUNT(*) AS count FROM manifest_seal_generations WHERE generation = 2").one().count).toBe(1);
      expect(state.storage.sql.exec<{ count: number }>("SELECT COUNT(*) AS count FROM manifest_seal_digest_entries WHERE generation = 1").one().count).toBe(0);
    });
    await expect(bucket.stats()).resolves.toMatchObject({ v2_reservations: 0 });
  });

  it("restarts the same quarantined seal generation after audited evidence is resolved", async () => {
    const route = await assigned(`tx-seal-repair-${crypto.randomUUID()}`);
    await route.worker.reserveManifest({ reservation: route.reservation, reservation_hash: route.reservation_hash });
    const intent: ManifestFinalizeIntentV1 = {
      protocol_version: CURRENT_PROTOCOL_VERSION,
      format_version: MANIFEST_TERMINAL_INTENT_FORMAT_VERSION,
      tx_id: route.reservation.tx_id,
      reservation_hash: route.reservation_hash,
      redo_envelope_hash: await hashCanonicalJson({ redo: route.reservation.tx_id }),
      operation_hash: route.operationHash,
      decision_epoch: 1,
      idempotency_key: `finalize-${route.reservation.tx_id}`,
    };
    const finalized = await route.worker.finalizeManifest({
      reservation: route.reservation,
      reservation_hash: route.reservation_hash,
      intent,
    });
    if (!finalized.ok) throw new Error(finalized.error.message);
    const sealRequest = {
      protocol_version: CURRENT_PROTOCOL_VERSION,
      format_version: MANIFEST_SEAL_FORMAT_VERSION,
      fleet_id: route.reservation.fleet_id,
      reservation_utc_day: route.reservation.reservation_utc_day,
      partition: route.reservation.partition,
      partition_count: route.reservation.partition_count,
      routing_key: route.reservation.routing_key,
      partition_config_hash: route.reservation.partition_config_hash,
      cutoff: finalized.record.commit_decided_at,
      idempotency_key: `repaired-seal-${route.reservation.tx_id}`,
    } as const;
    const bucket = env.JOURNAL_MANIFEST.getByName(await manifestObjectNameForReservation(route.reservation));
    await runInDurableObject(bucket, async (_instance, state) => {
      state.storage.sql.exec(
        "UPDATE manifest_reservations SET quarantine_state = 'RESOLVED' WHERE tx_id = ?",
        route.reservation.tx_id,
      );
      state.storage.sql.exec(
        `INSERT INTO manifest_seal_generations
          (generation, idempotency_key, cutoff_ms, mode, status, digest_count, digest_root, created_at_ms, updated_at_ms)
         VALUES (1, ?, ?, 'ADVANCE', 'QUARANTINED', 7, 'stale-digest', 0, 0)`,
        sealRequest.idempotency_key,
        finalized.record.commit_decided_at_ms,
      );
      state.storage.sql.exec(
        "UPDATE manifest_bucket_state SET next_seal_generation = 2 WHERE id = 1",
      );
    });
    await expect(bucket.closeThrough(sealRequest)).resolves.toMatchObject({
      ok: true,
      status: "complete",
      generation: 1,
      receipt: { record_count: 1 },
    });
  });

  it("restarts a draining seal when quarantine evidence changes before publication", async () => {
    const route = await assigned(`tx-seal-evidence-race-${crypto.randomUUID()}`);
    await route.worker.reserveManifest({ reservation: route.reservation, reservation_hash: route.reservation_hash });
    const intent: ManifestFinalizeIntentV1 = {
      protocol_version: CURRENT_PROTOCOL_VERSION,
      format_version: MANIFEST_TERMINAL_INTENT_FORMAT_VERSION,
      tx_id: route.reservation.tx_id,
      reservation_hash: route.reservation_hash,
      redo_envelope_hash: await hashCanonicalJson({ redo: route.reservation.tx_id }),
      operation_hash: route.operationHash,
      decision_epoch: 1,
      idempotency_key: `finalize-${route.reservation.tx_id}`,
    };
    const finalized = await route.worker.finalizeManifest({
      reservation: route.reservation,
      reservation_hash: route.reservation_hash,
      intent,
    });
    if (!finalized.ok) throw new Error(finalized.error.message);
    const sealRequest = {
      protocol_version: CURRENT_PROTOCOL_VERSION,
      format_version: MANIFEST_SEAL_FORMAT_VERSION,
      fleet_id: route.reservation.fleet_id,
      reservation_utc_day: route.reservation.reservation_utc_day,
      partition: route.reservation.partition,
      partition_count: route.reservation.partition_count,
      routing_key: route.reservation.routing_key,
      partition_config_hash: route.reservation.partition_config_hash,
      cutoff: finalized.record.commit_decided_at,
      idempotency_key: `evidence-race-seal-${route.reservation.tx_id}`,
    } as const;
    const bucket = env.JOURNAL_MANIFEST.getByName(await manifestObjectNameForReservation(route.reservation));
    await runInDurableObject(bucket, async (_instance, state) => {
      state.storage.sql.exec(
        `INSERT INTO manifest_seal_generations
          (generation, idempotency_key, cutoff_ms, mode, status,
           cursor_decided_at_ms, cursor_decision_sequence, cursor_tx_id,
           digest_count, digest_root, evidence_revision, created_at_ms, updated_at_ms)
         VALUES (1, ?, ?, 'ADVANCE', 'DRAINING', ?, ?, ?, 1, 'stale-digest', 0, 0, 0)`,
        sealRequest.idempotency_key,
        finalized.record.commit_decided_at_ms,
        finalized.record.commit_decided_at_ms,
        finalized.record.decision_sequence,
        route.reservation.tx_id,
      );
      state.storage.sql.exec(
        "UPDATE manifest_bucket_state SET next_seal_generation = 2, evidence_revision = 1 WHERE id = 1",
      );
    });

    await expect(bucket.closeThrough(sealRequest)).resolves.toMatchObject({ ok: true, status: "pending", generation: 1 });
    const sealed = await bucket.closeThrough(sealRequest);
    expect(sealed).toMatchObject({ ok: true, status: "complete", generation: 1 });
    if (!sealed.ok || sealed.status !== "complete" || sealed.receipt === undefined) {
      throw new Error("Expected the restarted seal to complete.");
    }
    expect(sealed.receipt.records_root).not.toBe("stale-digest");
  });

  it("publishes an exact-cutoff receipt and assigns later finalizations above its floor", async () => {
    const first = await assigned(`tx-seal-first-${crypto.randomUUID()}`);
    await first.worker.reserveManifest({ reservation: first.reservation, reservation_hash: first.reservation_hash });
    const firstIntent: ManifestFinalizeIntentV1 = {
      protocol_version: CURRENT_PROTOCOL_VERSION,
      format_version: MANIFEST_TERMINAL_INTENT_FORMAT_VERSION,
      tx_id: first.reservation.tx_id,
      reservation_hash: first.reservation_hash,
      redo_envelope_hash: await hashCanonicalJson({ redo: first.reservation.tx_id }),
      operation_hash: first.operationHash,
      decision_epoch: 1,
      idempotency_key: `finalize-${first.reservation.tx_id}`,
    };
    const finalized = await first.worker.finalizeManifest({
      reservation: first.reservation,
      reservation_hash: first.reservation_hash,
      intent: firstIntent,
    });
    if (!finalized.ok) throw new Error(finalized.error.message);
    const bucket = env.JOURNAL_MANIFEST.getByName(await manifestObjectNameForReservation(first.reservation));
    const sealRequest = {
      protocol_version: CURRENT_PROTOCOL_VERSION,
      format_version: MANIFEST_SEAL_FORMAT_VERSION,
      fleet_id: first.reservation.fleet_id,
      reservation_utc_day: first.reservation.reservation_utc_day,
      partition: first.reservation.partition,
      partition_count: first.reservation.partition_count,
      routing_key: first.reservation.routing_key,
      partition_config_hash: first.reservation.partition_config_hash,
      cutoff: finalized.record.commit_decided_at,
      idempotency_key: `seal-${first.reservation.tx_id}`,
    } as const;
    const sealed = await bucket.closeThrough(sealRequest);
    expect(sealed).toMatchObject({
      ok: true,
      status: "complete",
      receipt: {
        record_count: 1,
        sealed_through_ms: finalized.record.commit_decided_at_ms,
        records_root: expect.stringMatching(/^[a-f0-9]{64}$/),
        receipt_hash: expect.stringMatching(/^[a-f0-9]{64}$/),
      },
    });

    let secondTx = "";
    for (let attempt = 0; attempt < 1_000; attempt += 1) {
      const candidate = `tx-seal-second-${attempt}-${crypto.randomUUID()}`;
      const route = await manifestReservationRoute(
        candidate,
        first.reservation.reserved_at,
        first.reservation.partition_config_hash,
      );
      if (route.partition === first.reservation.partition) {
        secondTx = candidate;
        break;
      }
    }
    if (secondTx === "") throw new Error("Could not find a same-partition test transaction.");
    const secondOperationHash = await hashCanonicalJson([{ txId: secondTx, operation: "insert" }]);
    const secondReservation = {
      ...first.reservation,
      tx_id: secondTx,
      coordinator_id: `coordinator-${secondTx}`,
      operation_hash: secondOperationHash,
    };
    const secondHash = await hashManifestReservation(secondReservation);
    await bucket.reserve(secondReservation, secondHash, finalized.record.commit_decided_at_ms);
    const secondIntent: ManifestFinalizeIntentV1 = {
      protocol_version: CURRENT_PROTOCOL_VERSION,
      format_version: MANIFEST_TERMINAL_INTENT_FORMAT_VERSION,
      tx_id: secondTx,
      reservation_hash: secondHash,
      redo_envelope_hash: await hashCanonicalJson({ redo: secondTx }),
      operation_hash: secondOperationHash,
      decision_epoch: 1,
      idempotency_key: `finalize-${secondTx}`,
    };
    const secondFinalized = await bucket.finalize(secondIntent);
    expect(secondFinalized).toMatchObject({ ok: true });
    if (!secondFinalized.ok) throw new Error(secondFinalized.error.message);
    expect(secondFinalized.record.commit_decided_at_ms).toBeGreaterThan(finalized.record.commit_decided_at_ms);

    await expect(bucket.closeThrough(sealRequest)).resolves.toEqual(sealed);
    const secondSealRequest = {
      ...sealRequest,
      cutoff: secondFinalized.record.commit_decided_at,
      idempotency_key: `seal-${secondTx}`,
    };
    const secondSeal = await bucket.closeThrough(secondSealRequest);
    if (!secondSeal.ok || secondSeal.status !== "complete" || secondSeal.receipt === undefined) {
      throw new Error("Expected the second exact seal to complete.");
    }
    const localRequest = {
      protocol_version: CURRENT_PROTOCOL_VERSION,
      format_version: MANIFEST_PAGE_FORMAT_VERSION,
      fleet_id: first.reservation.fleet_id,
      reservation_utc_day: first.reservation.reservation_utc_day,
      partition: first.reservation.partition,
      partition_count: first.reservation.partition_count,
      routing_key: first.reservation.routing_key,
      partition_config_hash: first.reservation.partition_config_hash,
      coverage_start: first.reservation.reserved_at,
      cutoff: secondFinalized.record.commit_decided_at,
      expected_retention_epoch: secondSeal.receipt.retention_epoch,
      seal_generation: secondSeal.generation,
      seal_receipt_hash: secondSeal.receipt.receipt_hash,
      limit: 1,
      cursor: null,
    } as const;
    const localFirst = await bucket.localPage(localRequest);
    if (!localFirst.ok || localFirst.next_cursor === null) throw new Error("Expected a local cursor.");
    await runInDurableObject(bucket, async (_instance, state) => {
      state.storage.sql.exec(
        `UPDATE manifest_bucket_state
            SET retention_epoch = retention_epoch + 1, records_deleted_through_ms = ?
          WHERE id = 1`,
        new Date(localRequest.coverage_start).getTime() - 1,
      );
    });
    const afterDisjointRetention = await bucket.stats();
    await expect(bucket.localPage({
      ...localRequest,
      expected_retention_epoch: afterDisjointRetention.retention_epoch,
      cursor: localFirst.next_cursor,
    })).resolves.toMatchObject({
      ok: true,
      records: [{ tx_id: secondTx }],
      next_cursor: null,
    });
    await runInDurableObject(bucket, async (_instance, state) => {
      state.storage.sql.exec(
        "INSERT OR REPLACE INTO manifest_alarm_schedule (purpose, fire_at_ms, generation, payload_hash) VALUES (?, 0, 1, '')",
        "seal:1",
      );
    });
    await expect(bucket.closeThrough(sealRequest)).resolves.toEqual(sealed);
    await runInDurableObject(bucket, async (_instance, state) => {
      expect(state.storage.sql.exec<{ count: number }>(
        "SELECT COUNT(*) AS count FROM manifest_alarm_schedule WHERE purpose = 'seal:1'",
      ).one().count).toBe(0);
    });
    await runInDurableObject(bucket, async (instance, state) => {
      expect(state.storage.sql.exec<{ count: number }>(
        "SELECT COUNT(*) AS count FROM manifest_page_cursors",
      ).one().count).toBeGreaterThan(0);
      state.storage.sql.exec("UPDATE manifest_page_cursors SET lease_expires_at_ms = 0");
      state.storage.sql.exec(
        "INSERT OR REPLACE INTO manifest_alarm_schedule (purpose, fire_at_ms, generation, payload_hash) VALUES ('retention', 0, 0, '')",
      );
      await instance.alarm?.();
      expect(state.storage.sql.exec<{ count: number }>(
        "SELECT COUNT(*) AS count FROM manifest_page_cursors",
      ).one().count).toBe(0);
    });
  });

  it("closes every cataloged bucket through an exact fleet cutoff and returns an immutable root", async () => {
    const route = await assigned(`tx-fleet-close-${crypto.randomUUID()}`);
    await route.worker.reserveManifest({ reservation: route.reservation, reservation_hash: route.reservation_hash });
    const intent: ManifestFinalizeIntentV1 = {
      protocol_version: CURRENT_PROTOCOL_VERSION,
      format_version: MANIFEST_TERMINAL_INTENT_FORMAT_VERSION,
      tx_id: route.reservation.tx_id,
      reservation_hash: route.reservation_hash,
      redo_envelope_hash: await hashCanonicalJson({ redo: route.reservation.tx_id }),
      operation_hash: route.operationHash,
      decision_epoch: 1,
      idempotency_key: `finalize-${route.reservation.tx_id}`,
    };
    const finalized = await route.worker.finalizeManifest({
      reservation: route.reservation,
      reservation_hash: route.reservation_hash,
      intent,
    });
    if (!finalized.ok) throw new Error(finalized.error.message);
    const fleetCatalog = env.FLEET_MANIFEST_CATALOG.getByName(`fleet:${route.reservation.fleet_id}`);
    const futureConfigDay = new Date(
      Date.parse(`${route.reservation.reservation_utc_day}T00:00:00.000Z`) + 2 * 86_400_000,
    ).toISOString().slice(0, 10);
    await fleetCatalog.appendPartitionConfig({
      fleet_id: route.reservation.fleet_id,
      effective_from_day: futureConfigDay,
      partition_count: 16,
    });

    const routedBucket = env.JOURNAL_MANIFEST.getByName(await manifestObjectNameForReservation(route.reservation));
    await runInDurableObject(routedBucket, async (_instance, state) => {
      state.storage.sql.exec(
        `INSERT INTO manifest_seal_generations
          (generation, idempotency_key, cutoff_ms, mode, status, digest_count, digest_root, created_at_ms, updated_at_ms)
         VALUES (999, 'foreign-close-in-progress', 0, 'ADVANCE', 'DRAINING', 0, '', 0, 0)`,
      );
    });
    await expect(route.worker.closeFleetThrough({
      fleet_id: route.reservation.fleet_id,
      cutoff: finalized.record.commit_decided_at,
    })).resolves.toMatchObject({ ok: true, status: "pending" });
    await runInDurableObject(routedBucket, async (_instance, state) => {
      state.storage.sql.exec("DELETE FROM manifest_seal_generations WHERE generation = 999");
    });

    let completed = await route.worker.closeFleetThrough({
      fleet_id: route.reservation.fleet_id,
      cutoff: finalized.record.commit_decided_at,
    });
    for (let attempt = 0; attempt < 10 && completed.ok && completed.status === "pending"; attempt += 1) {
      completed = await route.worker.closeFleetThrough({
          fleet_id: route.reservation.fleet_id,
          cutoff: finalized.record.commit_decided_at,
        });
    }
    expect(completed).toMatchObject({
      ok: true,
      status: "complete",
      completed_buckets: 16,
      total_buckets: 16,
      snapshot_hash: expect.stringMatching(/^[a-f0-9]{64}$/),
      fleet_root_hash: expect.stringMatching(/^[a-f0-9]{64}$/),
    });
    await expect(route.worker.closeFleetThrough({
      fleet_id: route.reservation.fleet_id,
      cutoff: finalized.record.commit_decided_at,
    })).resolves.toEqual(completed);

    if (!("status" in completed) || completed.status !== "complete") {
      throw new Error("Fleet close did not complete.");
    }
    const coverageState = await fleetCatalog.coverageState(route.reservation.fleet_id);
    if (coverageState.reservation_required_since_ms === null) throw new Error("Expected a V2 coverage boundary.");
    const coverageStart = new Date(coverageState.reservation_required_since_ms).toISOString();
    const enumeration = await route.worker.enumerateManifest({
      protocol_version: CURRENT_PROTOCOL_VERSION,
      format_version: MANIFEST_ENUMERATION_FORMAT_VERSION,
      fleet_id: route.reservation.fleet_id,
      coverage_start: coverageStart,
      cutoff: finalized.record.commit_decided_at,
      partition_config_hash: route.reservation.partition_config_hash,
      catalog_generation: completed.snapshot_generation,
      catalog_snapshot_hash: completed.snapshot_hash,
      limit: 10,
      cursor: null,
    });
    expect(enumeration).toMatchObject({
      coverage: "incomplete",
      complete: false,
      records: [{ tx_id: route.reservation.tx_id }],
      next_cursor: expect.objectContaining({ local_cursor: null }),
      diagnostics: { inspected_buckets: expect.any(Number), incomplete_buckets: 1, returned_records: 1 },
    });
    if ("ok" in enumeration) throw new Error(enumeration.error.message);
    expect(enumeration.evidence.length).toBeGreaterThan(0);
    expect(enumeration.evidence[0].seal_receipt_hash).toMatch(/^[a-f0-9]{64}$/);
    const completedEnumeration = await route.worker.enumerateManifest({
      protocol_version: CURRENT_PROTOCOL_VERSION,
      format_version: MANIFEST_ENUMERATION_FORMAT_VERSION,
      fleet_id: route.reservation.fleet_id,
      coverage_start: coverageStart,
      cutoff: finalized.record.commit_decided_at,
      partition_config_hash: route.reservation.partition_config_hash,
      catalog_generation: completed.snapshot_generation,
      catalog_snapshot_hash: completed.snapshot_hash,
      limit: 10,
      cursor: enumeration.next_cursor,
    });
    expect(completedEnumeration).toMatchObject({
      coverage: "complete",
      complete: true,
      records: [],
      next_cursor: null,
    });

    const beforeReservationBoundary = await route.worker.enumerateManifest({
      protocol_version: CURRENT_PROTOCOL_VERSION,
      format_version: MANIFEST_ENUMERATION_FORMAT_VERSION,
      fleet_id: route.reservation.fleet_id,
      coverage_start: new Date(coverageState.reservation_required_since_ms - 1).toISOString(),
      cutoff: finalized.record.commit_decided_at,
      partition_config_hash: route.reservation.partition_config_hash,
      catalog_generation: completed.snapshot_generation,
      catalog_snapshot_hash: completed.snapshot_hash,
      limit: 10,
      cursor: null,
    });
    expect(beforeReservationBoundary).toMatchObject({
      coverage: "unproven_legacy_window",
      complete: false,
    });

    const paged = await route.worker.enumerateManifest({
      protocol_version: CURRENT_PROTOCOL_VERSION,
      format_version: MANIFEST_ENUMERATION_FORMAT_VERSION,
      fleet_id: route.reservation.fleet_id,
      coverage_start: coverageStart,
      cutoff: finalized.record.commit_decided_at,
      partition_config_hash: route.reservation.partition_config_hash,
      catalog_generation: completed.snapshot_generation,
      catalog_snapshot_hash: completed.snapshot_hash,
      limit: 1,
      cursor: null,
    });
    if ("ok" in paged || paged.next_cursor === null) throw new Error("Expected a catalog-issued cursor.");
    const forgedCursor = {
      ...paged.next_cursor,
      partition: paged.next_cursor.partition === 15 ? 14 : paged.next_cursor.partition + 1,
    };
    await expect(route.worker.enumerateManifest({
      protocol_version: CURRENT_PROTOCOL_VERSION,
      format_version: MANIFEST_ENUMERATION_FORMAT_VERSION,
      fleet_id: route.reservation.fleet_id,
      coverage_start: coverageStart,
      cutoff: finalized.record.commit_decided_at,
      partition_config_hash: route.reservation.partition_config_hash,
      catalog_generation: completed.snapshot_generation,
      catalog_snapshot_hash: completed.snapshot_hash,
      limit: 1,
      cursor: forgedCursor,
    })).resolves.toMatchObject({
      ok: false,
      status: "rejected",
      error: { code: "MANIFEST_CURSOR_MISMATCH" },
    });
  });
});
