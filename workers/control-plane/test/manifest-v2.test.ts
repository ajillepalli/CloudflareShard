import { createExecutionContext, env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
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

    const authorizationSourceHash = await hashCanonicalJson(cancelIntent);
    const coordinatorState = {
      tx_id: assignedRoute.reservation.tx_id,
      coordinator_id: assignedRoute.reservation.coordinator_id,
      state: "aborted_pending_manifest_cancel",
      decision: "abort",
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
        "INSERT OR REPLACE INTO manifest_alarm_schedule (purpose, fire_at_ms, generation, payload_hash) VALUES ('retention', 0, 0, '')",
      );
      await instance.alarm?.();
    });
    await expect(bucket.stats()).resolves.toMatchObject({ v2_reservations: 0 });
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
    await expect(bucket.localPage({ ...localRequest, cursor: localFirst.next_cursor })).resolves.toMatchObject({
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
    const enumeration = await route.worker.enumerateManifest({
      protocol_version: CURRENT_PROTOCOL_VERSION,
      format_version: MANIFEST_ENUMERATION_FORMAT_VERSION,
      fleet_id: route.reservation.fleet_id,
      coverage_start: route.reservation.reserved_at,
      cutoff: finalized.record.commit_decided_at,
      partition_config_hash: route.reservation.partition_config_hash,
      catalog_generation: completed.snapshot_generation,
      catalog_snapshot_hash: completed.snapshot_hash,
      conflict_resolution_root: "0".repeat(64),
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
      coverage_start: route.reservation.reserved_at,
      cutoff: finalized.record.commit_decided_at,
      partition_config_hash: route.reservation.partition_config_hash,
      catalog_generation: completed.snapshot_generation,
      catalog_snapshot_hash: completed.snapshot_hash,
      conflict_resolution_root: "0".repeat(64),
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
      coverage_start: new Date(new Date(route.reservation.reserved_at).getTime() - 1).toISOString(),
      cutoff: finalized.record.commit_decided_at,
      partition_config_hash: route.reservation.partition_config_hash,
      catalog_generation: completed.snapshot_generation,
      catalog_snapshot_hash: completed.snapshot_hash,
      conflict_resolution_root: "0".repeat(64),
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
      coverage_start: route.reservation.reserved_at,
      cutoff: finalized.record.commit_decided_at,
      partition_config_hash: route.reservation.partition_config_hash,
      catalog_generation: completed.snapshot_generation,
      catalog_snapshot_hash: completed.snapshot_hash,
      conflict_resolution_root: "0".repeat(64),
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
      coverage_start: route.reservation.reserved_at,
      cutoff: finalized.record.commit_decided_at,
      partition_config_hash: route.reservation.partition_config_hash,
      catalog_generation: completed.snapshot_generation,
      catalog_snapshot_hash: completed.snapshot_hash,
      conflict_resolution_root: "0".repeat(64),
      limit: 1,
      cursor: forgedCursor,
    })).resolves.toMatchObject({
      ok: false,
      status: "rejected",
      error: { code: "MANIFEST_CURSOR_MISMATCH" },
    });
  });
});
