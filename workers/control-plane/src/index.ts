import { WorkerEntrypoint } from "cloudflare:workers";
import {
  CURRENT_PROTOCOL_VERSION,
  MANIFEST_CURSOR_FORMAT_VERSION,
  MANIFEST_ENUMERATION_FORMAT_VERSION,
  MANIFEST_PAGE_FORMAT_VERSION,
  MANIFEST_SEAL_FORMAT_VERSION,
  TransactionContractViolation,
  hashCanonicalJson,
  hashManifestRequest,
  assertManifestCursorMatchesRequest,
  validateManifestEnumerationRequest,
  type ManifestEnumerationRequestV1,
  type ManifestEnumerationResultV1,
  type ManifestRecordV2,
  transactionError,
  type ManifestRegistrationV1,
} from "../../../packages/contracts/src/index.js";
import {
  type ManifestAdmissionResult,
  manifestError,
  toManifestRpcError,
  type ManifestLookupRequest,
  type ManifestReleaseRequest,
  type ManifestReleaseResult,
  type ManifestServiceLookupResult,
  type ManifestServiceRegisterResult,
  type ManifestCancelRequestV1,
  type ManifestCancelResult,
  type ManifestFinalizeRequestV1,
  type ManifestFinalizeResult,
  type ManifestFleetCloseRequestV1,
  type ManifestFleetCloseResult,
  type ManifestFleetEnumerationServiceResult,
  type ManifestReserveRequestV1,
  type ManifestReserveResult,
  type ManifestRouteAssignmentRequestV1,
  type ManifestRouteAssignmentResult,
  type ManifestV2ReleaseRequest,
  type ManifestQuarantineResolutionRequestV1,
  type ManifestQuarantineResolutionResult,
} from "./manifest-types.js";
import {
  CIRCUIT_DIRECTIVE,
  admissionThroughManifestStub,
  asProtocolError,
  lookupThroughManifestStub,
  manifestRegistrationTxId,
  manifestObjectNameForRegistration,
  manifestObjectName,
  manifestObjectNameForRoute,
  manifestObjectNameForReservation,
  registerThroughManifestStub,
  validatedManifestRegistration,
} from "./service.js";
import type { JournalManifestDO } from "./journal-manifest.js";
import type { FleetManifestCatalogDO } from "./fleet-manifest-catalog.js";

export {
  JournalManifestDO,
  LIFECYCLE_FAILURE_RETRY_MS,
  executeLifecycleAlarm,
} from "./journal-manifest.js";
export {
  FleetManifestCatalogDO,
  FleetManifestCatalogStore,
} from "./fleet-manifest-catalog.js";
export type {
  CatalogActivationRequest,
  CatalogActivationResult,
  CatalogAlarmPurpose,
  CatalogCloseOperation,
  CatalogCloseProgressEntry,
  CatalogEnumerationEntry,
  CatalogSnapshotEntry,
  CatalogSnapshotRequest,
  CatalogSnapshotResult,
} from "./fleet-manifest-catalog.js";
export * from "./manifest-types.js";
export * from "./service.js";

export interface ControlPlaneEnv {
  JOURNAL_MANIFEST: DurableObjectNamespace<JournalManifestDO>;
  FLEET_MANIFEST_CATALOG: DurableObjectNamespace<FleetManifestCatalogDO>;
}

function log(level: "info" | "warn" | "error", event: string, fields: Readonly<Record<string, unknown>>): void {
  const entry = JSON.stringify({ level, event, timestamp: new Date().toISOString(), ...fields });
  if (level === "error") console.error(entry);
  else if (level === "warn") console.warn(entry);
  else console.log(entry);
}

export default class ControlPlaneWorker extends WorkerEntrypoint<ControlPlaneEnv> {
  async fetch(): Promise<Response> {
    // This Worker is deliberately unreachable from the public data plane.
    // The fetch handler exists only because unnamed default RPC entrypoints
    // currently require one; workers_dev, preview URLs, and routes stay off.
    return new Response("Not Found", { status: 404 });
  }

  async assignManifestRoute(request: ManifestRouteAssignmentRequestV1): Promise<ManifestRouteAssignmentResult> {
    try {
      if (request === null || typeof request !== "object" || Array.isArray(request)) {
        throw new TransactionContractViolation(transactionError("MANIFEST_INVALID_REQUEST", "Route assignment must be an object."));
      }
      const { draft, idempotency_key: idempotencyKey } = request;
      if (
        draft === null || typeof draft !== "object" || Array.isArray(draft)
        || Object.keys(draft).sort().join(",") !== "coordinator_id,decision_epoch,fleet_id,operation_hash,tx_id"
        || typeof draft.fleet_id !== "string" || draft.fleet_id.length === 0
        || typeof draft.tx_id !== "string" || draft.tx_id.length === 0
        || typeof draft.coordinator_id !== "string" || draft.coordinator_id.length === 0
        || typeof draft.operation_hash !== "string" || !/^[a-f0-9]{64}$/.test(draft.operation_hash)
        || !Number.isSafeInteger(draft.decision_epoch) || draft.decision_epoch < 1
        || typeof idempotencyKey !== "string" || idempotencyKey.length === 0
      ) {
        throw new TransactionContractViolation(transactionError("MANIFEST_INVALID_REQUEST", "Route assignment fields are invalid."));
      }
      const catalog = this.env.FLEET_MANIFEST_CATALOG.getByName(`fleet:${draft.fleet_id}`);
      const assignment = await catalog.assignManifestRoute(draft, idempotencyKey);
      return { ok: true, ...assignment };
    } catch (error) {
      const protocolError = asProtocolError(error);
      return {
        ok: false,
        status: protocolError.retryable ? "unavailable" : "rejected",
        http_status: protocolError.http_status,
        error: protocolError,
      };
    }
  }

  async reserveManifest(request: ManifestReserveRequestV1): Promise<ManifestReserveResult> {
    try {
      const objectName = await manifestObjectNameForReservation(request.reservation, request.reservation_hash);
      const reservation = request.reservation;
      const catalog = this.env.FLEET_MANIFEST_CATALOG.getByName(`fleet:${reservation.fleet_id}`);
      const activation = await catalog.activateBucket({
        protocol_version: 2,
        fleet_id: reservation.fleet_id,
        reservation_day: reservation.reservation_utc_day,
        partition: reservation.partition,
        partition_count: reservation.partition_count,
        partition_config_hash: reservation.partition_config_hash,
        activation_key: request.reservation_hash,
      });
      const bucket = this.env.JOURNAL_MANIFEST.getByName(objectName);
      return await bucket.reserve(
        reservation,
        request.reservation_hash,
        activation.required_decision_floor_ms,
      );
    } catch (error) {
      const protocolError = asProtocolError(error);
      return {
        ok: false,
        status: protocolError.retryable ? "unavailable" : "rejected_absent",
        bucket_row_may_exist: protocolError.retryable,
        http_status: protocolError.http_status,
        error: protocolError,
      };
    }
  }

  async finalizeManifest(request: ManifestFinalizeRequestV1): Promise<ManifestFinalizeResult> {
    try {
      const objectName = await manifestObjectNameForReservation(request.reservation, request.reservation_hash);
      if (request.intent.reservation_hash !== request.reservation_hash || request.intent.tx_id !== request.reservation.tx_id) {
        throw new TransactionContractViolation(transactionError("MANIFEST_TERMINAL_CONFLICT", "Finalize request does not identify its frozen reservation."));
      }
      return await this.env.JOURNAL_MANIFEST.getByName(objectName).finalize(request.intent);
    } catch (error) {
      const protocolError = asProtocolError(error);
      return {
        ok: false,
        status: protocolError.retryable ? "unavailable" : "conflict",
        http_status: protocolError.http_status,
        error: protocolError,
      };
    }
  }

  async cancelManifest(request: ManifestCancelRequestV1): Promise<ManifestCancelResult> {
    try {
      const objectName = await manifestObjectNameForReservation(request.reservation, request.reservation_hash);
      if (request.intent.reservation_hash !== request.reservation_hash || request.intent.tx_id !== request.reservation.tx_id) {
        throw new TransactionContractViolation(transactionError("MANIFEST_TERMINAL_CONFLICT", "Cancel request does not identify its frozen reservation."));
      }
      return await this.env.JOURNAL_MANIFEST.getByName(objectName).cancel(request.intent);
    } catch (error) {
      const protocolError = asProtocolError(error);
      return {
        ok: false,
        status: protocolError.retryable ? "unavailable" : "conflict",
        http_status: protocolError.http_status,
        error: protocolError,
      };
    }
  }

  async resolveManifestQuarantine(
    request: ManifestQuarantineResolutionRequestV1,
  ): Promise<ManifestQuarantineResolutionResult> {
    try {
      const objectName = await manifestObjectNameForReservation(request.reservation, request.reservation_hash);
      return await this.env.JOURNAL_MANIFEST.getByName(objectName).resolveQuarantine(request);
    } catch (error) {
      const protocolError = asProtocolError(error);
      return {
        ok: false,
        status: protocolError.retryable ? "unavailable" : "conflict",
        http_status: protocolError.http_status,
        error: protocolError,
      };
    }
  }

  async releaseManifestV2(request: ManifestV2ReleaseRequest): Promise<ManifestReleaseResult> {
    try {
      const objectName = await manifestObjectNameForReservation(request.reservation, request.reservation_hash);
      return await this.env.JOURNAL_MANIFEST.getByName(objectName).releaseV2(
        request.reservation.tx_id,
        request.reservation_hash,
        request.record_hash,
      );
    } catch (error) {
      const protocolError = asProtocolError(error);
      return {
        ok: false,
        status: protocolError.retryable ? "unavailable" : "rejected",
        http_status: protocolError.http_status,
        error: protocolError,
      };
    }
  }

  async closeFleetThrough(request: ManifestFleetCloseRequestV1): Promise<ManifestFleetCloseResult> {
    try {
      if (
        request === null || typeof request !== "object" || Array.isArray(request)
        || Object.keys(request).sort().join(",") !== "cutoff,fleet_id"
        || typeof request.fleet_id !== "string" || request.fleet_id.length === 0
        || typeof request.cutoff !== "string"
      ) {
        throw new TransactionContractViolation(transactionError("MANIFEST_INVALID_REQUEST", "Fleet close request is invalid."));
      }
      const parsedCutoff = new Date(request.cutoff);
      if (!Number.isFinite(parsedCutoff.getTime()) || parsedCutoff.toISOString() !== request.cutoff) {
        throw new TransactionContractViolation(transactionError("MANIFEST_INVALID_REQUEST", "Fleet close cutoff must be canonical UTC milliseconds."));
      }
      const cutoffMs = parsedCutoff.getTime();
      if (cutoffMs > Date.now()) {
        throw new TransactionContractViolation(transactionError("MANIFEST_FUTURE_CUTOFF", "Fleet close cutoff cannot be in the future."));
      }
      const catalog = this.env.FLEET_MANIFEST_CATALOG.getByName(`fleet:${request.fleet_id}`);
      const coverageState = await catalog.coverageState(request.fleet_id);
      if (coverageState.reservation_required_since_day === null) {
        throw new TransactionContractViolation(
          transactionError("MANIFEST_UNPROVEN_LEGACY_WINDOW", "Fleet has no durable reservation coverage boundary."),
        );
      }
      const firstUnscannedDay = coverageState.legacy_scanned_through_day === null
        ? coverageState.reservation_required_since_day
        : new Date(Date.parse(`${coverageState.legacy_scanned_through_day}T00:00:00.000Z`) + 86_400_000)
            .toISOString().slice(0, 10);
      const cutoffDay = request.cutoff.slice(0, 10);
      const firstUnmaterializedDay = coverageState.legacy_grid_materialized_through_day === null
        ? coverageState.reservation_required_since_day
        : new Date(Date.parse(`${coverageState.legacy_grid_materialized_through_day}T00:00:00.000Z`) + 86_400_000)
            .toISOString().slice(0, 10);
      const daysToMaterialize: string[] = [];
      for (
        let day = firstUnmaterializedDay;
        day <= cutoffDay;
        day = new Date(Date.parse(`${day}T00:00:00.000Z`) + 86_400_000).toISOString().slice(0, 10)
      ) {
        if (daysToMaterialize.length === 36) break;
        daysToMaterialize.push(day);
      }
      for (const day of daysToMaterialize) {
        const dayConfig = await catalog.partitionConfigForDay(request.fleet_id, day);
        for (let partition = 0; partition < dayConfig.partition_count; partition += 1) {
          await catalog.activateBucket({
            protocol_version: 2,
            fleet_id: request.fleet_id,
            reservation_day: day,
            partition,
            partition_count: dayConfig.partition_count,
            partition_config_hash: dayConfig.config_hash,
            activation_key: await hashCanonicalJson(["legacy-grid", request.fleet_id, day, partition, dayConfig.config_hash]),
          });
        }
        await catalog.advanceLegacyGridMaterializedThrough(request.fleet_id, day);
      }
      if (daysToMaterialize.at(-1) !== undefined && daysToMaterialize.at(-1)! < cutoffDay) {
        return {
          ok: true,
          status: "pending",
          cutoff_ms: cutoffMs,
          snapshot_generation: coverageState.current_snapshot_generation,
          completed_buckets: 0,
          total_buckets: 0,
        };
      }
      const config = await catalog.partitionConfigForDay(request.fleet_id, request.cutoff.slice(0, 10));
      const snapshotKey = await hashCanonicalJson([
        "catalog-snapshot",
        request.fleet_id,
        cutoffMs,
        config.config_hash,
      ]);
      const snapshot = await catalog.snapshotThrough({
        protocol_version: 2,
        fleet_id: request.fleet_id,
        cutoff_ms: cutoffMs,
        idempotency_key: snapshotKey,
      });
      if (snapshot.status !== "complete" || snapshot.snapshot_hash === null) {
        return {
          ok: true,
          status: "pending",
          cutoff_ms: cutoffMs,
          snapshot_generation: snapshot.generation,
          completed_buckets: 0,
          total_buckets: snapshot.entry_count,
        };
      }
      let operation = await catalog.beginClose({
        fleet_id: request.fleet_id,
        cutoff_ms: cutoffMs,
        snapshot_generation: snapshot.generation,
      });
      operation = await catalog.materializeCloseProgress(operation.close_key, 128);
      const progress = await catalog.closeProgress(operation.close_key, 0, 128);
      for (const entry of progress) {
        if (entry.status === "complete") continue;
        const routingKey = `${entry.reservation_day}:${entry.partition.toString().padStart(2, "0")}`;
        const sealRequest = {
          protocol_version: CURRENT_PROTOCOL_VERSION,
          format_version: MANIFEST_SEAL_FORMAT_VERSION,
          fleet_id: request.fleet_id,
          reservation_utc_day: entry.reservation_day,
          partition: entry.partition,
          partition_count: entry.partition_count,
          routing_key: routingKey,
          partition_config_hash: entry.partition_config_hash,
          cutoff: request.cutoff,
          idempotency_key: entry.bucket_close_key,
        };
        const bucket = this.env.JOURNAL_MANIFEST.getByName(
          await manifestObjectName(request.fleet_id, entry.reservation_day, entry.partition),
        );
        await bucket.initializeForSeal(sealRequest, snapshot.decision_floor_ms);
        const sealed = await bucket.closeThrough(sealRequest);
        if (!sealed.ok) {
          return {
            ok: false,
            status: sealed.status === "quarantined" ? "quarantined" : sealed.status === "unavailable" ? "unavailable" : "rejected",
            http_status: sealed.http_status,
            error: sealed.error,
          };
        }
        if (sealed.status !== "complete" || sealed.receipt === undefined) {
          return {
            ok: true,
            status: "pending",
            cutoff_ms: cutoffMs,
            snapshot_generation: snapshot.generation,
            completed_buckets: operation.completed_entries,
            total_buckets: operation.total_entries,
          };
        }
        operation = await catalog.recordCloseReceipt({
          close_key: operation.close_key,
          activation_sequence: entry.activation_sequence,
          exact_receipt_hash: sealed.receipt.receipt_hash,
        });
      }
      operation = await catalog.finalizeClose(operation.close_key);
      if (operation.status !== "complete" || operation.fleet_root_hash === null) {
        return {
          ok: true,
          status: "pending",
          cutoff_ms: cutoffMs,
          snapshot_generation: snapshot.generation,
          completed_buckets: operation.completed_entries,
          total_buckets: operation.total_entries,
        };
      }
      const daysToCertify: string[] = [];
      for (
        let day = firstUnscannedDay;
        day <= cutoffDay && daysToCertify.length < 36;
        day = new Date(Date.parse(`${day}T00:00:00.000Z`) + 86_400_000).toISOString().slice(0, 10)
      ) {
        daysToCertify.push(day);
      }
      for (const day of daysToCertify) {
        await catalog.advanceLegacyScannedThrough(request.fleet_id, day, operation.close_key);
      }
      if (daysToCertify.at(-1) !== undefined && daysToCertify.at(-1)! < cutoffDay) {
        return {
          ok: true,
          status: "pending",
          cutoff_ms: cutoffMs,
          snapshot_generation: snapshot.generation,
          completed_buckets: operation.completed_entries,
          total_buckets: operation.total_entries,
        };
      }
      return {
        ok: true,
        status: "complete",
        cutoff_ms: cutoffMs,
        snapshot_generation: snapshot.generation,
        snapshot_hash: operation.snapshot_hash,
        fleet_root_hash: operation.fleet_root_hash,
        completed_buckets: operation.completed_entries,
        total_buckets: operation.total_entries,
      };
    } catch (error) {
      const protocolError = asProtocolError(error);
      return {
        ok: false,
        status: protocolError.retryable ? "unavailable" : "rejected",
        http_status: protocolError.http_status,
        error: protocolError,
      };
    }
  }

  async enumerateManifest(input: unknown): Promise<ManifestFleetEnumerationServiceResult> {
    try {
      validateManifestEnumerationRequest(input);
      const request: ManifestEnumerationRequestV1 = input;
      const requestHash = await hashManifestRequest(request);
      if (request.cursor !== null) assertManifestCursorMatchesRequest(request.cursor, requestHash);
      const catalog = this.env.FLEET_MANIFEST_CATALOG.getByName(`fleet:${request.fleet_id}`);
      type PriorEvidencePin = {
        reservation_day: string;
        partition: number;
        retention_epoch: number;
        seal_receipt_hash: string;
        lease_expires_at_ms: number;
      };
      const priorEvidencePins = request.cursor === null
        ? []
        : await catalog.enumerationCursorEvidence(request.cursor) as PriorEvidencePin[] | null;
      if (priorEvidencePins === null) {
        throw new TransactionContractViolation(
          transactionError("MANIFEST_CURSOR_MISMATCH", "Enumeration cursor was not issued by the fleet catalog."),
        );
      }
      for (const pin of priorEvidencePins) {
        if (!Number.isSafeInteger(pin.lease_expires_at_ms) || pin.lease_expires_at_ms <= Date.now()) {
          return {
            protocol_version: CURRENT_PROTOCOL_VERSION,
            format_version: MANIFEST_ENUMERATION_FORMAT_VERSION,
            request_hash: requestHash,
            coverage: "retention_expired",
            complete: false,
            records: [],
            evidence: [],
            next_cursor: null,
            diagnostics: { inspected_buckets: 0, incomplete_buckets: 1, returned_records: 0 },
          };
        }
      }
      const coverageState = await catalog.coverageState(request.fleet_id);
      if (
        coverageState.reservation_required_since_day === null
        || coverageState.reservation_required_since_ms === null
        || new Date(request.coverage_start).getTime() < coverageState.reservation_required_since_ms
        || coverageState.legacy_scanned_through_day === null
        || request.cutoff.slice(0, 10) > coverageState.legacy_scanned_through_day
      ) {
        return {
          protocol_version: CURRENT_PROTOCOL_VERSION,
          format_version: MANIFEST_ENUMERATION_FORMAT_VERSION,
          request_hash: requestHash,
          coverage: "unproven_legacy_window",
          complete: false,
          records: [],
          evidence: [],
          next_cursor: null,
          diagnostics: { inspected_buckets: 0, incomplete_buckets: 1, returned_records: 0 },
        };
      }
      const snapshot = await catalog.snapshotByGeneration(request.catalog_generation);
      const close = await catalog.closeForSnapshot(request.catalog_generation);
      if (
        snapshot.status !== "complete"
        || snapshot.cutoff_ms !== new Date(request.cutoff).getTime()
        || snapshot.snapshot_hash !== request.catalog_snapshot_hash
        || snapshot.partition_config_root_hash !== request.partition_config_hash
        || close === null
        || close.status !== "complete"
        || close.snapshot_hash !== request.catalog_snapshot_hash
      ) {
        return {
          protocol_version: CURRENT_PROTOCOL_VERSION,
          format_version: MANIFEST_ENUMERATION_FORMAT_VERSION,
          request_hash: requestHash,
          coverage: "incomplete",
          complete: false,
          records: [],
          evidence: [],
          next_cursor: null,
          diagnostics: { inspected_buckets: 0, incomplete_buckets: 1, returned_records: 0 },
        };
      }
      if (request.conflict_resolution_root !== "0".repeat(64)) {
        throw new TransactionContractViolation(
          transactionError("MANIFEST_CURSOR_MISMATCH", "Requested conflict-resolution root is not the current catalog root."),
        );
      }
      const cursor = request.cursor;
      const afterDay = cursor?.reservation_utc_day ?? "";
      const afterPartition = cursor === null
        ? -1
        : cursor.local_cursor === null
          ? cursor.partition
          : cursor.partition - 1;
      const entries = await catalog.enumerationEntries(
        request.catalog_generation,
        afterDay,
        afterPartition,
        128,
      );
      const records: ManifestRecordV2[] = [];
      const evidence: ManifestEnumerationResultV1["evidence"][number][] = [];
      const newEvidencePins: PriorEvidencePin[] = [];
      let inspectedBuckets = 0;
      let nextCursor: ManifestEnumerationResultV1["next_cursor"] = null;
      for (const entry of entries) {
        if (entry.partition_count !== 16) {
          throw new TransactionContractViolation(
            transactionError("MANIFEST_VERSION_UNSUPPORTED", "Catalog entry uses an unsupported partition count."),
          );
        }
        if (entry.exact_receipt_hash === null) {
          return {
            protocol_version: CURRENT_PROTOCOL_VERSION,
            format_version: MANIFEST_ENUMERATION_FORMAT_VERSION,
            request_hash: requestHash,
            coverage: "incomplete",
            complete: false,
            records,
            evidence,
            next_cursor: null,
            diagnostics: { inspected_buckets: inspectedBuckets, incomplete_buckets: 1, returned_records: records.length },
          };
        }
        const bucket = this.env.JOURNAL_MANIFEST.getByName(
          await manifestObjectName(request.fleet_id, entry.reservation_day, entry.partition),
        );
        const receipt = await bucket.sealReceipt(entry.exact_receipt_hash);
        if (receipt === null) {
          return {
            protocol_version: CURRENT_PROTOCOL_VERSION,
            format_version: MANIFEST_ENUMERATION_FORMAT_VERSION,
            request_hash: requestHash,
            coverage: "incomplete",
            complete: false,
            records,
            evidence,
            next_cursor: null,
            diagnostics: { inspected_buckets: inspectedBuckets, incomplete_buckets: 1, returned_records: records.length },
          };
        }
        const stats = await bucket.stats();
        const localCursor = cursor !== null
          && cursor.reservation_utc_day === entry.reservation_day
          && cursor.partition === entry.partition
          ? cursor.local_cursor
          : null;
        const local = await bucket.localPage({
          protocol_version: CURRENT_PROTOCOL_VERSION,
          format_version: MANIFEST_PAGE_FORMAT_VERSION,
          fleet_id: request.fleet_id,
          reservation_utc_day: entry.reservation_day,
          partition: entry.partition,
          partition_count: entry.partition_count,
          routing_key: `${entry.reservation_day}:${entry.partition.toString().padStart(2, "0")}`,
          partition_config_hash: entry.partition_config_hash,
          coverage_start: request.coverage_start,
          cutoff: request.cutoff,
          expected_retention_epoch: stats.retention_epoch,
          seal_generation: receipt.generation,
          seal_receipt_hash: receipt.receipt_hash,
          limit: request.limit,
          cursor: localCursor,
        });
        inspectedBuckets += 1;
        if (!local.ok) {
          const coverage = local.status === "retention_expired"
            ? "retention_expired"
            : local.status === "quarantined"
              ? "quarantined"
              : "incomplete";
          return {
            protocol_version: CURRENT_PROTOCOL_VERSION,
            format_version: MANIFEST_ENUMERATION_FORMAT_VERSION,
            request_hash: requestHash,
            coverage,
            complete: false,
            records,
            evidence,
            next_cursor: null,
            diagnostics: { inspected_buckets: inspectedBuckets, incomplete_buckets: 1, returned_records: records.length },
          };
        }
        records.push(...local.records);
        evidence.push({
          protocol_version: CURRENT_PROTOCOL_VERSION,
          format_version: MANIFEST_ENUMERATION_FORMAT_VERSION,
          reservation_utc_day: entry.reservation_day,
          partition: entry.partition,
          partition_count: entry.partition_count,
          routing_key: `${entry.reservation_day}:${entry.partition.toString().padStart(2, "0")}`,
          partition_config_hash: entry.partition_config_hash,
          cutoff: request.cutoff,
          seal_generation: receipt.generation,
          seal_receipt_hash: receipt.receipt_hash,
          retention_epoch: local.retention_epoch,
          records_deleted_through_ms: local.records_deleted_through_ms,
          local_legacy_certificate_hash: receipt.local_legacy_certificate_hash,
        });
        newEvidencePins.push({
          reservation_day: entry.reservation_day,
          partition: entry.partition,
          retention_epoch: local.retention_epoch,
          seal_receipt_hash: receipt.receipt_hash,
          lease_expires_at_ms: local.lease_expires_at_ms,
        });
        if (local.next_cursor !== null || local.records.length > 0) {
          nextCursor = {
            protocol_version: CURRENT_PROTOCOL_VERSION,
            format_version: MANIFEST_CURSOR_FORMAT_VERSION,
            request_hash: requestHash,
            catalog_generation: request.catalog_generation,
            catalog_snapshot_hash: request.catalog_snapshot_hash,
            conflict_resolution_root: request.conflict_resolution_root,
            reservation_utc_day: entry.reservation_day,
            partition: entry.partition,
            local_cursor: local.next_cursor,
          };
          break;
        }
      }
      if (nextCursor === null && entries.length === 128) {
        const lastEntry = entries.at(-1)!;
        nextCursor = {
          protocol_version: CURRENT_PROTOCOL_VERSION,
          format_version: MANIFEST_CURSOR_FORMAT_VERSION,
          request_hash: requestHash,
          catalog_generation: request.catalog_generation,
          catalog_snapshot_hash: request.catalog_snapshot_hash,
          conflict_resolution_root: request.conflict_resolution_root,
          reservation_utc_day: lastEntry.reservation_day,
          partition: lastEntry.partition,
          local_cursor: null,
        };
      }
      if (nextCursor !== null) {
        await catalog.issueEnumerationCursor(nextCursor, [
          ...priorEvidencePins,
          ...newEvidencePins,
        ]);
      }
      const exhaustedCatalog = nextCursor === null && entries.length < 128;
      return {
        protocol_version: CURRENT_PROTOCOL_VERSION,
        format_version: MANIFEST_ENUMERATION_FORMAT_VERSION,
        request_hash: requestHash,
        coverage: exhaustedCatalog ? "complete" : "incomplete",
        complete: exhaustedCatalog,
        records,
        evidence,
        next_cursor: nextCursor,
        diagnostics: {
          inspected_buckets: inspectedBuckets,
          incomplete_buckets: exhaustedCatalog ? 0 : 1,
          returned_records: records.length,
        },
      };
    } catch (error) {
      const protocolError = asProtocolError(error);
      return {
        ok: false,
        status: protocolError.retryable ? "unavailable" : "rejected",
        http_status: protocolError.http_status,
        error: protocolError,
      };
    }
  }

  async registerManifest(registration: unknown): Promise<ManifestServiceRegisterResult> {
    let validated: ManifestRegistrationV1 | undefined;
    try {
      validated = await validatedManifestRegistration(registration);
      if (new Date(validated.record.commit_decided_at).getTime() > Date.now()) {
        return {
          ok: false,
          status: "rejected",
          http_status: 400,
          error: manifestError("TX_ENVELOPE_INVALID", "Legacy manifest decision time cannot be in the future."),
        };
      }
      const objectName = await manifestObjectNameForRegistration(validated);
      const legacyCatalog = this.env.FLEET_MANIFEST_CATALOG.getByName(`fleet:${validated.record.fleet_id}`);
      const legacyConfig = await legacyCatalog.partitionConfigForDay(
        validated.record.fleet_id,
        validated.record.utc_day,
      );
      const legacyAdmission = await legacyCatalog.admitLegacyRegistration({
        fleet_id: validated.record.fleet_id,
        reservation_day: validated.record.utc_day,
        partition: validated.record.partition,
        partition_count: validated.record.partition_count,
        partition_config_hash: legacyConfig.config_hash,
        record_hash: validated.record_hash,
      });
      if (!legacyAdmission.ok) {
        return {
          ok: false,
          status: "rejected",
          http_status: 409,
          error: manifestError("V1_CLOSED", "Legacy manifest admission is permanently fenced; use the V2 bridge."),
        };
      }
      const stub = this.env.JOURNAL_MANIFEST.getByName(objectName);
      const result = await registerThroughManifestStub(
        { register: async (value) => await stub.register(value) },
        validated,
      );
      if (result.status === "commit_pending_manifest") {
        log("warn", "control_plane.manifest_registration_ambiguous", {
          tx_id: validated.record.tx_id,
          routing_key: validated.record.routing_key,
        });
      }
      return result;
    } catch (error) {
      const protocolError = asProtocolError(error);
      log(protocolError.code === "TX_MANIFEST_UNAVAILABLE" ? "error" : "warn", "control_plane.manifest_registration_rejected", {
        tx_id: manifestRegistrationTxId(registration),
        code: protocolError.code,
      });
      if (protocolError.code === "TX_MANIFEST_UNAVAILABLE" && validated !== undefined) {
        return {
          ok: false,
          status: "commit_pending_manifest",
          http_status: 202,
          tx_id: validated.record.tx_id,
          retry_identical_registration: true,
          circuit: CIRCUIT_DIRECTIVE,
        };
      }
      return {
        ok: false,
        status: "rejected",
        http_status: protocolError.http_status,
        error: protocolError,
      };
    }
  }

  async lookupManifest(request: ManifestLookupRequest): Promise<ManifestServiceLookupResult> {
    try {
      const objectName = await manifestObjectNameForRoute(request.fleet_id, request.tx_id, request.commit_decided_at);
      const stub = this.env.JOURNAL_MANIFEST.getByName(objectName);
      return await lookupThroughManifestStub(
        { lookup: async (txId) => await stub.lookup(txId) },
        request.tx_id,
      );
    } catch (error) {
      const protocolError = asProtocolError(error);
      return {
        ok: false,
        found: false,
        status: error instanceof TransactionContractViolation ? "rejected" : "unavailable",
        http_status: protocolError.http_status,
        error: protocolError,
        ...(protocolError.code === "TX_MANIFEST_UNAVAILABLE" ? { circuit: CIRCUIT_DIRECTIVE } : {}),
      };
    }
  }

  async releaseManifestRetention(request: ManifestReleaseRequest): Promise<ManifestReleaseResult> {
    try {
      const objectName = await manifestObjectNameForRoute(request.fleet_id, request.tx_id, request.commit_decided_at);
      const stub = this.env.JOURNAL_MANIFEST.getByName(objectName);
      return await stub.release(request.tx_id, request.record_hash);
    } catch (error) {
      return {
        ok: false,
        status: error instanceof TransactionContractViolation ? "rejected" : "unavailable",
        http_status: error instanceof TransactionContractViolation ? error.protocolError.http_status : 503,
        error:
          error instanceof TransactionContractViolation
            ? toManifestRpcError(error.protocolError)
            : manifestError("TX_MANIFEST_UNAVAILABLE", "Manifest lifecycle release is temporarily unavailable."),
      };
    }
  }

  async checkManifestAdmission(request: ManifestLookupRequest): Promise<ManifestAdmissionResult> {
    try {
      const objectName = await manifestObjectNameForRoute(request.fleet_id, request.tx_id, request.commit_decided_at);
      const stub = this.env.JOURNAL_MANIFEST.getByName(objectName);
      return await admissionThroughManifestStub({ admission: async () => await stub.admission() });
    } catch (error) {
      const protocolError = asProtocolError(error);
      return {
        ok: false,
        status: "unavailable",
        http_status: 503,
        error: protocolError,
        circuit: CIRCUIT_DIRECTIVE,
      };
    }
  }
}
