import {
  transactionError,
  type TransactionErrorCode,
  type ManifestRecordV1,
  type ManifestRegistrationV1,
  type TransactionProtocolError,
  type ManifestCancelIntentV1,
  type ManifestFinalizeIntentV1,
  type ManifestEnumerationResultV2,
  type ManifestLocalPageCursorV1,
  type ManifestRecordV2,
  type ManifestReservationV1,
  type ManifestSealReceiptV1,
} from "../../../packages/contracts/src/index.js";

export const MANIFEST_CIRCUIT_POLICY = {
  failure_threshold: 3,
  failure_window_ms: 30_000,
  maximum_cooldown_ms: 5 * 60_000,
} as const;

export const HELD_RETENTION_RECHECK_MS = 24 * 60 * 60 * 1000;

/** RPC-safe projection of ADR-1's typed error. Recursive `details` remain in
 * local logs/storage; omitting them avoids an unbounded Cap'n Proto type. */
export interface ManifestRpcError {
  readonly schema_version: 1;
  readonly code: TransactionErrorCode;
  readonly message: string;
  readonly http_status: number;
  readonly retryable: boolean;
  readonly overloaded?: true;
  readonly retry_after_ms?: number;
}

export function toManifestRpcError(error: TransactionProtocolError): ManifestRpcError {
  return {
    schema_version: error.schema_version,
    code: error.code,
    message: error.message,
    http_status: error.http_status,
    retryable: error.retryable,
    ...(error.overloaded ? { overloaded: true as const } : {}),
    ...(error.retry_after_ms === undefined ? {} : { retry_after_ms: error.retry_after_ms }),
  };
}

export function manifestError(code: TransactionErrorCode, message: string): ManifestRpcError {
  return toManifestRpcError(transactionError(code, message));
}

export interface ManifestRouteRequest {
  readonly fleet_id: string;
  readonly tx_id: string;
  readonly commit_decided_at: string;
}

export interface ManifestLookupRequest extends ManifestRouteRequest {}

export interface ManifestReleaseRequest extends ManifestRouteRequest {
  readonly record_hash: string;
}

export interface ManifestCircuitDirective {
  readonly count_toward_open: true;
  readonly failure_threshold: typeof MANIFEST_CIRCUIT_POLICY.failure_threshold;
  readonly failure_window_ms: typeof MANIFEST_CIRCUIT_POLICY.failure_window_ms;
  readonly maximum_cooldown_ms: typeof MANIFEST_CIRCUIT_POLICY.maximum_cooldown_ms;
}

export type ManifestRegisterResult =
  | {
      readonly ok: true;
      readonly status: "registered" | "already_registered";
      readonly http_status: 200;
      readonly record_hash: string;
      readonly quarantined: false;
    }
  | {
      readonly ok: false;
      readonly status: "quarantined" | "rejected" | "unavailable";
      readonly http_status: number;
      readonly error: ManifestRpcError;
      readonly original_record_hash?: string;
      readonly conflicting_record_hash?: string;
    };

export type ManifestServiceRegisterResult =
  | ManifestRegisterResult
  | {
      readonly ok: false;
      readonly status: "commit_pending_manifest";
      readonly http_status: 202;
      readonly tx_id: string;
      readonly retry_identical_registration: true;
      readonly circuit: ManifestCircuitDirective;
      readonly overloaded?: true;
      readonly retry_after_ms?: number;
    };

export type ManifestLookupResult =
  | { readonly ok: true; readonly found: false }
  | {
      readonly ok: true;
      readonly found: true;
      readonly record: ManifestRecordV1;
      readonly record_hash: string;
      readonly quarantined: boolean;
      readonly conflicting_record_hashes: readonly string[];
      readonly lifecycle_released: boolean;
    };

export type ManifestServiceLookupResult =
  | ManifestLookupResult
  | {
      readonly ok: false;
      readonly found: false;
      readonly status: "unavailable" | "rejected";
      readonly http_status: number;
      readonly error: ManifestRpcError;
      readonly circuit?: ManifestCircuitDirective;
    };

export type ManifestReleaseResult =
  | { readonly ok: true; readonly status: "released" | "already_released" | "not_found" }
  | {
      readonly ok: false;
      readonly status: "quarantined" | "rejected" | "unavailable";
      readonly http_status: number;
      readonly error: ManifestRpcError;
    };

export type ManifestAdmissionResult =
  | {
      readonly ok: true;
      readonly status: "ready";
      readonly circuit_policy: typeof MANIFEST_CIRCUIT_POLICY;
    }
  | {
      readonly ok: false;
      readonly status: "unavailable";
      readonly http_status: 503;
      readonly error: ManifestRpcError;
      readonly circuit: ManifestCircuitDirective;
    };

export interface ManifestRegistrar {
  register(registration: ManifestRegistrationV1): Promise<ManifestRegisterResult>;
}

export interface ManifestLookupReader {
  lookup(txId: string): Promise<ManifestLookupResult>;
}

export interface ManifestAdmissionProbe {
  admission(): Promise<{ readonly ok: true; readonly status: "ready" }>;
}

/** V2 route assignment is deliberately separate from bucket reservation. The
 * coordinator persists the returned bytes before the first bucket mutation. */
export interface ManifestRouteAssignmentRequestV1 {
  readonly draft: {
    readonly fleet_id: string;
    readonly tx_id: string;
    readonly coordinator_id: string;
    readonly operation_hash: string;
    readonly decision_epoch: number;
  };
  readonly idempotency_key: string;
}

export type ManifestRouteAssignmentResult =
  | {
      readonly ok: true;
      readonly status: "assigned" | "already_assigned";
      readonly reservation: ManifestReservationV1;
      readonly reservation_hash: string;
    }
  | {
      readonly ok: false;
      readonly status: "rejected" | "unavailable";
      readonly http_status: number;
      readonly error: ManifestRpcError;
    };

export interface ManifestReserveRequestV1 {
  readonly reservation: ManifestReservationV1;
  readonly reservation_hash: string;
}

export interface ManifestFinalizeRequestV1 extends ManifestReserveRequestV1 {
  readonly intent: ManifestFinalizeIntentV1;
}

export interface ManifestCancelRequestV1 extends ManifestReserveRequestV1 {
  readonly intent: ManifestCancelIntentV1;
}

export interface ManifestV2ReleaseRequest {
  readonly reservation: ManifestReservationV1;
  readonly reservation_hash: string;
  readonly record_hash: string;
  readonly retention_deadline_ms: number;
}

export interface ManifestCoordinatorStateAttestationV1 {
  readonly tx_id: string;
  readonly coordinator_id: string;
  readonly state: string;
  readonly decision: string;
  readonly epoch: number;
  readonly operation_hash: string;
  readonly reservation_hash: string;
  readonly authorization_source_hash: string;
}

export interface ManifestQuarantineResolutionRequestV1 extends ManifestReserveRequestV1 {
  readonly resolution: "FINALIZED" | "CANCELLED";
  readonly selected_hash: string;
  readonly evidence_hash: string;
  readonly actor: string;
  readonly reason: string;
  readonly terminal_intent: ManifestFinalizeIntentV1 | ManifestCancelIntentV1;
  readonly coordinator_state: ManifestCoordinatorStateAttestationV1;
  readonly coordinator_state_hash: string;
  readonly idempotency_key: string;
}

export type ManifestQuarantineResolutionResult =
  | {
      readonly ok: true;
      readonly status: "resolved" | "already_resolved";
      readonly resolution: "FINALIZED" | "CANCELLED";
      readonly resolution_attestation_hash: string;
      readonly record?: ManifestRecordV2;
      readonly record_hash?: string;
    }
  | {
      readonly ok: false;
      readonly status: "conflict" | "unavailable";
      readonly http_status: number;
      readonly error: ManifestRpcError;
    };

export type ManifestReserveResult =
  | {
      readonly ok: true;
      readonly status: "reserved" | "already_reserved";
      readonly reservation_hash: string;
      readonly required_decision_floor_ms: number;
      readonly local_legacy_certificate_hash: string;
    }
  | {
      readonly ok: false;
      readonly status: "rejected_absent" | "quarantined" | "unavailable";
      readonly bucket_row_may_exist: boolean;
      readonly http_status: number;
      readonly error: ManifestRpcError;
    };

export type ManifestFinalizeResult =
  | {
      readonly ok: true;
      readonly status: "finalized" | "already_finalized";
      readonly record: ManifestRecordV2;
      readonly record_hash: string;
    }
  | {
      readonly ok: false;
      readonly status: "quarantined" | "conflict" | "unavailable";
      readonly http_status: number;
      readonly error: ManifestRpcError;
    };

export type ManifestCancelResult =
  | { readonly ok: true; readonly status: "cancelled" | "already_cancelled" }
  | {
      readonly ok: false;
      readonly status: "quarantined_pending_resolution" | "conflict" | "unavailable";
      readonly http_status: number;
      readonly error: ManifestRpcError;
    };

export type ManifestSealResult =
  | {
      readonly ok: true;
      readonly status: "pending" | "complete" | "covering";
      readonly generation: number;
      readonly cutoff_ms: number;
      readonly receipt?: ManifestSealReceiptV1;
    }
  | {
      readonly ok: false;
      readonly status: "seal_in_progress" | "quarantined" | "rejected" | "unavailable";
      readonly http_status: number;
      readonly error: ManifestRpcError;
      readonly generation?: number;
      readonly idempotency_key?: string;
      readonly cutoff_ms?: number;
    };

export type ManifestLocalPageResult =
  | {
      readonly ok: true;
      readonly records: readonly ManifestRecordV2[];
      readonly next_cursor: ManifestLocalPageCursorV1 | null;
      readonly complete: boolean;
      readonly retention_epoch: number;
      readonly records_deleted_through_ms: number | null;
      readonly lease_expires_at_ms: number;
      readonly seal_receipt: ManifestSealReceiptV1;
    }
  | {
      readonly ok: false;
      readonly status: "cursor_mismatch" | "retention_expired" | "quarantined" | "coverage_gap" | "rejected";
      readonly http_status: number;
      readonly error: ManifestRpcError;
    };

export interface ManifestFleetCloseRequestV1 {
  readonly fleet_id: string;
  readonly cutoff: string;
}

export type ManifestFleetCloseResult =
  | {
      readonly ok: true;
      readonly status: "pending";
      readonly cutoff_ms: number;
      readonly snapshot_generation: number;
      readonly completed_buckets: number;
      readonly total_buckets: number;
    }
  | {
      readonly ok: true;
      readonly status: "complete";
      readonly cutoff_ms: number;
      readonly snapshot_generation: number;
      readonly catalog_close_key: string;
      readonly snapshot_hash: string;
      readonly fleet_root_hash: string;
      readonly partition_config_hash: string;
      readonly coverage_start: string;
      readonly completed_buckets: number;
      readonly total_buckets: number;
    }
  | {
      readonly ok: false;
      readonly status: "rejected" | "unavailable" | "quarantined";
      readonly http_status: number;
      readonly error: ManifestRpcError;
    };

export type ManifestFleetEnumerationServiceResult =
  | ManifestEnumerationResultV2
  | {
      readonly ok: false;
      readonly status: "rejected" | "unavailable";
      readonly http_status: number;
      readonly error: ManifestRpcError;
    };

/** Narrow recovery-facing RPC surface exported for the root restore
 * coordinator. Implementations must only emit current, root-bound pages. */
export interface RestoreManifestService {
  closeFleetThrough(request: ManifestFleetCloseRequestV1): Promise<ManifestFleetCloseResult>;
  enumerateManifest(input: unknown): Promise<ManifestFleetEnumerationServiceResult>;
}
