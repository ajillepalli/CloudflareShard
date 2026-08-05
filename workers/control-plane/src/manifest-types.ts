import {
  transactionError,
  type TransactionErrorCode,
  type ManifestRecordV1,
  type ManifestRegistrationV1,
  type TransactionProtocolError,
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
}

export function toManifestRpcError(error: TransactionProtocolError): ManifestRpcError {
  return {
    schema_version: error.schema_version,
    code: error.code,
    message: error.message,
    http_status: error.http_status,
    retryable: error.retryable,
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
