import {
  TransactionContractViolation,
  canonicalJson,
  classifyDurableObjectFailure,
  durableObjectUnavailableError,
  manifestRoute,
  hashManifestReservation,
  sha256Hex,
  transactionError,
  validateManifestRegistration,
  type ManifestRegistrationV1,
  type ManifestReservationV1,
} from "../../../packages/contracts/src/index.js";
import {
  MANIFEST_CIRCUIT_POLICY,
  toManifestRpcError,
  type ManifestAdmissionProbe,
  type ManifestAdmissionResult,
  type ManifestCircuitDirective,
  type ManifestLookupReader,
  type ManifestRpcError,
  type ManifestServiceLookupResult,
  type ManifestServiceRegisterResult,
  type ManifestRegistrar,
} from "./manifest-types.js";

export const CIRCUIT_DIRECTIVE: ManifestCircuitDirective = {
  count_toward_open: true,
  ...MANIFEST_CIRCUIT_POLICY,
};

/** Validate an untrusted RPC value before granting it the wire-contract type. */
export async function validatedManifestRegistration(value: unknown): Promise<ManifestRegistrationV1> {
  if (value !== null && typeof value === "object" && !Array.isArray(value)) {
    const record = (value as Record<string, unknown>).record;
    if (record !== null && typeof record === "object" && !Array.isArray(record)) {
      const fields = record as Record<string, unknown>;
      if (typeof fields.protocol_version !== "number" || typeof fields.format_version !== "number") {
        throw new TransactionContractViolation(
          transactionError("TX_ENVELOPE_INVALID", "Manifest record version fields must be numbers."),
        );
      }
    }
  }
  await validateManifestRegistration(value);
  return value as ManifestRegistrationV1;
}

/** Best-effort diagnostic extraction only; never grants trust to the payload. */
export function manifestRegistrationTxId(value: unknown): string | undefined {
  if (value === null || typeof value !== "object" || Array.isArray(value)) return undefined;
  const record = (value as Record<string, unknown>).record;
  if (record === null || typeof record !== "object" || Array.isArray(record)) return undefined;
  const txId = (record as Record<string, unknown>).tx_id;
  return typeof txId === "string" && txId.length > 0 ? txId : undefined;
}

export async function manifestObjectName(fleetId: string, utcDay: string, partition: number): Promise<string> {
  const digest = await sha256Hex(canonicalJson([fleetId, utcDay, partition]));
  return `journal-v1:${digest}`;
}

export async function manifestObjectNameForRegistration(registration: ManifestRegistrationV1): Promise<string> {
  await validateManifestRegistration(registration);
  const { fleet_id: fleetId, utc_day: utcDay, partition } = registration.record;
  return manifestObjectName(fleetId, utcDay, partition);
}

/** V2 routing is frozen at reservation time; never derive it from the later
 * bucket-issued commit decision. */
export async function manifestObjectNameForReservation(
  reservation: ManifestReservationV1,
  claimedReservationHash?: string,
): Promise<string> {
  const expectedHash = await hashManifestReservation(reservation);
  if (claimedReservationHash !== undefined && claimedReservationHash !== expectedHash) {
    throw new TransactionContractViolation(
      transactionError("MANIFEST_RESERVATION_CONFLICT", "reservation_hash does not match the frozen reservation."),
    );
  }
  return manifestObjectName(
    reservation.fleet_id,
    reservation.reservation_utc_day,
    reservation.partition,
  );
}

export async function manifestObjectNameForRoute(
  fleetId: string,
  txId: string,
  commitDecidedAt: string,
): Promise<string> {
  const route = await manifestRoute(txId, commitDecidedAt);
  return manifestObjectName(fleetId, route.utc_day, route.partition);
}

export function asProtocolError(error: unknown): ManifestRpcError {
  if (error instanceof TransactionContractViolation) return toManifestRpcError(error.protocolError);
  return toManifestRpcError(durableObjectUnavailableError(
    error,
    "TX_MANIFEST_UNAVAILABLE",
    "Journal manifest service is temporarily unavailable.",
  ));
}

export async function registerThroughManifestStub(
  stub: ManifestRegistrar,
  registration: ManifestRegistrationV1,
): Promise<ManifestServiceRegisterResult> {
  let overloaded = false;
  let retryAfterMs: number | undefined;
  try {
    const result = await stub.register(registration);
    if (result.status !== "unavailable") return result;
    overloaded = result.error.overloaded === true;
    retryAfterMs = result.error.retry_after_ms;
  } catch (error) {
    // The registration may have committed before the response was lost. The
    // only safe response after commit_decided is a pollable ambiguous result.
    const classification = classifyDurableObjectFailure(error);
    overloaded = classification.overloaded;
    retryAfterMs = classification.overloaded || classification.retryable
      ? classification.retry_after_ms
      : undefined;
  }
  return {
    ok: false,
    status: "commit_pending_manifest",
    http_status: 202,
    tx_id: registration.record.tx_id,
    retry_identical_registration: true,
    circuit: CIRCUIT_DIRECTIVE,
    ...(overloaded ? { overloaded: true as const } : {}),
    ...(retryAfterMs === undefined ? {} : { retry_after_ms: retryAfterMs }),
  };
}

export async function lookupThroughManifestStub(
  stub: ManifestLookupReader,
  txId: string,
): Promise<ManifestServiceLookupResult> {
  try {
    return await stub.lookup(txId);
  } catch (error) {
    return {
      ok: false,
      found: false,
      status: "unavailable",
      http_status: 503,
      error: asProtocolError(error),
      circuit: CIRCUIT_DIRECTIVE,
    };
  }
}

export async function admissionThroughManifestStub(stub: ManifestAdmissionProbe): Promise<ManifestAdmissionResult> {
  try {
    await stub.admission();
    return { ok: true, status: "ready", circuit_policy: MANIFEST_CIRCUIT_POLICY };
  } catch (error) {
    return {
      ok: false,
      status: "unavailable",
      http_status: 503,
      error: asProtocolError(error),
      circuit: CIRCUIT_DIRECTIVE,
    };
  }
}
