import { WorkerEntrypoint } from "cloudflare:workers";
import {
  TransactionContractViolation,
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
} from "./manifest-types.js";
import {
  CIRCUIT_DIRECTIVE,
  admissionThroughManifestStub,
  asProtocolError,
  lookupThroughManifestStub,
  manifestRegistrationTxId,
  manifestObjectNameForRegistration,
  manifestObjectNameForRoute,
  registerThroughManifestStub,
  validatedManifestRegistration,
} from "./service.js";
import type { JournalManifestDO } from "./journal-manifest.js";

export {
  JournalManifestDO,
  LIFECYCLE_FAILURE_RETRY_MS,
  executeLifecycleAlarm,
} from "./journal-manifest.js";
export * from "./manifest-types.js";
export * from "./service.js";

export interface ControlPlaneEnv {
  JOURNAL_MANIFEST: DurableObjectNamespace<JournalManifestDO>;
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

  async registerManifest(registration: unknown): Promise<ManifestServiceRegisterResult> {
    let validated: ManifestRegistrationV1 | undefined;
    try {
      validated = await validatedManifestRegistration(registration);
      const objectName = await manifestObjectNameForRegistration(validated);
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
