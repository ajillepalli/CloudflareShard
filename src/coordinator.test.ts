import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import {
  COORDINATOR_RETENTION_DAYS,
  createManifestRegistration,
  createManifestReservation,
  hashCanonicalJson,
  hashManifestRecordV2,
  hashManifestReservation,
  hashParticipantOperations,
  sha256Hex,
  type ManifestRecordV2,
  type RedoEnvelopeV1,
} from "../packages/contracts/src/index.js";
import type { CoordinatorDO, TransactionManifestService } from "./coordinator";
import type { ShardDO } from "./shard";

async function freshCoordinator() {
  const id = env.COORDINATOR.idFromName(`coordinator-${crypto.randomUUID()}`);
  return env.COORDINATOR.get(id);
}

async function freshShard(name: string) {
  const id = env.SHARD.idFromName(name);
  return env.SHARD.get(id);
}

function post(path: string, body: unknown) {
  return new Request(`https://coordinator.internal${path}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
}

function shardPost(path: string, body: unknown) {
  return new Request(`https://shard.internal${path}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
}

function defaultV2ManifestMethods(): Pick<
  TransactionManifestService,
  "assignManifestRoute" | "reserveManifest" | "finalizeManifest" | "cancelManifest" | "releaseManifestV2"
> {
  return {
    async assignManifestRoute({ draft }) {
      const partitionConfigHash = await hashCanonicalJson({ config: "coordinator-test-v2" });
      const reservation = await createManifestReservation(
        { ...draft, partition_config_hash: partitionConfigHash },
        new Date().toISOString(),
      );
      return {
        ok: true as const,
        status: "assigned" as const,
        reservation,
        reservation_hash: await hashManifestReservation(reservation),
      };
    },
    async reserveManifest({ reservation_hash: reservationHash }) {
      return {
        ok: true as const,
        status: "reserved" as const,
        reservation_hash: reservationHash,
        required_decision_floor_ms: 0,
        local_legacy_certificate_hash: await hashCanonicalJson({ legacy: "none" }),
      };
    },
    async finalizeManifest({ reservation, reservation_hash: reservationHash, intent }) {
      const commitDecidedAtMs = Date.now();
      const record: ManifestRecordV2 = {
        protocol_version: 1,
        format_version: 2,
        fleet_id: reservation.fleet_id,
        reservation_utc_day: reservation.reservation_utc_day,
        partition: reservation.partition,
        partition_count: reservation.partition_count,
        routing_key: reservation.routing_key,
        partition_config_hash: reservation.partition_config_hash,
        tx_id: reservation.tx_id,
        coordinator_id: reservation.coordinator_id,
        operation_hash: reservation.operation_hash,
        decision_epoch: reservation.decision_epoch,
        reserved_at: reservation.reserved_at,
        reservation_hash: reservationHash,
        envelope_hash: intent.redo_envelope_hash,
        commit_decided_at: new Date(commitDecidedAtMs).toISOString(),
        commit_decided_at_ms: commitDecidedAtMs,
        decision_sequence: 1,
        retention_deadline: new Date(commitDecidedAtMs + COORDINATOR_RETENTION_DAYS * 24 * 60 * 60 * 1000).toISOString(),
      };
      return { ok: true as const, status: "finalized" as const, record, record_hash: await hashManifestRecordV2(record) };
    },
    async cancelManifest() {
      return { ok: true as const, status: "cancelled" as const };
    },
    async releaseManifestV2() {
      return { ok: true as const, status: "released" as const };
    },
  };
}

async function seedV1CoordinatorForBridge(status: "prepared" | "commit_pending_manifest") {
  const txId = `tx-v1-v2-bridge-${status}-${crypto.randomUUID()}`;
  const shardId = `shard-${txId}`;
  const intent = { sql: "INSERT INTO t (id) VALUES (?)", params: [txId], tenantId: "t1", table: "t", partitionKey: txId };
  const participants = [{ shardId, intents: [intent] }];
  const redoParticipants = [{
    participant_id: shardId,
    epoch: 1,
    intents: [{
      intent_seq: 0,
      sql: intent.sql,
      params: intent.params,
      tenant_id: intent.tenantId,
      table_name: intent.table,
      partition_key: intent.partitionKey,
      vbucket: null,
      operation: null,
      mirror_target_participant_id: null,
    }],
  }];
  const operationHash = await hashParticipantOperations(redoParticipants);
  const decidedAtMs = Date.now();
  const envelope: RedoEnvelopeV1 = {
    protocol_version: 1,
    format_version: 1,
    tx_id: txId,
    fleet_id: "default",
    coordinator_id: txId,
    decision: "commit",
    decision_epoch: 1,
    commit_decided_at: new Date(decidedAtMs).toISOString(),
    retention_deadline: new Date(decidedAtMs + COORDINATOR_RETENTION_DAYS * 24 * 60 * 60 * 1000).toISOString(),
    operation_hash: operationHash,
    participants: redoParticipants,
  };
  const registration = await createManifestRegistration(envelope);
  const coordinator = env.COORDINATOR.get(env.COORDINATOR.idFromName(txId));
  await coordinator.fetch(post("/tx-status", { txId: "schema-warmup" }));
  await runInDurableObject(coordinator, async (_instance: CoordinatorDO, state: DurableObjectState) => {
    const now = new Date().toISOString();
    state.storage.sql.exec(
      `INSERT INTO transactions
        (tx_id, status, participant_shards_json, operation_json, operation_hash,
         created_at, updated_at, protocol_version, state_model_version, epoch,
         decision, fleet_id, coordinator_id, redo_envelope_json, manifest_registration_json)
       VALUES (?, ?, ?, ?, ?, ?, ?, 1, 1, 1, ?, 'default', ?, ?, ?)`,
      txId,
      status,
      JSON.stringify([shardId]),
      JSON.stringify(participants),
      operationHash,
      now,
      now,
      status === "prepared" ? "undecided" : "commit",
      txId,
      status === "prepared" ? null : JSON.stringify(envelope),
      status === "prepared" ? null : JSON.stringify(registration),
    );
    state.storage.sql.exec(
      `INSERT INTO transaction_participants
        (tx_id, shard_id, phase_status, updated_at, epoch, operation_hash)
       VALUES (?, ?, 'prepared', ?, 1, ?)`,
      txId,
      shardId,
      now,
      operationHash,
    );
  });
  return { coordinator, txId, participants };
}

describe("CoordinatorDO shell (/tx-status, route guards, error boundary)", () => {
  it("/tx-status reports found:false for an unknown txId", async () => {
    const stub = await freshCoordinator();
    const res = await stub.fetch(post("/tx-status", { txId: "never-existed" }));
    expect(res.status).toBe(200);
    const body = (await res.json()) as { found: boolean };
    expect(body.found).toBe(false);
  });

  it("/tx-status requires txId", async () => {
    const stub = await freshCoordinator();
    const res = await stub.fetch(post("/tx-status", {}));
    expect(res.status).toBe(400);
  });

  it("rejects non-POST methods with 405", async () => {
    const stub = await freshCoordinator();
    const res = await stub.fetch(new Request("https://coordinator.internal/tx-status", { method: "GET" }));
    expect(res.status).toBe(405);
  });

  it("returns 404 for an unknown coordinator route", async () => {
    const stub = await freshCoordinator();
    const res = await stub.fetch(post("/not-a-real-route", {}));
    expect(res.status).toBe(404);
  });

  it("returns a clean 500 instead of an unhandled crash on malformed JSON", async () => {
    const stub = await freshCoordinator();
    const res = await stub.fetch(
      new Request("https://coordinator.internal/tx-status", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: "{not valid json",
      }),
    );
    expect(res.status).toBe(500);
    const body = (await res.json()) as { error: string };
    expect(body.error).toBe("Internal error.");
  });
});

describe("CoordinatorDO /begin (2PC orchestration)", () => {
  async function createTable(shardName: string) {
    const stub = await freshShard(shardName);
    await stub.fetch(shardPost("/execute", { sql: "CREATE TABLE IF NOT EXISTS t (id TEXT PRIMARY KEY, v TEXT)", requestId: `req-schema-${shardName}`, isMutation: true }));
    return stub;
  }

  it("commits every participant when all shards prepare successfully", async () => {
    const txId = `tx-begin-${crypto.randomUUID()}`;
    const shardA = `shard-a-${txId}`;
    const shardB = `shard-b-${txId}`;
    await createTable(shardA);
    await createTable(shardB);

    const coordinatorId = env.COORDINATOR.idFromName(txId);
    const coordinator = env.COORDINATOR.get(coordinatorId);
    const res = await coordinator.fetch(
      post("/begin", {
        txId,
        participants: [
          { shardId: shardA, intents: [{ sql: "INSERT INTO t (id, v) VALUES (?, ?)", params: ["row-1", "a"], tenantId: "t1", table: "t", partitionKey: "row-1" }] },
          { shardId: shardB, intents: [{ sql: "INSERT INTO t (id, v) VALUES (?, ?)", params: ["row-2", "b"], tenantId: "t1", table: "t", partitionKey: "row-2" }] },
        ],
      }),
    );
    expect(res.status).toBe(200);
    const body = (await res.json()) as { ok: boolean; status: string };
    expect(body.ok).toBe(true);
    expect(body.status).toBe("committed");
    await runInDurableObject(coordinator, async (_instance: CoordinatorDO, state: DurableObjectState) => {
      const durable = state.storage.sql.exec<{ state_model_version: number }>(
        "SELECT state_model_version FROM transactions WHERE tx_id = ?",
        txId,
      ).one();
      expect(durable.state_model_version).toBe(2);
    });

    const stubA = await freshShard(shardA);
    const checkA = await stubA.fetch(shardPost("/execute", { sql: "SELECT * FROM t WHERE id = ?", params: ["row-1"], requestId: "check-a", isMutation: false }));
    expect(((await checkA.json()) as { rows: unknown[] }).rows).toHaveLength(1);
    const stubB = await freshShard(shardB);
    const checkB = await stubB.fetch(shardPost("/execute", { sql: "SELECT * FROM t WHERE id = ?", params: ["row-2"], requestId: "check-b", isMutation: false }));
    expect(((await checkB.json()) as { rows: unknown[] }).rows).toHaveLength(1);
  });

  it("aborts every participant and leaves no trace when one shard fails to prepare", async () => {
    const txId = `tx-begin-fail-${crypto.randomUUID()}`;
    const shardA = `shard-a-${txId}`;
    const shardB = `shard-b-${txId}`;
    await createTable(shardA);
    await createTable(shardB);

    const coordinatorId = env.COORDINATOR.idFromName(txId);
    const coordinator = env.COORDINATOR.get(coordinatorId);
    const res = await coordinator.fetch(
      post("/begin", {
        txId,
        participants: [
          { shardId: shardA, intents: [{ sql: "INSERT INTO t (id, v) VALUES (?, ?)", params: ["row-3", "a"], tenantId: "t1", table: "t", partitionKey: "row-3" }] },
          // References a nonexistent column — prepare's SQL execution fails on this shard.
          { shardId: shardB, intents: [{ sql: "INSERT INTO t (id, nonexistent_col) VALUES (?, ?)", params: ["row-4", "boom"], tenantId: "t1", table: "t", partitionKey: "row-4" }] },
        ],
      }),
    );
    expect(res.status).toBe(409);
    const body = (await res.json()) as { error: { code: string } };
    expect(body.error.code).toBe("TX_ABORTED");

    const stubA = await freshShard(shardA);
    const checkA = await stubA.fetch(shardPost("/execute", { sql: "SELECT * FROM t WHERE id = ?", params: ["row-3"], requestId: "check-a2", isMutation: false }));
    expect(((await checkA.json()) as { rows: unknown[] }).rows).toHaveLength(0);

    const statusRes = await coordinator.fetch(post("/tx-status", { txId }));
    const statusBody = (await statusRes.json()) as { found: boolean; status: string };
    expect(statusBody.found).toBe(true);
    expect(statusBody.status).toBe("aborted");
  });

  it("is idempotent: retrying /begin with the same txId after commit returns the committed status without re-running prepare", async () => {
    const txId = `tx-begin-idem-${crypto.randomUUID()}`;
    const shardA = `shard-a-${txId}`;
    await createTable(shardA);

    const coordinatorId = env.COORDINATOR.idFromName(txId);
    const coordinator = env.COORDINATOR.get(coordinatorId);
    const participants = [{ shardId: shardA, intents: [{ sql: "INSERT INTO t (id, v) VALUES (?, ?)", params: ["row-5", "a"], tenantId: "t1", table: "t", partitionKey: "row-5" }] }];

    const first = await coordinator.fetch(post("/begin", { txId, participants }));
    expect(first.status).toBe(200);

    const retry = await coordinator.fetch(post("/begin", { txId, participants }));
    expect(retry.status).toBe(200);
    const retryBody = (await retry.json()) as { status: string };
    expect(retryBody.status).toBe("committed");

    const stubA = await freshShard(shardA);
    const countRes = await stubA.fetch(shardPost("/execute", { sql: "SELECT COUNT(*) as n FROM t WHERE id = ?", params: ["row-5"], requestId: "check-count", isMutation: false }));
    const countBody = (await countRes.json()) as { rows: Array<{ n: number }> };
    expect(countBody.rows[0].n).toBe(1);
  });

  it("rejects /begin with missing txId or empty participants", async () => {
    const stub = await freshCoordinator();
    const res = await stub.fetch(post("/begin", { txId: "x", participants: [] }));
    expect(res.status).toBe(400);
  });

  it("regression (Codex-found): retrying an in-flight txId with a different participant set is rejected, not silently resumed with the new data", async () => {
    const txId = `tx-mismatch-inflight-${crypto.randomUUID()}`;
    const shardA = `shard-a-${txId}`;
    await createTable(shardA);

    const coordinatorId = env.COORDINATOR.idFromName(txId);
    const coordinator = env.COORDINATOR.get(coordinatorId);
    // Seed a still-in-flight transaction directly (simulating a /begin call
    // that started but never finished) with one operation_json payload...
    await coordinator.fetch(post("/tx-status", { txId: "schema-warmup" }));
    await runInDurableObject(coordinator, async (_instance: CoordinatorDO, state: DurableObjectState) => {
      const now = new Date().toISOString();
      const originalParticipants = [{ shardId: shardA, intents: [{ sql: "INSERT INTO t (id, v) VALUES (?, ?)", params: ["row-orig", "a"], tenantId: "t1", table: "t", partitionKey: "row-orig" }] }];
      state.storage.sql.exec(
        `INSERT INTO transactions (tx_id, status, participant_shards_json, operation_json, operation_hash, created_at, updated_at) VALUES (?, 'preparing', ?, ?, 'seeded-original-hash', ?, ?)`,
        txId,
        JSON.stringify([shardA]),
        JSON.stringify(originalParticipants),
        now,
        now,
      );
    });

    // ...then retry /begin with the SAME txId but a DIFFERENT mutation set —
    // this must be rejected, not silently applied under the original txId.
    const res = await coordinator.fetch(
      post("/begin", {
        txId,
        participants: [{ shardId: shardA, intents: [{ sql: "INSERT INTO t (id, v) VALUES (?, ?)", params: ["row-different", "z"], tenantId: "t1", table: "t", partitionKey: "row-different" }] }],
      }),
    );
    expect(res.status).toBe(409);
    const body = (await res.json()) as { error: { code: string } };
    expect(body.error.code).toBe("TX_ID_REQUEST_MISMATCH");

    const stubA = await freshShard(shardA);
    const checkRes = await stubA.fetch(shardPost("/execute", { sql: "SELECT * FROM t WHERE id = ?", params: ["row-different"], requestId: "check-mismatch", isMutation: false }));
    expect(((await checkRes.json()) as { rows: unknown[] }).rows).toHaveLength(0);
  });

  it("regression (Codex-found): retrying a COMMITTED txId with a different participant set is rejected, not returned as a stale success", async () => {
    const txId = `tx-mismatch-committed-${crypto.randomUUID()}`;
    const shardA = `shard-a-${txId}`;
    await createTable(shardA);

    const coordinatorId = env.COORDINATOR.idFromName(txId);
    const coordinator = env.COORDINATOR.get(coordinatorId);
    const originalParticipants = [{ shardId: shardA, intents: [{ sql: "INSERT INTO t (id, v) VALUES (?, ?)", params: ["row-committed", "a"], tenantId: "t1", table: "t", partitionKey: "row-committed" }] }];
    const first = await coordinator.fetch(post("/begin", { txId, participants: originalParticipants }));
    expect(first.status).toBe(200);

    const retryWithDifferentData = await coordinator.fetch(
      post("/begin", {
        txId,
        participants: [{ shardId: shardA, intents: [{ sql: "INSERT INTO t (id, v) VALUES (?, ?)", params: ["row-different-2", "z"], tenantId: "t1", table: "t", partitionKey: "row-different-2" }] }],
      }),
    );
    expect(retryWithDifferentData.status).toBe(409);
    const body = (await retryWithDifferentData.json()) as { error: { code: string } };
    expect(body.error.code).toBe("TX_ID_REQUEST_MISMATCH");
  });

  it("regression (Codex-found): a transactions table from before operation_hash existed is migrated in place, not crashed on", async () => {
    const txId = `tx-migration-${crypto.randomUUID()}`;
    const coordinatorId = env.COORDINATOR.idFromName(txId);
    const coordinator = env.COORDINATOR.get(coordinatorId);

    // Simulate a pre-migration transactions table (no operation_hash column)
    // by creating it directly, before ensureSchema() ever runs on this DO.
    await runInDurableObject(coordinator, async (_instance: CoordinatorDO, state: DurableObjectState) => {
      state.storage.sql.exec(`
        CREATE TABLE transactions (
          tx_id TEXT PRIMARY KEY,
          status TEXT NOT NULL,
          participant_shards_json TEXT NOT NULL,
          operation_json TEXT NOT NULL,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          attempt_count INTEGER NOT NULL DEFAULT 0,
          last_error TEXT
        )
      `);
      const now = new Date().toISOString();
      state.storage.sql.exec(
        "INSERT INTO transactions (tx_id, status, participant_shards_json, operation_json, created_at, updated_at) VALUES (?, 'preparing', '[]', '[]', ?, ?)",
        txId,
        now,
        now,
      );
    });

    // A /begin retry against this pre-migration row must not 500 — it should
    // degrade to the same fail-closed mismatch rejection as any other
    // content mismatch, since the backfilled operation_hash default ('')
    // never equals a real hash.
    const res = await coordinator.fetch(post("/begin", { txId, participants: [{ shardId: "irrelevant-shard", intents: [] }] }));
    expect(res.status).toBe(409);
    const body = (await res.json()) as { error: { code: string } };
    expect(body.error.code).toBe("TX_ID_REQUEST_MISMATCH");
  });

  it("additively expands a model-1 transaction row without rewriting its V1 artifacts", async () => {
    const txId = `tx-v2-schema-expand-${crypto.randomUUID()}`;
    const coordinator = env.COORDINATOR.get(env.COORDINATOR.idFromName(txId));
    const redoEnvelope = JSON.stringify({ format_version: 1, sentinel: "original-redo" });
    const manifestRegistration = JSON.stringify({ format_version: 1, sentinel: "original-registration" });
    const result = JSON.stringify({ status: "committed", sentinel: "original-result" });

    await runInDurableObject(coordinator, async (_instance: CoordinatorDO, state: DurableObjectState) => {
      state.storage.sql.exec(`
        CREATE TABLE transactions (
          tx_id TEXT PRIMARY KEY,
          status TEXT NOT NULL,
          participant_shards_json TEXT NOT NULL,
          operation_json TEXT NOT NULL,
          operation_hash TEXT NOT NULL DEFAULT '',
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          attempt_count INTEGER NOT NULL DEFAULT 0,
          last_error TEXT,
          protocol_version INTEGER NOT NULL DEFAULT 1,
          state_model_version INTEGER NOT NULL DEFAULT 1,
          epoch INTEGER NOT NULL DEFAULT 1,
          decision TEXT NOT NULL DEFAULT 'undecided',
          fleet_id TEXT NOT NULL DEFAULT 'default',
          coordinator_id TEXT NOT NULL DEFAULT '',
          redo_envelope_json TEXT,
          manifest_registration_json TEXT,
          result_json TEXT
        )
      `);
      const now = new Date().toISOString();
      state.storage.sql.exec(
        `INSERT INTO transactions
          (tx_id, status, participant_shards_json, operation_json, operation_hash,
           created_at, updated_at, protocol_version, state_model_version, epoch,
           decision, fleet_id, coordinator_id, redo_envelope_json,
           manifest_registration_json, result_json)
         VALUES (?, 'committed', '[]', '[]', 'v1-operation-hash', ?, ?, 1, 1, 1,
                 'commit', 'legacy-fleet', 'legacy-coordinator', ?, ?, ?)`,
        txId,
        now,
        now,
        redoEnvelope,
        manifestRegistration,
        result,
      );
    });

    const status = await coordinator.fetch(post("/tx-status", { txId }));
    expect(status.status).toBe(200);
    expect(await status.json()).toMatchObject({ found: true, status: "committed" });

    await runInDurableObject(coordinator, async (_instance: CoordinatorDO, state: DurableObjectState) => {
      const durable = state.storage.sql.exec<{
        status: string;
        state_model_version: number;
        redo_envelope_json: string;
        manifest_registration_json: string;
        result_json: string;
        manifest_route_assignment_request_json: string | null;
        manifest_reservation_json: string | null;
        manifest_reservation_hash: string | null;
        manifest_finalize_request_json: string | null;
        manifest_cancel_request_json: string | null;
        redo_envelope_intent_json: string | null;
        manifest_record_json: string | null;
        manifest_record_hash: string | null;
        commit_decided_at_ms: number | null;
        decision_sequence: number | null;
      }>(
        `SELECT status, state_model_version, redo_envelope_json,
                manifest_registration_json, result_json,
                manifest_route_assignment_request_json, manifest_reservation_json,
                manifest_reservation_hash, manifest_finalize_request_json,
                manifest_cancel_request_json, redo_envelope_intent_json,
                manifest_record_json, manifest_record_hash, commit_decided_at_ms,
                decision_sequence
           FROM transactions WHERE tx_id = ?`,
        txId,
      ).one();
      expect(durable).toEqual({
        status: "committed",
        state_model_version: 1,
        redo_envelope_json: redoEnvelope,
        manifest_registration_json: manifestRegistration,
        result_json: result,
        manifest_route_assignment_request_json: null,
        manifest_reservation_json: null,
        manifest_reservation_hash: null,
        manifest_finalize_request_json: null,
        manifest_cancel_request_json: null,
        redo_envelope_intent_json: null,
        manifest_record_json: null,
        manifest_record_hash: null,
        commit_decided_at_ms: null,
        decision_sequence: null,
      });
    });
  });

  it.each(["manifest_reserving", "commit_deciding", "aborted_pending_manifest_cancel"] as const)(
    "reads the model-2-only %s state during expand-first deployment",
    async (durableStatus) => {
      const txId = `tx-v2-readable-${durableStatus}-${crypto.randomUUID()}`;
      const coordinator = env.COORDINATOR.get(env.COORDINATOR.idFromName(txId));
      await coordinator.fetch(post("/tx-status", { txId: "schema-warmup" }));
      await runInDurableObject(coordinator, async (_instance: CoordinatorDO, state: DurableObjectState) => {
        const now = new Date().toISOString();
        state.storage.sql.exec(
          `INSERT INTO transactions
            (tx_id, status, participant_shards_json, operation_json, operation_hash,
             created_at, updated_at, protocol_version, state_model_version, epoch,
             decision, fleet_id, coordinator_id)
           VALUES (?, ?, '[]', '[]', 'model-2-operation-hash', ?, ?, 1, 2, 1,
                   'undecided', 'fleet-v2', 'coordinator-v2')`,
          txId,
          durableStatus,
          now,
          now,
        );
      });

      const status = await coordinator.fetch(post("/tx-status", { txId }));
      expect(status.status).toBe(200);
      expect(await status.json()).toMatchObject({ found: true, status: durableStatus, commitAuthorized: false });
    },
  );

  it("rejects a model-2-only state mislabeled as state model 1", async () => {
    const txId = `tx-v1-invalid-v2-state-${crypto.randomUUID()}`;
    const coordinator = env.COORDINATOR.get(env.COORDINATOR.idFromName(txId));
    await coordinator.fetch(post("/tx-status", { txId: "schema-warmup" }));
    await runInDurableObject(coordinator, async (_instance: CoordinatorDO, state: DurableObjectState) => {
      const now = new Date().toISOString();
      state.storage.sql.exec(
        `INSERT INTO transactions
          (tx_id, status, participant_shards_json, operation_json, operation_hash,
           created_at, updated_at, protocol_version, state_model_version, epoch,
           decision, fleet_id, coordinator_id)
         VALUES (?, 'manifest_reserving', '[]', '[]', 'invalid-model-state', ?, ?,
                 1, 1, 1, 'undecided', 'fleet-v1', 'coordinator-v1')`,
        txId,
        now,
        now,
      );
    });

    const status = await coordinator.fetch(post("/tx-status", { txId }));
    expect(status.status).toBe(503);
    expect(await status.json()).toMatchObject({ error: { code: "TX_VERSION_UNSUPPORTED" } });
  });

  it("two different txIds land on two different CoordinatorDO instances and don't interfere", async () => {
    const txA = `tx-iso-a-${crypto.randomUUID()}`;
    const txB = `tx-iso-b-${crypto.randomUUID()}`;
    const idA = env.COORDINATOR.idFromName(txA);
    const idB = env.COORDINATOR.idFromName(txB);
    expect(idA.equals(idB)).toBe(false);

    const shardA = `shard-iso-${txA}`;
    await createTable(shardA);
    const coordinatorA = env.COORDINATOR.get(idA);
    await coordinatorA.fetch(
      post("/begin", { txId: txA, participants: [{ shardId: shardA, intents: [{ sql: "INSERT INTO t (id, v) VALUES (?, ?)", params: ["row-iso", "a"], tenantId: "t1", table: "t", partitionKey: "row-iso" }] }] }),
    );

    const coordinatorB = env.COORDINATOR.get(idB);
    const statusB = await coordinatorB.fetch(post("/tx-status", { txId: txB }));
    const bodyB = (await statusB.json()) as { found: boolean };
    expect(bodyB.found).toBe(false);
  });
});

describe("CoordinatorDO /force-abort", () => {
  it("aborts a stuck transaction and marks it aborted", async () => {
    const txId = `tx-force-${crypto.randomUUID()}`;
    const shardA = `shard-force-${txId}`;
    const stub = await freshShard(shardA);
    await stub.fetch(shardPost("/execute", { sql: "CREATE TABLE IF NOT EXISTS t (id TEXT PRIMARY KEY)", requestId: "req-schema", isMutation: true }));
    await stub.fetch(shardPost("/prepare", { coordinatorTxId: txId, intents: [{ sql: "INSERT INTO t (id) VALUES (?)", params: ["row-f"], tenantId: "t1", table: "t", partitionKey: "row-f" }] }));

    const coordinatorId = env.COORDINATOR.idFromName(txId);
    const coordinator = env.COORDINATOR.get(coordinatorId);
    await coordinator.fetch(post("/tx-status", { txId: "schema-warmup" }));
    await runInDurableObject(coordinator, async (_instance: CoordinatorDO, state: DurableObjectState) => {
      state.storage.sql.exec(
        `INSERT INTO transactions (tx_id, status, participant_shards_json, operation_json, created_at, updated_at) VALUES (?, 'preparing', ?, ?, ?, ?)`,
        txId,
        JSON.stringify([shardA]),
        JSON.stringify([{ shardId: shardA, intents: [] }]),
        new Date().toISOString(),
        new Date().toISOString(),
      );
    });

    const res = await coordinator.fetch(post("/force-abort", { txId }));
    expect(res.status).toBe(200);
    const body = (await res.json()) as { ok: boolean; status: string };
    expect(body.status).toBe("aborted");

    const checkRes = await stub.fetch(shardPost("/execute", { sql: "SELECT * FROM t WHERE id = ?", params: ["row-f"], requestId: "check-f", isMutation: false }));
    expect(((await checkRes.json()) as { rows: unknown[] }).rows).toHaveLength(0);
  });

  it("returns 404 for an unknown txId", async () => {
    const stub = await freshCoordinator();
    const res = await stub.fetch(post("/force-abort", { txId: "never-existed" }));
    expect(res.status).toBe(404);
  });
});

describe("CoordinatorDO recovery sweep (alarm-driven retry of an unacknowledged commit)", () => {
  it("converges: a queued recovery row is retried by the alarm and cleared once the shard acknowledges", async () => {
    const txId = `tx-recover-${crypto.randomUUID()}`;
    const shardA = `shard-recover-${txId}`;
    const stub = await freshShard(shardA);
    await stub.fetch(shardPost("/execute", { sql: "CREATE TABLE IF NOT EXISTS t (id TEXT PRIMARY KEY)", requestId: "req-schema", isMutation: true }));
    await stub.fetch(shardPost("/prepare", { coordinatorTxId: txId, intents: [{ sql: "INSERT INTO t (id) VALUES (?)", params: ["row-r"], tenantId: "t1", table: "t", partitionKey: "row-r" }] }));

    const coordinatorId = env.COORDINATOR.idFromName(txId);
    const coordinator = env.COORDINATOR.get(coordinatorId);
    await coordinator.fetch(post("/tx-status", { txId: "schema-warmup" }));
    await runInDurableObject(coordinator, async (_instance: CoordinatorDO, state: DurableObjectState) => {
      const now = new Date().toISOString();
      state.storage.sql.exec(
        `INSERT INTO transactions (tx_id, status, participant_shards_json, operation_json, created_at, updated_at) VALUES (?, 'committed', ?, ?, ?, ?)`,
        txId,
        JSON.stringify([shardA]),
        JSON.stringify([{ shardId: shardA, intents: [] }]),
        now,
        now,
      );
      state.storage.sql.exec(
        "INSERT INTO recovery_queue (tx_id, action, next_attempt_at, attempt_count) VALUES (?, '/commit', ?, 0)",
        txId,
        now,
      );
    });

    await runInDurableObject(coordinator, async (instance: CoordinatorDO) => {
      await instance.alarm();
    });

    const checkRes = await stub.fetch(shardPost("/execute", { sql: "SELECT * FROM t WHERE id = ?", params: ["row-r"], requestId: "check-r", isMutation: false }));
    expect(((await checkRes.json()) as { rows: unknown[] }).rows).toHaveLength(1);

    await runInDurableObject(coordinator, async (_instance: CoordinatorDO, state: DurableObjectState) => {
      const remaining = Array.from(state.storage.sql.exec("SELECT * FROM recovery_queue WHERE tx_id = ?", txId));
      expect(remaining).toHaveLength(0);
    });
  });
});

describe("CoordinatorDO protocol-0 deploy-boundary adoption", () => {
  type LegacyIntent = {
    sql: string;
    params: string[];
    tenantId: string;
    table: string;
    partitionKey: string;
  };

  async function seedLegacyPreparedParticipant(shardName: string, txId: string, intents: LegacyIntent[]) {
    const shard = await freshShard(shardName);
    await shard.fetch(shardPost("/execute", {
      sql: "CREATE TABLE IF NOT EXISTS t (id TEXT PRIMARY KEY)",
      requestId: `schema-${txId}`,
      isMutation: true,
    }));
    expect((await shard.fetch(shardPost("/prepare", { coordinatorTxId: txId, intents }))).status).toBe(200);
    await runInDurableObject(shard, async (_instance: ShardDO, state: DurableObjectState) => {
      state.storage.sql.exec(
        "UPDATE pending_intents SET protocol_version = 0, operation_hash = '' WHERE coordinator_tx_id = ?",
        txId,
      );
    });
    return shard;
  }

  async function seedLegacyCoordinator(
    txId: string,
    shardName: string,
    intents: LegacyIntent[],
    options: { status?: "preparing" | "prepared" | "committed" | "aborted"; queueResume?: boolean } = {},
  ) {
    const coordinator = env.COORDINATOR.get(env.COORDINATOR.idFromName(txId));
    await coordinator.fetch(post("/tx-status", { txId: "schema-warmup" }));
    const operationJson = JSON.stringify([{ shardId: shardName, intents }]);
    const predecessorHash = await sha256Hex(operationJson);
    const status = options.status ?? "prepared";
    await runInDurableObject(coordinator, async (_instance: CoordinatorDO, state: DurableObjectState) => {
      const now = new Date().toISOString();
      state.storage.sql.exec(
        `INSERT INTO transactions
          (tx_id, status, participant_shards_json, operation_json, operation_hash, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?)`,
        txId,
        status,
        JSON.stringify([shardName]),
        operationJson,
        predecessorHash,
        now,
        now,
      );
      if (options.queueResume) {
        state.storage.sql.exec(
          "INSERT INTO recovery_queue (tx_id, action, next_attempt_at, attempt_count) VALUES (?, 'commit', ?, 0)",
          txId,
          now,
        );
      }
    });
    return coordinator;
  }

  it("replays exact prepare adoption before manifest registration and converges to terminal commit", async () => {
    const txId = `tx-legacy-adopt-${crypto.randomUUID()}`;
    const shardName = `shard-${txId}`;
    const intents = [{ sql: "INSERT INTO t (id) VALUES (?)", params: [txId], tenantId: "t1", table: "t", partitionKey: txId }];
    const shard = await seedLegacyPreparedParticipant(shardName, txId, intents);
    const coordinator = await seedLegacyCoordinator(txId, shardName, intents);

    const resumed = await coordinator.fetch(post("/begin", { txId, participants: [{ shardId: shardName, intents }] }));
    expect(resumed.status).toBe(200);
    expect(((await resumed.json()) as { status: string }).status).toBe("committed");

    const read = await shard.fetch(shardPost("/execute", {
      sql: "SELECT id FROM t WHERE id = ?",
      params: [txId],
      requestId: `read-${txId}`,
      isMutation: false,
    }));
    expect(((await read.json()) as { rows: unknown[] }).rows).toHaveLength(1);
    await runInDurableObject(shard, async (_instance: ShardDO, state: DurableObjectState) => {
      const adopted = state.storage.sql.exec<{ protocol_version: number; operation_hash: string; status: string }>(
        "SELECT protocol_version, operation_hash, status FROM pending_intents WHERE coordinator_tx_id = ?",
        txId,
      ).one();
      expect(adopted.protocol_version).toBe(1);
      expect(adopted.operation_hash).toMatch(/^[a-f0-9]{64}$/);
      expect(adopted.status).toBe("committed");
    });
  });

  it("adopts an exact predecessor preparing row without stranding its participant lock", async () => {
    const txId = `tx-legacy-preparing-${crypto.randomUUID()}`;
    const shardName = `shard-${txId}`;
    const intents = [{ sql: "INSERT INTO t (id) VALUES (?)", params: [txId], tenantId: "t1", table: "t", partitionKey: txId }];
    const shard = await seedLegacyPreparedParticipant(shardName, txId, intents);
    const coordinator = await seedLegacyCoordinator(txId, shardName, intents, { status: "preparing" });

    const resumed = await coordinator.fetch(post("/begin", { txId, participants: [{ shardId: shardName, intents }] }));
    expect(resumed.status).toBe(200);
    expect(((await resumed.json()) as { status: string }).status).toBe("committed");

    await runInDurableObject(shard, async (_instance: ShardDO, state: DurableObjectState) => {
      expect(Array.from(state.storage.sql.exec("SELECT * FROM row_locks WHERE coordinator_tx_id = ?", txId))).toHaveLength(0);
      const intent = state.storage.sql.exec<{ protocol_version: number; status: string }>(
        "SELECT protocol_version, status FROM pending_intents WHERE coordinator_tx_id = ?",
        txId,
      ).one();
      expect(intent).toEqual({ protocol_version: 1, status: "committed" });
    });
  });

  it.each([
    ["committed", 200, "committed", undefined],
    ["aborted", 409, undefined, "TX_ABORTED"],
  ] as const)("returns the predecessor %s terminal result for an exact replay", async (status, httpStatus, expectedStatus, expectedCode) => {
    const txId = `tx-legacy-terminal-${status}-${crypto.randomUUID()}`;
    const shardName = `shard-${txId}`;
    const intents = [{ sql: "INSERT INTO t (id) VALUES (?)", params: [txId], tenantId: "t1", table: "t", partitionKey: txId }];
    const coordinator = await seedLegacyCoordinator(txId, shardName, intents, { status });

    const replay = await coordinator.fetch(post("/begin", { txId, participants: [{ shardId: shardName, intents }] }));
    expect(replay.status).toBe(httpStatus);
    const body = (await replay.json()) as { status?: string; error?: { code: string } };
    expect(body.status).toBe(expectedStatus);
    expect(body.error?.code).toBe(expectedCode);
  });

  it("accepts an exact predecessor retry when its stored participant order was caller-controlled", async () => {
    const txId = `tx-legacy-order-${crypto.randomUUID()}`;
    const shardAName = `shard-a-${txId}`;
    const shardBName = `shard-b-${txId}`;
    const intentsA = [{ sql: "INSERT INTO t (id) VALUES (?)", params: [`a-${txId}`], tenantId: "t1", table: "t", partitionKey: `a-${txId}` }];
    const intentsB = [{ sql: "INSERT INTO t (id) VALUES (?)", params: [`b-${txId}`], tenantId: "t1", table: "t", partitionKey: `b-${txId}` }];
    const shardA = await seedLegacyPreparedParticipant(shardAName, txId, intentsA);
    const shardB = await seedLegacyPreparedParticipant(shardBName, txId, intentsB);
    const storedParticipants = [
      { shardId: shardBName, intents: intentsB },
      { shardId: shardAName, intents: intentsA },
    ];
    const operationJson = JSON.stringify(storedParticipants);
    const predecessorHash = await sha256Hex(operationJson);
    const coordinator = env.COORDINATOR.get(env.COORDINATOR.idFromName(txId));
    await coordinator.fetch(post("/tx-status", { txId: "schema-warmup" }));
    await runInDurableObject(coordinator, async (_instance: CoordinatorDO, state: DurableObjectState) => {
      const now = new Date().toISOString();
      state.storage.sql.exec(
        `INSERT INTO transactions
          (tx_id, status, participant_shards_json, operation_json, operation_hash, created_at, updated_at)
         VALUES (?, 'prepared', ?, ?, ?, ?, ?)`,
        txId,
        JSON.stringify([shardBName, shardAName]),
        operationJson,
        predecessorHash,
        now,
        now,
      );
    });

    // The gateway normalizes incoming participants to A,B. The legacy digest
    // must still be verified against the original stored B,A serialization.
    const resumed = await coordinator.fetch(post("/begin", { txId, participants: storedParticipants }));
    expect(resumed.status).toBe(200);
    expect(((await resumed.json()) as { status: string }).status).toBe("committed");

    for (const [shard, id] of [[shardA, `a-${txId}`], [shardB, `b-${txId}`]] as const) {
      const read = await shard.fetch(shardPost("/execute", {
        sql: "SELECT id FROM t WHERE id = ?",
        params: [id],
        requestId: `read-${id}`,
        isMutation: false,
      }));
      expect(((await read.json()) as { rows: unknown[] }).rows).toHaveLength(1);
    }
  });

  it("alarm resume rejects mismatched stored participant identity and aborts without committing", async () => {
    const txId = `tx-legacy-mismatch-${crypto.randomUUID()}`;
    const shardName = `shard-${txId}`;
    const coordinatorIntents = [{ sql: "INSERT INTO t (id) VALUES (?)", params: [`expected-${txId}`], tenantId: "t1", table: "t", partitionKey: `expected-${txId}` }];
    const participantIntents = [{ sql: "INSERT INTO t (id) VALUES (?)", params: [`different-${txId}`], tenantId: "t1", table: "t", partitionKey: `different-${txId}` }];
    const shard = await seedLegacyPreparedParticipant(shardName, txId, participantIntents);
    const coordinator = await seedLegacyCoordinator(txId, shardName, coordinatorIntents, { queueResume: true });

    await runInDurableObject(coordinator, async (instance: CoordinatorDO) => {
      await instance.alarm();
    });
    const status = await coordinator.fetch(post("/tx-status", { txId }));
    expect(((await status.json()) as { status: string }).status).toBe("aborted");

    await runInDurableObject(shard, async (_instance: ShardDO, state: DurableObjectState) => {
      expect(Array.from(state.storage.sql.exec("SELECT * FROM pending_intents WHERE coordinator_tx_id = ?", txId))).toHaveLength(0);
      expect(state.storage.sql.exec<{ decision: string }>("SELECT decision FROM participant_decision_tombstones WHERE tx_id = ?", txId).one().decision).toBe("abort");
    });
  });

  it("force-aborts an unadopted prepared row and its abort tombstone blocks a later commit", async () => {
    const txId = `tx-legacy-force-abort-${crypto.randomUUID()}`;
    const shardName = `shard-${txId}`;
    const intents = [{ sql: "INSERT INTO t (id) VALUES (?)", params: [txId], tenantId: "t1", table: "t", partitionKey: txId }];
    const shard = await seedLegacyPreparedParticipant(shardName, txId, intents);
    const coordinator = await seedLegacyCoordinator(txId, shardName, intents);

    const forced = await coordinator.fetch(post("/force-abort", { txId }));
    expect(forced.status).toBe(200);
    expect(((await forced.json()) as { status: string }).status).toBe("aborted");
    await runInDurableObject(shard, async (_instance: ShardDO, state: DurableObjectState) => {
      expect(Array.from(state.storage.sql.exec("SELECT * FROM pending_intents WHERE coordinator_tx_id = ?", txId))).toHaveLength(0);
      expect(Array.from(state.storage.sql.exec("SELECT * FROM row_locks WHERE coordinator_tx_id = ?", txId))).toHaveLength(0);
    });

    const lateCommit = await shard.fetch(shardPost("/commit", { coordinatorTxId: txId }));
    expect(lateCommit.status).toBe(409);
    expect(((await lateCommit.json()) as { error: { code: string } }).error.code).toBe("TX_DECISION_CONFLICT");
  });
});

describe("CoordinatorDO manifest admission and lifecycle", () => {
  it.each(["prepared", "commit_pending_manifest"] as const)(
    "bridges a model-1 %s transaction through V2 after V1_CLOSED without re-preparing participants",
    async (seedStatus) => {
      const { coordinator, txId, participants } = await seedV1CoordinatorForBridge(seedStatus);
      await runInDurableObject(coordinator, async (instance: CoordinatorDO, state: DurableObjectState) => {
        const v2 = defaultV2ManifestMethods();
        let legacyRegistrations = 0;
        let assignments = 0;
        let reserves = 0;
        let finalizations = 0;
        const service: TransactionManifestService = {
          ...v2,
          async assignManifestRoute(request) {
            assignments += 1;
            return v2.assignManifestRoute(request);
          },
          async reserveManifest(request) {
            reserves += 1;
            return v2.reserveManifest(request);
          },
          async finalizeManifest(request) {
            finalizations += 1;
            return v2.finalizeManifest(request);
          },
          async checkManifestAdmission() {
            return { ok: true as const, status: "ready" as const, circuit_policy: { failure_threshold: 3 as const, failure_window_ms: 30_000 as const, maximum_cooldown_ms: 300_000 as const } };
          },
          async registerManifest() {
            legacyRegistrations += 1;
            return {
              ok: false as const,
              status: "rejected" as const,
              http_status: 409,
              error: { schema_version: 1 as const, code: "V1_CLOSED" as const, message: "legacy manifest bucket is fenced", http_status: 409, retryable: false },
            };
          },
          async releaseManifestRetention() {
            return { ok: true as const, status: "released" as const };
          },
        };
        const phases: string[] = [];
        const mutable = instance as unknown as {
          coordinatorEnv: { CONTROL_PLANE?: TransactionManifestService };
          callShard: (row: unknown, participant: unknown, phase: "prepare" | "commit" | "abort") => Promise<Response>;
        };
        mutable.coordinatorEnv.CONTROL_PLANE = service;
        mutable.callShard = async (_row, _participant, phase) => {
          phases.push(phase);
          return new Response("{}", { status: 200 });
        };

        const response = await instance.fetch(post("/begin", { txId, participants }));
        expect(response.status).toBe(200);
        expect(await response.json()).toMatchObject({ status: "committed" });
        expect(phases).toEqual(["commit"]);
        expect(legacyRegistrations).toBe(1);
        expect(assignments).toBe(1);
        expect(reserves).toBe(1);
        expect(finalizations).toBe(1);
        const durable = state.storage.sql.exec<{
          status: string;
          state_model_version: number;
          redo_envelope_json: string;
          manifest_registration_json: string;
          manifest_reservation_json: string;
          manifest_finalize_request_json: string;
          manifest_record_json: string;
        }>(
          `SELECT status, state_model_version, redo_envelope_json,
                  manifest_registration_json, manifest_reservation_json,
                  manifest_finalize_request_json, manifest_record_json
             FROM transactions WHERE tx_id = ?`,
          txId,
        ).one();
        expect(durable.status).toBe("committed");
        expect(durable.state_model_version).toBe(2);
        expect(JSON.parse(durable.redo_envelope_json)).toMatchObject({ tx_id: txId });
        expect(JSON.parse(durable.manifest_registration_json)).toMatchObject({ record: { tx_id: txId } });
        expect(JSON.parse(durable.manifest_reservation_json)).toMatchObject({ tx_id: txId });
        expect(JSON.parse(durable.manifest_finalize_request_json)).toMatchObject({ intent: { tx_id: txId } });
        expect(JSON.parse(durable.manifest_record_json)).toMatchObject({ tx_id: txId });
      });
    },
  );

  it("recovers an ambiguous V1_CLOSED bridge reserve by alarm without re-registering V1 or preparing again", async () => {
    const { coordinator, txId, participants } = await seedV1CoordinatorForBridge("prepared");
    let reserves = 0;
    let legacyRegistrations = 0;
    const v2 = defaultV2ManifestMethods();
    const service: TransactionManifestService = {
      ...v2,
      async reserveManifest(request) {
        reserves += 1;
        if (reserves === 1) throw new Error("ambiguous bridge reserve acknowledgement");
        return v2.reserveManifest(request);
      },
      async checkManifestAdmission() {
        return { ok: true as const, status: "ready" as const, circuit_policy: { failure_threshold: 3 as const, failure_window_ms: 30_000 as const, maximum_cooldown_ms: 300_000 as const } };
      },
      async registerManifest() {
        legacyRegistrations += 1;
        return {
          ok: false as const,
          status: "rejected" as const,
          http_status: 409,
          error: { schema_version: 1 as const, code: "V1_CLOSED" as const, message: "legacy manifest bucket is fenced", http_status: 409, retryable: false },
        };
      },
      async releaseManifestRetention() {
        return { ok: true as const, status: "released" as const };
      },
    };
    const phases: string[] = [];

    await runInDurableObject(coordinator, async (instance: CoordinatorDO, state: DurableObjectState) => {
      const mutable = instance as unknown as {
        coordinatorEnv: { CONTROL_PLANE?: TransactionManifestService };
        callShard: (row: unknown, participant: unknown, phase: "prepare" | "commit" | "abort") => Promise<Response>;
      };
      mutable.coordinatorEnv.CONTROL_PLANE = service;
      mutable.callShard = async (_row, _participant, phase) => {
        phases.push(phase);
        return new Response("{}", { status: 200 });
      };
      const response = await instance.fetch(post("/begin", { txId, participants }));
      expect(response.status).toBe(202);
      expect(await response.json()).toMatchObject({ status: "commit_pending_manifest" });
      const pending = state.storage.sql.exec<{
        status: string;
        state_model_version: number;
        manifest_reservation_json: string;
      }>(
        "SELECT status, state_model_version, manifest_reservation_json FROM transactions WHERE tx_id = ?",
        txId,
      ).one();
      expect(pending.status).toBe("commit_pending_manifest");
      expect(pending.state_model_version).toBe(2);
      expect(JSON.parse(pending.manifest_reservation_json)).toMatchObject({ tx_id: txId });
      expect(state.storage.sql.exec<{ action: string }>("SELECT action FROM recovery_queue WHERE tx_id = ?", txId).one().action).toBe("reserve");
      expect(phases).toEqual([]);
    });

    await runInDurableObject(coordinator, async (instance: CoordinatorDO, state: DurableObjectState) => {
      (instance as unknown as { coordinatorEnv: { CONTROL_PLANE?: TransactionManifestService } }).coordinatorEnv.CONTROL_PLANE = service;
      (instance as unknown as { callShard: (row: unknown, participant: unknown, phase: "prepare" | "commit" | "abort") => Promise<Response> }).callShard = async (_row, _participant, phase) => {
        phases.push(phase);
        return new Response("{}", { status: 200 });
      };
      state.storage.sql.exec("UPDATE recovery_queue SET next_attempt_at = ? WHERE tx_id = ?", new Date(0).toISOString(), txId);
      await instance.alarm();
      expect(state.storage.sql.exec<{ status: string }>("SELECT status FROM transactions WHERE tx_id = ?", txId).one().status).toBe("committed");
    });
    expect(reserves).toBe(2);
    expect(legacyRegistrations).toBe(1);
    expect(phases).toEqual(["commit"]);
  });

  it("persists manifest_reserving and exact reserve recovery before participant prepare", async () => {
    const txId = `tx-reserve-boundary-${crypto.randomUUID()}`;
    const coordinator = env.COORDINATOR.get(env.COORDINATOR.idFromName(txId));
    await runInDurableObject(coordinator, async (instance: CoordinatorDO, state: DurableObjectState) => {
      const v2 = defaultV2ManifestMethods();
      let releaseReserve!: () => void;
      let markReserveEntered!: () => void;
      const reserveEntered = new Promise<void>((resolve) => { markReserveEntered = resolve; });
      const reserveBarrier = new Promise<void>((resolve) => { releaseReserve = resolve; });
      const service: TransactionManifestService = {
        ...v2,
        async reserveManifest(request) {
          markReserveEntered();
          await reserveBarrier;
          return v2.reserveManifest(request);
        },
        async checkManifestAdmission() {
          return { ok: true as const, status: "ready" as const, circuit_policy: { failure_threshold: 3 as const, failure_window_ms: 30_000 as const, maximum_cooldown_ms: 300_000 as const } };
        },
        async registerManifest(registration) {
          return { ok: true as const, status: "registered" as const, http_status: 200 as const, record_hash: registration.record_hash, quarantined: false as const };
        },
        async releaseManifestRetention() {
          return { ok: true as const, status: "released" as const };
        },
      };
      let prepareCalls = 0;
      const mutable = instance as unknown as {
        coordinatorEnv: { CONTROL_PLANE?: TransactionManifestService };
        callShard: (row: unknown, participant: unknown, phase: "prepare" | "commit" | "abort") => Promise<Response>;
      };
      mutable.coordinatorEnv.CONTROL_PLANE = service;
      mutable.callShard = async (_row, _participant, phase) => {
        if (phase === "prepare") prepareCalls += 1;
        return new Response("{}", { status: 200 });
      };

      const pending = instance.fetch(post("/begin", {
        txId,
        participants: [{ shardId: `shard-${txId}`, intents: [{ sql: "INSERT INTO t (id) VALUES (?)", params: [txId], tenantId: "t1", table: "t", partitionKey: txId }] }],
      }));
      await reserveEntered;
      const durable = state.storage.sql.exec<{
        status: string;
        manifest_reservation_json: string;
        manifest_reservation_hash: string;
      }>(
        "SELECT status, manifest_reservation_json, manifest_reservation_hash FROM transactions WHERE tx_id = ?",
        txId,
      ).one();
      expect(durable.status).toBe("manifest_reserving");
      expect(JSON.parse(durable.manifest_reservation_json)).toMatchObject({ tx_id: txId });
      expect(durable.manifest_reservation_hash).toMatch(/^[a-f0-9]{64}$/);
      expect(state.storage.sql.exec<{ action: string }>("SELECT action FROM recovery_queue WHERE tx_id = ?", txId).one().action).toBe("reserve");
      expect(prepareCalls).toBe(0);

      releaseReserve();
      expect((await pending).status).toBe(200);
      expect(prepareCalls).toBe(1);
    });
  });

  it("persists irreversible commit_deciding before finalize and stores the bucket-issued record before participant commit", async () => {
    const txId = `tx-finalize-boundary-${crypto.randomUUID()}`;
    const coordinator = env.COORDINATOR.get(env.COORDINATOR.idFromName(txId));
    await runInDurableObject(coordinator, async (instance: CoordinatorDO, state: DurableObjectState) => {
      const v2 = defaultV2ManifestMethods();
      let releaseFinalize!: () => void;
      let markFinalizeEntered!: () => void;
      const finalizeEntered = new Promise<void>((resolve) => { markFinalizeEntered = resolve; });
      const finalizeBarrier = new Promise<void>((resolve) => { releaseFinalize = resolve; });
      const service: TransactionManifestService = {
        ...v2,
        async finalizeManifest(request) {
          markFinalizeEntered();
          await finalizeBarrier;
          return v2.finalizeManifest(request);
        },
        async checkManifestAdmission() {
          return { ok: true as const, status: "ready" as const, circuit_policy: { failure_threshold: 3 as const, failure_window_ms: 30_000 as const, maximum_cooldown_ms: 300_000 as const } };
        },
        async registerManifest(registration) {
          return { ok: true as const, status: "registered" as const, http_status: 200 as const, record_hash: registration.record_hash, quarantined: false as const };
        },
        async releaseManifestRetention() {
          return { ok: true as const, status: "released" as const };
        },
      };
      let commitCalls = 0;
      const mutable = instance as unknown as {
        coordinatorEnv: { CONTROL_PLANE?: TransactionManifestService };
        callShard: (row: unknown, participant: unknown, phase: "prepare" | "commit" | "abort") => Promise<Response>;
      };
      mutable.coordinatorEnv.CONTROL_PLANE = service;
      mutable.callShard = async (_row, _participant, phase) => {
        if (phase === "commit") commitCalls += 1;
        return new Response("{}", { status: 200 });
      };

      const pending = instance.fetch(post("/begin", {
        txId,
        participants: [{ shardId: `shard-${txId}`, intents: [{ sql: "INSERT INTO t (id) VALUES (?)", params: [txId], tenantId: "t1", table: "t", partitionKey: txId }] }],
      }));
      await finalizeEntered;
      const deciding = state.storage.sql.exec<{ status: string; decision: string; manifest_finalize_request_json: string }>(
        "SELECT status, decision, manifest_finalize_request_json FROM transactions WHERE tx_id = ?",
        txId,
      ).one();
      expect(deciding.status).toBe("commit_deciding");
      expect(deciding.decision).toBe("commit");
      expect(JSON.parse(deciding.manifest_finalize_request_json)).toMatchObject({ intent: { tx_id: txId } });
      expect(commitCalls).toBe(0);
      const forced = await instance.fetch(post("/force-abort", { txId }));
      expect(forced.status).toBe(409);
      expect(((await forced.json()) as { error: { code: string } }).error.code).toBe("TX_COMMIT_ALREADY_DECIDED");

      releaseFinalize();
      expect((await pending).status).toBe(200);
      expect(commitCalls).toBe(1);
      const finalized = state.storage.sql.exec<{
        status: string;
        manifest_record_json: string;
        manifest_record_hash: string;
        commit_decided_at_ms: number;
        decision_sequence: number;
      }>(
        `SELECT status, manifest_record_json, manifest_record_hash,
                commit_decided_at_ms, decision_sequence
           FROM transactions WHERE tx_id = ?`,
        txId,
      ).one();
      expect(finalized.status).toBe("committed");
      expect(JSON.parse(finalized.manifest_record_json)).toMatchObject({ tx_id: txId });
      expect(finalized.manifest_record_hash).toMatch(/^[a-f0-9]{64}$/);
      expect(finalized.commit_decided_at_ms).toBeGreaterThan(0);
      expect(finalized.decision_sequence).toBe(1);
      expect(state.storage.sql.exec<{ action: string }>(
        "SELECT action FROM recovery_queue WHERE tx_id = ?",
        txId,
      ).one().action).toBe("release");

      service.resolveManifestQuarantine = async (resolution) => {
        expect(resolution).toMatchObject({
          resolution: "FINALIZED",
          coordinator_state: { state: "committed", decision: "commit" },
        });
        return {
          ok: true as const,
          status: "resolved" as const,
          resolution: "FINALIZED" as const,
          resolution_attestation_hash: "e".repeat(64),
          record: JSON.parse(finalized.manifest_record_json) as ManifestRecordV2,
          record_hash: finalized.manifest_record_hash,
        };
      };
      const repair = await instance.fetch(post("/resolve-manifest-quarantine", {
        txId,
        resolution: "FINALIZED",
        selectedHash: finalized.manifest_record_hash,
        evidenceHash: "d".repeat(64),
        actor: "operator@example.com",
        reason: "Canonical committed record resolves a stale terminal conflict.",
        idempotencyKey: `resolve-${txId}`,
      }));
      expect(repair.status).toBe(200);
      expect(await repair.json()).toMatchObject({ status: "committed" });
      expect(state.storage.sql.exec<{ status: string }>(
        "SELECT status FROM transactions WHERE tx_id = ?",
        txId,
      ).one().status).toBe("committed");
      expect(state.storage.sql.exec<{ action: string }>(
        "SELECT action FROM recovery_queue WHERE tx_id = ?",
        txId,
      ).one().action).toBe("release");
      expect(commitCalls).toBe(1);
    });
  });

  it("aborts participants before cancellation and parks a quarantined cancellation without a retry alarm", async () => {
    const txId = `tx-cancel-park-${crypto.randomUUID()}`;
    const coordinator = env.COORDINATOR.get(env.COORDINATOR.idFromName(txId));
    await runInDurableObject(coordinator, async (instance: CoordinatorDO, state: DurableObjectState) => {
      let cancelCalls = 0;
      const service: TransactionManifestService = {
        ...defaultV2ManifestMethods(),
        async cancelManifest() {
          cancelCalls += 1;
          return {
            ok: false as const,
            status: "quarantined_pending_resolution" as const,
            http_status: 409,
            error: { schema_version: 1 as const, code: "MANIFEST_QUARANTINED" as const, message: "operator resolution required", http_status: 409, retryable: false },
          };
        },
        async checkManifestAdmission() {
          return { ok: true as const, status: "ready" as const, circuit_policy: { failure_threshold: 3 as const, failure_window_ms: 30_000 as const, maximum_cooldown_ms: 300_000 as const } };
        },
        async registerManifest(registration) {
          return { ok: true as const, status: "registered" as const, http_status: 200 as const, record_hash: registration.record_hash, quarantined: false as const };
        },
        async releaseManifestRetention() {
          return { ok: true as const, status: "released" as const };
        },
      };
      const phases: string[] = [];
      const mutable = instance as unknown as {
        coordinatorEnv: { CONTROL_PLANE?: TransactionManifestService };
        callShard: (row: unknown, participant: unknown, phase: "prepare" | "commit" | "abort") => Promise<Response>;
      };
      mutable.coordinatorEnv.CONTROL_PLANE = service;
      mutable.callShard = async (_row, _participant, phase) => {
        phases.push(phase);
        return new Response("{}", { status: phase === "prepare" ? 409 : 200 });
      };

      const begin = await instance.fetch(post("/begin", {
        txId,
        participants: [{ shardId: `shard-${txId}`, intents: [{ sql: "bad mutation", params: [], tenantId: "t1", table: "t", partitionKey: txId }] }],
      }));
      expect(begin.status).toBe(409);
      expect(phases).toEqual(["prepare", "abort"]);
      expect(cancelCalls).toBe(1);
      const parked = state.storage.sql.exec<{ status: string; decision: string; manifest_cancel_request_json: string }>(
        "SELECT status, decision, manifest_cancel_request_json FROM transactions WHERE tx_id = ?",
        txId,
      ).one();
      expect(parked.status).toBe("aborted_pending_manifest_cancel");
      expect(parked.decision).toBe("abort");
      expect(JSON.parse(parked.manifest_cancel_request_json)).toMatchObject({ intent: { tx_id: txId } });
      expect(Array.from(state.storage.sql.exec("SELECT * FROM recovery_queue WHERE tx_id = ?", txId))).toHaveLength(0);

      const retry = await instance.fetch(post("/begin", {
        txId,
        participants: [{ shardId: `shard-${txId}`, intents: [{ sql: "bad mutation", params: [], tenantId: "t1", table: "t", partitionKey: txId }] }],
      }));
      expect(retry.status).toBe(202);
      expect(cancelCalls, "parked cancellation must not hot-loop on ordinary retries").toBe(1);

      service.resolveManifestQuarantine = async (resolution) => {
        expect(resolution).toMatchObject({
          resolution: "CANCELLED",
          reservation: { tx_id: txId },
          coordinator_state: { state: "aborted_pending_manifest_cancel", decision: "abort" },
        });
        return {
          ok: true as const,
          status: "resolved" as const,
          resolution: "CANCELLED" as const,
          resolution_attestation_hash: "e".repeat(64),
        };
      };
      const repair = await instance.fetch(post("/resolve-manifest-quarantine", {
        txId,
        resolution: "CANCELLED",
        selectedHash: "c".repeat(64),
        evidenceHash: "d".repeat(64),
        actor: "operator@example.com",
        reason: "Durable coordinator abort authorizes cancellation.",
        idempotencyKey: `resolve-${txId}`,
      }));
      expect(repair.status).toBe(200);
      expect(await repair.json()).toMatchObject({
        ok: true,
        txId,
        status: "aborted",
        resolutionAttestationHash: "e".repeat(64),
      });
      expect(state.storage.sql.exec<{ status: string }>("SELECT status FROM transactions WHERE tx_id = ?", txId).one().status).toBe("aborted");
    });
  });

  it("concurrent identical prepare failures converge to a typed abort instead of a CAS 500", async () => {
    const txId = `tx-abort-race-${crypto.randomUUID()}`;
    const coordinator = env.COORDINATOR.get(env.COORDINATOR.idFromName(txId));
    await runInDurableObject(coordinator, async (instance: CoordinatorDO, state: DurableObjectState) => {
      const service: TransactionManifestService = {
        ...defaultV2ManifestMethods(),
        async checkManifestAdmission() {
          return { ok: true as const, status: "ready" as const, circuit_policy: { failure_threshold: 3 as const, failure_window_ms: 30_000 as const, maximum_cooldown_ms: 300_000 as const } };
        },
        async registerManifest(registration) {
          return { ok: true as const, status: "registered" as const, http_status: 200 as const, record_hash: registration.record_hash, quarantined: false as const };
        },
        async releaseManifestRetention() {
          return { ok: true as const, status: "released" as const };
        },
      };
      let prepareCalls = 0;
      let releasePrepares!: () => void;
      const prepareBarrier = new Promise<void>((resolve) => { releasePrepares = resolve; });
      const mutable = instance as unknown as {
        coordinatorEnv: { CONTROL_PLANE?: TransactionManifestService };
        callShard: (row: unknown, participant: unknown, phase: "prepare" | "commit" | "abort") => Promise<Response>;
      };
      mutable.coordinatorEnv.CONTROL_PLANE = service;
      mutable.callShard = async (_row, _participant, phase) => {
        if (phase !== "prepare") return new Response("{}", { status: 200 });
        prepareCalls += 1;
        if (prepareCalls === 2) releasePrepares();
        await prepareBarrier;
        return new Response("{}", { status: 409 });
      };
      const payload = {
        txId,
        participants: [{ shardId: `shard-${txId}`, intents: [{ sql: "bad mutation", params: [], tenantId: "t1", table: "t", partitionKey: txId }] }],
      };
      const [first, second] = await Promise.all([
        instance.fetch(post("/begin", payload)),
        instance.fetch(post("/begin", payload)),
      ]);
      expect([first.status, second.status]).toEqual([409, 409]);
      expect(state.storage.sql.exec<{ status: string }>(
        "SELECT status FROM transactions WHERE tx_id = ?",
        txId,
      ).one().status).toBe("aborted");
    });
  });

  it("concurrent identical retries waiting on manifest finalization converge instead of returning a CAS 500", async () => {
    const txId = `tx-manifest-race-${crypto.randomUUID()}`;
    const coordinator = env.COORDINATOR.get(env.COORDINATOR.idFromName(txId));

    await runInDurableObject(coordinator, async (instance: CoordinatorDO, state: DurableObjectState) => {
      let finalizationMode: "ambiguous" | "barrier" = "ambiguous";
      let barrierCalls = 0;
      let releaseBarrier!: () => void;
      const barrier = new Promise<void>((resolve) => { releaseBarrier = resolve; });
      const v2 = defaultV2ManifestMethods();
      const service: TransactionManifestService = {
        ...v2,
        async checkManifestAdmission() {
          return { ok: true as const, status: "ready" as const, circuit_policy: { failure_threshold: 3 as const, failure_window_ms: 30_000 as const, maximum_cooldown_ms: 300_000 as const } };
        },
        async finalizeManifest(request) {
          if (finalizationMode === "ambiguous") {
            return {
              ok: false as const,
              status: "unavailable" as const,
              http_status: 503 as const,
              error: { schema_version: 1 as const, code: "MANIFEST_TEMPORARILY_UNAVAILABLE" as const, message: "test outage", http_status: 503, retryable: true },
            };
          }
          barrierCalls += 1;
          if (barrierCalls === 2) releaseBarrier();
          await barrier;
          return v2.finalizeManifest(request);
        },
        async registerManifest(registration) {
          return { ok: true as const, status: "registered" as const, http_status: 200 as const, record_hash: registration.record_hash, quarantined: false as const };
        },
        async releaseManifestRetention() {
          return { ok: true as const, status: "released" as const };
        },
      };
      const mutable = instance as unknown as {
        coordinatorEnv: { CONTROL_PLANE?: TransactionManifestService };
        callShard: (row: unknown, participant: unknown, phase: "prepare" | "commit" | "abort") => Promise<Response>;
      };
      mutable.coordinatorEnv.CONTROL_PLANE = service;
      mutable.callShard = async () => new Response("{}", { status: 200, headers: { "content-type": "application/json" } });
      const payload = {
        txId,
        participants: [{ shardId: `shard-${txId}`, intents: [{ sql: "INSERT INTO t (id) VALUES (?)", params: [txId], tenantId: "t1", table: "t", partitionKey: txId }] }],
      };

      const first = await instance.fetch(post("/begin", payload));
      expect(first.status).toBe(202);
      expect(((await first.json()) as { status: string }).status).toBe("commit_pending_manifest");

      finalizationMode = "barrier";
      const [retryA, retryB] = await Promise.all([
        instance.fetch(post("/begin", payload)),
        instance.fetch(post("/begin", payload)),
      ]);
      expect([retryA.status, retryB.status]).toEqual([200, 200]);
      expect((await retryA.json()) as { status: string }).toMatchObject({ status: "committed" });
      expect((await retryB.json()) as { status: string }).toMatchObject({ status: "committed" });
      expect(state.storage.sql.exec<{ status: string }>("SELECT status FROM transactions WHERE tx_id = ?", txId).one().status).toBe("committed");
      expect(state.storage.sql.exec<{ action: string }>("SELECT action FROM recovery_queue WHERE tx_id = ?", txId).one().action).toBe("release");
    });
  });

  it("concurrent identical retries waiting on participant acknowledgements converge instead of returning a CAS 500", async () => {
    const txId = `tx-ack-race-${crypto.randomUUID()}`;
    const coordinator = env.COORDINATOR.get(env.COORDINATOR.idFromName(txId));

    await runInDurableObject(coordinator, async (instance: CoordinatorDO, state: DurableObjectState) => {
      const service: TransactionManifestService = {
        ...defaultV2ManifestMethods(),
        async checkManifestAdmission() {
          return { ok: true as const, status: "ready" as const, circuit_policy: { failure_threshold: 3 as const, failure_window_ms: 30_000 as const, maximum_cooldown_ms: 300_000 as const } };
        },
        async registerManifest(registration) {
          return { ok: true as const, status: "registered" as const, http_status: 200 as const, record_hash: registration.record_hash, quarantined: false as const };
        },
        async releaseManifestRetention() {
          return { ok: true as const, status: "released" as const };
        },
      };
      let commitMode: "fail" | "barrier" = "fail";
      let barrierCalls = 0;
      let releaseBarrier!: () => void;
      const barrier = new Promise<void>((resolve) => { releaseBarrier = resolve; });
      const mutable = instance as unknown as {
        coordinatorEnv: { CONTROL_PLANE?: TransactionManifestService };
        callShard: (row: unknown, participant: unknown, phase: "prepare" | "commit" | "abort") => Promise<Response>;
      };
      mutable.coordinatorEnv.CONTROL_PLANE = service;
      mutable.callShard = async (_row, _participant, phase) => {
        if (phase !== "commit") return new Response("{}", { status: 200 });
        if (commitMode === "fail") return new Response("{}", { status: 503 });
        barrierCalls += 1;
        if (barrierCalls === 2) releaseBarrier();
        await barrier;
        return new Response("{}", { status: 200 });
      };
      const payload = {
        txId,
        participants: [{ shardId: `shard-${txId}`, intents: [{ sql: "INSERT INTO t (id) VALUES (?)", params: [txId], tenantId: "t1", table: "t", partitionKey: txId }] }],
      };

      const first = await instance.fetch(post("/begin", payload));
      expect(first.status).toBe(202);
      expect(((await first.json()) as { status: string }).status).toBe("committed_pending_ack");

      commitMode = "barrier";
      const [retryA, retryB] = await Promise.all([
        instance.fetch(post("/begin", payload)),
        instance.fetch(post("/begin", payload)),
      ]);
      expect([retryA.status, retryB.status]).toEqual([200, 200]);
      expect((await retryA.json()) as { status: string }).toMatchObject({ status: "committed" });
      expect((await retryB.json()) as { status: string }).toMatchObject({ status: "committed" });
      expect(state.storage.sql.exec<{ status: string }>("SELECT status FROM transactions WHERE tx_id = ?", txId).one().status).toBe("committed");
      expect(state.storage.sql.exec<{ action: string }>("SELECT action FROM recovery_queue WHERE tx_id = ?", txId).one().action).toBe("release");
    });
  });

  it("preserves monotonic recovery backoff across repeated committed-pending-ack failures", async () => {
    const txId = `tx-ack-backoff-${crypto.randomUUID()}`;
    const coordinator = env.COORDINATOR.get(env.COORDINATOR.idFromName(txId));

    await runInDurableObject(coordinator, async (instance: CoordinatorDO, state: DurableObjectState) => {
      const service: TransactionManifestService = {
        ...defaultV2ManifestMethods(),
        async checkManifestAdmission() {
          return { ok: true as const, status: "ready" as const, circuit_policy: { failure_threshold: 3 as const, failure_window_ms: 30_000 as const, maximum_cooldown_ms: 300_000 as const } };
        },
        async registerManifest(registration) {
          return { ok: true as const, status: "registered" as const, http_status: 200 as const, record_hash: registration.record_hash, quarantined: false as const };
        },
        async releaseManifestRetention() {
          return { ok: true as const, status: "released" as const };
        },
      };
      const mutable = instance as unknown as {
        coordinatorEnv: { CONTROL_PLANE?: TransactionManifestService };
        callShard: (row: unknown, participant: unknown, phase: "prepare" | "commit" | "abort") => Promise<Response>;
      };
      mutable.coordinatorEnv.CONTROL_PLANE = service;
      mutable.callShard = async (_row, _participant, phase) => new Response("{}", { status: phase === "commit" ? 503 : 200 });
      const payload = {
        txId,
        participants: [{ shardId: `shard-${txId}`, intents: [{ sql: "INSERT INTO t (id) VALUES (?)", params: [txId], tenantId: "t1", table: "t", partitionKey: txId }] }],
      };
      const attemptCount = () => state.storage.sql.exec<{ attempt_count: number }>(
        "SELECT attempt_count FROM recovery_queue WHERE tx_id = ?",
        txId,
      ).one().attempt_count;

      const first = await instance.fetch(post("/begin", payload));
      expect(first.status).toBe(202);
      expect(attemptCount()).toBe(1);

      const second = await instance.fetch(post("/begin", payload));
      expect(second.status).toBe(202);
      expect(attemptCount()).toBe(2);

      const third = await instance.fetch(post("/begin", payload));
      expect(third.status).toBe(202);
      expect(attemptCount()).toBe(3);
    });
  });

  it("opens the admission circuit after three route-assignment failures and recovers through a half-open probe", async () => {
    const txId = `tx-circuit-${crypto.randomUUID()}`;
    const shardName = `shard-${txId}`;
    const shard = await freshShard(shardName);
    await shard.fetch(shardPost("/execute", {
      sql: "CREATE TABLE IF NOT EXISTS t (id TEXT PRIMARY KEY)",
      requestId: `schema-${txId}`,
      isMutation: true,
    }));
    const coordinator = env.COORDINATOR.get(env.COORDINATOR.idFromName(txId));

    await runInDurableObject(coordinator, async (instance: CoordinatorDO, state: DurableObjectState) => {
      let failAssignment = true;
      let assignmentCalls = 0;
      const v2 = defaultV2ManifestMethods();
      const service: TransactionManifestService = {
        ...v2,
        async assignManifestRoute(request) {
          assignmentCalls += 1;
          if (failAssignment) {
            return {
              ok: false as const,
              status: "unavailable" as const,
              http_status: 503,
              error: { schema_version: 1 as const, code: "MANIFEST_TEMPORARILY_UNAVAILABLE" as const, message: "test outage", http_status: 503, retryable: true },
            };
          }
          return v2.assignManifestRoute(request);
        },
        async checkManifestAdmission() {
          return { ok: true as const, status: "ready" as const, circuit_policy: { failure_threshold: 3 as const, failure_window_ms: 30_000 as const, maximum_cooldown_ms: 300_000 as const } };
        },
        async registerManifest(registration) {
          return { ok: true as const, status: "registered" as const, http_status: 200 as const, record_hash: registration.record_hash, quarantined: false as const };
        },
        async releaseManifestRetention() {
          return { ok: true as const, status: "released" as const };
        },
      };
      (instance as unknown as { coordinatorEnv: { CONTROL_PLANE?: TransactionManifestService } }).coordinatorEnv.CONTROL_PLANE = service;

      const begin = () => instance.fetch(post("/begin", {
        txId,
        participants: [{ shardId: shardName, intents: [{ sql: "INSERT INTO t (id) VALUES (?)", params: [txId], tenantId: "t1", table: "t", partitionKey: txId }] }],
      }));
      expect((await begin()).status).toBe(503);
      expect((await begin()).status).toBe(503);
      expect((await begin()).status).toBe(503);
      expect(assignmentCalls).toBe(3);
      expect((await begin()).status).toBe(503);
      expect(assignmentCalls).toBe(3);
      expect(Array.from(state.storage.sql.exec("SELECT * FROM transactions WHERE tx_id = ?", txId))).toHaveLength(0);
      state.storage.sql.exec(
        "UPDATE manifest_admission_circuit SET open_until_ms = 1, half_open_probe = 0, half_open_probe_until_ms = 0 WHERE singleton = 1",
      );
      failAssignment = false;
      expect((await begin()).status).toBe(200);
      expect(assignmentCalls).toBe(4);
      expect(state.storage.sql.exec<{ state_model_version: number }>(
        "SELECT state_model_version FROM transactions WHERE tx_id = ?",
        txId,
      ).one().state_model_version).toBe(2);
    });
  });

  it("keeps participants prepared while manifest finalization is ambiguous, grows durable backoff, and converges by alarm", async () => {
    const txId = `tx-manifest-ambiguous-${crypto.randomUUID()}`;
    const shardName = `shard-${txId}`;
    const shard = await freshShard(shardName);
    await shard.fetch(shardPost("/execute", {
      sql: "CREATE TABLE IF NOT EXISTS t (id TEXT PRIMARY KEY)",
      requestId: `schema-${txId}`,
      isMutation: true,
    }));
    const coordinator = env.COORDINATOR.get(env.COORDINATOR.idFromName(txId));
    let finalizations = 0;
    const v2 = defaultV2ManifestMethods();
    const service: TransactionManifestService = {
      ...v2,
      async checkManifestAdmission() {
        return { ok: true as const, status: "ready" as const, circuit_policy: { failure_threshold: 3 as const, failure_window_ms: 30_000 as const, maximum_cooldown_ms: 300_000 as const } };
      },
      async finalizeManifest(request) {
        finalizations += 1;
        if (finalizations <= 2) throw new Error("ambiguous finalization transport failure");
        return v2.finalizeManifest(request);
      },
      async registerManifest(registration) {
        return { ok: true as const, status: "registered" as const, http_status: 200 as const, record_hash: registration.record_hash, quarantined: false as const };
      },
      async releaseManifestRetention() {
        return { ok: true as const, status: "released" as const };
      },
    };

    await runInDurableObject(coordinator, async (instance: CoordinatorDO, state: DurableObjectState) => {
      (instance as unknown as { coordinatorEnv: { CONTROL_PLANE?: TransactionManifestService } }).coordinatorEnv.CONTROL_PLANE = service;

      const begin = await instance.fetch(post("/begin", {
        txId,
        participants: [{ shardId: shardName, intents: [{ sql: "INSERT INTO t (id) VALUES (?)", params: [txId], tenantId: "t1", table: "t", partitionKey: txId }] }],
      }));
      expect(begin.status).toBe(202);
      expect(((await begin.json()) as { status: string }).status).toBe("commit_pending_manifest");
      expect(finalizations).toBe(1);

      expect(state.storage.sql.exec<{ attempt_count: number }>(
        "SELECT attempt_count FROM recovery_queue WHERE tx_id = ?",
        txId,
      ).one().attempt_count).toBe(1);
    });

    const beforeManifest = await shard.fetch(shardPost("/execute", {
      sql: "SELECT id FROM t WHERE id = ?",
      params: [txId],
      requestId: `read-before-manifest-${txId}`,
      isMutation: false,
    }));
    expect(((await beforeManifest.json()) as { rows: unknown[] }).rows).toHaveLength(0);

    await runInDurableObject(coordinator, async (instance: CoordinatorDO, state: DurableObjectState) => {
      (instance as unknown as { coordinatorEnv: { CONTROL_PLANE?: TransactionManifestService } }).coordinatorEnv.CONTROL_PLANE = service;
      state.storage.sql.exec("UPDATE recovery_queue SET next_attempt_at = ? WHERE tx_id = ?", new Date(0).toISOString(), txId);
      await instance.alarm();
      expect(finalizations).toBe(2);
      const secondRetry = state.storage.sql.exec<{ attempt_count: number; next_attempt_at: string }>(
        "SELECT attempt_count, next_attempt_at FROM recovery_queue WHERE tx_id = ?",
        txId,
      ).one();
      expect(secondRetry.attempt_count).toBe(2);
      expect(new Date(secondRetry.next_attempt_at).getTime() - Date.now()).toBeGreaterThanOrEqual(9_000);
    });

    const stillUncommitted = await shard.fetch(shardPost("/execute", {
      sql: "SELECT id FROM t WHERE id = ?",
      params: [txId],
      requestId: `read-second-ambiguity-${txId}`,
      isMutation: false,
    }));
    expect(((await stillUncommitted.json()) as { rows: unknown[] }).rows).toHaveLength(0);

    await runInDurableObject(coordinator, async (instance: CoordinatorDO, state: DurableObjectState) => {
      (instance as unknown as { coordinatorEnv: { CONTROL_PLANE?: TransactionManifestService } }).coordinatorEnv.CONTROL_PLANE = service;
      state.storage.sql.exec("UPDATE recovery_queue SET next_attempt_at = ? WHERE tx_id = ?", new Date(0).toISOString(), txId);
      await instance.alarm();
      expect(finalizations).toBe(3);
      const status = await instance.fetch(post("/tx-status", { txId }));
      expect(((await status.json()) as { status: string }).status).toBe("committed");
      expect(state.storage.sql.exec<{ action: string; attempt_count: number }>(
        "SELECT action, attempt_count FROM recovery_queue WHERE tx_id = ?",
        txId,
      ).one()).toEqual({ action: "release", attempt_count: 0 });
    });

    const committed = await shard.fetch(shardPost("/execute", {
      sql: "SELECT id FROM t WHERE id = ?",
      params: [txId],
      requestId: `read-committed-${txId}`,
      isMutation: false,
    }));
    expect(((await committed.json()) as { rows: unknown[] }).rows).toHaveLength(1);

    await runInDurableObject(coordinator, async (instance: CoordinatorDO, state: DurableObjectState) => {
      (instance as unknown as { coordinatorEnv: { CONTROL_PLANE?: TransactionManifestService } }).coordinatorEnv.CONTROL_PLANE = service;
      state.storage.sql.exec("UPDATE recovery_queue SET next_attempt_at = ? WHERE tx_id = ?", new Date(0).toISOString(), txId);
      await instance.alarm();
      expect(Array.from(state.storage.sql.exec("SELECT * FROM recovery_queue WHERE tx_id = ?", txId))).toHaveLength(0);
    });
  });

  it("quarantines deterministic manifest rejection and removes nonretryable recovery work", async () => {
    const txId = `tx-manifest-rejected-${crypto.randomUUID()}`;
    const shardName = `shard-${txId}`;
    const shard = await freshShard(shardName);
    await shard.fetch(shardPost("/execute", {
      sql: "CREATE TABLE IF NOT EXISTS t (id TEXT PRIMARY KEY)",
      requestId: `schema-${txId}`,
      isMutation: true,
    }));
    const coordinator = env.COORDINATOR.get(env.COORDINATOR.idFromName(txId));

    await runInDurableObject(coordinator, async (instance: CoordinatorDO, state: DurableObjectState) => {
      const service: TransactionManifestService = {
        ...defaultV2ManifestMethods(),
        async finalizeManifest() {
          return {
            ok: false as const,
            status: "conflict" as const,
            http_status: 409,
            error: { schema_version: 1 as const, code: "MANIFEST_TERMINAL_CONFLICT" as const, message: "conflicting terminal intent", http_status: 409, retryable: false },
          };
        },
        async checkManifestAdmission() {
          return { ok: true as const, status: "ready" as const, circuit_policy: { failure_threshold: 3 as const, failure_window_ms: 30_000 as const, maximum_cooldown_ms: 300_000 as const } };
        },
        async registerManifest() {
          return {
            ok: false as const,
            status: "rejected" as const,
            http_status: 503,
            error: { schema_version: 1 as const, code: "TX_VERSION_UNSUPPORTED" as const, message: "unsupported manifest version", http_status: 503, retryable: false },
          };
        },
        async releaseManifestRetention() {
          return { ok: true as const, status: "released" as const };
        },
      };
      (instance as unknown as { coordinatorEnv: { CONTROL_PLANE?: TransactionManifestService } }).coordinatorEnv.CONTROL_PLANE = service;

      const begin = await instance.fetch(post("/begin", {
        txId,
        participants: [{ shardId: shardName, intents: [{ sql: "INSERT INTO t (id) VALUES (?)", params: [txId], tenantId: "t1", table: "t", partitionKey: txId }] }],
      }));
      expect(begin.status).toBe(409);

      const durable = state.storage.sql.exec<{ status: string; decision: string; last_error: string }>(
        "SELECT status, decision, last_error FROM transactions WHERE tx_id = ?",
        txId,
      ).one();
      expect(durable.status).toBe("quarantined");
      expect(durable.decision).toBe("quarantined");
      expect(JSON.parse(durable.last_error)).toMatchObject({ code: "MANIFEST_TERMINAL_CONFLICT" });
      expect(Array.from(state.storage.sql.exec("SELECT * FROM recovery_queue WHERE tx_id = ?", txId))).toHaveLength(0);

      await instance.alarm();
      expect(Array.from(state.storage.sql.exec("SELECT * FROM recovery_queue WHERE tx_id = ?", txId))).toHaveLength(0);
      const replay = await instance.fetch(post("/begin", {
        txId,
        participants: [{ shardId: shardName, intents: [{ sql: "INSERT INTO t (id) VALUES (?)", params: [txId], tenantId: "t1", table: "t", partitionKey: txId }] }],
      }));
      expect(replay.status).toBe(409);
      expect(((await replay.json()) as { error: { code: string } }).error.code).toBe("TX_QUARANTINED");
    });
  });

  it("releases retention only after terminal commit and durably retries an ambiguous release", async () => {
    const txId = `tx-release-${crypto.randomUUID()}`;
    const shardName = `shard-${txId}`;
    const shard = await freshShard(shardName);
    await shard.fetch(shardPost("/execute", {
      sql: "CREATE TABLE IF NOT EXISTS t (id TEXT PRIMARY KEY)",
      requestId: `schema-${txId}`,
      isMutation: true,
    }));
    const coordinator = env.COORDINATOR.get(env.COORDINATOR.idFromName(txId));

    await runInDurableObject(coordinator, async (instance: CoordinatorDO, state: DurableObjectState) => {
      let releases = 0;
      const service: TransactionManifestService = {
        ...defaultV2ManifestMethods(),
        async releaseManifestV2() {
          releases += 1;
          if (releases === 1) throw new Error("ambiguous transport failure");
          return { ok: true as const, status: "released" as const };
        },
        async checkManifestAdmission() {
          return { ok: true as const, status: "ready" as const, circuit_policy: { failure_threshold: 3 as const, failure_window_ms: 30_000 as const, maximum_cooldown_ms: 300_000 as const } };
        },
        async registerManifest(registration) {
          return { ok: true as const, status: "registered" as const, http_status: 200 as const, record_hash: registration.record_hash, quarantined: false as const };
        },
        async releaseManifestRetention() {
          releases += 1;
          if (releases === 1) throw new Error("ambiguous transport failure");
          return { ok: true as const, status: "released" as const };
        },
      };
      (instance as unknown as { coordinatorEnv: { CONTROL_PLANE?: TransactionManifestService } }).coordinatorEnv.CONTROL_PLANE = service;

      const committed = await instance.fetch(post("/begin", {
        txId,
        participants: [{ shardId: shardName, intents: [{ sql: "INSERT INTO t (id) VALUES (?)", params: [txId], tenantId: "t1", table: "t", partitionKey: txId }] }],
      }));
      expect(committed.status).toBe(200);
      expect(releases, "release must not run before terminal participant acknowledgements").toBe(0);
      expect(state.storage.sql.exec<{ action: string }>("SELECT action FROM recovery_queue WHERE tx_id = ?", txId).one().action).toBe("release");

      await instance.alarm();
      expect(releases).toBe(1);
      const retry = state.storage.sql.exec<{ action: string; attempt_count: number }>(
        "SELECT action, attempt_count FROM recovery_queue WHERE tx_id = ?",
        txId,
      ).one();
      expect(retry.action).toBe("release");
      expect(retry.attempt_count).toBe(1);

      state.storage.sql.exec("UPDATE recovery_queue SET next_attempt_at = ? WHERE tx_id = ?", new Date(0).toISOString(), txId);
      await instance.alarm();
      expect(releases).toBe(2);
      expect(Array.from(state.storage.sql.exec("SELECT * FROM recovery_queue WHERE tx_id = ?", txId))).toHaveLength(0);
    });
  });
});
