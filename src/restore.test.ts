import { SELF, env, reset, runInDurableObject } from "cloudflare:test";
import { afterEach, describe, expect, it } from "vitest";
import { RestoreContractViolation, restoreError } from "../packages/contracts/src/index.js";
import type { RestoreCoordinatorDO } from "./restore";
import type { ParticipantPitrPort, ShardDO } from "./shard";

function internal(path: string, body: unknown): Request {
  return new Request(`https://restore.internal${path}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
}

function admin(path: string, body: unknown): Promise<Response> {
  return SELF.fetch(`https://worker.internal${path}`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      authorization: `Bearer ${env.ADMIN_TOKEN}`,
    },
    body: JSON.stringify(body),
  });
}

function restoreStub() {
  return env.RESTORE_COORDINATOR.getByName("deployment-restore-authority");
}

afterEach(async () => {
  await reset();
});

describe("RestoreCoordinatorDO external fleet gate", () => {
  it("serializes mutating work across awaited operations", async () => {
    const stub = restoreStub();
    await stub.fetch(internal("/gate", { fleet_id: "default" }));
    await runInDurableObject(stub, async (instance: RestoreCoordinatorDO) => {
      const order: string[] = [];
      let releaseFirst!: () => void;
      const firstBlocked = new Promise<void>((resolve) => { releaseFirst = resolve; });
      const runMutation = (instance as unknown as {
        runMutation<T>(work: () => Promise<T>): Promise<T>;
      }).runMutation.bind(instance);
      const first = runMutation(async () => {
        order.push("first:start");
        await firstBlocked;
        order.push("first:end");
      });
      const second = runMutation(async () => {
        order.push("second:start");
      });
      await Promise.resolve();
      expect(order).toEqual(["first:start"]);
      releaseFirst();
      await Promise.all([first, second]);
      expect(order).toEqual(["first:start", "first:end", "second:start"]);
    });
  });

  it("linearizes coordinator inventory before final fence release", async () => {
    const stub = restoreStub();
    await stub.fetch(internal("/gate", { fleet_id: "default" }));
    await runInDurableObject(stub, async (instance: RestoreCoordinatorDO, state: DurableObjectState) => {
      const now = new Date().toISOString();
      state.storage.sql.exec(
        `INSERT INTO restore_operations
          (restore_id, fleet_id, cutoff, idempotency_key, parameter_hash, phase, stage,
           previewed_at, execute_before, fence_generation, fence_installed_at, started_at, updated_at)
         VALUES ('restore-release-race', 'default', ?, 'release-race', ?, 'verifying',
                 'releasing_participants', ?, ?, 31, ?, ?, ?)`,
        new Date(Date.now() - 60_000).toISOString(),
        "a".repeat(64),
        now,
        new Date(Date.now() + 60_000).toISOString(),
        now,
        now,
        now,
      );
      state.storage.sql.exec(
        `UPDATE fleet_restore_gate SET active = 1, restore_id = 'restore-release-race', generation = 31,
         phase = 'verifying', activated_at = ? WHERE singleton = 1`,
        now,
      );
      const mutable = instance as unknown as {
        env: { CATALOG: unknown };
        operation: (restoreId: string) => unknown;
        completeRestore: (operation: unknown) => Promise<void>;
        handleRegisterCoordinator: (request: Request) => Promise<Response>;
        catalogShardCount: () => number;
      };
      let releaseCatalog!: () => void;
      let catalogEntered!: () => void;
      const catalogBlocked = new Promise<void>((resolve) => { releaseCatalog = resolve; });
      const catalogStarted = new Promise<void>((resolve) => { catalogEntered = resolve; });
      mutable.catalogShardCount = () => 1;
      mutable.env.CATALOG = {
        getByName: () => ({
          fetch: async () => {
            catalogEntered();
            await catalogBlocked;
            return Response.json({ ok: true, released: true });
          },
        }),
      };
      const completion = mutable.completeRestore(mutable.operation("restore-release-race"));
      await catalogStarted;
      const registration = () => mutable.handleRegisterCoordinator(internal("/register-coordinator", {
          fleet_id: "default",
          tx_id: "late-release-race",
          existing_created_at: new Date(Date.now() - 120_000).toISOString(),
        }));
      await expect(registration()).rejects.toMatchObject({
        protocolError: { code: "RESTORE_UNAVAILABLE", retryable: true },
      });
      releaseCatalog();
      await completion;

      expect(Array.from(state.storage.sql.exec<{ stage: string }>(
        "SELECT stage FROM restore_operations WHERE restore_id = 'restore-release-race'",
      ))[0].stage).toBe("complete");
      expect(Array.from(state.storage.sql.exec<{ active: number }>(
        "SELECT active FROM fleet_restore_gate WHERE singleton = 1",
      ))[0].active).toBe(0);
      const afterRelease = await registration();
      expect(await afterRelease.json()).toMatchObject({
        disposition: "discard_required",
        restore_id: "restore-release-race",
        generation: 31,
      });
    });
  });

  it("keeps coordinator inventory closed while final release is parked", async () => {
    const stub = restoreStub();
    await stub.fetch(internal("/gate", { fleet_id: "default" }));
    await runInDurableObject(stub, async (_instance: RestoreCoordinatorDO, state: DurableObjectState) => {
      const now = new Date().toISOString();
      state.storage.sql.exec(
        `INSERT INTO restore_operations
          (restore_id, fleet_id, cutoff, idempotency_key, parameter_hash, phase, stage,
           previewed_at, execute_before, fence_generation, fence_installed_at, started_at,
           resume_phase, resume_stage, updated_at)
         VALUES ('restore-release-parked', 'default', ?, 'release-parked', ?, 'manual_repair_required',
                 'manual_repair_required', ?, ?, 32, ?, ?, 'verifying', 'releasing_participants', ?)`,
        new Date(Date.now() - 60_000).toISOString(),
        "d".repeat(64),
        now,
        new Date(Date.now() + 60_000).toISOString(),
        now,
        now,
        now,
      );
      state.storage.sql.exec(
        `UPDATE fleet_restore_gate SET active = 1, restore_id = 'restore-release-parked', generation = 32,
         phase = 'manual_repair_required', activated_at = ? WHERE singleton = 1`,
        now,
      );
    });

    const response = await stub.fetch(internal("/register-coordinator", {
      fleet_id: "default",
      tx_id: "late-while-parked",
      existing_created_at: new Date(Date.now() - 120_000).toISOString(),
    }));

    expect(response.status).toBe(503);
    expect(await response.json()).toMatchObject({ error: { code: "RESTORE_UNAVAILABLE", retryable: true } });
  });

  it("rolls back completion when the fleet gate cannot be cleared atomically", async () => {
    const stub = restoreStub();
    await stub.fetch(internal("/gate", { fleet_id: "default" }));
    await runInDurableObject(stub, async (instance: RestoreCoordinatorDO, state: DurableObjectState) => {
      const now = new Date().toISOString();
      state.storage.sql.exec(
        `INSERT INTO restore_operations
          (restore_id, fleet_id, cutoff, idempotency_key, parameter_hash, phase, stage,
           previewed_at, execute_before, fence_generation, fence_installed_at, started_at, updated_at)
         VALUES ('restore-gate-mismatch', 'default', ?, 'gate-mismatch', ?, 'verifying',
                 'releasing_participants', ?, ?, 33, ?, ?, ?)`,
        new Date(Date.now() - 60_000).toISOString(),
        "e".repeat(64),
        now,
        new Date(Date.now() + 60_000).toISOString(),
        now,
        now,
        now,
      );
      state.storage.sql.exec(
        `UPDATE fleet_restore_gate SET active = 1, restore_id = 'restore-gate-mismatch', generation = 34,
         phase = 'verifying', activated_at = ? WHERE singleton = 1`,
        now,
      );
      const mutable = instance as unknown as {
        env: { CATALOG: unknown };
        operation: (restoreId: string) => unknown;
        completeRestore: (operation: unknown) => Promise<void>;
        catalogShardCount: () => number;
      };
      mutable.catalogShardCount = () => 1;
      mutable.env.CATALOG = {
        getByName: () => ({ fetch: async () => Response.json({ ok: true, released: true }) }),
      };

      await expect(mutable.completeRestore(mutable.operation("restore-gate-mismatch"))).rejects.toMatchObject({
        protocolError: { code: "RESTORE_INVARIANT_FAILED" },
      });

      expect(state.storage.sql.exec<{ stage: string }>(
        "SELECT stage FROM restore_operations WHERE restore_id = 'restore-gate-mismatch'",
      ).one().stage).toBe("releasing_participants");
      expect(state.storage.sql.exec<{ active: number }>(
        "SELECT active FROM fleet_restore_gate WHERE singleton = 1",
      ).one().active).toBe(1);
    });
  });

  it("keeps alarms armed for queued previews after another preview finishes", async () => {
    const stub = restoreStub();
    await stub.fetch(internal("/gate", { fleet_id: "default" }));
    await runInDurableObject(stub, async (instance: RestoreCoordinatorDO, state: DurableObjectState) => {
      const now = new Date().toISOString();
      const later = new Date(Date.now() + 1).toISOString();
      for (const [restoreId, updatedAt] of [["restore-preview-a", now], ["restore-preview-b", later]]) {
        state.storage.sql.exec(
          `INSERT INTO restore_operations
            (restore_id, fleet_id, cutoff, idempotency_key, parameter_hash, phase, stage,
             previewed_at, execute_before, updated_at)
           VALUES (?, 'default', ?, ?, ?, 'previewing', 'closing_manifest', ?, ?, ?)`,
          restoreId,
          new Date(Date.now() - 60_000).toISOString(),
          restoreId,
          restoreId === "restore-preview-a" ? "a".repeat(64) : "b".repeat(64),
          now,
          new Date(Date.now() + 60_000).toISOString(),
          updatedAt,
        );
      }
      const mutable = instance as unknown as { advance: (restoreId: string) => Promise<void> };
      mutable.advance = async (restoreId) => {
        state.storage.sql.exec(
          "UPDATE restore_operations SET stage = 'previewed', phase = 'previewed' WHERE restore_id = ?",
          restoreId,
        );
      };

      await instance.alarm();

      expect(state.storage.sql.exec<{ stage: string }>(
        "SELECT stage FROM restore_operations WHERE restore_id = 'restore-preview-b'",
      ).one().stage).toBe("closing_manifest");
      expect(await state.storage.getAlarm()).not.toBeNull();
    });
  });

  it("backs off transient advance failures and eventually surfaces a blocker", async () => {
    const stub = restoreStub();
    await stub.fetch(internal("/gate", { fleet_id: "default" }));
    await runInDurableObject(stub, async (instance: RestoreCoordinatorDO, state: DurableObjectState) => {
      const now = new Date().toISOString();
      state.storage.sql.exec(
        `INSERT INTO restore_operations
          (restore_id, fleet_id, cutoff, idempotency_key, parameter_hash, phase, stage,
           previewed_at, execute_before, updated_at)
         VALUES ('restore-retry', 'default', ?, 'restore-retry', ?, 'previewing',
                 'closing_manifest', ?, ?, ?)`,
        new Date(Date.now() - 60_000).toISOString(),
        "c".repeat(64),
        now,
        new Date(Date.now() + 60_000).toISOString(),
        now,
      );
      const mutable = instance as unknown as { advance: () => Promise<void> };
      mutable.advance = async () => {
        throw new RestoreContractViolation(restoreError("RESTORE_UNAVAILABLE", "temporary"));
      };

      await instance.alarm();

      expect(state.storage.sql.exec<{ stage: string; retry_count: number }>(
        "SELECT stage, retry_count FROM restore_operations WHERE restore_id = 'restore-retry'",
      ).one()).toEqual({ stage: "closing_manifest", retry_count: 1 });
      expect(await state.storage.getAlarm()).not.toBeNull();
      state.storage.sql.exec(
        "UPDATE restore_operations SET retry_not_before_ms = 0 WHERE restore_id = 'restore-retry'",
      );
      await state.storage.deleteAlarm();
      const beforeSecondRetry = Date.now();
      await instance.alarm();
      expect((await state.storage.getAlarm())! - beforeSecondRetry).toBeGreaterThanOrEqual(400);
      for (let attempt = 2; attempt < 10; attempt += 1) {
        state.storage.sql.exec(
          "UPDATE restore_operations SET retry_not_before_ms = 0 WHERE restore_id = 'restore-retry'",
        );
        await state.storage.deleteAlarm();
        await instance.alarm();
      }
      const parked = state.storage.sql.exec<{ stage: string; blocker_json: string }>(
        "SELECT stage, blocker_json FROM restore_operations WHERE restore_id = 'restore-retry'",
      ).one();
      expect(parked.stage).toBe("failed");
      expect(JSON.parse(parked.blocker_json)).toMatchObject([{ code: "RESTORE_UNAVAILABLE" }]);
    });
  });

  it("registers coordinator inventory coverage and rejects another logical fleet", async () => {
    const stub = restoreStub();
    const rejected = await stub.fetch(internal("/register-coordinator", {
      fleet_id: "another-fleet",
      coordinator_id: "tx-1",
    }));
    expect(rejected.status).toBe(400);

    const accepted = await stub.fetch(internal("/register-coordinator", {
      fleet_id: "default",
      coordinator_id: "tx-1",
    }));
    expect(accepted.status).toBe(200);

    await runInDurableObject(stub, async (_instance: RestoreCoordinatorDO, state: DurableObjectState) => {
      const coordinators = Array.from(state.storage.sql.exec(
        "SELECT coordinator_id, fleet_id FROM coordinator_registry",
      ));
      const coverage = Array.from(state.storage.sql.exec(
        "SELECT value FROM restore_metadata WHERE key = 'coordinator_coverage_start'",
      ));
      expect(coordinators).toEqual([{ coordinator_id: "tx-1", fleet_id: "default" }]);
      expect(coverage).toHaveLength(1);
    });
  });

  it("adds an existing coordinator that wakes under the active fence to restore work", async () => {
    const stub = restoreStub();
    await stub.fetch(internal("/gate", { fleet_id: "default" }));
    await runInDurableObject(stub, async (_instance: RestoreCoordinatorDO, state: DurableObjectState) => {
      const now = new Date().toISOString();
      state.storage.sql.exec(
        `INSERT INTO restore_operations
          (restore_id, fleet_id, cutoff, idempotency_key, parameter_hash, phase, stage,
           previewed_at, execute_before, fence_generation, fence_installed_at, started_at, updated_at)
         VALUES ('restore-active', 'default', ?, 'active-registration', ?, 'verifying', 'discarding_coordinators',
                 ?, ?, 4, ?, ?, ?)`,
        new Date(Date.now() - 60_000).toISOString(),
        "d".repeat(64),
        now,
        new Date(Date.now() + 60_000).toISOString(),
        now,
        now,
        now,
      );
      state.storage.sql.exec(
        `UPDATE fleet_restore_gate SET active = 1, restore_id = 'restore-active', generation = 4,
         phase = 'reconciling', activated_at = ? WHERE singleton = 1`,
        now,
      );
    });

    const existing = await stub.fetch(internal("/register-coordinator", {
      fleet_id: "default",
      tx_id: "tx-existing",
      existing_created_at: new Date(Date.now() - 60_000).toISOString(),
    }));
    expect(existing.status).toBe(200);
    expect(await existing.json()).toMatchObject({
      disposition: "registered",
      active_restore_id: "restore-active",
      generation: 4,
    });
    const fresh = await stub.fetch(internal("/register-coordinator", {
      fleet_id: "default",
      tx_id: "tx-fresh",
    }));
    expect(fresh.status).toBe(409);

    await runInDurableObject(stub, async (_instance: RestoreCoordinatorDO, state: DurableObjectState) => {
      expect(Array.from(state.storage.sql.exec(
        "SELECT coordinator_id, status FROM restore_coordinator_work WHERE restore_id = 'restore-active'",
      ))).toEqual([{ coordinator_id: "tx-existing", status: "pending_loss" }]);
      expect(Array.from(state.storage.sql.exec(
        "SELECT phase, stage FROM restore_operations WHERE restore_id = 'restore-active'",
      ))).toEqual([{ phase: "restoring", stage: "materializing_loss" }]);
    });
  });

  it("quarantines a pre-existing coordinator omitted by a completed restore", async () => {
    const stub = restoreStub();
    await stub.fetch(internal("/gate", { fleet_id: "default" }));
    const createdAt = new Date(Date.now() - 120_000).toISOString();
    await runInDurableObject(stub, async (_instance: RestoreCoordinatorDO, state: DurableObjectState) => {
      const now = new Date().toISOString();
      state.storage.sql.exec(
        `INSERT INTO restore_operations
          (restore_id, fleet_id, cutoff, idempotency_key, parameter_hash, phase, stage,
           previewed_at, execute_before, fence_generation, fence_installed_at, started_at,
           completed_at, updated_at)
         VALUES ('restore-complete', 'default', ?, 'late-registration', ?, 'complete', 'complete',
                 ?, ?, 8, ?, ?, ?, ?)`,
        createdAt,
        "a".repeat(64),
        createdAt,
        now,
        new Date(Date.now() - 60_000).toISOString(),
        createdAt,
        now,
        now,
      );
    });

    const response = await stub.fetch(internal("/register-coordinator", {
      fleet_id: "default",
      tx_id: "tx-omitted",
      existing_created_at: createdAt,
    }));
    expect(response.status).toBe(200);
    expect(await response.json()).toMatchObject({
      disposition: "discard_required",
      restore_id: "restore-complete",
      generation: 8,
    });
  });

  it("does not discard a coordinator after a restore was rolled back", async () => {
    const stub = restoreStub();
    await stub.fetch(internal("/gate", { fleet_id: "default" }));
    const createdAt = new Date(Date.now() - 120_000).toISOString();
    await runInDurableObject(stub, async (_instance: RestoreCoordinatorDO, state: DurableObjectState) => {
      const now = new Date().toISOString();
      state.storage.sql.exec(
        `INSERT INTO restore_operations
          (restore_id, fleet_id, cutoff, idempotency_key, parameter_hash, phase, stage,
           previewed_at, execute_before, fence_generation, fence_installed_at, started_at,
           completed_at, updated_at)
         VALUES ('restore-rolled-back', 'default', ?, 'rolled-back-registration', ?, 'rolled_back', 'rolled_back',
                 ?, ?, 9, ?, ?, ?, ?)`,
        createdAt,
        "f".repeat(64),
        createdAt,
        now,
        new Date(Date.now() - 60_000).toISOString(),
        createdAt,
        now,
        now,
      );
    });

    const response = await stub.fetch(internal("/register-coordinator", {
      fleet_id: "default",
      tx_id: "tx-after-rollback",
      existing_created_at: createdAt,
    }));

    expect(response.status).toBe(200);
    expect(await response.json()).toMatchObject({ disposition: "registered" });
  });

  it("revalidates the live fenced head rather than requiring the preview head to stay unchanged", async () => {
    const stub = restoreStub();
    await stub.fetch(internal("/gate", { fleet_id: "default" }));
    await runInDurableObject(stub, async (instance: RestoreCoordinatorDO, state: DurableObjectState) => {
      const now = new Date().toISOString();
      state.storage.sql.exec(
        `INSERT INTO restore_operations
          (restore_id, fleet_id, cutoff, idempotency_key, parameter_hash, phase, stage,
           previewed_at, execute_before, fence_generation, fence_installed_at, started_at, updated_at)
         VALUES ('restore-revalidate', 'default', ?, 'revalidate-head', ?, 'fencing', 'revalidating_participants',
                 ?, ?, 6, ?, ?, ?)`,
        new Date(Date.now() - 60_000).toISOString(),
        "e".repeat(64),
        now,
        new Date(Date.now() + 60_000).toISOString(),
        now,
        now,
        now,
      );
      state.storage.sql.exec(
        `INSERT INTO restore_participants
          (restore_id, participant_id, participant_kind, object_name, target_bookmark,
           preview_bookmark, pre_fence_bookmark, coverage_start, status)
         VALUES ('restore-revalidate', 'shard:one', 'shard', 'one', 'target',
                 'head-at-preview', 'head-at-fence', ?, 'fenced')`,
        now,
      );
      const mutable = instance as unknown as {
        participantStub(): { fetch(input: RequestInfo | URL): Promise<Response> };
        operation(restoreId: string): unknown;
        revalidateNextParticipant(operation: unknown): Promise<void>;
      };
      mutable.participantStub = () => ({
        fetch: async () => Response.json({ target_bookmark: "target", preview_bookmark: "head-at-fence" }),
      });
      await mutable.revalidateNextParticipant(mutable.operation("restore-revalidate"));
      expect(Array.from(state.storage.sql.exec(
        "SELECT status FROM restore_participants WHERE restore_id = 'restore-revalidate'",
      ))).toEqual([{ status: "ready" }]);
    });
  });

  it("resumes reconciliation after an ambiguous prepare response using hash-validated shard state", async () => {
    const stub = restoreStub();
    await stub.fetch(internal("/gate", { fleet_id: "default" }));
    await runInDurableObject(stub, async (instance: RestoreCoordinatorDO, state: DurableObjectState) => {
      const now = new Date().toISOString();
      const envelope = {
        protocol_version: 1,
        format_version: 2,
        tx_id: "tx-replayed-prepare",
        decision_epoch: 1,
        operation_hash: "f".repeat(64),
        participants: [{ participant_id: "one", epoch: 1, prepare_bookmark: "pre-restore-prepare", intents: [] }],
      };
      state.storage.sql.exec(
        `INSERT INTO restore_operations
          (restore_id, fleet_id, cutoff, idempotency_key, parameter_hash, phase, stage,
           previewed_at, execute_before, fence_generation, fence_installed_at, started_at, updated_at)
         VALUES ('restore-reconcile-retry', 'default', ?, 'reconcile-retry', ?, 'reconciling', 'reconciling',
                 ?, ?, 9, ?, ?, ?)`,
        new Date(Date.now() - 60_000).toISOString(),
        "1".repeat(64),
        now,
        new Date(Date.now() + 60_000).toISOString(),
        now,
        now,
        now,
      );
      state.storage.sql.exec(
        `INSERT INTO restore_manifest_records
          (restore_id, record_hash, tx_id, coordinator_id, commit_decided_at, envelope_hash,
           record_json, envelope_json, reconciliation_status)
         VALUES ('restore-reconcile-retry', ?, 'tx-replayed-prepare', 'tx-replayed-prepare', ?, ?, '{}', ?, 'pending')`,
        "2".repeat(64),
        now,
        "3".repeat(64),
        JSON.stringify(envelope),
      );
      const calls: string[] = [];
      const mutable = instance as unknown as {
        shardStub(): { fetch(input: RequestInfo | URL): Promise<Response> };
        operation(restoreId: string): unknown;
        reconcileNextTransaction(operation: unknown): Promise<void>;
      };
      mutable.shardStub = () => ({
        fetch: async (input) => {
          const path = new URL(input instanceof Request ? input.url : input.toString()).pathname;
          calls.push(path);
          if (path === "/tx-status") {
            return Response.json({ found: true, status: "prepared", prepare_bookmark_present: false });
          }
          return Response.json({ ok: true, status: "committed" });
        },
      });
      await mutable.reconcileNextTransaction(mutable.operation("restore-reconcile-retry"));
      expect(calls).toEqual(["/tx-status", "/recover"]);
      expect(Array.from(state.storage.sql.exec(
        "SELECT reconciliation_status FROM restore_manifest_records WHERE restore_id = 'restore-reconcile-retry'",
      ))).toEqual([{ reconciliation_status: "complete" }]);
    });
  });

  it("only authorizes the active restore generation while fenced", async () => {
    const stub = restoreStub();
    await stub.fetch(internal("/gate", { fleet_id: "default" }));
    await runInDurableObject(stub, async (_instance: RestoreCoordinatorDO, state: DurableObjectState) => {
      state.storage.sql.exec(
        `UPDATE fleet_restore_gate SET active = 1, restore_id = 'restore-a', generation = 7,
         phase = 'restoring', activated_at = ? WHERE singleton = 1`,
        new Date().toISOString(),
      );
    });

    const ordinary = await stub.fetch(internal("/gate", { fleet_id: "default" }));
    expect(await ordinary.json()).toMatchObject({ active: true, allowed: false, generation: 7 });

    const owner = await stub.fetch(internal("/gate", {
      fleet_id: "default",
      restore_id: "restore-a",
      generation: 7,
    }));
    expect(await owner.json()).toMatchObject({ active: true, allowed: true, generation: 7 });
  });

  it("blocks ordinary root ingress but leaves authenticated restore control reachable", async () => {
    const stub = restoreStub();
    await stub.fetch(internal("/gate", { fleet_id: "default" }));
    await runInDurableObject(stub, async (_instance: RestoreCoordinatorDO, state: DurableObjectState) => {
      state.storage.sql.exec(
        `UPDATE fleet_restore_gate SET active = 1, restore_id = 'restore-a', generation = 1,
         phase = 'manual_repair_required', activated_at = ? WHERE singleton = 1`,
        new Date().toISOString(),
      );
    });

    const ordinary = await admin("/admin/status", {});
    expect(ordinary.status).toBe(503);
    expect(await ordinary.json()).toMatchObject({ error: { code: "FLEET_RESTORE_IN_PROGRESS" } });

    const restoreStatus = await admin("/admin/restore-status", {
      protocol_version: 1,
      format_version: 1,
      restore_id: "missing",
    });
    expect(restoreStatus.status).toBe(400);
  });

  it("uses the canonical shard fence wire shape and keeps coordinators out of PITR", async () => {
    const authority = restoreStub();
    await authority.fetch(internal("/gate", { fleet_id: "default" }));
    await runInDurableObject(authority, async (_instance: RestoreCoordinatorDO, state: DurableObjectState) => {
      state.storage.sql.exec(
        `UPDATE fleet_restore_gate SET active = 1, restore_id = 'restore-wire', generation = 9,
         phase = 'fencing', activated_at = ? WHERE singleton = 1`,
        new Date().toISOString(),
      );
    });

    const body = { restore_id: "restore-wire", generation: 9, action: "install" };
    const shard = env.SHARD.getByName("restore-wire-shard");
    const shardResponse = await shard.fetch(internal("/restore-fence", body));
    expect(shardResponse.status).toBe(200);
    expect(await shardResponse.json()).toMatchObject({
      restore_id: "restore-wire",
      generation: 9,
      externally_fenced: true,
      pre_fence_bookmark: expect.any(String),
      closed_through: expect.any(String),
      closed_through_bookmark: expect.any(String),
    });

    const coordinator = env.COORDINATOR.getByName("restore-wire-tx");
    const coordinatorResponse = await coordinator.fetch(internal("/restore-fence", body));
    expect(coordinatorResponse.status).toBe(404);
  });
});

describe("restore admin validation", () => {
  it("authenticates before parsing and rejects unknown restore fields", async () => {
    const unauthenticated = await SELF.fetch("https://worker.internal/admin/restore-preview", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: "{not-json",
    });
    expect(unauthenticated.status).toBe(401);

    const response = await admin("/admin/restore-preview", {
      protocol_version: 1,
      format_version: 1,
      fleet_id: "default",
      cutoff: new Date().toISOString(),
      idempotency_key: "preview-1",
      replacement_plan: {},
    });
    expect(response.status).toBe(400);
    expect(await response.json()).toMatchObject({ error: { code: "RESTORE_INVALID_REQUEST" } });
  });

  it("rejects a future cutoff before any durable mutation", async () => {
    const response = await admin("/admin/restore-preview", {
      protocol_version: 1,
      format_version: 1,
      fleet_id: "default",
      cutoff: new Date(Date.now() + 60_000).toISOString(),
      idempotency_key: "preview-future",
    });
    expect(response.status).toBe(409);
    expect(await response.json()).toMatchObject({ error: { code: "RESTORE_CUTOFF_IN_FUTURE" } });
  });

  it("exposes rollback only through the exact versioned plan identity", async () => {
    const malformed = await admin("/admin/restore-rollback", {
      restore_id: "restore-missing",
      plan_hash: "0".repeat(64),
    });
    expect(malformed.status).toBe(400);
    expect(await malformed.json()).toMatchObject({ error: { code: "RESTORE_INVALID_REQUEST" } });

    const missing = await admin("/admin/restore-rollback", {
      protocol_version: 1,
      format_version: 1,
      restore_id: "restore-missing",
      plan_hash: "0".repeat(64),
    });
    expect(missing.status).toBe(409);
    expect(await missing.json()).toMatchObject({ error: { code: "RESTORE_PLAN_HASH_MISMATCH" } });
  });

  it("rejects every unsafe rollback precondition", async () => {
    const authority = restoreStub();
    await authority.fetch(internal("/gate", { fleet_id: "default" }));
    const cases = [
      { name: "expired", stage: "restoring_participants", expired: true, undo: true, gate: true, discarded: false, code: "RESTORE_PLAN_STALE" },
      { name: "complete", stage: "complete", expired: false, undo: true, gate: false, discarded: false, code: "RESTORE_CONFLICT" },
      { name: "discarded", stage: "reconciling", expired: false, undo: true, gate: true, discarded: true, code: "RESTORE_CONFLICT" },
      { name: "participant-released", stage: "releasing_participants", expired: false, undo: true, gate: true, discarded: false, participantStatus: "released", code: "RESTORE_CONFLICT" },
      { name: "gate-mismatch", stage: "restoring_participants", expired: false, undo: true, gate: false, discarded: false, code: "RESTORE_CONFLICT" },
      { name: "no-undo", stage: "fencing_participants", expired: false, undo: false, gate: true, discarded: false, code: "RESTORE_CONFLICT" },
    ] as const;

    for (const scenario of cases) {
      const restoreId = `restore-rollback-${scenario.name}`;
      const planHash = "c".repeat(64);
      await runInDurableObject(authority, async (_instance: RestoreCoordinatorDO, state: DurableObjectState) => {
        const now = new Date().toISOString();
        const expiresAt = new Date(Date.now() + (scenario.expired ? -60_000 : 60_000)).toISOString();
        state.storage.sql.exec(
          `INSERT INTO restore_operations
            (restore_id, fleet_id, cutoff, idempotency_key, parameter_hash, phase, stage, plan_json, plan_hash,
             previewed_at, execute_before, fence_generation, fence_installed_at, started_at, completed_at, updated_at)
           VALUES (?, 'default', ?, ?, ?, ?, ?, ?, ?, ?, ?, 20, ?, ?, ?, ?)`,
          restoreId,
          new Date(Date.now() - 120_000).toISOString(),
          `rollback-${scenario.name}`,
          "b".repeat(64),
          scenario.stage,
          scenario.stage,
          JSON.stringify({ rollback: { undo_supported: true, undo_expires_at: expiresAt } }),
          planHash,
          now,
          new Date(Date.now() + 30_000).toISOString(),
          now,
          now,
          scenario.stage === "complete" ? now : null,
          now,
        );
        if (scenario.undo) {
          state.storage.sql.exec(
            `INSERT INTO restore_participants
              (restore_id, participant_id, participant_kind, object_name, status, undo_bookmark)
             VALUES (?, ?, 'shard', ?, ?, 'undo')`,
            restoreId,
            `shard:${scenario.name}`,
            scenario.name,
            "participantStatus" in scenario ? scenario.participantStatus : "restored",
          );
        }
        if (scenario.discarded) {
          state.storage.sql.exec(
            `INSERT INTO restore_coordinator_work (restore_id, coordinator_id, status)
             VALUES (?, 'tx-discarded', 'discarded')`,
            restoreId,
          );
        }
        state.storage.sql.exec(
          `UPDATE fleet_restore_gate SET active = ?, restore_id = ?, generation = 20,
           phase = ?, activated_at = ? WHERE singleton = 1`,
          scenario.gate ? 1 : 0,
          scenario.gate ? restoreId : null,
          scenario.gate ? scenario.stage : null,
          scenario.gate ? now : null,
        );
      });

      const response = await authority.fetch(internal("/rollback", {
        protocol_version: 1,
        format_version: 1,
        restore_id: restoreId,
        plan_hash: planHash,
      }));
      expect(response.status, scenario.name).toBe(409);
      expect(await response.json(), scenario.name).toMatchObject({ error: { code: scenario.code } });
    }
  });

  it("durably stages participant undo under the original active fence", async () => {
    const restoreId = "restore-rollback-stage";
    const planHash = "a".repeat(64);
    const shardName = "restore-rollback-shard";
    const shard = env.SHARD.getByName(shardName);
    const undoBookmark = "external-undo-bookmark";
    await runInDurableObject(shard, async (instance: ShardDO) => {
      (instance as unknown as { pitrPort: ParticipantPitrPort }).pitrPort = {
        async getCurrentBookmark() { return "rollback-current"; },
        async getBookmarkForTime() { return "unused-approximate"; },
        async stageRestoreBookmark(bookmark) { return `redo:${bookmark}`; },
        abort() {},
      };
    });
    const authority = restoreStub();
    await authority.fetch(internal("/gate", { fleet_id: "default" }));
    await runInDurableObject(authority, async (_instance: RestoreCoordinatorDO, state: DurableObjectState) => {
      const now = new Date().toISOString();
      state.storage.sql.exec(
        `INSERT INTO restore_operations
          (restore_id, fleet_id, cutoff, idempotency_key, parameter_hash, phase, stage, plan_json, plan_hash,
           previewed_at, execute_before, fence_generation, fence_installed_at, started_at, blocker_json, updated_at)
         VALUES (?, 'default', ?, 'rollback-stage-key', ?, 'manual_repair_required', 'manual_repair_required', ?, ?,
                 ?, ?, 12, ?, ?, ?, ?)`,
        restoreId,
        new Date(Date.now() - 60_000).toISOString(),
        "b".repeat(64),
        JSON.stringify({ rollback: { undo_supported: true, undo_expires_at: new Date(Date.now() + 60_000).toISOString() } }),
        planHash,
        now,
        new Date(Date.now() + 30_000).toISOString(),
        now,
        now,
        JSON.stringify([{ code: "RESTORE_INTERRUPTED", message: "test", participant_id: null, tx_id: null }]),
        now,
      );
      state.storage.sql.exec(
        `INSERT INTO restore_participants
          (restore_id, participant_id, participant_kind, object_name, target_bookmark, preview_bookmark,
           coverage_start, status, undo_bookmark)
         VALUES (?, ?, 'shard', ?, ?, ?, ?, 'restored', ?)`,
        restoreId,
        `shard:${shardName}`,
        shardName,
        undoBookmark,
        undoBookmark,
        now,
        undoBookmark,
      );
      state.storage.sql.exec(
        `UPDATE fleet_restore_gate SET active = 1, restore_id = ?, generation = 12,
         phase = 'manual_repair_required', activated_at = ? WHERE singleton = 1`,
        restoreId,
        now,
      );
    });

    const response = await admin("/admin/restore-rollback", {
      protocol_version: 1,
      format_version: 1,
      restore_id: restoreId,
      plan_hash: planHash,
    });
    expect(response.status).toBe(202);
    await runInDurableObject(authority, async (_instance: RestoreCoordinatorDO, state: DurableObjectState) => {
      expect(Array.from(state.storage.sql.exec(
        "SELECT phase, stage FROM restore_operations WHERE restore_id = ?",
        restoreId,
      ))).toEqual([{ phase: "rolling_back", stage: "rollback_participants" }]);
      expect(Array.from(state.storage.sql.exec(
        "SELECT status FROM restore_participants WHERE restore_id = ?",
        restoreId,
      ))).toEqual([{ status: "rollback_staged" }]);
    });
  });
});
