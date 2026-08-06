import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it, vi } from "vitest";
import {
  CATALOG_ALARM_BASE_RETRY_MS,
  CATALOG_ALARM_MAX_RETRY_MS,
  CATALOG_HISTORY_RETENTION_MS,
  FLEET_CATALOG_PROTOCOL_VERSION,
  FleetManifestCatalogDO,
  FleetManifestCatalogStore,
  INITIAL_PARTITION_COUNT,
  type CatalogActivationRequest,
} from "../src/fleet-manifest-catalog.js";
import type { ManifestRegistrationV1 } from "../../../packages/contracts/src/index.js";
import {
  MANIFEST_CIRCUIT_POLICY,
  admissionThroughManifestStub,
  lookupThroughManifestStub,
  registerThroughManifestStub,
} from "../src/index.js";

const FLEET = "fleet-catalog-test";
const DAY = "2026-08-05";

async function withCatalog<T>(name: string, callback: (catalog: FleetManifestCatalogStore) => Promise<T> | T): Promise<T> {
  const stub = env.JOURNAL_MANIFEST.getByName(`catalog-store-${name}-${crypto.randomUUID()}`);
  return await runInDurableObject(stub, async (_instance, state) => {
    const catalog = new FleetManifestCatalogStore(state.storage);
    catalog.migrate();
    return await callback(catalog);
  });
}

async function activation(catalog: FleetManifestCatalogStore, partition: number, key: string, day = DAY): Promise<CatalogActivationRequest> {
  const config = await catalog.partitionConfigForDay(FLEET, day);
  return {
    protocol_version: FLEET_CATALOG_PROTOCOL_VERSION,
    fleet_id: FLEET,
    reservation_day: day,
    partition,
    partition_count: config.partition_count,
    partition_config_hash: config.config_hash,
    activation_key: key,
  };
}

describe("FleetManifestCatalogStore", () => {
  it("places the V2 boundary after every legacy decision admitted before the fence transaction", async () => {
    await withCatalog("legacy-boundary", async (catalog) => {
      const decisionMs = Date.now() + 60_000;
      const config = await catalog.partitionConfigForDay(FLEET, DAY);
      await expect(catalog.admitLegacyRegistration({
        fleet_id: FLEET,
        reservation_day: DAY,
        partition: 0,
        partition_count: config.partition_count,
        partition_config_hash: config.config_hash,
        record_hash: "b".repeat(64),
        commit_decided_at_ms: decisionMs,
      })).resolves.toMatchObject({ ok: true });

      await catalog.assignManifestRoute({
        fleet_id: FLEET,
        tx_id: "tx-after-legacy-boundary",
        coordinator_id: "coordinator-after-legacy-boundary",
        operation_hash: "c".repeat(64),
        decision_epoch: 1,
      }, "route-after-legacy-boundary", 0);
      const coverage = await catalog.coverageState(FLEET);
      expect(coverage.reservation_required_since_ms).toBe(decisionMs + 1);
      expect(coverage.reservation_required_since_day).toBe(new Date(decisionMs + 1).toISOString().slice(0, 10));
      await expect(catalog.admitLegacyRegistration({
        fleet_id: FLEET,
        reservation_day: DAY,
        partition: 1,
        partition_count: config.partition_count,
        partition_config_hash: config.config_hash,
        record_hash: "d".repeat(64),
        commit_decided_at_ms: decisionMs + 1,
      })).resolves.toEqual({ ok: false, status: "v1_closed" });
    });
  });

  it("releases terminal route-assignment idempotency state", async () => {
    await withCatalog("route-release", async (catalog) => {
      const draft = {
        fleet_id: FLEET,
        tx_id: "tx-route-release",
        coordinator_id: "coordinator-route-release",
        operation_hash: "a".repeat(64),
        decision_epoch: 1,
      };
      const assigned = await catalog.assignManifestRoute(draft, "route-release-key", Date.UTC(2026, 7, 5, 12));
      expect(assigned.status).toBe("assigned");
      catalog.releaseManifestRoute(FLEET, draft.tx_id, assigned.reservation_hash, Date.UTC(2026, 7, 5, 13));
      await expect(catalog.assignManifestRoute(draft, "route-release-key", Date.UTC(2026, 7, 5, 12, 30))).resolves.toMatchObject({
        status: "already_assigned",
      });
      catalog.purgeReleasedRoutes(Date.UTC(2026, 7, 5, 13));
      await expect(catalog.assignManifestRoute(draft, "route-release-key", Date.UTC(2026, 7, 5, 13))).resolves.toMatchObject({
        status: "assigned",
      });
    });
  });

  it("starts with an immutable 16-partition configuration and rejects near-term resharding", async () => {
    await withCatalog("partition-config", async (catalog) => {
      const initial = await catalog.partitionConfigForDay(FLEET, DAY);
      expect(initial).toMatchObject({
        effective_from_day: "0000-01-01",
        protocol_version: 2,
        partition_count: INITIAL_PARTITION_COUNT,
        prior_hash: "0".repeat(64),
      });
      expect(initial.config_hash).toMatch(/^[a-f0-9]{64}$/);

      const now = Date.UTC(2026, 7, 5, 12);
      await expect(
        catalog.appendPartitionConfig({ fleet_id: FLEET, effective_from_day: "2026-08-06", partition_count: 16 }, now),
      ).rejects.toThrow("later than the next UTC day");
      await expect(catalog.appendPartitionConfig(
        { fleet_id: FLEET, effective_from_day: "2026-08-07", partition_count: 32 },
        now,
      )).rejects.toThrow("must remain 16");
      const appended = await catalog.appendPartitionConfig(
        { fleet_id: FLEET, effective_from_day: "2026-08-07", partition_count: 16 },
        now,
      );
      expect(appended).toMatchObject({ partition_count: 16, prior_hash: initial.config_hash });
      await expect(catalog.partitionConfigForDay(FLEET, "2026-08-07")).resolves.toMatchObject({
        partition_count: 16,
        config_hash: appended.config_hash,
      });
    });
  });

  it("serializes activation against a cutoff fence without losing a candidate", async () => {
    await withCatalog("activation-fence", async (catalog) => {
      const before = await catalog.activateBucket(await activation(catalog, 1, "activation-before"));
      expect(before).toMatchObject({ status: "activated", required_decision_floor_ms: 0 });

      const cutoff = Date.UTC(2026, 7, 5, 10);
      const snapshot = await catalog.snapshotThrough({
        protocol_version: 2,
        fleet_id: FLEET,
        cutoff_ms: cutoff,
        idempotency_key: "snapshot-fence",
        page_size: 1,
      }, cutoff + 1);
      expect(snapshot.status).toBe("complete");

      const after = await catalog.activateBucket(await activation(catalog, 2, "activation-after"));
      expect(after.required_decision_floor_ms).toBe(cutoff);
      expect(after.activation_sequence).toBeGreaterThan(snapshot.fence_sequence);
      expect(catalog.snapshotEntries(snapshot.generation)).toEqual([
        expect.objectContaining({ partition: 1, activation_sequence: before.activation_sequence }),
      ]);
    });
  });

  it("builds bounded idempotent snapshots and hash-chains generations", async () => {
    await withCatalog("snapshot-chain", async (catalog) => {
      for (let partition = 0; partition < 3; partition += 1) {
        await catalog.activateBucket(await activation(catalog, partition, `activate-${partition}`));
      }
      const cutoff = Date.UTC(2026, 7, 5, 11);
      const request = {
        protocol_version: 2 as const,
        fleet_id: FLEET,
        cutoff_ms: cutoff,
        idempotency_key: "snapshot-one",
        page_size: 1,
      };
      const firstPage = await catalog.snapshotThrough(request, cutoff + 1);
      expect(firstPage).toMatchObject({ status: "pending", entry_count: 1 });
      const secondPage = await catalog.snapshotThrough(request, cutoff + 1);
      expect(secondPage).toMatchObject({ status: "pending", generation: firstPage.generation, entry_count: 2 });
      const complete = await catalog.snapshotThrough(request, cutoff + 1);
      expect(complete).toMatchObject({ status: "complete", generation: firstPage.generation, entry_count: 3 });
      expect(complete.snapshot_hash).toMatch(/^[a-f0-9]{64}$/);
      await expect(catalog.snapshotThrough(request, cutoff + 1)).resolves.toEqual(complete);

      const next = await catalog.snapshotThrough({ ...request, idempotency_key: "snapshot-two", cutoff_ms: cutoff + 1 }, cutoff + 2);
      while (next.status === "pending") {
        const resumed = await catalog.resumeSnapshot(next.generation, 128, cutoff + 2);
        if (resumed.status === "complete") {
          expect(resumed.prior_snapshot_hash).toBe(complete.snapshot_hash);
          expect(resumed.snapshot_hash).not.toBe(complete.snapshot_hash);
          break;
        }
      }
    });
  });

  it("garbage-collects expired close generations while preserving the snapshot hash chain", async () => {
    await withCatalog("history-gc", async (catalog) => {
      const first = await catalog.snapshotThrough({
        protocol_version: 2,
        fleet_id: FLEET,
        cutoff_ms: 0,
        idempotency_key: "history-snapshot-one",
      }, 1);
      const close = await catalog.beginClose({ fleet_id: FLEET, cutoff_ms: 0, snapshot_generation: first.generation }, 1);
      await catalog.finalizeClose(close.close_key, 1);
      const second = await catalog.snapshotThrough({
        protocol_version: 2,
        fleet_id: FLEET,
        cutoff_ms: 1,
        idempotency_key: "history-snapshot-two",
      }, 2);
      expect(second.prior_snapshot_hash).toBe(first.snapshot_hash);

      const gc = catalog.purgeHistory(CATALOG_HISTORY_RETENTION_MS + 2);
      expect(gc.deleted).toBeGreaterThanOrEqual(2);
      expect(catalog.closeForSnapshot(first.generation)).toBeNull();
      expect(() => catalog.snapshotByGeneration(first.generation)).toThrow();
      expect(catalog.snapshotByGeneration(second.generation).snapshot_hash).toBe(second.snapshot_hash);
    });
  });

  it("keeps pre-fence activation history when retirement and reactivation race bounded snapshot work", async () => {
    await withCatalog("retirement", async (catalog) => {
      await catalog.activateBucket(await activation(catalog, 0, "force-bounded-page"));
      const request = await activation(catalog, 4, "active-original");
      const original = await catalog.activateBucket(request);
      const cutoff = Date.UTC(2026, 7, 5, 12);
      const oldSnapshot = await catalog.snapshotThrough(
        { protocol_version: 2, fleet_id: FLEET, cutoff_ms: cutoff, idempotency_key: "before-retire", page_size: 1 },
        cutoff + 1,
      );
      expect(oldSnapshot.status).toBe("pending");
      const certificate = "a".repeat(64);
      await expect(catalog.retireBucket({ fleet_id: FLEET, reservation_day: DAY, partition: 4, retirement_certificate_hash: certificate })).resolves.toMatchObject({ status: "retired" });
      catalog.purgeHistory(Date.now() + CATALOG_HISTORY_RETENTION_MS + 1);
      const reactivated = await catalog.activateBucket({ ...request, activation_key: "active-again" });
      expect(reactivated.status).toBe("reactivated");
      expect(reactivated.activation_sequence).toBeGreaterThan(oldSnapshot.fence_sequence);
      const completed = await catalog.resumeSnapshot(oldSnapshot.generation, 128, cutoff + 1);
      expect(completed).toMatchObject({ status: "complete", entry_count: 2 });
      expect(catalog.snapshotEntries(oldSnapshot.generation)).toEqual([
        expect.objectContaining({ partition: 0 }),
        expect.objectContaining({ partition: 4, activation_sequence: original.activation_sequence }),
      ]);
    });
  });

  it("persists deterministic close ownership and materializes progress in bounded pages", async () => {
    await withCatalog("close-progress", async (catalog) => {
      for (let partition = 0; partition < 3; partition += 1) {
        await catalog.activateBucket(await activation(catalog, partition, `close-${partition}`));
      }
      const cutoff = Date.UTC(2026, 7, 5, 13);
      const snapshot = await catalog.snapshotThrough({ protocol_version: 2, fleet_id: FLEET, cutoff_ms: cutoff, idempotency_key: "close-snapshot" }, cutoff + 1);
      expect(snapshot.status).toBe("complete");
      const operation = await catalog.beginClose({ fleet_id: FLEET, cutoff_ms: cutoff, snapshot_generation: snapshot.generation });
      expect(operation).toMatchObject({ status: "pending", total_entries: 3, progress_cursor_sequence: 0 });
      expect(operation.close_key).toMatch(/^[a-f0-9]{64}$/);

      const pageOne = await catalog.materializeCloseProgress(operation.close_key, 2);
      expect(pageOne.progress_cursor_sequence).toBe(2);
      const pageTwo = await catalog.materializeCloseProgress(operation.close_key, 2);
      expect(pageTwo.progress_cursor_sequence).toBe(3);
      await expect(catalog.beginClose({ fleet_id: FLEET, cutoff_ms: cutoff, snapshot_generation: snapshot.generation })).resolves.toEqual(pageTwo);
    });
  });

  it("multiplexes alarm purposes with minimum-time ordering and bounded dispatch", async () => {
    await withCatalog("alarms", async (catalog) => {
      for (let index = 0; index < 18; index += 1) {
        catalog.schedulePurpose({
          purpose: `purpose-${index.toString().padStart(2, "0")}`,
          fire_at_ms: 1_000 + index,
          generation: index,
          payload_hash: index.toString(16).padStart(64, "0"),
        });
      }
      expect(catalog.nextAlarmAt()).toBe(1_000);
      const due = catalog.duePurposes(2_000);
      expect(due).toHaveLength(16);
      expect(due[0]).toMatchObject({ purpose: "purpose-00", fire_at_ms: 1_000 });
      for (const purpose of due) catalog.completePurpose(purpose.purpose, purpose.generation);
      expect(catalog.nextAlarmAt()).toBe(1_016);
    });
  });

  it("durably reschedules one failed alarm purpose without starving the rest of the due batch", async () => {
    const stub = env.FLEET_MANIFEST_CATALOG.getByName(`catalog-alarm-retry-${crypto.randomUUID()}`);
    await runInDurableObject(stub, async (durableObject, state) => {
      const instance = durableObject as unknown as FleetManifestCatalogDO;
      const catalog = (instance as unknown as { catalog: FleetManifestCatalogStore }).catalog;
      const now = Date.now();
      catalog.schedulePurpose({ purpose: "snapshot_resume", fire_at_ms: now - 1, generation: 7, payload_hash: "a".repeat(64) });
      catalog.schedulePurpose({ purpose: "future-purpose", fire_at_ms: now - 1, generation: 1, payload_hash: "b".repeat(64) });
      (catalog as unknown as { resumeSnapshot: (generation: number) => Promise<never> }).resumeSnapshot = async () => {
        throw Object.assign(new Error("token=secret fleet=private"), {
          overloaded: true,
          retryable: true,
          retryAfterMs: 42_000,
        });
      };
      const warnings: string[] = [];
      const warning = vi.spyOn(console, "warn").mockImplementation((value) => warnings.push(String(value)));
      try {
        await instance.alarm();
      } finally {
        warning.mockRestore();
      }

      const scheduled = catalog.duePurposes(Date.now() + CATALOG_ALARM_MAX_RETRY_MS + 1)
        .find((purpose) => purpose.purpose === "snapshot_resume");
      expect(scheduled).toMatchObject({
        purpose: "snapshot_resume",
        generation: 7,
        payload_hash: "a".repeat(64),
        attempt_count: 1,
      });
      expect((scheduled?.fire_at_ms ?? 0) - now).toBeGreaterThanOrEqual(42_000);
      expect((scheduled?.fire_at_ms ?? 0) - now).toBeLessThanOrEqual(CATALOG_ALARM_MAX_RETRY_MS);
      expect(catalog.duePurposes(Date.now() + CATALOG_ALARM_MAX_RETRY_MS + 1)
        .some((purpose) => purpose.purpose === "future-purpose")).toBe(false);
      expect(await state.storage.getAlarm()).toBe(scheduled?.fire_at_ms);
      expect(warnings).toHaveLength(1);
      expect(JSON.parse(warnings[0])).toMatchObject({
        schema_version: 1,
        event: "reliability.slo",
        component: "fleet_manifest_catalog",
        operation: "catalog_alarm",
        outcome: "retry_scheduled",
        overloaded: true,
        retryable: false,
        attempt_count: 1,
      });
      expect(warnings[0]).not.toContain("secret");
      expect(warnings[0]).not.toContain("private");
      expect(CATALOG_ALARM_BASE_RETRY_MS).toBeLessThan(42_000);
    });
  });

  it("migrates predecessor alarm rows with a zero durable attempt count", async () => {
    const stub = env.JOURNAL_MANIFEST.getByName(`catalog-alarm-migration-${crypto.randomUUID()}`);
    await runInDurableObject(stub, async (_instance, state) => {
      state.storage.sql.exec(`
        CREATE TABLE alarm_schedule (
          purpose TEXT PRIMARY KEY,
          fire_at_ms INTEGER NOT NULL,
          generation INTEGER NOT NULL,
          payload_hash TEXT NOT NULL
        )
      `);
      state.storage.sql.exec(
        "INSERT INTO alarm_schedule (purpose, fire_at_ms, generation, payload_hash) VALUES ('predecessor', 1, 2, ?)",
        "c".repeat(64),
      );
      const catalog = new FleetManifestCatalogStore(state.storage);
      catalog.migrate();
      expect(state.storage.sql.exec<{ name: string }>("PRAGMA table_info(alarm_schedule)").toArray()
        .map((column) => column.name)).toContain("attempt_count");
      expect(catalog.duePurposes(1)).toEqual([expect.objectContaining({
        purpose: "predecessor",
        generation: 2,
        attempt_count: 0,
      })]);
    });
  });

  it("caps repeated non-overload alarm backoff and emits recovery after a later success", async () => {
    const stub = env.FLEET_MANIFEST_CATALOG.getByName(`catalog-alarm-cap-${crypto.randomUUID()}`);
    await runInDurableObject(stub, async (durableObject) => {
      const instance = durableObject as unknown as FleetManifestCatalogDO;
      const catalog = (instance as unknown as { catalog: FleetManifestCatalogStore }).catalog;
      const now = Date.now();
      catalog.schedulePurpose({
        purpose: "snapshot_resume",
        fire_at_ms: now - 1,
        generation: 9,
        payload_hash: "d".repeat(64),
        attempt_count: 100,
      });
      (catalog as unknown as { resumeSnapshot: () => Promise<never> }).resumeSnapshot = async () => {
        throw new Error("sanitized by the SLO boundary");
      };
      await instance.alarm();
      const retry = catalog.duePurposes(Date.now() + CATALOG_ALARM_MAX_RETRY_MS + 1)
        .find((purpose) => purpose.purpose === "snapshot_resume");
      expect(retry).toMatchObject({ attempt_count: 101 });
      expect((retry?.fire_at_ms ?? 0) - now).toBeGreaterThanOrEqual(CATALOG_ALARM_MAX_RETRY_MS);

      (catalog as unknown as { resumeSnapshot: () => Promise<{ status: "complete" }> }).resumeSnapshot = async () => ({ status: "complete" });
      catalog.schedulePurpose({ ...retry!, fire_at_ms: 0 });
      const recovered: string[] = [];
      const info = vi.spyOn(console, "log").mockImplementation((value) => recovered.push(String(value)));
      try {
        await instance.alarm();
      } finally {
        info.mockRestore();
      }
      expect(catalog.duePurposes(Date.now() + CATALOG_ALARM_MAX_RETRY_MS + 1)
        .some((purpose) => purpose.purpose === "snapshot_resume")).toBe(false);
      expect(recovered.map((line) => JSON.parse(line))).toContainEqual(expect.objectContaining({
        event: "reliability.slo",
        component: "fleet_manifest_catalog",
        operation: "catalog_alarm",
        outcome: "recovered",
        attempt_count: 101,
        retry_after_ms: 0,
      }));
    });
  });

  it("projects an overload seam as a sanitized cooldown instead of a hot-loop retry", async () => {
    const providerError = Object.assign(new Error("token=secret tx=private"), {
      overloaded: true,
      retryable: true,
      retryAfterMs: 42_000,
    });
    const overloaded = { admission: async (): Promise<{ ok: true; status: "ready" }> => Promise.reject(providerError) };
    const result = await admissionThroughManifestStub(overloaded);
    expect(result).toEqual({
      ok: false,
      status: "unavailable",
      http_status: 503,
      error: expect.objectContaining({
        code: "TX_MANIFEST_UNAVAILABLE",
        retryable: true,
        overloaded: true,
        retry_after_ms: 42_000,
      }),
      circuit: {
        count_toward_open: true,
        ...MANIFEST_CIRCUIT_POLICY,
      },
    });
    expect(JSON.stringify(result)).not.toContain("secret");
    expect(JSON.stringify(result)).not.toContain("private");
  });

  it("preserves overload cooldown on an ambiguous post-decision registration", async () => {
    const providerError = Object.assign(new Error("token=secret tx=private"), {
      overloaded: true,
      retryable: true,
      retryAfterMs: 55_000,
    });
    const registration = { record: { tx_id: "tx-ambiguous-overload" } } as ManifestRegistrationV1;
    const result = await registerThroughManifestStub({ register: async () => { throw providerError; } }, registration);
    expect(result).toMatchObject({
      ok: false,
      status: "commit_pending_manifest",
      http_status: 202,
      tx_id: "tx-ambiguous-overload",
      retry_identical_registration: true,
      overloaded: true,
      retry_after_ms: 55_000,
    });
    expect(JSON.stringify(result)).not.toContain("secret");
    expect(JSON.stringify(result)).not.toContain("private");
  });

  it("preserves overload metadata returned as unavailable and on lookup transport failure", async () => {
    const registration = { record: { tx_id: "tx-returned-overload" } } as ManifestRegistrationV1;
    const returned = await registerThroughManifestStub({
      register: async () => ({
        ok: false as const,
        status: "unavailable" as const,
        http_status: 503,
        error: {
          schema_version: 1 as const,
          code: "TX_MANIFEST_UNAVAILABLE" as const,
          message: "sanitized",
          http_status: 503,
          retryable: true,
          overloaded: true as const,
          retry_after_ms: 61_000,
        },
      }),
    }, registration);
    expect(returned).toMatchObject({
      status: "commit_pending_manifest",
      overloaded: true,
      retry_after_ms: 61_000,
    });

    const providerError = Object.assign(new Error("token=secret tx=private"), {
      overloaded: true,
      retryable: true,
      retry_after_ms: 62_000,
    });
    const lookup = await lookupThroughManifestStub({ lookup: async () => { throw providerError; } }, "tx-lookup-overload");
    expect(lookup).toMatchObject({
      ok: false,
      found: false,
      status: "unavailable",
      error: {
        code: "TX_MANIFEST_UNAVAILABLE",
        retryable: true,
        overloaded: true,
        retry_after_ms: 62_000,
      },
      circuit: { count_toward_open: true },
    });
    expect(JSON.stringify(lookup)).not.toContain("secret");
    expect(JSON.stringify(lookup)).not.toContain("private");
  });

  it("expires and incrementally garbage-collects issued enumeration cursors", async () => {
    await withCatalog("cursor-gc", (catalog) => {
      const now = 10_000;
      const cursor = { request_hash: "a".repeat(64), partition: 3 };
      const evidence = [{ lease_expires_at_ms: now + 500 }];
      expect(catalog.issueEnumerationCursor(cursor, evidence, now)).toBe(now + 500);
      expect(catalog.enumerationCursorEvidence(cursor, now + 499)).toEqual(evidence);
      expect(catalog.enumerationCursorEvidence(cursor, now + 500)).toBeNull();
      expect(catalog.purgeEnumerationCursors(now + 500)).toBeNull();
    });
  });
});
