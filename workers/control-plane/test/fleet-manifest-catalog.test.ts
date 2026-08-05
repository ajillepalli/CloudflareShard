import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import {
  FLEET_CATALOG_PROTOCOL_VERSION,
  FleetManifestCatalogStore,
  INITIAL_PARTITION_COUNT,
  type CatalogActivationRequest,
} from "../src/fleet-manifest-catalog.js";

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
        catalog.appendPartitionConfig({ fleet_id: FLEET, effective_from_day: "2026-08-06", partition_count: 32 }, now),
      ).rejects.toThrow("later than the next UTC day");
      const appended = await catalog.appendPartitionConfig(
        { fleet_id: FLEET, effective_from_day: "2026-08-07", partition_count: 32 },
        now,
      );
      expect(appended).toMatchObject({ partition_count: 32, prior_hash: initial.config_hash });
      await expect(catalog.partitionConfigForDay(FLEET, "2026-08-07")).resolves.toMatchObject({
        partition_count: 32,
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
