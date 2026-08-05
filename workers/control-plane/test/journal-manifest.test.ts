import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import {
  COORDINATOR_RETENTION_DAYS,
  CURRENT_PROTOCOL_VERSION,
  REDO_ENVELOPE_FORMAT_VERSION,
  createManifestRegistration,
  hashCanonicalJson,
  hashParticipantOperations,
  type ManifestRegistrationV1,
  type RedoEnvelopeV1,
  type RedoParticipantV1,
} from "../../../packages/contracts/src/index.js";
import {
  CIRCUIT_DIRECTIVE,
  LIFECYCLE_FAILURE_RETRY_MS,
  MANIFEST_CIRCUIT_POLICY,
  admissionThroughManifestStub,
  executeLifecycleAlarm,
  manifestObjectNameForRegistration,
  registerThroughManifestStub,
} from "../src/index.js";

const DAY_MS = 24 * 60 * 60 * 1000;

async function registration(options: {
  txId?: string;
  coordinatorId?: string;
  commitDecidedAt?: string;
  value?: string;
} = {}): Promise<ManifestRegistrationV1> {
  const commitDecidedAt = options.commitDecidedAt ?? "2026-08-05T12:34:56.000Z";
  const participants: readonly RedoParticipantV1[] = [
    {
      participant_id: "shard-a",
      epoch: 1,
      intents: [
        {
          intent_seq: 0,
          sql: "INSERT INTO t (id, value) VALUES (?, ?)",
          params: ["row-1", options.value ?? "value-1"],
          tenant_id: "tenant-a",
          table_name: "t",
          partition_key: "row-1",
          vbucket: 7,
          operation: "insert",
          mirror_target_participant_id: null,
        },
      ],
    },
  ];
  const envelope: RedoEnvelopeV1 = {
    protocol_version: CURRENT_PROTOCOL_VERSION,
    format_version: REDO_ENVELOPE_FORMAT_VERSION,
    tx_id: options.txId ?? "tx-001",
    fleet_id: "fleet-a",
    coordinator_id: options.coordinatorId ?? "coordinator-a",
    decision: "commit",
    decision_epoch: 1,
    commit_decided_at: commitDecidedAt,
    retention_deadline: new Date(new Date(commitDecidedAt).getTime() + COORDINATOR_RETENTION_DAYS * DAY_MS).toISOString(),
    operation_hash: await hashParticipantOperations(participants),
    participants,
  };
  return createManifestRegistration(envelope);
}

async function stubFor(value: ManifestRegistrationV1) {
  return env.JOURNAL_MANIFEST.getByName(await manifestObjectNameForRegistration(value));
}

describe("JournalManifestDO registration and lookup", () => {
  it("registers versioned content and treats an identical retry as idempotent", async () => {
    const value = await registration({ txId: "tx-idempotent" });
    const stub = await stubFor(value);

    await expect(stub.register(value)).resolves.toMatchObject({ ok: true, status: "registered", record_hash: value.record_hash });
    await expect(stub.register(value)).resolves.toMatchObject({
      ok: true,
      status: "already_registered",
      record_hash: value.record_hash,
    });

    await expect(stub.lookup(value.record.tx_id)).resolves.toEqual({
      ok: true,
      found: true,
      record: value.record,
      record_hash: value.record_hash,
      quarantined: false,
      conflicting_record_hashes: [],
      lifecycle_released: false,
    });
  });

  it("quarantines same transaction identity with different content without overwriting the original", async () => {
    const original = await registration({ txId: "tx-conflict", coordinatorId: "coordinator-original" });
    const conflicting = await registration({ txId: "tx-conflict", coordinatorId: "coordinator-conflicting" });
    const stub = await stubFor(original);

    await stub.register(original);
    await expect(stub.register(conflicting)).resolves.toMatchObject({
      ok: false,
      status: "quarantined",
      error: { code: "TX_MANIFEST_CONFLICT" },
      original_record_hash: original.record_hash,
      conflicting_record_hash: conflicting.record_hash,
    });

    const lookup = await stub.lookup(original.record.tx_id);
    expect(lookup).toMatchObject({
      ok: true,
      found: true,
      record: { coordinator_id: "coordinator-original" },
      record_hash: original.record_hash,
      quarantined: true,
      conflicting_record_hashes: [conflicting.record_hash],
    });
    await expect(stub.stats()).resolves.toMatchObject({ records: 1, quarantined: 1, conflicts: 1 });
  });

  it("rejects a recomputed record hash whose UTC-day/16-way route is invalid", async () => {
    const original = await registration({ txId: "tx-bad-route" });
    const badRecord = {
      ...original.record,
      partition: (original.record.partition + 1) % original.record.partition_count,
      routing_key: `${original.record.utc_day}:${((original.record.partition + 1) % original.record.partition_count)
        .toString()
        .padStart(2, "0")}`,
    };
    const invalid: ManifestRegistrationV1 = {
      record: badRecord,
      record_hash: await hashCanonicalJson(badRecord),
    };
    const stub = env.JOURNAL_MANIFEST.getByName("deliberately-invalid-route");
    await expect(stub.register(invalid)).resolves.toMatchObject({
      ok: false,
      status: "rejected",
      error: { code: "TX_MANIFEST_CONFLICT" },
    });
    await expect(stub.lookup(original.record.tx_id)).resolves.toEqual({ ok: true, found: false });
  });

  it("returns typed 400 rejections for malformed untrusted RPC values", async () => {
    const stub = env.JOURNAL_MANIFEST.getByName(`malformed-${crypto.randomUUID()}`);
    const malformed: unknown[] = [null, {}, { record: null }, { record: { tx_id: "untrusted" } }];

    for (const value of malformed) {
      await expect(stub.register(value)).resolves.toMatchObject({
        ok: false,
        status: "rejected",
        http_status: 400,
        error: { code: "TX_ENVELOPE_INVALID" },
      });
    }
    await expect(stub.stats()).resolves.toMatchObject({ records: 0, conflicts: 0 });
  });
});

describe("ambiguous transport and overload seams", () => {
  it("returns commit_pending_manifest after response loss and converges on identical retry", async () => {
    const value = await registration({ txId: "tx-ambiguous" });
    const stub = await stubFor(value);
    const responseLostAfterDurability = {
      register: async (candidate: ManifestRegistrationV1) => {
        await stub.register(candidate);
        throw new Error("simulated response loss after durable registration");
      },
    };

    await expect(registerThroughManifestStub(responseLostAfterDurability, value)).resolves.toEqual({
      ok: false,
      status: "commit_pending_manifest",
      http_status: 202,
      tx_id: value.record.tx_id,
      retry_identical_registration: true,
      circuit: CIRCUIT_DIRECTIVE,
    });
    await expect(registerThroughManifestStub(stub, value)).resolves.toMatchObject({
      ok: true,
      status: "already_registered",
      record_hash: value.record_hash,
    });
  });

  it("returns the typed coordinator-owned 3/30s circuit signal on an overload seam", async () => {
    const overloaded = { admission: async (): Promise<{ ok: true; status: "ready" }> => Promise.reject(new Error("overload")) };
    await expect(admissionThroughManifestStub(overloaded)).resolves.toEqual({
      ok: false,
      status: "unavailable",
      http_status: 503,
      error: expect.objectContaining({ code: "TX_MANIFEST_UNAVAILABLE", retryable: true }),
      circuit: {
        count_toward_open: true,
        ...MANIFEST_CIRCUIT_POLICY,
      },
    });
  });
});

describe("35-day lifecycle alarm", () => {
  it("holds an expired record until release, then deletes it idempotently", async () => {
    const commitDecidedAt = new Date(Date.now() - 36 * DAY_MS).toISOString();
    const value = await registration({ txId: "tx-lifecycle", commitDecidedAt });
    const stub = await stubFor(value);
    await stub.register(value);

    await runInDurableObject(stub, async (instance) => instance.alarm?.());
    await expect(stub.lookup(value.record.tx_id)).resolves.toMatchObject({ found: true, lifecycle_released: false });

    await expect(stub.release(value.record.tx_id, value.record_hash)).resolves.toEqual({ ok: true, status: "released" });
    await runInDurableObject(stub, async (instance) => instance.alarm?.());
    await expect(stub.lookup(value.record.tx_id)).resolves.toEqual({ ok: true, found: false });
    await expect(stub.release(value.record.tx_id, value.record_hash)).resolves.toEqual({ ok: true, status: "not_found" });
  });

  it("never lifecycle-deletes a quarantined conflict", async () => {
    const commitDecidedAt = new Date(Date.now() - 36 * DAY_MS).toISOString();
    const original = await registration({ txId: "tx-quarantine-retention", commitDecidedAt, coordinatorId: "one" });
    const conflicting = await registration({ txId: "tx-quarantine-retention", commitDecidedAt, coordinatorId: "two" });
    const stub = await stubFor(original);
    await stub.register(original);
    await stub.register(conflicting);

    await expect(stub.release(original.record.tx_id, original.record_hash)).resolves.toMatchObject({
      ok: false,
      status: "quarantined",
    });
    await runInDurableObject(stub, async (instance) => instance.alarm?.());
    await expect(stub.lookup(original.record.tx_id)).resolves.toMatchObject({ found: true, quarantined: true });
  });

  it("installs one bounded fallback alarm when a lifecycle sweep fails", async () => {
    const now = Date.UTC(2026, 7, 5, 12, 0, 0);
    const scheduled: number[] = [];
    let deleteCalls = 0;

    await expect(
      executeLifecycleAlarm(
        {
          setAlarm: async (timestampMs) => {
            scheduled.push(timestampMs);
          },
          deleteAlarm: async () => {
            deleteCalls += 1;
          },
        },
        () => {
          throw new Error("deterministic sweep failure");
        },
        now,
      ),
    ).resolves.toBeUndefined();

    expect(scheduled).toEqual([now + LIFECYCLE_FAILURE_RETRY_MS]);
    expect(deleteCalls).toBe(0);
  });

  it("rethrows when fallback scheduling fails so platform alarm retries remain active", async () => {
    const now = Date.UTC(2026, 7, 5, 12, 0, 0);
    let setCalls = 0;

    await expect(
      executeLifecycleAlarm(
        {
          setAlarm: async () => {
            setCalls += 1;
            throw new Error("deterministic alarm storage failure");
          },
          deleteAlarm: async () => undefined,
        },
        () => {
          throw new Error("deterministic sweep failure");
        },
        now,
      ),
    ).rejects.toThrow("deterministic sweep failure");
    expect(setCalls).toBe(1);
  });
});
