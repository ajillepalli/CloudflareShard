import { createHash, randomUUID } from "node:crypto";
import { mkdir, rename, writeFile } from "node:fs/promises";
import path from "node:path";
import { sanitizeOnboardingResult, sanitizeOnboardingText, type OnboardingResult } from "./onboarding.js";

export const RECEIPT_VERSION = "cloudflareshard.receipt.v1" as const;

function stableValue(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(stableValue);
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>)
        .sort(([a], [b]) => a.localeCompare(b))
        .map(([key, child]) => [key, stableValue(child)]),
    );
  }
  return value;
}

export function stableStringify(value: unknown): string {
  return JSON.stringify(stableValue(value));
}

function sha256(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}

function redactString(value: string, secrets: string[]): string {
  let redacted = value.replace(/Bearer\s+[A-Za-z0-9._~+\/-]+/gi, "Bearer [REDACTED]");
  for (const secret of secrets.filter(Boolean)) {
    redacted = redacted.split(secret).join("[REDACTED]");
    redacted = redacted.split(encodeURIComponent(secret)).join("[REDACTED]");
  }
  return sanitizeOnboardingText(redacted);
}

const CREDENTIAL_KEY = /^(?:api[_-]?key|access[_-]?token|refresh[_-]?token|admin[_-]?token|tenant[_-]?token|client[_-]?secret|authorization|proxy[_-]?authorization|credential|credentials|cookie|token|secret|password)$/iu;

function redact(value: unknown, secrets: string[]): unknown {
  if (typeof value === "string") return redactString(value, secrets);
  if (Array.isArray(value)) return value.map((child) => redact(child, secrets));
  if (value && typeof value === "object") {
    return Object.fromEntries(Object.entries(value as Record<string, unknown>).map(([key, child]) => [
      key,
      CREDENTIAL_KEY.test(key) ? "[REDACTED]" : redact(child, secrets),
    ]));
  }
  return value;
}

export interface ReceiptFile {
  schemaVersion: typeof RECEIPT_VERSION;
  targetFingerprint: string;
  result: OnboardingResult;
  checksum: { algorithm: "sha256"; value: string };
}

export interface WriteReceiptOptions {
  directory?: string;
  cwd?: string;
  secrets?: string[];
}

export async function writeReceipt(
  result: OnboardingResult,
  targetUrl: string,
  options: WriteReceiptOptions = {},
): Promise<{ path: string; checksum: string }> {
  const origin = new URL(targetUrl).origin;
  const targetFingerprint = sha256(origin).slice(0, 16);
  const safeResult = sanitizeOnboardingResult(redact(result, [...(options.secrets ?? []), targetUrl, origin]) as OnboardingResult);
  const payload = { schemaVersion: RECEIPT_VERSION, targetFingerprint, result: safeResult };
  const checksum = sha256(stableStringify(payload));
  const receipt: ReceiptFile = { ...payload, checksum: { algorithm: "sha256", value: checksum } };
  const directory = options.directory ?? path.join(options.cwd ?? process.cwd(), ".cloudflareshard", "receipts");
  await mkdir(directory, { recursive: true });
  const stamp = result.finishedAt.replace(/[:.]/g, "-");
  const suffix = result.command === "verify" ? `-${result.runId}` : `-${checksum.slice(0, 8)}`;
  const filename = `${stamp}-${result.command}${suffix}.json`;
  const destination = path.join(directory, filename);
  const temporary = `${destination}.tmp-${process.pid}-${randomUUID()}`;
  await writeFile(temporary, `${stableStringify(receipt)}\n`, { encoding: "utf8", mode: 0o600 });
  await rename(temporary, destination);
  return { path: destination, checksum };
}
