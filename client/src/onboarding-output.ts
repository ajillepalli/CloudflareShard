import { sanitizeOnboardingResult, sanitizeOnboardingText, type OnboardingCheck, type OnboardingResult } from "./onboarding.js";

export const CLI_OUTPUT_VERSION = "cloudflareshard.cli-output.v1" as const;

export interface ReceiptReference {
  path: string;
  checksum: string;
}

export interface OnboardingOutputFailure {
  code: "RECEIPT_WRITE_FAILED";
  category: "PREREQUISITE";
  message: string;
}

export interface OnboardingOutput {
  schemaVersion: typeof CLI_OUTPUT_VERSION;
  result: OnboardingResult;
  receipt: ReceiptReference | null;
  failure?: OnboardingOutputFailure;
}

export function terminalWidth(columns: number | undefined): 80 | 120 {
  return typeof columns === "number" && columns >= 120 ? 120 : 80;
}

function wrap(text: string, width: number, indent: string): string[] {
  const available = Math.max(20, width - indent.length);
  const words = text.trim().split(/\s+/).flatMap((word) => {
    if (word.length <= available) return [word];
    const chunks: string[] = [];
    for (let start = 0; start < word.length; start += available) chunks.push(word.slice(start, start + available));
    return chunks;
  });
  const lines: string[] = [];
  let current = "";
  for (const word of words) {
    if (current.length > 0 && current.length + 1 + word.length > available) {
      lines.push(`${indent}${current}`);
      current = word;
    } else {
      current = current ? `${current} ${word}` : word;
    }
  }
  if (current) lines.push(`${indent}${current}`);
  return lines.length > 0 ? lines : [indent];
}

function statusGlyph(check: OnboardingCheck, color: boolean): string {
  const plain = check.status === "PASS" ? "PASS" : check.status === "WARN" ? "WARN" : "FAIL";
  if (!color) return plain;
  const code = check.status === "PASS" ? "32" : check.status === "WARN" ? "33" : "31";
  return `\u001b[${code}m${plain}\u001b[0m`;
}

export function renderHuman(output: OnboardingOutput, options: { width?: number; color?: boolean } = {}): string {
  const width = terminalWidth(options.width);
  const result = sanitizeOnboardingResult(output.result);
  const failure = output.failure
    ? { ...output.failure, message: sanitizeOnboardingText(output.failure.message) }
    : undefined;
  const lines = [`CloudflareShard ${result.command}: ${failure ? "PREREQUISITE_FAILED" : result.status}`];
  for (const check of result.checks) {
    lines.push(`  ${statusGlyph(check, options.color === true).padEnd(options.color ? 18 : 6)} ${check.label}`);
    lines.push(...wrap(check.message, width, "         "));
  }
  if (result.command === "verify") {
    lines.push(...wrap(`Proof: ${result.summary.distinctPlacements} placements, ${result.summary.rowsVerified} rows, replay ${result.summary.replayMatched ? "matched" : "not matched"}.`, width, "  "));
    lines.push(...wrap("Cleanup: verification resources remain on the disposable target; tear down the Worker when finished.", width, "  "));
  }
  if (failure) {
    lines.push(...wrap(`FAIL     Receipt output: ${failure.message}`, width, "  "));
  } else if (output.receipt) {
    lines.push(...wrap(`Receipt: ${sanitizeOnboardingText(output.receipt.path)}`, width, "  "));
    lines.push(...wrap(`SHA-256: ${output.receipt.checksum}`, width, "  "));
  }
  return `${lines.join("\n")}\n`;
}

export function renderJson(output: OnboardingOutput): string {
  return `${JSON.stringify({
    ...output,
    result: sanitizeOnboardingResult(output.result),
    receipt: output.receipt ? { ...output.receipt, path: sanitizeOnboardingText(output.receipt.path) } : null,
    ...(output.failure ? { failure: { ...output.failure, message: sanitizeOnboardingText(output.failure.message) } } : {}),
  })}\n`;
}

export function exitCodeFor(result: OnboardingResult): number {
  if (result.command === "doctor") return result.status === "NOT_READY" ? 3 : 0;
  if (result.status === "VERIFIED") return 0;
  if (result.status === "PENDING_RECONCILIATION") return 5;
  return 4;
}
