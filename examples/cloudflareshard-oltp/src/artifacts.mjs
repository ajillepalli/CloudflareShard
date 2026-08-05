import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import { renderJson, renderMarkdown } from "./render.mjs";

/** @param {import("./types.d.ts").BenchmarkResult} result */
export async function writeArtifacts(result, outputDir) {
  await mkdir(outputDir, { recursive: true });
  const jsonPath = path.join(outputDir, `${result.run.runId}.json`);
  const markdownPath = path.join(outputDir, `${result.run.runId}.md`);
  await Promise.all([
    writeFile(jsonPath, renderJson(result), { encoding: "utf8", mode: 0o600 }),
    writeFile(markdownPath, renderMarkdown(result), { encoding: "utf8", mode: 0o600 }),
  ]);
  return { jsonPath, markdownPath };
}
