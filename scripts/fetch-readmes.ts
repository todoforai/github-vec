import { $ } from "bun";
import pMap from "p-map";

const originsPath = "/tmp/origins-6k.txt";
const outputDir = "./readmes";
const MIN_SIZE = 500;
const MAX_CHARS = 50000;
const CONCURRENCY = 100;

await $`mkdir -p ${outputDir}`;

const origins = (await Bun.file(originsPath).text()).trim().split("\n");
console.log(`Fetching ${origins.length} repos (concurrency: ${CONCURRENCY})...\n`);

const branches = ["main", "master"];

let success = 0;
let failed = 0;
let skipped = 0;
let tooSmall = 0;
let truncated = 0;
let apiFallbacks = 0;
let processed = 0;

const token = process.env.GITHUB_TOKEN;
const start = Date.now();

async function fetchAndSave(url: string): Promise<void> {
  const match = url.match(/github\.com\/([^\/]+\/[^\/]+)/);
  if (!match) { failed++; return; }

  const repo = match[1].replace(/\.git$/, "");

  // Skip if already exists
  const prefix = repo.replace("/", "_") + "_";
  const files = await Array.fromAsync(new Bun.Glob(`${prefix}*`).scan(outputDir));
  if (files.length > 0) {
    skipped++;
    processed++;
    return;
  }

  let content = "";
  let branch = "";
  let filename = "";

  // Try README.md and readme.md on main/master
  outer: for (const readme of ["README.md", "readme.md"]) {
    for (const b of branches) {
      const rawUrl = `https://raw.githubusercontent.com/${repo}/${b}/${readme}`;
      try {
        const res = await fetch(rawUrl);
        if (res.ok) {
          content = await res.text();
          branch = b;
          filename = readme;
          break outer;
        }
      } catch {}
    }
  }

  // Fallback to GitHub API
  if (!content) {
    const apiUrl = `https://api.github.com/repos/${repo}/readme`;
    const headers: Record<string, string> = {};
    if (token) headers.Authorization = `Bearer ${token}`;

    try {
      const res = await fetch(apiUrl, { headers });
      if (res.ok) {
        apiFallbacks++;
        const data = await res.json();
        content = atob(data.content.replace(/\n/g, ""));
        branch = "default";
        filename = data.name;
      }
    } catch {}
  }

  processed++;

  if (!content) {
    failed++;
    return;
  }

  if (content.length < MIN_SIZE) {
    tooSmall++;
    return;
  }

  if (content.length > MAX_CHARS) {
    content = content.slice(0, MAX_CHARS) + "\n\n[TRUNCATED]";
    truncated++;
  }

  const outFile = `${repo.replace("/", "_")}_${branch}_${filename}`;
  await Bun.write(`${outputDir}/${outFile}`, content);
  success++;

  if (processed % 100 === 0) {
    const rate = (processed / ((Date.now() - start) / 1000)).toFixed(0);
    process.stdout.write(`\r[${processed}/${origins.length}] ✓ ${success} ✗ ${failed} (${rate}/s)`);
  }
}

await pMap(origins, fetchAndSave, { concurrency: CONCURRENCY });

console.log(`\n\nDone! ${success} READMEs saved to ${outputDir}/`);
console.log(`${skipped} skipped, ${tooSmall} too small, ${truncated} truncated`);
if (apiFallbacks > 0) console.log(`${apiFallbacks} used API fallback`);
if (failed > 0) console.log(`${failed} repos had no README`);
