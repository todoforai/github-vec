#!/usr/bin/env bun
import { Database } from "duckdb";
import { parseArgs } from "util";

const MIN_SIZE = 500;
const MAX_CHARS = 50000;
const CONCURRENCY = 1500;
const MAX_RETRIES = 3;

const BRANCHES = ["main", "master"];
// Ordered by frequency - README.md covers 89% of repos
const README_NAMES = [
  "README.md",       // 70% master + 19% main = 89%
  // Uncomment below for broader coverage (~11% more):
  // "readme.md",       // 1047
  // "README.rst",      // 721
  // "README",          // 587
  // "Readme.md",       // 359
  // "README.markdown", // 222
  // "README.txt",      // 181
  // "README.adoc",     // 111
  // "readme.txt",      // 108
  // "README.MD",       // 106
  // "README.rdoc",     // 96
  // "ReadMe.md",       // 69
];

interface Stats {
  success: number;
  errors: Map<number, number>;
  skipped: number;
  tooSmall: number;
  truncated: number;
  processed: number;
  fetched: number;  // actual fetch attempts (non-skipped)
  createdDirs: Set<string>;
}

class ProxyPool {
  private proxies: string[] = [];

  async load(proxyFile: string) {
    const content = await Bun.file(proxyFile).text();
    for (const line of content.trim().split("\n")) {
      const parts = line.trim().split(":");
      if (parts.length === 4) {
        const [ip, port, user, pwd] = parts;
        this.proxies.push(`http://${user}:${pwd}@${ip}:${port}`);
      } else if (parts.length === 2) {
        const [ip, port] = parts;
        this.proxies.push(`http://${ip}:${port}`);
      }
    }
    if (this.proxies.length > 0) {
      console.log(`Loaded ${this.proxies.length} proxies`);
    }
  }

  get(): string | null {
    if (this.proxies.length === 0) return null;
    return this.proxies[Math.floor(Math.random() * this.proxies.length)];
  }
}


async function fetchWithRetry(
  url: string,
  proxy: string | null,
  repo: string
): Promise<{ response: Response | null; error: string | null }> {
  let lastErr: string | null = null;

  for (let retry = 0; retry < MAX_RETRIES; retry++) {
    try {
      const response = await fetch(url, {
        proxy: proxy ?? undefined,
        timeout: 30000,
      } as any);

      if ([429, 500, 502, 503, 504].includes(response.status)) {
        const wait = 2 ** retry;
        console.log(`\n[WARN] ${repo}: ${response.status} error, retry ${retry + 1}/${MAX_RETRIES} in ${wait}s`);
        await Bun.sleep(wait * 1000);
        continue;
      }
      return { response, error: null };
    } catch (e: any) {
      lastErr = `${e.name}: ${e.message}`;
      if (retry < MAX_RETRIES - 1) {
        const wait = 2 ** retry;
        await Bun.sleep(wait * 1000);
        continue;
      }
      console.log(`\n[ERR] ${repo}: ${lastErr}`);
      return { response: null, error: lastErr };
    }
  }
  return { response: null, error: lastErr || "max retries exceeded" };
}

interface FetchResult {
  content: string | null;
  branch: string;
  filename: string;
  status: number;
}

async function tryRawFetch(repo: string, proxyPool: ProxyPool): Promise<FetchResult> {
  let lastStatus = 404;
  const proxy = proxyPool.get();

  for (const readme of README_NAMES) {
    for (const branch of BRANCHES) {
      const url = `https://raw.githubusercontent.com/${repo}/${branch}/${readme}`;
      const { response, error } = await fetchWithRetry(url, proxy, repo);

      if (error) {
        lastStatus = 0;
        continue;
      }

      if (!response) continue;

      if (response.status === 200) {
        const content = await response.text();
        return { content, branch, filename: readme, status: 200 };
      } else if (response.status === 451) {
        return { content: null, branch: "", filename: "", status: 451 };
      } else if (response.status !== 404) {
        lastStatus = response.status;
      }
    }
  }

  return { content: null, branch: "", filename: "", status: lastStatus };
}

async function fetchAndSave(
  url: string,
  outputDir: string,
  errorsDir: string,
  proxyPool: ProxyPool,
  stats: Stats,
  total: number,
  startTime: number,
  existingRepos: Set<string>,
  errorRepos: Set<string>,
  verbose: boolean
) {
  const match = url.match(/github\.com\/([^/]+\/[^/]+)/);
  if (!match) {
    stats.errors.set(0, (stats.errors.get(0) || 0) + 1);
    return;
  }

  const repo = match[1].replace(/\.git$/, "");
  const repoFile = repo.replace("/", "_");

  if (existingRepos.has(repoFile) || errorRepos.has(repoFile)) {
    stats.skipped++;
    stats.processed++;
    // Print progress for skipped items too
    if (stats.processed % 1000 === 0) {
      const elapsed = (Date.now() - startTime) / 1000;
      const rate = elapsed > 0 ? stats.processed / elapsed : 0;
      process.stdout.write(`\r[${stats.processed}/${total}] skipping... (${rate.toFixed(0)}/s)   `);
    }
    return;
  }

  stats.fetched++;
  const fetchStart = Date.now();
  const result = await tryRawFetch(repo, proxyPool);
  const fetchTime = Date.now() - fetchStart;
  if (verbose) {
    console.log(`[TIME] ${(fetchTime/1000).toFixed(1)}s status=${result.status} len=${result.content ? result.content.length : 0} ${repo}`);
  }
  stats.processed++;


  if (!result.content) {
    // For 404s, include count of README names tested (e.g. 404_1, 404_12)
    const statusKey = result.status === 404
      ? `404_${README_NAMES.length}`
      : String(result.status);
    const statusDir = `${errorsDir}/${statusKey}`;
    if (!stats.createdDirs.has(statusKey)) {
      await Bun.$`mkdir -p ${statusDir}`.quiet();
      stats.createdDirs.add(statusKey);
    }
    await Bun.write(`${statusDir}/${repoFile}`, "");
    stats.errors.set(result.status, (stats.errors.get(result.status) || 0) + 1);
  } else if (result.content.length < MIN_SIZE) {
    const tooSmallDir = `${errorsDir}/tooSmall`;
    if (!stats.createdDirs.has("tooSmall")) {
      await Bun.$`mkdir -p ${tooSmallDir}`.quiet();
      stats.createdDirs.add("tooSmall");
    }
    await Bun.write(`${tooSmallDir}/${repoFile}`, "");
    stats.tooSmall++;
  } else {
    let content = result.content;
    if (content.length > MAX_CHARS) {
      content = content.slice(0, MAX_CHARS) + "\n\n[TRUNCATED]";
      stats.truncated++;
    }
    const outFile = `${repoFile}_${result.branch}_${result.filename}`;
    await Bun.write(`${outputDir}/${outFile}`, content);
    stats.success++;
  }

  // Print progress after all stats updated
  if (stats.processed % 100 === 0) {
    const elapsed = (Date.now() - startTime) / 1000;
    const completed = stats.success + stats.tooSmall + Array.from(stats.errors.values()).reduce((a, b) => a + b, 0);
    const rate = elapsed > 0 ? completed / elapsed : 0;
    const errorTotal = Array.from(stats.errors.values()).reduce((a, b) => a + b, 0);
    process.stdout.write(`\r[${stats.processed}/${total}] fetched:${stats.fetched} ✓${stats.success} ✗${errorTotal} small:${stats.tooSmall} (${rate.toFixed(0)}/s)   `);
  }
}

async function main() {
  const { values } = parseArgs({
    args: Bun.argv.slice(2),
    options: {
      limit: { type: "string" },
      offset: { type: "string", default: "0" },
      full: { type: "boolean", default: false },
      "min-date": { type: "string" },
      proxies: { type: "string" },
      verbose: { type: "boolean", default: false },
    },
  });
  const verbose = values.verbose;

  const proxyPool = new ProxyPool();
  if (values.proxies) {
    await proxyPool.load(values.proxies);
  }

  const outputDir = process.env.READMES_DIR || "/home/root/data/readmes";
  const errorsDir = `${outputDir}/.errors`;
  await Bun.$`mkdir -p ${outputDir} ${errorsDir}`.quiet();

  // Use DuckDB to stream from parquet
  const db = new Database(":memory:");
  const parquetFile = values.full
    ? "/home/root/data/github_visits_full.parquet"
    : "/home/root/data/github_visits_6k.parquet";

  const minDate = values["min-date"] || "2023-01-01";
  const limit = values.limit ? parseInt(values.limit) : null;
  const offset = parseInt(values.offset || "0");

  // Get total count first
  const countResult = await new Promise<any[]>((resolve, reject) => {
    db.all(
      `SELECT COUNT(*) as cnt FROM '${parquetFile}' WHERE date >= '${minDate}'`,
      (err, rows) => (err ? reject(err) : resolve(rows))
    );
  });
  const totalFiltered = Number(countResult[0].cnt);
  console.log(`Total origins >= ${minDate}: ${totalFiltered.toLocaleString()}`);

  const total = limit ? Math.min(limit, totalFiltered - offset) : totalFiltered - offset;
  console.log(`Fetching ${total.toLocaleString()} repos (concurrency: ${CONCURRENCY})...`);
  if (offset > 0) {
    console.log(`Starting at offset: ${offset.toLocaleString()}`);
  }

  // Pre-scan existing files
  console.log("Scanning existing files...");
  const existingRepos = new Set<string>();
  const errorRepos = new Set<string>();

  for await (const file of new Bun.Glob("*").scan(outputDir)) {
    if (!file.startsWith(".")) {
      const parts = file.split("_");
      if (parts.length >= 3) {
        existingRepos.add(parts.slice(0, -2).join("_"));
      }
    }
  }

  try {
    const { readdirSync } = await import("fs");
    for (const statusDir of readdirSync(errorsDir)) {
      const statusPath = `${errorsDir}/${statusDir}`;
      for (const file of readdirSync(statusPath)) {
        errorRepos.add(file);
      }
    }
  } catch {}

  console.log(`  ${existingRepos.size.toLocaleString()} existing, ${errorRepos.size.toLocaleString()} errors\n`);

  const stats: Stats = {
    success: 0,
    errors: new Map(),
    skipped: 0,
    tooSmall: 0,
    truncated: 0,
    processed: 0,
    fetched: 0,
    createdDirs: new Set(),
  };
  const startTime = Date.now();

  // Bounded task pool - only keep CONCURRENCY tasks in flight
  const inFlight = new Set<Promise<void>>();

  const processUrl = async (url: string) => {
    await fetchAndSave(url, outputDir, errorsDir, proxyPool, stats, total, startTime, existingRepos, errorRepos, verbose);
  };

  // Process in batches - query DuckDB in chunks
  const BATCH_SIZE = 50000;
  let batchOffset = offset;
  let remaining = total;

  while (remaining > 0) {
    const batchLimit = Math.min(BATCH_SIZE, remaining);
    const query = `SELECT origin FROM '${parquetFile}' WHERE date >= '${minDate}' LIMIT ${batchLimit} OFFSET ${batchOffset}`;

    process.stdout.write(`\rLoading batch at offset ${batchOffset.toLocaleString()}...`);

    const urls = await new Promise<string[]>((resolve, reject) => {
      db.all(query, (err, rows: { origin: string }[]) => {
        if (err) reject(err);
        else resolve(rows.map((r) => r.origin));
      });
    });

    if (urls.length === 0) break;

    for (const url of urls) {
      // Wait if at capacity
      while (inFlight.size >= CONCURRENCY) {
        await Promise.race(inFlight);
      }

      // Start task and track it
      const task = processUrl(url).then(() => {
        inFlight.delete(task);
      });
      inFlight.add(task);
    }

    batchOffset += urls.length;
    remaining -= urls.length;
  }

  // Wait for remaining tasks
  await Promise.all(inFlight);

  const errorTotal = Array.from(stats.errors.values()).reduce((a, b) => a + b, 0);
  console.log(`\n\nDone! ${stats.success} READMEs saved to ${outputDir}/`);
  console.log(`${stats.skipped} skipped, ${stats.tooSmall} too small, ${stats.truncated} truncated`);
  if (stats.errors.size > 0) {
    console.log("Errors by status code:");
    const labels: Record<number, string> = { 0: "timeout/connection", 404: "not found", 451: "DMCA" };
    for (const [status, count] of Array.from(stats.errors.entries()).sort((a, b) => a[0] - b[0])) {
      const label = labels[status] || String(status);
      console.log(`  ${status} (${label}): ${count.toLocaleString()}`);
    }
  }

  db.close();
}

main().catch(console.error);
