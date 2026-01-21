#!/usr/bin/env bun
import { Database } from "duckdb";
import { parseArgs } from "util";

const MIN_SIZE = 500;
const MAX_CHARS = 50000;
const CONCURRENCY = 1000;
const MAX_RETRIES = 5;
const TIMEOUT = 10000;
const BAD_PROXY_PENALTY = 15000;

let activeConnections = 0;
let penalizedCount = 0;

const BRANCHES = ["master", "main"];  // master is 70%, main is 19%
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
  totalFetchTime: number;
  totalWriteTime: number;
}

class ProxyPool {
  private urls: string[] = [];
  private times: number[] = [];
  private indexMap: Map<string, number> = new Map(); // url -> index for O(1) lookup

  async load(proxyFile: string) {
    const content = await Bun.file(proxyFile).text();
    for (const line of content.trim().split("\n")) {
      const parts = line.trim().split(":");
      let url: string;
      if (parts.length === 4) {
        const [ip, port, user, pwd] = parts;
        url = `http://${user}:${pwd}@${ip}:${port}`;
      } else if (parts.length === 2) {
        const [ip, port] = parts;
        url = `http://${ip}:${port}`;
      } else {
        continue;
      }
      if (!this.indexMap.has(url)) {
        this.indexMap.set(url, this.urls.length);
        this.urls.push(url);
        this.times.push(1000); // initial estimate 1s
      }
    }
    console.log(`Loaded ${proxyFile}: ${this.urls.length} total proxies`);
  }

  // Power of Two Choices: pick 2 random, return faster one
  get(): string | null {
    const len = this.urls.length;
    if (len === 0) return null;

    const i1 = Math.floor(Math.random() * len);
    let i2 = Math.floor(Math.random() * len);
    if (i2 === i1) i2 = (i2 + 1) % len;

    return this.times[i1] <= this.times[i2] ? this.urls[i1] : this.urls[i2];
  }

  // Update average response time (exponential moving average)
  report(url: string, timeMs: number) {
    const idx = this.indexMap.get(url);
    if (idx !== undefined) {
      this.times[idx] = this.times[idx] * 0.8 + timeMs * 0.2;
    }
  }
}


async function fetchWithRetry(
  url: string,
  proxyPool: ProxyPool,
  repo: string
): Promise<{ response: Response | null; error: string | null }> {
  let lastErr: string | null = null;

  for (let retry = 0; retry < MAX_RETRIES; retry++) {
    const proxy = proxyPool.get(); // Fresh proxy each attempt
    const start = Date.now();
    activeConnections++;

    try {
      const response = await fetch(url, {
        proxy: proxy ?? undefined,
        timeout: TIMEOUT,
      } as any);
      activeConnections--;

      // Report timing on success
      if (proxy) proxyPool.report(proxy, Date.now() - start);

      if ([429, 500, 502, 503, 504].includes(response.status)) {
        const wait = 2 ** retry;
        console.log(`\n[WARN] ${repo}: ${response.status} error, retry ${retry + 1}/${MAX_RETRIES} in ${wait}s`);
        await Bun.sleep(wait * 1000);
        continue;
      }
      return { response, error: null };
    } catch (e: any) {
      activeConnections--;
      // Report slow time on failure (penalize bad proxy)
      if (proxy) {
        proxyPool.report(proxy, BAD_PROXY_PENALTY);
        penalizedCount++;
      }

      lastErr = `${e.name}: ${e.message}`;
      if (retry < MAX_RETRIES - 1) {
        continue; // Try next proxy immediately, no wait
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

  for (const readme of README_NAMES) {
    for (const branch of BRANCHES) {
      const url = `https://raw.githubusercontent.com/${repo}/${branch}/${readme}`;
      const { response, error } = await fetchWithRetry(url, proxyPool, repo);

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

  // Skip repos with absurdly long names (filesystem limit is 255)
  if (repoFile.length > 200) {
    stats.skipped++;
    stats.processed++;
    return;
  }

  // Check if already processed (use sets if available, otherwise check filesystem)
  let alreadyDone = existingRepos.has(repoFile) || errorRepos.has(repoFile);
  if (!alreadyDone && existingRepos.size === 0 && errorRepos.size === 0) {
    // Parallel instance: check filesystem directly
    const existsSuccess = await Bun.file(`${outputDir}/${repoFile}_master_README.md`).exists() ||
                          await Bun.file(`${outputDir}/${repoFile}_main_README.md`).exists();
    const existsError = await Bun.file(`${errorsDir}/404_1/${repoFile}`).exists();
    alreadyDone = existsSuccess || existsError;
  }

  if (alreadyDone) {
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
  stats.processed++;

  // Time the write operation
  const writeStart = Date.now();


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
  const writeTime = Date.now() - writeStart;
  stats.totalFetchTime += fetchTime;
  stats.totalWriteTime += writeTime;

  if (verbose) {
    console.log(`[TIME] fetch=${(fetchTime/1000).toFixed(2)}s write=${(writeTime/1000).toFixed(3)}s status=${result.status} len=${result.content ? result.content.length : 0} ${repo}`);
  }

  // Print progress after all stats updated
  if (stats.processed % 100 === 0) {
    const avgFetch = stats.fetched > 0 ? stats.totalFetchTime / stats.fetched : 0;
    const avgWrite = stats.fetched > 0 ? stats.totalWriteTime / stats.fetched : 0;
    const elapsed = (Date.now() - startTime) / 1000;
    const completed = stats.success + stats.tooSmall + Array.from(stats.errors.values()).reduce((a, b) => a + b, 0);
    const rate = elapsed > 0 ? completed / elapsed : 0;
    const errorTotal = Array.from(stats.errors.values()).reduce((a, b) => a + b, 0);
    process.stdout.write(`\r[${stats.processed}/${total}] fetched:${stats.fetched} ✓${stats.success} ✗${errorTotal} small:${stats.tooSmall} (${rate.toFixed(0)}/s) conn:${activeConnections} pen:${penalizedCount} avg:${(avgFetch/1000).toFixed(2)}s   `);
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
      proxies: { type: "string", multiple: true },
      verbose: { type: "boolean", default: false },
    },
  });
  const verbose = values.verbose;

  const proxyPool = new ProxyPool();
  if (values.proxies) {
    for (const proxyFile of values.proxies) {
      await proxyPool.load(proxyFile);
    }
  }

  const minDate = values["min-date"] || "2023-01-01";
  const limit = values.limit ? parseInt(values.limit) : null;
  const offset = parseInt(values.offset || "0");

  const outputDir = process.env.READMES_DIR || "/home/root/data/readmes";
  const errorsDir = `${outputDir}/.errors`;
  await Bun.$`mkdir -p ${outputDir} ${errorsDir}`.quiet();

  // Use DuckDB - persistent for main instance, in-memory for parallel instances
  const dbPath = `${outputDir}/.fetch-cache.duckdb`;
  const isParallel = offset > 0;
  const db = isParallel ? new Database(":memory:") : new Database(dbPath);
  const parquetFile = values.full
    ? "/home/root/data/github_visits_full.parquet"
    : "/home/root/data/github_visits_6k.parquet";

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

  // Pre-scan existing files (skip for parallel instances to save ~5GB RAM)
  const existingRepos = new Set<string>();
  const errorRepos = new Set<string>();

  if (!isParallel) {
    console.log("Scanning existing files...");
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
  } else {
    console.log("Skipping file scan (parallel instance)\n");
  }

  const stats: Stats = {
    success: 0,
    errors: new Map(),
    skipped: 0,
    tooSmall: 0,
    truncated: 0,
    processed: 0,
    fetched: 0,
    createdDirs: new Set(),
    totalFetchTime: 0,
    totalWriteTime: 0,
  };
  const startTime = Date.now();

  // Bounded task pool - only keep CONCURRENCY tasks in flight
  const inFlight = new Set<Promise<void>>();

  const processUrl = async (url: string) => {
    await fetchAndSave(url, outputDir, errorsDir, proxyPool, stats, total, startTime, existingRepos, errorRepos, verbose);
  };

  // Create indexed table for fast offset queries
  const tableName = `urls_${minDate.replace(/-/g, "_")}`;

  // Check if table already exists
  const tableExists = await new Promise<boolean>((resolve, reject) => {
    db.all(
      `SELECT COUNT(*) as cnt FROM information_schema.tables WHERE table_name = '${tableName}'`,
      (err, rows: any[]) => {
        if (err) reject(err);
        else resolve(Number(rows[0].cnt) > 0);
      }
    );
  });

  if (isParallel) {
    // Parallel instance: create small table with just our slice (saves ~20GB RAM)
    const sliceEnd = limit ? offset + limit : offset + 10000000;
    console.log(`Creating slice table for range ${offset.toLocaleString()}-${sliceEnd.toLocaleString()}...`);
    await new Promise<void>((resolve, reject) => {
      db.exec(
        `CREATE TABLE ${tableName} AS
         SELECT * FROM (
           SELECT ROW_NUMBER() OVER () as id, origin
           FROM '${parquetFile}' WHERE date >= '${minDate}'
         ) WHERE id > ${offset} AND id <= ${sliceEnd}`,
        (err) => (err ? reject(err) : resolve())
      );
    });
  } else if (!tableExists) {
    console.log(`Creating indexed table ${tableName}...`);
    await new Promise<void>((resolve, reject) => {
      db.exec(
        `CREATE TABLE ${tableName} AS
         SELECT ROW_NUMBER() OVER () as id, origin
         FROM '${parquetFile}' WHERE date >= '${minDate}'`,
        (err) => (err ? reject(err) : resolve())
      );
    });
  } else {
    console.log(`Using existing table ${tableName}`);
  }

  // Create progress table
  await new Promise<void>((resolve, reject) => {
    db.exec(
      `CREATE TABLE IF NOT EXISTS progress (table_name TEXT PRIMARY KEY, last_id INTEGER)`,
      (err) => (err ? reject(err) : resolve())
    );
  });

  // Use unique progress key for parallel instances
  const progressKey = isParallel ? `${tableName}_${offset}` : tableName;

  // Get total count from table
  const tableCount = await new Promise<number>((resolve, reject) => {
    db.all(`SELECT COUNT(*) as cnt FROM ${tableName}`, (err, rows: any[]) => {
      if (err) reject(err);
      else resolve(Number(rows[0].cnt));
    });
  });
  console.log(`Table ${tableName} has ${tableCount.toLocaleString()} URLs`);

  // Load saved progress
  const savedProgress = await new Promise<number>((resolve, reject) => {
    db.all(
      `SELECT last_id FROM progress WHERE table_name = '${progressKey}'`,
      (err, rows: any[]) => {
        if (err) reject(err);
        else resolve(rows.length > 0 ? Number(rows[0].last_id) : 0);
      }
    );
  });

  // Process in batches using indexed id (fast seek, no scan)
  const BATCH_SIZE = 50000;
  let currentId = Math.max(offset, savedProgress);
  if (savedProgress > offset) {
    console.log(`Resuming from saved progress: ${savedProgress.toLocaleString()}`);
  }
  const endId = limit ? offset + limit : tableCount;

  while (currentId < endId) {
    const batchLimit = Math.min(BATCH_SIZE, endId - currentId);
    const query = `SELECT origin FROM ${tableName} WHERE id > ${currentId} ORDER BY id LIMIT ${batchLimit}`;

    const urls = await new Promise<string[]>((resolve, reject) => {
      db.all(query, (err, rows: { origin: string }[]) => {
        if (err) reject(err);
        else resolve(rows.map((r) => r.origin));
      });
    });

    if (urls.length === 0) break;

    // Semaphore-style: queue of waiters for free slots
    let resolveWaiter: (() => void) | null = null;

    for (const url of urls) {
      // Wait if at capacity
      while (inFlight.size >= CONCURRENCY) {
        await new Promise<void>((resolve) => {
          resolveWaiter = resolve;
        });
      }

      // Start task and track it
      const task = processUrl(url)
        .catch((e) => console.error(`\n[FATAL] ${url}: ${e.message}`))
        .then(() => {
          inFlight.delete(task);
          if (resolveWaiter) {
            resolveWaiter();
            resolveWaiter = null;
          }
        });
      inFlight.add(task);
    }

    currentId += urls.length;

    // Save progress
    await new Promise<void>((resolve, reject) => {
      db.exec(
        `INSERT OR REPLACE INTO progress (table_name, last_id) VALUES ('${progressKey}', ${currentId})`,
        (err) => (err ? reject(err) : resolve())
      );
    });
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
