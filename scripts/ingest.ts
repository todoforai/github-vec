import { Glob } from "bun";
import { createHash } from "crypto";
import { QdrantClient } from "@qdrant/js-client-rest";
import pMap from "p-map";
import {
  embedRealtime,
  submitBatchJob,
  pollBatchJob,
  EMBEDDING_DIM,
  listBatches,
  getBatchStatus,
  downloadBatchResults,
  BudgetExhaustedError,
  type Provider,
  type EmbedItem,
  type NebiusBatchStatus,
} from "./embed";

// === Config ===
const DATA_DIR = process.env.DATA_DIR || "/home/root/data";
const READMES_DIR = `${DATA_DIR}/readmes`;

// Provider selection: --provider=deepinfra|nebius|nebius-batch
const providerFlag = process.argv.find((a) => a.startsWith("--provider="));
const PROVIDER = (providerFlag?.split("=")[1] || process.env.EMBED_PROVIDER || "deepinfra") as Provider;

// Multi-key support: --keys=N (default 1)
const keysFlag = process.argv.find((a) => a.startsWith("--keys="));
const numKeys = keysFlag ? parseInt(keysFlag.split("=")[1]) : 1;

const KEY_ENV_PREFIX = PROVIDER.startsWith("nebius") ? "NEBIUS_API_KEY" : "DEEPINFRA_API_KEY";
const API_KEYS: string[] = [];
for (let i = 1; i <= numKeys; i++) {
  const envName = i === 1 ? KEY_ENV_PREFIX : `${KEY_ENV_PREFIX}_${i}`;
  const key = process.env[envName];
  if (!key) throw new Error(`${envName} required (using --keys=${numKeys})`);
  API_KEYS.push(key);
}

// Tuning params
const BATCH_SIZE = 64;
const MAX_BATCH_CHARS = 120000;
const FILE_READERS = 16;
const EMBED_WORKERS = 48;
const BUFFER_MAX = Math.floor(1.5 * EMBED_WORKERS * BATCH_SIZE);
const MAX_CONTENT_LEN = 16000;

// Nebius batch chunking: --chunk=N (default 25000), --parallel=N (default 3)
const chunkFlag = process.argv.find((a) => a.startsWith("--chunk="));
const BATCH_CHUNK_SIZE = chunkFlag ? parseInt(chunkFlag.split("=")[1]) : 25000;
const parallelFlag = process.argv.find((a) => a.startsWith("--parallel="));
const BATCH_PARALLEL = parallelFlag ? parseInt(parallelFlag.split("=")[1]) : 3;

// File loading chunk size - process files in batches to limit RAM usage
// At least 2x the batch pipeline capacity to keep workers fed
const FILE_CHUNK_SIZE = BATCH_CHUNK_SIZE * BATCH_PARALLEL * 2;

const COLLECTION = "github_readmes_qwen_4k";
const qdrant = new QdrantClient({ url: process.env.QDRANT_URL || "http://localhost:6333" });

// Batch state file to track in-flight batches
const BATCH_STATE_FILE = `${DATA_DIR}/batch-state.json`;
interface BatchItemMeta {
  id: string;
  repo_name: string;
  content_hash: string;
}
interface BatchStateEntry {
  items: BatchItemMeta[];
  createdAt: string;
  // Legacy format (will be migrated)
  itemIds?: string[];
}
interface BatchState {
  [batchId: string]: BatchStateEntry;
}

async function loadBatchState(): Promise<BatchState> {
  try {
    const file = Bun.file(BATCH_STATE_FILE);
    if (await file.exists()) return file.json();
  } catch {}
  return {};
}

async function saveBatchState(state: BatchState): Promise<void> {
  await Bun.write(BATCH_STATE_FILE, JSON.stringify(state, null, 2));
}

// Shared function to upsert batch results and manage state
interface ProcessBatchParams {
  batchId: string;
  results: Map<string, number[]>;
  items: { id: string; repo_name: string; content_hash: string }[];
  state: BatchState;
  logPrefix: string;
}

async function processBatchResults({ batchId, results, items, state, logPrefix }: ProcessBatchParams): Promise<number> {
  const itemsMap = new Map(items.map((item) => [item.id, item]));

  // Upsert all results (no content stored in Qdrant)
  const toUpsert = [...results.entries()].filter(([id]) => itemsMap.has(id));
  for (let i = 0; i < toUpsert.length; i += 100) {
    const batch = toUpsert.slice(i, i + 100);
    await qdrant.upsert(COLLECTION, {
      wait: false,
      points: batch.map(([id, vector]) => {
        const item = itemsMap.get(id)!;
        return { id, vector, payload: { repo_name: item.repo_name, content_hash: item.content_hash } };
      }),
    });
  }

  // Remove from state if success rate >= 99% OR batch is small (not worth retrying)
  const successRate = toUpsert.length / items.length;
  const isSmallBatch = items.length < 50;
  if (successRate >= 0.99 || isSmallBatch) {
    delete state[batchId];
    await saveBatchState(state);
    if (isSmallBatch && successRate < 0.99) {
      console.log(`${logPrefix} small batch (${items.length} items), removing despite ${(successRate * 100).toFixed(1)}% success`);
    }
  } else {
    console.warn(`${logPrefix} keeping in state (${(successRate * 100).toFixed(1)}% success, need 99%)`);
  }

  return toUpsert.length;
}

// Pricing
const PRICING: Record<Provider, number> = {
  deepinfra: 0.005,
  nebius: 0.01,
  "nebius-batch": 0.005,
};

// Global progress tracking: batchId -> completed count
const batchProgress = new Map<string, number>();
let globalCost = 0;

console.log(`Provider: ${PROVIDER} | Keys: ${API_KEYS.length}${PROVIDER === "nebius-batch" ? ` | Chunk: ${BATCH_CHUNK_SIZE} | Parallel: ${BATCH_PARALLEL}` : ""}`);

// === Types ===
interface ReadmeItem {
  id: string;
  repo_name: string;
  content: string;
  content_hash: string;
}

// === Helpers ===
function sha1ToUuid(sha1: string): string {
  const h = sha1.slice(0, 32);
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20, 32)}`;
}

function parseFilename(filename: string): { owner: string; repo: string } | null {
  const parts = filename.split("_");
  const branchIdx = parts.findIndex((p) => p === "main" || p === "master" || p === "default");
  if (branchIdx < 2) return null;
  return { owner: parts[0], repo: parts.slice(1, branchIdx).join("_") };
}

async function ensureCollection(): Promise<void> {
  const { collections } = await qdrant.getCollections();
  if (!collections.some((c) => c.name === COLLECTION)) {
    await qdrant.createCollection(COLLECTION, {
      vectors: { size: EMBEDDING_DIM, distance: "Cosine" },
    });
    await qdrant.createPayloadIndex(COLLECTION, {
      field_name: "repo_name",
      field_schema: "keyword",
    });
    console.log(`Created collection: ${COLLECTION}`);
  }
}

async function fetchExistingIds(): Promise<Set<string>> {
  const ids = new Set<string>();
  let offset: string | undefined;
  while (true) {
    const result = await qdrant.scroll(COLLECTION, {
      limit: 1000,
      offset,
      with_payload: false,
      with_vector: false,
    });
    for (const point of result.points) {
      ids.add(point.id as string);
    }
    if (!result.next_page_offset) break;
    offset = result.next_page_offset as string;
  }
  return ids;
}

// === Load all items to embed ===
async function loadItems(files: string[], existingIds: Set<string>): Promise<ReadmeItem[]> {
  const items: ReadmeItem[] = [];
  const seenIds = new Set<string>();
  let existing = 0;
  let duplicates = 0;
  let empty = 0;
  let processed = 0;

  await pMap(
    files,
    async (file) => {
      const parsed = parseFilename(file);
      if (!parsed) return;

      const content = await Bun.file(`${READMES_DIR}/${file}`).text();

      // Skip empty or whitespace-only content (causes TextEncodeInput errors)
      const trimmed = content.trim();
      if (!trimmed || trimmed.length < 10) {
        empty++;
        processed++;
        return;
      }

      const content_hash = createHash("sha1").update(content).digest("hex");
      const id = sha1ToUuid(content_hash);

      processed++;
      if (existingIds.has(id)) {
        existing++;
        return;
      }
      if (seenIds.has(id)) {
        duplicates++;
        return;
      }
      seenIds.add(id);

      items.push({
        id,
        repo_name: `${parsed.owner}/${parsed.repo}`,
        content: trimmed.slice(0, MAX_CONTENT_LEN),
        content_hash,
      });

      if (processed % 1000 === 0) {
        console.log(`[load] ${processed}/${files.length} | ${items.length} new | ${existing} existing | ${duplicates} dupes | ${empty} empty`);
      }
    },
    { concurrency: FILE_READERS }
  );

  console.log(`[load] done: ${items.length} new, ${existing} existing, ${duplicates} dupes, ${empty} empty`);
  return items;
}

// === Real-time streaming pipeline (DeepInfra, Nebius real-time) ===
class AsyncBuffer<T> {
  private items: T[] = [];
  private waitingProducers: (() => void)[] = [];
  private waitingConsumers: ((items: T[]) => void)[] = [];
  private done = false;

  constructor(private maxSize: number, private batchSize: number) {}

  async push(item: T): Promise<void> {
    while (this.items.length >= this.maxSize && !this.done) {
      await new Promise<void>((resolve) => this.waitingProducers.push(resolve));
    }
    if (this.done) return;
    this.items.push(item);
    this.tryFlush();
  }

  async pull(): Promise<T[] | null> {
    while (this.items.length < this.batchSize && !this.done) {
      const batch = await new Promise<T[]>((resolve) => this.waitingConsumers.push(resolve));
      if (batch.length > 0) return batch;
      if (this.done && this.items.length === 0) return null;
    }
    return this.drain();
  }

  private tryFlush(): void {
    if (this.items.length >= this.batchSize && this.waitingConsumers.length > 0) {
      const batch = this.items.splice(0, this.batchSize);
      this.waitingConsumers.shift()!(batch);
      this.releaseProducers();
    }
  }

  private drain(): T[] | null {
    if (this.items.length === 0) return this.done ? null : [];
    const batch = this.items.splice(0, this.batchSize);
    this.releaseProducers();
    return batch;
  }

  private releaseProducers(): void {
    while (this.waitingProducers.length > 0 && this.items.length < this.maxSize) {
      this.waitingProducers.shift()!();
    }
  }

  finish(): void {
    this.done = true;
    this.waitingConsumers.forEach((c) => c([]));
    this.waitingConsumers = [];
    this.releaseProducers();
  }

  get pending(): number {
    return this.items.length;
  }
}

function splitBatchByChars(batch: ReadmeItem[], maxChars: number): ReadmeItem[][] {
  const result: ReadmeItem[][] = [];
  let current: ReadmeItem[] = [];
  let currentChars = 0;
  for (const item of batch) {
    if (currentChars + item.content.length > maxChars && current.length > 0) {
      result.push(current);
      current = [];
      currentChars = 0;
    }
    current.push(item);
    currentChars += item.content.length;
  }
  if (current.length > 0) result.push(current);
  return result;
}

let keyIndex = 0;
function getNextKey(): string {
  const key = API_KEYS[keyIndex % API_KEYS.length];
  keyIndex++;
  return key;
}

async function runRealtimePipeline(items: ReadmeItem[]): Promise<void> {
  const provider = PROVIDER as "deepinfra" | "nebius";
  let totalTokens = 0;
  let totalCost = 0;
  let embedded = 0;
  const startTime = Date.now();

  const buffer = new AsyncBuffer<ReadmeItem>(BUFFER_MAX, BATCH_SIZE);

  // Producer: feed items into buffer
  const producerTask = (async () => {
    for (const item of items) {
      await buffer.push(item);
    }
    buffer.finish();
  })();

  // Consumer workers
  const consumer = async (workerId: number) => {
    while (true) {
      const batch = await buffer.pull();
      if (!batch || batch.length === 0) break;

      const subBatches = splitBatchByChars(batch, MAX_BATCH_CHARS);
      for (const subBatch of subBatches) {
        const totalChars = subBatch.reduce((s, r) => s + r.content.length, 0);
        console.log(`[W${workerId}] embedding ${subBatch.length} items (${(totalChars / 1000).toFixed(0)}k chars)...`);

        const result = await embedRealtime(
          subBatch.map((r) => r.content),
          provider,
          getNextKey()
        );

        await qdrant.upsert(COLLECTION, {
          wait: false,
          points: subBatch.map((r, i) => ({
            id: r.id,
            vector: result.embeddings[i],
            payload: { repo_name: r.repo_name, content_hash: r.content_hash },
          })),
        });

        embedded += subBatch.length;
        totalTokens += result.tokens;
        totalCost += result.cost;

        const elapsed = (Date.now() - startTime) / 1000;
        console.log(
          `[W${workerId}] ${embedded}/${items.length} | ${(embedded / elapsed).toFixed(1)} items/s | ${(totalTokens / 1e6).toFixed(2)}M tok | $${totalCost.toFixed(4)}`
        );
      }
    }
  };

  const consumerTasks = Array.from({ length: EMBED_WORKERS }, (_, i) => consumer(i));
  await Promise.all([producerTask, ...consumerTasks]);

  const elapsed = (Date.now() - startTime) / 1000;
  console.log(`Done! ${embedded} items | ${(totalTokens / 1e6).toFixed(2)}M tokens | $${totalCost.toFixed(4)} | ${elapsed.toFixed(1)}s`);
}

// === Resume pending batches from state file ===
async function resumePendingBatches(): Promise<Set<string>> {
  const startTime = Date.now();
  let state = await loadBatchState();
  const batchIds = Object.keys(state);
  const processedIds = new Set<string>();

  if (batchIds.length === 0) {
    console.log("[nebius-batch] No pending batches in state file");
    return processedIds;
  }

  console.log(`[nebius-batch] Found ${batchIds.length} batch(es) in state file, checking status...`);

  // First pass: check all statuses, categorize batches
  const toProcess: { batchId: string; fileId?: string }[] = [];

  for (const batchId of batchIds) {
    const batchState = state[batchId];

    // Skip legacy format entries (no item metadata to upsert)
    if (!batchState.items || batchState.items.length === 0) {
      if (batchState.itemIds) {
        console.log(`[nebius-batch] ${batchId.slice(-8)}: legacy format (no metadata), removing`);
        delete state[batchId];
        await saveBatchState(state);
      }
      continue;
    }

    // Old format entries with content can still be processed - we just won't store content in Qdrant
    // The state has all we need: id, repo_name, content_hash

    const status = await getBatchStatus(batchId, API_KEYS[0]);
    console.log(`[nebius-batch] ${batchId.slice(-8)}: ${status.status}`);

    if (status.status === "completed" && status.output_file_id) {
      toProcess.push({ batchId, fileId: status.output_file_id });
      batchState.items.forEach((item) => processedIds.add(item.id));
    } else if (status.status === "in_progress" || status.status === "validating") {
      toProcess.push({ batchId }); // No fileId = needs polling
      batchState.items.forEach((item) => processedIds.add(item.id));
    } else {
      console.log(`[nebius-batch] ${batchId.slice(-8)} ${status.status}, removing from state`);
      delete state[batchId];
      await saveBatchState(state);
    }
  }

  if (toProcess.length === 0) {
    return processedIds;
  }

  // Process all batches in parallel (download completed, poll pending)
  console.log(`[nebius-batch] Processing ${toProcess.length} batch(es) in parallel...`);

  await pMap(
    toProcess,
    async ({ batchId, fileId }) => {
      let results: Map<string, number[]>;
      let failed: { id: string; error: string }[];
      let cost = 0;

      // Build itemsMap from stored batch state
      const batchState = state[batchId];
      const itemsMap = new Map(batchState.items.map((item) => [item.id, item]));

      if (fileId) {
        // Already completed - just download
        console.log(`[nebius-batch] Downloading ${batchId.slice(-8)}...`);
        const downloaded = await downloadBatchResults(fileId, API_KEYS[0]);
        results = downloaded.results;
        failed = downloaded.failed;
      } else {
        // Still pending - poll then download
        const result = await pollBatchJob(batchId, API_KEYS[0], 30000, (id, completed) => {
          batchProgress.set(id, completed);
          const total = [...batchProgress.values()].reduce((a, b) => a + b, 0);
          const elapsed = (Date.now() - startTime) / 1000;
          console.log(`[progress] ${total.toLocaleString()} | ${id.slice(-8)} | ${completed} | ${(total / elapsed).toFixed(1)}/s | $${globalCost.toFixed(2)}`);
        });
        results = result.results;
        failed = result.failed;
        cost = result.cost;
        globalCost += cost;
      }

      for (const f of failed) {
        const item = itemsMap.get(f.id);
        console.error(`[nebius-batch] FAILED ${item?.repo_name || f.id}: ${f.error}`);
      }
      if (failed.length > 0) {
        console.warn(`[nebius-batch] ${failed.length} items failed, ${results.size} succeeded`);
      }

      const upserted = await processBatchResults({
        batchId,
        results,
        items: batchState.items,
        state,
        logPrefix: `[nebius-batch] ${batchId.slice(-8)}`,
      });

      console.log(`[nebius-batch] ${batchId.slice(-8)} done, upserted ${upserted}${cost ? ` | $${cost.toFixed(4)}` : ""}`);
    },
    { concurrency: BATCH_PARALLEL }
  );

  return processedIds;
}

// === Async batch pipeline (Nebius batch) ===
async function runBatchPipeline(items: ReadmeItem[]): Promise<void> {
  const startTime = Date.now();

  // Resume any pending batches first (uses metadata from state file)
  const inFlightIds = await resumePendingBatches();
  if (inFlightIds.size > 0) {
    console.log(`[nebius-batch] ${inFlightIds.size} items already in-flight or processed`);
  }

  // Refresh existing IDs from Qdrant
  const existingIds = await fetchExistingIds();

  // Filter out items already in Qdrant or in-flight
  const remainingItems = items.filter((r) => !existingIds.has(r.id) && !inFlightIds.has(r.id));

  if (remainingItems.length === 0) {
    console.log("[nebius-batch] All items already embedded or in-flight!");
    return;
  }

  // Split into chunks
  const chunks: ReadmeItem[][] = [];
  for (let i = 0; i < remainingItems.length; i += BATCH_CHUNK_SIZE) {
    chunks.push(remainingItems.slice(i, i + BATCH_CHUNK_SIZE));
  }

  console.log(`[nebius-batch] Processing ${remainingItems.length} items in ${chunks.length} chunk(s) of ${BATCH_CHUNK_SIZE} (${BATCH_PARALLEL} parallel)`);

  // Track progress across all chunks
  let totalTokens = 0;
  let totalCost = 0;
  let totalEmbedded = 0;
  let chunksCompleted = 0;

  // Load current state
  let state = await loadBatchState();

  // Process chunks in parallel with concurrency limit
  await pMap(
    chunks.map((chunk, idx) => ({ chunk, idx })),
    async ({ chunk, idx }) => {
      const chunkId = idx + 1;
      console.log(`[C${chunkId}] Starting chunk (${chunk.length} items)...`);

      // Convert to EmbedItem format
      const embedItems: EmbedItem[] = chunk.map((r) => ({ id: r.id, text: r.content }));

      // Submit batch
      const batchId = await submitBatchJob(embedItems, API_KEYS[0]);

      // Save to state file with metadata for resume (no content to avoid OOM)
      state[batchId] = {
        items: chunk.map((r) => ({ id: r.id, repo_name: r.repo_name, content_hash: r.content_hash })),
        createdAt: new Date().toISOString(),
      };
      await saveBatchState(state);
      console.log(`[C${chunkId}] Batch ${batchId} submitted and saved to state`);

      // Poll until complete
      const result = await pollBatchJob(batchId, API_KEYS[0], 30000, (id, completed) => {
        batchProgress.set(id, completed);
        const total = [...batchProgress.values()].reduce((a, b) => a + b, 0);
        const elapsed = (Date.now() - startTime) / 1000;
        console.log(`[progress] ${total.toLocaleString()} | ${id.slice(-8)} | ${completed} | ${(total / elapsed).toFixed(1)}/s | $${globalCost.toFixed(2)}`);
      });

      globalCost += result.cost;

      // Log failed items with repo names
      const chunkMap = new Map(chunk.map((r) => [r.id, r]));
      for (const f of result.failed) {
        const item = chunkMap.get(f.id);
        console.error(`[C${chunkId}] FAILED ${item?.repo_name || f.id}: ${f.error}`);
      }
      if (result.failed.length > 0) {
        console.warn(`[C${chunkId}] ${result.failed.length} items failed, ${result.results.size} succeeded`);
      }

      // Upsert results to Qdrant and manage state
      console.log(`[C${chunkId}] Upserting ${result.results.size} vectors | $${result.cost.toFixed(4)}`);

      const upserted = await processBatchResults({
        batchId,
        results: result.results,
        items: chunk,
        state,
        logPrefix: `[C${chunkId}]`,
      });

      // Update totals
      totalTokens += result.tokens;
      totalCost += result.cost;
      totalEmbedded += upserted;
      chunksCompleted++;

      const elapsed = (Date.now() - startTime) / 1000;
      console.log(
        `[C${chunkId}] Done | Progress: ${chunksCompleted}/${chunks.length} chunks | ${totalEmbedded}/${remainingItems.length} items | ${(totalTokens / 1e6).toFixed(2)}M tok | $${totalCost.toFixed(4)} | ${elapsed.toFixed(0)}s`
      );
    },
    { concurrency: BATCH_PARALLEL }
  );

  const elapsed = (Date.now() - startTime) / 1000;
  console.log(`\nDone! ${totalEmbedded} items | ${(totalTokens / 1e6).toFixed(2)}M tokens | $${totalCost.toFixed(4)} | ${elapsed.toFixed(1)}s`);
}

// === Main ===
async function main(): Promise<void> {
  await ensureCollection();

  console.log("Fetching existing IDs from Qdrant...");
  const existingIds = await fetchExistingIds();
  console.log(`Found ${existingIds.size} existing items in Qdrant`);

  const glob = new Glob("*");
  const files = await Array.fromAsync(glob.scan(READMES_DIR));
  console.log(`Found ${files.length} files`);
  console.log(`Processing in chunks of ${FILE_CHUNK_SIZE} files to limit RAM usage\n`);

  let totalProcessed = 0;
  let totalNew = 0;

  // Process files in chunks to limit RAM usage
  for (let i = 0; i < files.length; i += FILE_CHUNK_SIZE) {
    const chunkFiles = files.slice(i, i + FILE_CHUNK_SIZE);
    const chunkNum = Math.floor(i / FILE_CHUNK_SIZE) + 1;
    const totalChunks = Math.ceil(files.length / FILE_CHUNK_SIZE);

    console.log(`\n=== File chunk ${chunkNum}/${totalChunks}: files ${i + 1}-${i + chunkFiles.length} of ${files.length} ===`);

    // Load items for this chunk
    const items = await loadItems(chunkFiles, existingIds);
    totalProcessed += chunkFiles.length;

    if (items.length === 0) {
      console.log(`[chunk ${chunkNum}] No new items to embed, skipping`);
      continue;
    }

    totalNew += items.length;

    // Estimate cost for this chunk
    const avgChars = items.reduce((s, r) => s + r.content.length, 0) / items.length;
    const estTokens = Math.round((avgChars / 4) * items.length);
    const estCost = (estTokens / 1_000_000) * PRICING[PROVIDER];
    console.log(`[chunk ${chunkNum}] ${items.length} items | ~${(estTokens / 1e6).toFixed(1)}M tokens | ~$${estCost.toFixed(2)}`);

    // Run appropriate pipeline
    if (PROVIDER === "nebius-batch") {
      await runBatchPipeline(items);
    } else {
      await runRealtimePipeline(items);
    }

    // Add processed IDs to existingIds to avoid reprocessing in subsequent chunks
    for (const item of items) {
      existingIds.add(item.id);
    }

    console.log(`[chunk ${chunkNum}] Complete. Total progress: ${totalNew} new items from ${totalProcessed}/${files.length} files`);
  }

  console.log(`\n=== All done! Processed ${totalNew} new items from ${files.length} files ===`);
}

main().catch((err) => {
  if (err instanceof BudgetExhaustedError) {
    console.log("\n[!] Budget exhausted. State saved - will resume on next run.");
    console.log("    Add funds and re-run the script to continue.");
    process.exit(0);
  }
  console.error(err);
  process.exit(1);
});
