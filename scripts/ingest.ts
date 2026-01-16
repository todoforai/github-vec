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

// Nebius batch chunking: --chunk=N (default 50000), --parallel=N (default 3)
const chunkFlag = process.argv.find((a) => a.startsWith("--chunk="));
const BATCH_CHUNK_SIZE = chunkFlag ? parseInt(chunkFlag.split("=")[1]) : 50000;
const parallelFlag = process.argv.find((a) => a.startsWith("--parallel="));
const BATCH_PARALLEL = parallelFlag ? parseInt(parallelFlag.split("=")[1]) : 3;

const COLLECTION = "github_readmes_qwen_4k";
const qdrant = new QdrantClient({ url: process.env.QDRANT_URL || "http://localhost:6333" });

// Batch state file to track in-flight batches
const BATCH_STATE_FILE = `${DATA_DIR}/batch-state.json`;
interface BatchState {
  [batchId: string]: { itemIds: string[]; createdAt: string };
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
  let processed = 0;

  await pMap(
    files,
    async (file) => {
      const parsed = parseFilename(file);
      if (!parsed) return;

      const content = await Bun.file(`${READMES_DIR}/${file}`).text();
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
        content: content.slice(0, MAX_CONTENT_LEN),
        content_hash,
      });

      if (processed % 1000 === 0) {
        console.log(`[load] ${processed}/${files.length} | ${items.length} new | ${existing} existing | ${duplicates} dupes`);
      }
    },
    { concurrency: FILE_READERS }
  );

  console.log(`[load] done: ${items.length} new, ${existing} existing, ${duplicates} dupes`);
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
            payload: { repo_name: r.repo_name, content: r.content, content_hash: r.content_hash },
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
async function resumePendingBatches(itemsMap: Map<string, ReadmeItem>): Promise<Set<string>> {
  const startTime = Date.now();
  let state = await loadBatchState();
  const batchIds = Object.keys(state);
  const processedIds = new Set<string>();

  if (batchIds.length === 0) {
    console.log("[nebius-batch] No pending batches in state file");
    return processedIds;
  }

  console.log(`[nebius-batch] Found ${batchIds.length} batch(es) in state file, checking status...`);

  // First pass: check all statuses, handle completed/failed, collect pending
  const pendingBatches: string[] = [];

  for (const batchId of batchIds) {
    const batchState = state[batchId];
    const status = await getBatchStatus(batchId, API_KEYS[0]);
    console.log(`[nebius-batch] ${batchId.slice(-8)}: ${status.status}`);

    if (status.status === "completed" && status.output_file_id) {
      // Download and upsert immediately
      console.log(`[nebius-batch] Downloading results for ${batchId.slice(-8)}...`);
      const { results, failed } = await downloadBatchResults(status.output_file_id, API_KEYS[0]);

      for (const f of failed) {
        const item = itemsMap.get(f.id);
        console.error(`[nebius-batch] FAILED ${item?.repo_name || f.id}: ${f.error}`);
      }
      if (failed.length > 0) {
        console.warn(`[nebius-batch] ${failed.length} items failed, ${results.size} succeeded`);
      }

      const toUpsert = [...results.entries()].filter(([id]) => itemsMap.has(id));
      for (let i = 0; i < toUpsert.length; i += 100) {
        const batch = toUpsert.slice(i, i + 100);
        await qdrant.upsert(COLLECTION, {
          wait: false,
          points: batch.map(([id, vector]) => {
            const item = itemsMap.get(id)!;
            return { id, vector, payload: { repo_name: item.repo_name, content: item.content, content_hash: item.content_hash } };
          }),
        });
      }

      console.log(`[nebius-batch] ${batchId.slice(-8)} done, upserted ${toUpsert.length} items`);
      batchState.itemIds.forEach((id) => processedIds.add(id));
      delete state[batchId];
      await saveBatchState(state);
    } else if (status.status === "in_progress" || status.status === "validating") {
      pendingBatches.push(batchId);
      batchState.itemIds.forEach((id) => processedIds.add(id));
    } else {
      console.log(`[nebius-batch] ${batchId.slice(-8)} ${status.status}, removing from state`);
      delete state[batchId];
      await saveBatchState(state);
    }
  }

  if (pendingBatches.length === 0) {
    return processedIds;
  }

  // Poll all pending batches in parallel
  console.log(`[nebius-batch] Polling ${pendingBatches.length} pending batch(es) in parallel...`);

  await pMap(
    pendingBatches,
    async (batchId) => {
      const batchState = state[batchId];
      const result = await pollBatchJob(batchId, API_KEYS[0], 30000, (id, completed) => {
        batchProgress.set(id, completed);
        const total = [...batchProgress.values()].reduce((a, b) => a + b, 0);
        const elapsed = (Date.now() - startTime) / 1000;
        console.log(`[progress] ${total.toLocaleString()} | ${id.slice(-8)} | ${completed} | ${(total / elapsed).toFixed(1)}/s | $${globalCost.toFixed(2)}`);
      });

      globalCost += result.cost;

      for (const f of result.failed) {
        const item = itemsMap.get(f.id);
        console.error(`[nebius-batch] FAILED ${item?.repo_name || f.id}: ${f.error}`);
      }
      if (result.failed.length > 0) {
        console.warn(`[nebius-batch] ${result.failed.length} items failed, ${result.results.size} succeeded`);
      }

      const toUpsert = [...result.results.entries()].filter(([id]) => itemsMap.has(id));
      for (let i = 0; i < toUpsert.length; i += 100) {
        const batch = toUpsert.slice(i, i + 100);
        await qdrant.upsert(COLLECTION, {
          wait: false,
          points: batch.map(([id, vector]) => {
            const item = itemsMap.get(id)!;
            return { id, vector, payload: { repo_name: item.repo_name, content: item.content, content_hash: item.content_hash } };
          }),
        });
      }

      console.log(`[nebius-batch] ${batchId.slice(-8)} done, upserted ${toUpsert.length} | $${result.cost.toFixed(4)}`);
      delete state[batchId];
      await saveBatchState(state);
    },
    { concurrency: BATCH_PARALLEL }
  );

  return processedIds;
}

// === Async batch pipeline (Nebius batch) ===
async function runBatchPipeline(items: ReadmeItem[]): Promise<void> {
  const startTime = Date.now();

  // Build items map for resumption
  const itemsMap = new Map(items.map((r) => [r.id, r]));

  // Resume any pending batches first
  const inFlightIds = await resumePendingBatches(itemsMap);
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
      const itemIds = chunk.map((r) => r.id);

      // Submit batch
      const batchId = await submitBatchJob(embedItems, API_KEYS[0]);

      // Save to state file
      state[batchId] = { itemIds, createdAt: new Date().toISOString() };
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

      // Upsert results to Qdrant
      console.log(`[C${chunkId}] Upserting ${result.results.size} vectors | $${result.cost.toFixed(4)}`);

      const itemsWithVectors = chunk.filter((r) => result.results.has(r.id));

      // Upsert in batches of 100 to stay under Qdrant 32MB payload limit
      for (let i = 0; i < itemsWithVectors.length; i += 100) {
        const batch = itemsWithVectors.slice(i, i + 100);
        await qdrant.upsert(COLLECTION, {
          wait: false,
          points: batch.map((r) => ({
            id: r.id,
            vector: result.results.get(r.id)!,
            payload: { repo_name: r.repo_name, content: r.content, content_hash: r.content_hash },
          })),
        });
      }

      // Remove from state file
      delete state[batchId];
      await saveBatchState(state);

      // Update totals
      totalTokens += result.tokens;
      totalCost += result.cost;
      totalEmbedded += result.results.size;
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

  // Load items
  const items = await loadItems(files, existingIds);
  if (items.length === 0) {
    console.log("Nothing to embed!");
    return;
  }

  // Estimate cost
  const avgChars = items.reduce((s, r) => s + r.content.length, 0) / items.length;
  const estTokens = Math.round((avgChars / 4) * items.length);
  const estCost = (estTokens / 1_000_000) * PRICING[PROVIDER];
  console.log(`Estimate: ${items.length} items | ~${(estTokens / 1e6).toFixed(1)}M tokens | ~$${estCost.toFixed(2)}`);
  console.log();

  // Run appropriate pipeline
  if (PROVIDER === "nebius-batch") {
    await runBatchPipeline(items);
  } else {
    await runRealtimePipeline(items);
  }
}

main().catch(console.error);
