import { Glob } from "bun";
import { createHash } from "crypto";
import { QdrantClient } from "@qdrant/js-client-rest";
import pMap from "p-map";

// Config
const DATA_DIR = process.env.DATA_DIR || "/home/root/data";
const READMES_DIR = `${DATA_DIR}/readmes`;

// Multi-key support: use --keys=N flag (default 1)
const keysFlag = process.argv.find(a => a.startsWith("--keys="));
const numKeys = keysFlag ? parseInt(keysFlag.split("=")[1]) : 1;

const API_KEYS: string[] = [];
for (let i = 1; i <= numKeys; i++) {
  const envName = i === 1 ? "DEEPINFRA_API_KEY" : `DEEPINFRA_API_KEY_${i}`;
  const key = process.env[envName];
  if (!key) throw new Error(`${envName} required (using --keys=${numKeys})`);
  API_KEYS.push(key);
}
console.log(`Using ${API_KEYS.length} API key(s)`);

const EMBEDDING_DIM = 1536;
const BATCH_SIZE = 64;            // Items per DeepInfra request
const MAX_BATCH_CHARS = 120000;   // Max total chars per batch (avoid 500 errors)
const FILE_READERS = 16;          // Parallel file reads
const EMBED_WORKERS = 24;         // Parallel DeepInfra requests
const BUFFER_MAX = Math.floor(1.5 * EMBED_WORKERS * BATCH_SIZE);
const MAX_CONTENT_LEN = 16000;

const COLLECTION = "github_readmes_qwen";
const qdrant = new QdrantClient({ url: process.env.QDRANT_URL || "http://localhost:6333" });

// DeepInfra Qwen3-Embedding-8B pricing
const COST_PER_1M_TOKENS = 0.005;

interface ReadmeItem {
  id: string;
  repo_name: string;
  content: string;
  content_hash: string;
}

// === Bounded Buffer with Backpressure ===
class AsyncBuffer<T> {
  private items: T[] = [];
  private waitingProducers: (() => void)[] = [];
  private waitingConsumers: ((items: T[]) => void)[] = [];
  private done = false;

  constructor(private maxSize: number, private batchSize: number) {}

  async push(item: T): Promise<void> {
    while (this.items.length >= this.maxSize && !this.done) {
      await new Promise<void>(resolve => this.waitingProducers.push(resolve));
    }
    if (this.done) return;

    this.items.push(item);
    this.tryFlush();
  }

  async pull(): Promise<T[] | null> {
    while (this.items.length < this.batchSize && !this.done) {
      const batch = await new Promise<T[]>(resolve => this.waitingConsumers.push(resolve));
      if (batch.length > 0) return batch;
      if (this.done && this.items.length === 0) return null;
    }
    return this.drain();
  }

  private tryFlush(): void {
    if (this.items.length >= this.batchSize && this.waitingConsumers.length > 0) {
      const batch = this.items.splice(0, this.batchSize);
      const consumer = this.waitingConsumers.shift()!;
      consumer(batch);
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
    this.waitingConsumers.forEach(c => c([]));
    this.waitingConsumers = [];
    this.releaseProducers();
  }

  get pending(): number { return this.items.length; }
}

// === DeepInfra Batch Embedding ===
interface EmbedResponse {
  embeddings: number[][];
  input_tokens: number;
  inference_status: {
    runtime_ms: number;
    cost: number;
    tokens_input: number;
  };
}

let keyIndex = 0;
function getNextKey(): string {
  const key = API_KEYS[keyIndex % API_KEYS.length];
  keyIndex++;
  return key;
}

async function embedBatch(texts: string[], retries = 10): Promise<EmbedResponse> {
  const apiKey = getNextKey();
  const res = await fetch("https://api.deepinfra.com/v1/inference/Qwen/Qwen3-Embedding-8B-batch", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      inputs: texts,
      normalize: false,  // Qdrant Cosine handles this
      dimensions: EMBEDDING_DIM,
    }),
  });

  if (!res.ok) {
    const err = await res.text();
    const totalChars = texts.reduce((s, t) => s + t.length, 0);
    if (retries > 0 && (res.status >= 500 || res.status === 429)) {
      const delay = (11 - retries) * 2000;  // 2s, 4s, 6s... up to 20s
      console.log(`DeepInfra ${res.status} (${texts.length} items, ${totalChars} chars), retry in ${delay/1000}s... (${retries} left)`);
      await Bun.sleep(delay);
      return embedBatch(texts, retries - 1);
    }
    throw new Error(`DeepInfra error ${res.status} (${texts.length} items, ${totalChars} chars): ${err}`);
  }

  return res.json() as Promise<EmbedResponse>;
}

// === Helpers ===
function sha1ToUuid(sha1: string): string {
  const h = sha1.slice(0, 32);
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20, 32)}`;
}

function parseFilename(filename: string): { owner: string; repo: string } | null {
  const parts = filename.split("_");
  const branchIdx = parts.findIndex(p => p === "main" || p === "master" || p === "default");
  if (branchIdx < 2) return null;
  return { owner: parts[0], repo: parts.slice(1, branchIdx).join("_") };
}

async function ensureCollection(): Promise<void> {
  const { collections } = await qdrant.getCollections();
  if (!collections.some(c => c.name === COLLECTION)) {
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

// === Pipeline ===
let totalTokens = 0;
let totalCost = 0;
let embedded = 0;
let filesRead = 0;
let startTime = 0;

let skipped = 0;

async function producer(files: string[], buffer: AsyncBuffer<ReadmeItem>, existingIds: Set<string>): Promise<void> {
  await pMap(files, async (file) => {
    const parsed = parseFilename(file);
    if (!parsed) return;

    const content = await Bun.file(`${READMES_DIR}/${file}`).text();
    const content_hash = createHash("sha1").update(content).digest("hex");
    const id = sha1ToUuid(content_hash);

    if (existingIds.has(id)) {
      skipped++;
      filesRead++;
      return;
    }

    const trimmed = content.slice(0, MAX_CONTENT_LEN);
    await buffer.push({  // blocks when buffer full → backpressure
      id,
      repo_name: `${parsed.owner}/${parsed.repo}`,
      content: trimmed,
      content_hash,
    });
    filesRead++;
    if (filesRead % 100 === 0) {
      console.log(`[read] ${filesRead} files | skipped=${skipped} | buf=${buffer.pending}`);
    }
  }, { concurrency: FILE_READERS });

  console.log(`[read] done, ${filesRead} total, ${skipped} skipped`);
  buffer.finish();
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

async function consumer(buffer: AsyncBuffer<ReadmeItem>, workerId: number): Promise<void> {
  console.log(`[W${workerId}] consumer started`);
  while (true) {
    const batch = await buffer.pull();
    if (!batch || batch.length === 0) break;

    // Split by char limit to avoid 500 errors on large batches
    const subBatches = splitBatchByChars(batch, MAX_BATCH_CHARS);

    for (const subBatch of subBatches) {
      const totalChars = subBatch.reduce((s, r) => s + r.content.length, 0);
      console.log(`[W${workerId}] embedding ${subBatch.length} items (${(totalChars/1000).toFixed(0)}k chars)...`);
      const texts = subBatch.map(r => r.content);
      const response = await embedBatch(texts);
      console.log(`[W${workerId}] got ${response.embeddings.length} vectors (${response.inference_status.runtime_ms}ms), upserting...`);

      await qdrant.upsert(COLLECTION, {
        wait: false,
        points: subBatch.map((r, i) => ({
          id: r.id,
          vector: response.embeddings[i],
          payload: { repo_name: r.repo_name, content: r.content, content_hash: r.content_hash },
        })),
      });

      embedded += subBatch.length;
      totalTokens += response.input_tokens;
      totalCost += response.inference_status.cost;

      const elapsed = (Date.now() - startTime) / 1000;
      const itemsPerSec = (embedded / elapsed).toFixed(1);
      const tokPerSec = (totalTokens / elapsed).toFixed(0);
      console.log(`[W${workerId}] ${embedded}/${filesRead} | ${itemsPerSec} items/s | ${tokPerSec} tok/s | ${(totalTokens/1e6).toFixed(2)}M tok | $${totalCost.toFixed(4)}`);
    }
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

async function estimateCost(files: string[], existingIds: Set<string>): Promise<{ toEmbed: number; estTokens: number; estCost: number }> {
  // Sample files to estimate average tokens
  const SAMPLE_SIZE = Math.min(1000, files.length);
  const sampleIndices = new Set<number>();
  while (sampleIndices.size < SAMPLE_SIZE) {
    sampleIndices.add(Math.floor(Math.random() * files.length));
  }

  let totalChars = 0;
  let sampled = 0;
  let skipped = 0;

  for (const i of sampleIndices) {
    const file = files[i];
    const parsed = parseFilename(file);
    if (!parsed) continue;

    const content = await Bun.file(`${READMES_DIR}/${file}`).text();
    const content_hash = createHash("sha1").update(content).digest("hex");
    const id = sha1ToUuid(content_hash);

    if (existingIds.has(id)) {
      skipped++;
      continue;
    }

    totalChars += Math.min(content.length, MAX_CONTENT_LEN);
    sampled++;
  }

  if (sampled === 0) return { toEmbed: 0, estTokens: 0, estCost: 0 };

  const avgChars = totalChars / sampled;
  const skipRate = skipped / SAMPLE_SIZE;
  const toEmbed = Math.round(files.length * (1 - skipRate));
  const estTokens = Math.round((avgChars / 4) * toEmbed);  // ~4 chars per token
  const estCost = (estTokens / 1_000_000) * COST_PER_1M_TOKENS;

  return { toEmbed, estTokens, estCost };
}

async function main(): Promise<void> {
  await ensureCollection();

  console.log("Fetching existing IDs from Qdrant...");
  const existingIds = await fetchExistingIds();
  console.log(`Found ${existingIds.size} existing items in Qdrant`);

  const glob = new Glob("*");
  const files = await Array.fromAsync(glob.scan(READMES_DIR));
  console.log(`Found ${files.length} files | readers=${FILE_READERS} workers=${EMBED_WORKERS} batch=${BATCH_SIZE}`);

  // Estimate cost
  console.log("Estimating cost (sampling 1000 files)...");
  const { toEmbed, estTokens, estCost } = await estimateCost(files, existingIds);
  console.log(`Estimate: ${toEmbed.toLocaleString()} files to embed | ~${(estTokens/1e6).toFixed(1)}M tokens | ~$${estCost.toFixed(2)}`);
  console.log();

  const buffer = new AsyncBuffer<ReadmeItem>(BUFFER_MAX, BATCH_SIZE);
  startTime = Date.now();

  const producerTask = producer(files, buffer, existingIds);
  const consumerTasks = Array.from({ length: EMBED_WORKERS }, (_, i) => consumer(buffer, i));

  await Promise.all([producerTask, ...consumerTasks]);

  const info = await qdrant.getCollection(COLLECTION);
  const elapsed = (Date.now() - startTime) / 1000;
  console.log(`Done! ${embedded} items | ${(totalTokens/1e6).toFixed(2)}M tokens | $${totalCost.toFixed(4)} | ${elapsed.toFixed(1)}s | ${(embedded/elapsed).toFixed(1)} items/s`);
}

main().catch(console.error);
