import { createReadStream } from "node:fs";
import { createInterface } from "node:readline";
import pMap from "p-map";
import { QdrantClient } from "@qdrant/js-client-rest";
import { embedBatch, EMBEDDING_DIM } from "./embed";

const DATA_PATH = "./data/readmes.jsonl";

interface GitHubReadme {
  content_hash: string;
  repo_name: string;
  content: string;
}

const BATCH_SIZE = 50;
const CONCURRENCY = 20;
const MAX_CONTENT_LEN = 16000;
const COST_PER_1M_TOKENS = 0.02; // OpenRouter Qwen3-Embedding-8B

const COLLECTION = "github_readmes_qwen";
const qdrant = new QdrantClient({ url: process.env.QDRANT_URL || "http://localhost:6333" });

let totalChars = 0;
let embedded = 0;

function sha1ToUuid(sha1: string): string {
  const h = sha1.slice(0, 32);
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20, 32)}`;
}

async function ensureCollection() {
  const collections = await qdrant.getCollections();
  const exists = collections.collections.some(c => c.name === COLLECTION);

  if (!exists) {
    await qdrant.createCollection(COLLECTION, {
      vectors: { size: EMBEDDING_DIM, distance: "Cosine" },
    });
    await qdrant.createPayloadIndex(COLLECTION, {
      field_name: "repo_name",
      field_schema: "keyword",
    });
    console.log(`Created collection: ${COLLECTION} (dim=${EMBEDDING_DIM})`);
  }
}

async function getCount() {
  const info = await qdrant.getCollection(COLLECTION);
  return info.points_count ?? 0;
}

async function processBatch(readmes: GitHubReadme[]) {
  const texts = readmes.map(r => r.content.slice(0, MAX_CONTENT_LEN));
  totalChars += texts.reduce((sum, t) => sum + t.length, 0);

  const vectors = await embedBatch(texts);

  await qdrant.upsert(COLLECTION, {
    wait: false,
    points: readmes.map((r, i) => ({
      id: sha1ToUuid(r.content_hash),
      vector: vectors[i]!,
      payload: { repo_name: r.repo_name, content: r.content.slice(0, MAX_CONTENT_LEN), content_hash: r.content_hash },
    })),
  });

  embedded += readmes.length;
}

async function ingest() {
  await ensureCollection();
  const startCount = await getCount();
  console.log(`Starting. Count: ${startCount} | Concurrency: ${CONCURRENCY} | Dim: ${EMBEDDING_DIM}`);

  const rl = createInterface({ input: createReadStream(DATA_PATH), crlfDelay: Infinity });

  const batches: GitHubReadme[][] = [];
  let batch: GitHubReadme[] = [];
  let skipped = 0;
  const start = Date.now();

  for await (const line of rl) {
    if (!line.trim()) continue;

    if (skipped < startCount) {
      skipped++;
      if (skipped % 10000 === 0) console.log(`Skipping... ${skipped.toLocaleString()}/${startCount}`);
      continue;
    }

    batch.push(JSON.parse(line));

    if (batch.length >= BATCH_SIZE) {
      batches.push(batch);
      batch = [];

      if (batches.length >= CONCURRENCY) {
        await pMap(batches.splice(0), processBatch, { concurrency: CONCURRENCY });
        const rate = (embedded / ((Date.now() - start) / 1000)).toFixed(1);
        const tokens = totalChars / 4;
        const cost = ((tokens / 1_000_000) * COST_PER_1M_TOKENS).toFixed(4);
        console.log(`${embedded.toLocaleString()} | ${rate}/s | ${(tokens / 1e6).toFixed(2)}M tok | $${cost}`);
      }
    }
  }

  if (batch.length) batches.push(batch);
  if (batches.length) await pMap(batches, processBatch, { concurrency: CONCURRENCY });

  const tokens = totalChars / 4;
  console.log(`Done! ${embedded.toLocaleString()} | Qdrant: ${await getCount()} | $${((tokens / 1_000_000) * COST_PER_1M_TOKENS).toFixed(4)}`);
}

ingest().catch(console.error);
