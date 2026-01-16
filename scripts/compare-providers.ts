import { embedRealtime, embedBatchAsync, EMBEDDING_DIM } from "./embed";

const DEEPINFRA_KEY = process.env.DEEPINFRA_API_KEY!;
const NEBIUS_KEY = process.env.NEBIUS_API_KEY!;
const TEST_BATCH = process.argv.includes("--batch");

const TEXTS = [
  "A simple React component for displaying user profiles",
  "Machine learning library for natural language processing in Python",
  "Fast and lightweight web server written in Rust",
];

const cosine = (a: number[], b: number[]) => {
  let dot = 0, nA = 0, nB = 0;
  for (let i = 0; i < a.length; i++) { dot += a[i] * b[i]; nA += a[i] ** 2; nB += b[i] ** 2; }
  return dot / (Math.sqrt(nA) * Math.sqrt(nB));
};

const norm = (v: number[]) => Math.sqrt(v.reduce((s, x) => s + x * x, 0));

console.log("Fetching DeepInfra...");
const di = await embedRealtime(TEXTS, "deepinfra", DEEPINFRA_KEY);

console.log("Fetching Nebius...");
const nb = await embedRealtime(TEXTS, "nebius", NEBIUS_KEY);

console.log("\n=== DeepInfra vs Nebius ===\n");
for (let i = 0; i < TEXTS.length; i++) {
  const sim = cosine(di.embeddings[i], nb.embeddings[i]);
  console.log(`Text ${i + 1}: cosine=${sim.toFixed(6)} | DI norm=${norm(di.embeddings[i]).toFixed(4)} | NB norm=${norm(nb.embeddings[i]).toFixed(4)}`);
}

if (TEST_BATCH) {
  console.log("\n=== Nebius Batch Async ===\n");
  const items = TEXTS.map((text, i) => ({ id: `t${i}`, text }));
  const batch = await embedBatchAsync(items, NEBIUS_KEY, EMBEDDING_DIM, 5000);

  console.log("\n=== Batch vs Realtime ===\n");
  for (let i = 0; i < TEXTS.length; i++) {
    const bVec = batch.results.get(`t${i}`)!;
    const sim = cosine(nb.embeddings[i], bVec);
    console.log(`Text ${i + 1}: cosine=${sim.toFixed(6)} ${sim > 0.9999 ? "✓" : ""}`);
  }
}
