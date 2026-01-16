import { unlinkSync } from "fs";

// === Errors ===
export class BudgetExhaustedError extends Error {
  constructor(message: string = "Budget exhausted") {
    super(message);
    this.name = "BudgetExhaustedError";
  }
}

// === Types ===
export interface EmbedResult {
  embeddings: number[][];
  tokens: number;
  cost: number;
}

export interface EmbedItem {
  id: string;
  text: string;
}

export interface BatchEmbedResult {
  results: Map<string, number[]>; // id -> embedding
  failed: { id: string; error: string }[];
  tokens: number;
  cost: number;
}

export type Provider = "deepinfra" | "nebius" | "nebius-batch";

// === Config ===
export const EMBEDDING_DIM = 4096;

const PRICING: Record<Provider, number> = {
  deepinfra: 0.005,      // $0.005/1M tokens (batch endpoint)
  nebius: 0.01,          // $0.01/1M tokens (real-time)
  "nebius-batch": 0.005, // $0.005/1M tokens (50% off async)
};

const ENDPOINTS = {
  deepinfra: "https://api.deepinfra.com/v1/inference/Qwen/Qwen3-Embedding-8B-batch",
  nebius: "https://api.studio.nebius.com/v1/embeddings",
  "nebius-batch": "https://api.studio.nebius.com/v1",
};

const MODEL = "Qwen/Qwen3-Embedding-8B";

// === Real-time Embedding (DeepInfra & Nebius) ===
export async function embedRealtime(
  texts: string[],
  provider: "deepinfra" | "nebius",
  apiKey: string,
  dimensions: number = EMBEDDING_DIM,
  retries: number = 10
): Promise<EmbedResult> {
  const isNebius = provider === "nebius";
  const url = ENDPOINTS[provider];

  const body = isNebius
    ? { model: MODEL, input: texts, dimensions }
    : { inputs: texts, normalize: false, dimensions };

  const res = await fetch(url, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  });

  if (!res.ok) {
    const err = await res.text();
    const totalChars = texts.reduce((s, t) => s + t.length, 0);
    if (retries > 0 && (res.status >= 500 || res.status === 429)) {
      const delay = (11 - retries) * 2000;
      console.log(`${provider} ${res.status} (${texts.length} items, ${totalChars} chars), retry in ${delay / 1000}s...`);
      await Bun.sleep(delay);
      return embedRealtime(texts, provider, apiKey, dimensions, retries - 1);
    }
    throw new Error(`${provider} error ${res.status}: ${err}`);
  }

  const json = await res.json();

  if (isNebius) {
    const data = json as { data: { embedding: number[]; index: number }[]; usage: { prompt_tokens: number } };
    const embeddings = data.data.sort((a, b) => a.index - b.index).map((d) => d.embedding);
    const tokens = data.usage.prompt_tokens;
    return { embeddings, tokens, cost: (tokens / 1_000_000) * PRICING[provider] };
  } else {
    const data = json as { embeddings: number[][]; input_tokens: number; inference_status: { cost: number } };
    return { embeddings: data.embeddings, tokens: data.input_tokens, cost: data.inference_status.cost };
  }
}

// === Nebius Async Batch Embedding ===
export interface NebiusBatchStatus {
  id: string;
  status: "validating" | "in_progress" | "completed" | "failed" | "expired" | "cancelled";
  output_file_id?: string;
  error_file_id?: string;
  request_counts?: { completed: number; failed: number; total: number };
}

// Nebius batch result row (JSONL line format)
interface NebiusBatchResultRow {
  id: string;
  custom_id: string;
  response: {
    data: { embedding: number[] }[];
    usage?: { prompt_tokens: number };
  };
}

async function nebiusRequest(path: string, apiKey: string, options: RequestInit = {}): Promise<Response> {
  return fetch(`${ENDPOINTS["nebius-batch"]}${path}`, {
    ...options,
    headers: {
      Authorization: `Bearer ${apiKey}`,
      ...options.headers,
    },
  });
}

async function uploadBatchFile(items: EmbedItem[], apiKey: string, dimensions: number): Promise<string> {
  const lines = items.map((item) =>
    JSON.stringify({
      custom_id: item.id,
      method: "POST",
      url: "/v1/embeddings",
      body: { model: MODEL, input: item.text, dimensions },
    })
  );
  const content = lines.join("\n");

  const tmpPath = `/tmp/nebius-batch-${Date.now()}.jsonl`;
  await Bun.write(tmpPath, content);

  const formData = new FormData();
  formData.append("file", Bun.file(tmpPath));
  formData.append("purpose", "batch");

  const res = await nebiusRequest("/files", apiKey, {
    method: "POST",
    body: formData,
  });

  unlinkSync(tmpPath);

  if (!res.ok) {
    throw new Error(`Failed to upload batch file: ${res.status} ${await res.text()}`);
  }

  const data = (await res.json()) as { id: string };
  return data.id;
}

async function createBatch(fileId: string, apiKey: string): Promise<string> {
  const res = await nebiusRequest("/batches", apiKey, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      input_file_id: fileId,
      endpoint: "/v1/embeddings",
      completion_window: "24h",
    }),
  });

  if (!res.ok) {
    throw new Error(`Failed to create batch: ${res.status} ${await res.text()}`);
  }

  const data = (await res.json()) as { id: string };
  return data.id;
}

export async function getBatchStatus(batchId: string, apiKey: string): Promise<NebiusBatchStatus> {
  const res = await nebiusRequest(`/batches/${batchId}`, apiKey);
  if (!res.ok) {
    throw new Error(`Failed to get batch status: ${res.status} ${await res.text()}`);
  }
  return res.json() as Promise<NebiusBatchStatus>;
}

export async function listBatches(apiKey: string): Promise<NebiusBatchStatus[]> {
  const res = await nebiusRequest("/batches", apiKey);
  if (!res.ok) {
    throw new Error(`Failed to list batches: ${res.status} ${await res.text()}`);
  }
  const data = (await res.json()) as { data: NebiusBatchStatus[] };
  return data.data;
}

export interface BatchDownloadResult {
  results: Map<string, number[]>;
  failed: { id: string; error: string }[];
}

export async function downloadBatchResults(fileId: string, apiKey: string): Promise<BatchDownloadResult> {
  const res = await nebiusRequest(`/files/${fileId}/content`, apiKey);
  if (!res.ok) {
    const text = await res.text();
    if (res.status === 402) {
      throw new BudgetExhaustedError(text);
    }
    throw new Error(`Failed to download results: ${res.status} ${text}`);
  }

  const results = new Map<string, number[]>();
  const failed: { id: string; error: string }[] = [];

  // Stream line by line to avoid memory issues with large files
  const reader = res.body!.getReader();
  const decoder = new TextDecoder();
  let buffer = "";

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;

    buffer += decoder.decode(value, { stream: true });
    const lines = buffer.split("\n");
    buffer = lines.pop()!; // Keep incomplete line in buffer

    for (const line of lines) {
      if (!line.trim()) continue;
      const row = JSON.parse(line) as NebiusBatchResultRow;
      if (!row.response?.data?.[0]?.embedding) {
        failed.push({ id: row.custom_id, error: JSON.stringify(row.response || row).slice(0, 200) });
        continue;
      }
      results.set(row.custom_id, row.response.data[0].embedding);
    }
  }

  // Process remaining buffer
  if (buffer.trim()) {
    const row = JSON.parse(buffer) as NebiusBatchResultRow;
    if (!row.response?.data?.[0]?.embedding) {
      failed.push({ id: row.custom_id, error: JSON.stringify(row.response || row).slice(0, 200) });
    } else {
      results.set(row.custom_id, row.response.data[0].embedding);
    }
  }

  return { results, failed };
}

// Submit batch, returns batch ID (use for tracking)
export async function submitBatchJob(
  items: EmbedItem[],
  apiKey: string,
  dimensions: number = EMBEDDING_DIM
): Promise<string> {
  console.log(`[nebius-batch] Uploading ${items.length} items...`);
  const fileId = await uploadBatchFile(items, apiKey, dimensions);
  console.log(`[nebius-batch] File uploaded: ${fileId}`);

  console.log(`[nebius-batch] Creating batch job...`);
  const batchId = await createBatch(fileId, apiKey);
  console.log(`[nebius-batch] Batch created: ${batchId}`);
  return batchId;
}

// Poll batch until complete, download and return results
export async function pollBatchJob(
  batchId: string,
  apiKey: string,
  pollIntervalMs: number = 30000,
  onPoll?: (batchId: string, completed: number, total: number) => void
): Promise<BatchEmbedResult> {
  let status: NebiusBatchStatus;
  while (true) {
    status = await getBatchStatus(batchId, apiKey);
    const counts = status.request_counts;
    if (counts) onPoll?.(batchId, counts.completed, counts.total);

    if (status.status === "completed") break;
    if (status.status === "failed" || status.status === "expired" || status.status === "cancelled") {
      throw new Error(`Batch ${status.status}: ${batchId}`);
    }
    await Bun.sleep(pollIntervalMs);
  }

  if (!status.output_file_id) throw new Error("No output file ID");

  console.log(`[nebius-batch] Downloading results...`);
  const res = await nebiusRequest(`/files/${status.output_file_id}/content`, apiKey);
  if (!res.ok) {
    const text = await res.text();
    if (res.status === 402) {
      throw new BudgetExhaustedError(text);
    }
    throw new Error(`Failed to download results: ${res.status} ${text}`);
  }

  const results = new Map<string, number[]>();
  const failed: { id: string; error: string }[] = [];
  let totalTokens = 0;

  // Stream line by line to avoid memory issues with large files
  const reader = res.body!.getReader();
  const decoder = new TextDecoder();
  let buffer = "";

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;

    buffer += decoder.decode(value, { stream: true });
    const lines = buffer.split("\n");
    buffer = lines.pop()!;

    for (const line of lines) {
      if (!line.trim()) continue;
      const row = JSON.parse(line) as NebiusBatchResultRow;
      if (!row.response?.data?.[0]?.embedding) {
        failed.push({ id: row.custom_id, error: JSON.stringify(row.response || row).slice(0, 200) });
        continue;
      }
      results.set(row.custom_id, row.response.data[0].embedding);
      totalTokens += row.response.usage?.prompt_tokens || 0;
    }
  }

  if (buffer.trim()) {
    const row = JSON.parse(buffer) as NebiusBatchResultRow;
    if (!row.response?.data?.[0]?.embedding) {
      failed.push({ id: row.custom_id, error: JSON.stringify(row.response || row).slice(0, 200) });
    } else {
      results.set(row.custom_id, row.response.data[0].embedding);
      totalTokens += row.response.usage?.prompt_tokens || 0;
    }
  }

  const cost = (totalTokens / 1_000_000) * PRICING["nebius-batch"];
  return { results, failed, tokens: totalTokens, cost };
}

// Convenience: submit + poll in one call
export async function embedBatchAsync(
  items: EmbedItem[],
  apiKey: string,
  dimensions: number = EMBEDDING_DIM,
  pollIntervalMs: number = 30000
): Promise<BatchEmbedResult> {
  const batchId = await submitBatchJob(items, apiKey, dimensions);
  return pollBatchJob(batchId, apiKey, pollIntervalMs);
}

// === Simple single-text embed (for search queries) ===
export async function embed(text: string, provider: "deepinfra" | "nebius", apiKey: string): Promise<number[]> {
  const result = await embedRealtime([text], provider, apiKey);
  return result.embeddings[0];
}

// === Convenience: Create configured embedder ===
export function createEmbedder(provider: Provider, apiKeys: string[], dimensions: number = EMBEDDING_DIM) {
  let keyIndex = 0;
  const getKey = () => {
    const key = apiKeys[keyIndex % apiKeys.length];
    keyIndex++;
    return key;
  };

  return {
    provider,
    pricing: PRICING[provider],
    dimensions,

    // Real-time batch (DeepInfra, Nebius real-time)
    embed: provider !== "nebius-batch"
      ? async (texts: string[]) => embedRealtime(texts, provider as "deepinfra" | "nebius", getKey(), dimensions)
      : undefined,

    // Async batch (Nebius batch only)
    embedAsync: provider === "nebius-batch"
      ? async (items: EmbedItem[], onProgress?: (status: NebiusBatchStatus) => void) =>
          embedBatchAsync(items, getKey(), dimensions, 30000, onProgress)
      : undefined,

    // Single text (for queries)
    embedOne: async (text: string) => {
      const p = provider === "nebius-batch" ? "nebius" : provider;
      const result = await embedRealtime([text], p as "deepinfra" | "nebius", getKey(), dimensions);
      return result.embeddings[0];
    },
  };
}
