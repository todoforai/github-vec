// Qwen3-Embedding-8B via OpenRouter
const OPENROUTER_API_KEY = process.env.OPENROUTER_API_KEY;
if (!OPENROUTER_API_KEY) throw new Error("OPENROUTER_API_KEY required");

const MODEL = "qwen/qwen3-embedding-8b";
const ENDPOINT = "https://openrouter.ai/api/v1/embeddings";

// Qwen3-8B supports 32-4096 dims (MRL). Using 1536 for Qdrant compat with gemini collection.
export const EMBEDDING_DIM = 1536;

interface OpenRouterEmbedResponse {
  data: { embedding: number[]; index: number }[];
}

export async function embedBatch(texts: string[], retries = 5): Promise<number[][]> {
  const res = await fetch(ENDPOINT, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${OPENROUTER_API_KEY}`,
    },
    body: JSON.stringify({
      model: MODEL,
      input: texts,
      dimensions: EMBEDDING_DIM,
      provider: {
        order: ["DeepInfra"],
        allow_fallbacks: false,
      },
    }),
  });

  if (res.status === 429 && retries > 0) {
    const delay = (6 - retries) * 5000;
    console.log(`Rate limited, waiting ${delay / 1000}s... (${retries} retries left)`);
    await Bun.sleep(delay);
    return embedBatch(texts, retries - 1);
  }

  const data = (await res.json()) as OpenRouterEmbedResponse;

  if (!res.ok || !data.data) {
    console.error(`Error: status=${res.status} body=${JSON.stringify(data)}`);
    if (retries > 0) {
      console.log(`Provider error, waiting 15s... (${retries} retries left)`);
      await Bun.sleep(15000);
      return embedBatch(texts, retries - 1);
    }
    throw new Error(`Embed failed: ${res.status} ${JSON.stringify(data)}`);
  }

  return data.data.sort((a, b) => a.index - b.index).map((d) => d.embedding);
}

export async function embed(text: string): Promise<number[]> {
  const [vec] = await embedBatch([text]);
  return vec!;
}
