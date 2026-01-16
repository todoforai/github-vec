const OPENROUTER_API_KEY = process.env.OPENROUTER_API_KEY;
if (!OPENROUTER_API_KEY) throw new Error("OPENROUTER_API_KEY required");

const MODEL = "qwen/qwen3-embedding-8b";
const ENDPOINT = "https://openrouter.ai/api/v1/embeddings";
const EMBEDDING_DIM = 4096;

interface OpenRouterEmbedResponse {
  data: { embedding: number[]; index: number }[];
}

export async function embed(text: string): Promise<number[]> {
  const res = await fetch(ENDPOINT, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${OPENROUTER_API_KEY}`,
    },
    body: JSON.stringify({
      model: MODEL,
      input: [text],
      dimensions: EMBEDDING_DIM,
    }),
  });

  if (!res.ok) {
    throw new Error(`Embed failed: ${res.status}`);
  }

  const data = (await res.json()) as OpenRouterEmbedResponse;
  return data.data[0].embedding;
}
