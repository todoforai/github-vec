import { createOpenAI } from "@ai-sdk/openai";
import { generateText } from "ai";

const llm = createOpenAI({
  baseURL: process.env.LLM_BASE_URL,
  apiKey: process.env.LLM_API_KEY || process.env.OPENAI_API_KEY,
  compatibility: "strict", // use /chat/completions instead of /responses
});

export async function generateHypotheticalDoc(query: string): Promise<string> {
  const { text } = await generateText({
    model: llm(process.env.LLM_MODEL || "gpt-5.2"),
    system: `You are a technical writer. Given a search query about GitHub repositories, write a hypothetical README excerpt that would perfectly answer this query. Write 2-3 paragraphs describing what such a repository would do, its features, and use cases. Be specific and technical.`,
    prompt: query,
  });

  return text;
}
