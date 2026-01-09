import { QdrantClient } from "@qdrant/js-client-rest";
import { embed } from "./embed";

const COLLECTION = "github_readmes_qwen";
const qdrant = new QdrantClient({ url: process.env.QDRANT_URL || "http://localhost:6333" });

interface SearchResult {
  score: number;
  repo: string;
  content?: string;
}

async function search(query: string, limit: number, includeContent: boolean): Promise<SearchResult[]> {
  const vector = await embed(query);
  const results = await qdrant.search(COLLECTION, {
    vector,
    limit,
    with_payload: true,
  });

  return results.map(r => {
    const payload = r.payload as { repo_name: string; content: string };
    const result: SearchResult = {
      score: r.score,
      repo: payload.repo_name,
    };
    if (includeContent) {
      result.content = payload.content;
    }
    return result;
  });
}

Bun.serve({
  port: process.env.PORT || 5555,

  routes: {
    "/search": async (req) => {
      const url = new URL(req.url);
      const q = url.searchParams.get("q");
      const limit = parseInt(url.searchParams.get("limit") || "10");

      if (!q) {
        return Response.json({ error: "Missing ?q= parameter" }, { status: 400 });
      }

      const results = await search(q, limit, false);
      return Response.json({ query: q, results });
    },

    "/search/full": async (req) => {
      const url = new URL(req.url);
      const q = url.searchParams.get("q");
      const limit = parseInt(url.searchParams.get("limit") || "10");

      if (!q) {
        return Response.json({ error: "Missing ?q= parameter" }, { status: 400 });
      }

      const results = await search(q, limit, true);
      return Response.json({ query: q, results });
    },

    "/health": () => Response.json({ status: "ok" }),
  },

  fetch(req) {
    return Response.json({ error: "Not found" }, { status: 404 });
  },
});

console.log(`Server running on http://localhost:${process.env.PORT || 5555}`);
