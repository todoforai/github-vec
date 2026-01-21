#!/usr/bin/env bun
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";

const API_URL = process.env.GITHUB_VEC_URL || "https://github-vec.com";

interface SearchResult {
  score: number;
  repo: string;
}

interface SearchResponse {
  query: string;
  results: SearchResult[];
}

async function searchRepos(query: string, limit: number = 10): Promise<SearchResponse> {
  const params = new URLSearchParams({ q: query, limit: String(limit) });
  const res = await fetch(`${API_URL}/search?${params}`);
  if (!res.ok) {
    throw new Error(`Search failed: ${res.status}`);
  }
  return res.json();
}

const server = new McpServer({
  name: "github-vec",
  version: "1.0.0",
});

server.tool(
  "search_github_repos",
  "Semantic search across millions of GitHub repositories by meaning. Use this to find libraries, tools, or projects that match a concept or description.",
  {
    query: z.string().describe("What you're looking for - describe the functionality, concept, or type of project"),
    limit: z.number().optional().default(10).describe("Number of results to return (default: 10, max: 50)"),
  },
  async ({ query, limit }) => {
    try {
      const results = await searchRepos(query, Math.min(limit || 10, 50));

      if (results.results.length === 0) {
        return {
          content: [{ type: "text", text: `No repositories found for: "${query}"` }],
        };
      }

      const formatted = results.results
        .map((r, i) => `${i + 1}. **${r.repo}** (${(r.score * 100).toFixed(0)}% match)\n   https://github.com/${r.repo}`)
        .join("\n\n");

      return {
        content: [
          {
            type: "text",
            text: `Found ${results.results.length} repositories for "${query}":\n\n${formatted}`,
          },
        ],
      };
    } catch (error) {
      return {
        content: [{ type: "text", text: `Search failed: ${error}` }],
        isError: true,
      };
    }
  }
);

const transport = new StdioServerTransport();
await server.connect(transport);
