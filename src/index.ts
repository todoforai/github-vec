#!/usr/bin/env bun
import { parseArgs } from "util";

const API_URL = process.env.GITHUB_VEC_URL || "https://github-vec.com";

interface SearchResult {
  score: number;
  repo: string;
  content?: string;
}

interface SearchResponse {
  query: string;
  results: SearchResult[];
}

async function searchRepos(query: string, limit: number): Promise<SearchResponse> {
  const params = new URLSearchParams({ q: query, limit: String(limit) });
  const res = await fetch(`${API_URL}/search/full?${params}`);
  if (!res.ok) {
    throw new Error(`Search failed: ${res.status}`);
  }
  return res.json();
}

const { values, positionals } = parseArgs({
  args: Bun.argv.slice(2),
  options: {
    limit: { type: "string", short: "l", default: "10" },
    help: { type: "boolean", short: "h" },
  },
  allowPositionals: true,
});

if (values.help || positionals.length === 0) {
  console.log(`github-vec - Semantic search across GitHub repositories

Usage: github-vec <query> [options]

Options:
  -l, --limit <n>  Number of results (default: 10, max: 50)
  -h, --help       Show this help

Examples:
  github-vec "vector database for embeddings"
  github-vec "lightweight web framework" --limit 20`);
  process.exit(0);
}

const query = positionals.join(" ");
const limit = Math.min(parseInt(values.limit || "10"), 50);

try {
  const results = await searchRepos(query, limit);

  if (results.results.length === 0) {
    console.log(`No repositories found for: "${query}"`);
    process.exit(0);
  }

  for (const r of results.results) {
    console.log(`\n## ${r.repo} - ${r.score.toFixed(2)}\n`);
    if (r.content) {
      console.log(r.content);
    }
  }
} catch (error) {
  console.error(`Error: ${error}`);
  process.exit(1);
}
