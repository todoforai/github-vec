export interface SearchResult {
  score: number;
  repo: string;
}

export interface SearchResponse {
  query: string;
  results: SearchResult[];
}

export const PAGE_SIZE = 20;

export async function search(query: string, limit = PAGE_SIZE): Promise<SearchResponse> {
  const params = new URLSearchParams({ q: query, limit: String(limit) });
  const res = await fetch(`/search?${params}`);
  if (!res.ok) throw new Error("Search failed");
  return res.json();
}
