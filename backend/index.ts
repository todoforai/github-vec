import { QdrantClient } from "@qdrant/js-client-rest";
import { embed } from "./embed";

const COLLECTION = "github_readmes_qwen_4k";
const qdrant = new QdrantClient({ url: process.env.QDRANT_URL || "http://localhost:6333" });

// Stats cache (1 hour TTL)
let statsCache: { count: number; cachedAt: number } | null = null;
const STATS_CACHE_TTL = 60 * 60 * 1000; // 1 hour

async function getStats(): Promise<{ count: number; cachedAt: number }> {
  const now = Date.now();
  if (statsCache && now - statsCache.cachedAt < STATS_CACHE_TTL) {
    return statsCache;
  }

  const info = await qdrant.getCollection(COLLECTION);
  statsCache = { count: info.points_count ?? 0, cachedAt: now };
  return statsCache;
}

// Rate limiting config
const ANON_LIMIT = 10; // requests per minute
const WINDOW_MS = 60_000; // 1 minute

const rateLimits = new Map<string, { count: number; resetAt: number }>();
const validApiKeys = new Set(
  (process.env.API_KEYS || "").split(",").filter(Boolean)
);

interface RateLimitResult {
  allowed: boolean;
  limit: number;
  remaining: number;
  resetAt: number;
}

function checkRateLimit(req: Request): RateLimitResult {
  const apiKey = req.headers.get("X-API-Key");

  // API key users bypass rate limit
  if (apiKey && validApiKeys.has(apiKey)) {
    return { allowed: true, limit: -1, remaining: -1, resetAt: 0 };
  }

  // Get client IP from X-Forwarded-For (behind proxy) or fallback
  const forwarded = req.headers.get("X-Forwarded-For");
  const ip = forwarded ? forwarded.split(",")[0].trim() : "unknown";

  const now = Date.now();
  const limit = rateLimits.get(ip);

  // New window or expired
  if (!limit || now > limit.resetAt) {
    const resetAt = now + WINDOW_MS;
    rateLimits.set(ip, { count: 1, resetAt });
    return { allowed: true, limit: ANON_LIMIT, remaining: ANON_LIMIT - 1, resetAt };
  }

  // Check if over limit
  if (limit.count >= ANON_LIMIT) {
    return { allowed: false, limit: ANON_LIMIT, remaining: 0, resetAt: limit.resetAt };
  }

  limit.count++;
  return { allowed: true, limit: ANON_LIMIT, remaining: ANON_LIMIT - limit.count, resetAt: limit.resetAt };
}

function rateLimitHeaders(rl: RateLimitResult): Record<string, string> {
  if (rl.limit === -1) return {}; // API key user, no headers
  return {
    "X-RateLimit-Limit": String(rl.limit),
    "X-RateLimit-Remaining": String(rl.remaining),
    "X-RateLimit-Reset": String(Math.floor(rl.resetAt / 1000)),
  };
}

function rateLimitedResponse(rl: RateLimitResult): Response {
  const retryAfter = Math.ceil((rl.resetAt - Date.now()) / 1000);
  return Response.json(
    { error: "Rate limit exceeded. Use an API key for higher limits." },
    {
      status: 429,
      headers: {
        ...rateLimitHeaders(rl),
        "Retry-After": String(Math.max(1, retryAfter)),
      },
    }
  );
}

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
  idleTimeout: 60, // 60 seconds for slow embedding calls

  async fetch(req) {
    const url = new URL(req.url);
    const path = url.pathname;

    // Health check - no rate limit
    if (path === "/health") {
      return Response.json({ status: "ok" });
    }

    // Stats endpoint - no rate limit, cached
    if (path === "/stats") {
      const stats = await getStats();
      return Response.json({
        repos: stats.count,
        cachedAt: new Date(stats.cachedAt).toISOString(),
      });
    }

    // Rate limit check for all other endpoints
    const rl = checkRateLimit(req);
    if (!rl.allowed) {
      return rateLimitedResponse(rl);
    }

    const headers = rateLimitHeaders(rl);

    if (path === "/search" || path === "/search/full") {
      const q = url.searchParams.get("q");
      const limit = parseInt(url.searchParams.get("limit") || "10");

      if (!q) {
        return Response.json({ error: "Missing ?q= parameter" }, { status: 400, headers });
      }

      const includeContent = path === "/search/full";
      const results = await search(q, limit, includeContent);
      return Response.json({ query: q, results }, { headers });
    }

    return Response.json({ error: "Not found" }, { status: 404 });
  },
});

console.log(`Server running on http://localhost:${process.env.PORT || 5555}`);
