export interface RepoInfo {
  description: string | null;
  stars: number;
  language: string | null;
  forks: number;
}

const cache = new Map<string, { data: RepoInfo; timestamp: number }>();
const CACHE_TTL = 24 * 60 * 60 * 1000; // 24 hours

export async function fetchRepoInfo(repo: string): Promise<RepoInfo | null> {
  const cached = cache.get(repo);
  if (cached && Date.now() - cached.timestamp < CACHE_TTL) {
    return cached.data;
  }

  // Also check localStorage
  const lsKey = `gh:${repo}`;
  const lsCached = localStorage.getItem(lsKey);
  if (lsCached) {
    try {
      const { data, timestamp } = JSON.parse(lsCached);
      if (Date.now() - timestamp < CACHE_TTL) {
        cache.set(repo, { data, timestamp });
        return data;
      }
    } catch {}
  }

  try {
    const res = await fetch(`https://api.github.com/repos/${repo}`);
    if (!res.ok) return null;

    const json = await res.json();
    const data: RepoInfo = {
      description: json.description,
      stars: json.stargazers_count,
      language: json.language,
      forks: json.forks_count,
    };

    cache.set(repo, { data, timestamp: Date.now() });
    localStorage.setItem(lsKey, JSON.stringify({ data, timestamp: Date.now() }));

    return data;
  } catch {
    return null;
  }
}
