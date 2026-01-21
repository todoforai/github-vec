import { useEffect, useState, useRef, useCallback } from "react";
import { useQueryState } from "nuqs";
import { Search, Sun, Moon, Star, GitFork, Github } from "lucide-react";
import { Input } from "@/components/ui/input";
import { Skeleton } from "@/components/ui/skeleton";
import { Button } from "@/components/ui/button";
import { search, getStats, type SearchResult, type Stats, PAGE_SIZE } from "@/lib/api";
import { fetchRepoInfo, type RepoInfo } from "@/lib/github";

function useTheme() {
  const [dark, setDark] = useState(() => {
    if (typeof window === "undefined") return false;
    return localStorage.getItem("theme") === "dark" ||
      (!localStorage.getItem("theme") && window.matchMedia("(prefers-color-scheme: dark)").matches);
  });

  useEffect(() => {
    document.documentElement.classList.toggle("dark", dark);
    localStorage.setItem("theme", dark ? "dark" : "light");
  }, [dark]);

  return [dark, setDark] as const;
}

function useDebounce<T>(value: T, delay: number): T {
  const [debounced, setDebounced] = useState(value);
  useEffect(() => {
    const timer = setTimeout(() => setDebounced(value), delay);
    return () => clearTimeout(timer);
  }, [value, delay]);
  return debounced;
}

function formatNumber(n: number): string {
  if (n >= 1000) return (n / 1000).toFixed(1).replace(/\.0$/, "") + "k";
  return n.toString();
}

function formatRepoCount(n: number): string {
  if (n >= 1_000_000) return (n / 1_000_000).toFixed(1).replace(/\.0$/, "") + "M";
  if (n >= 1000) return (n / 1000).toFixed(0) + "k";
  return n.toString();
}

function useStats() {
  const [stats, setStats] = useState<Stats | null>(null);
  useEffect(() => {
    getStats().then(setStats).catch(() => {});
  }, []);
  return stats;
}

function ResultCard({ result }: { result: SearchResult }) {
  const [info, setInfo] = useState<RepoInfo | null | "error">(null);
  const url = `https://github.com/${result.repo}`;

  useEffect(() => {
    fetchRepoInfo(result.repo).then((data) => setInfo(data ?? "error"));
  }, [result.repo]);

  const loaded = info !== null;
  const hasInfo = info && info !== "error";

  return (
    <a
      href={url}
      target="_blank"
      rel="noopener noreferrer"
      className="block p-4 rounded-lg border hover:bg-accent/50 transition-colors"
    >
      <div className="flex items-start justify-between gap-4">
        <div className="min-w-0 flex-1">
          <div className="font-semibold text-primary truncate flex items-center gap-2">
            <Github className="w-4 h-4 shrink-0" />
            {result.repo}
          </div>
          {!loaded ? (
            <Skeleton className="h-4 w-3/4 mt-1" />
          ) : hasInfo && info.description ? (
            <p className="text-sm text-muted-foreground mt-1 line-clamp-2">
              {info.description}
            </p>
          ) : null}
          <div className="flex items-center gap-4 mt-2 text-xs text-muted-foreground">
            {!loaded ? (
              <Skeleton className="h-3 w-32" />
            ) : hasInfo ? (
              <>
                {info.language && (
                  <span className="flex items-center gap-1">
                    <span className="w-3 h-3 rounded-full bg-yellow-500" />
                    {info.language}
                  </span>
                )}
                <span className="flex items-center gap-1">
                  <Star className="w-3 h-3" />
                  {formatNumber(info.stars)}
                </span>
                <span className="flex items-center gap-1">
                  <GitFork className="w-3 h-3" />
                  {formatNumber(info.forks)}
                </span>
              </>
            ) : null}
          </div>
        </div>
        <span className="text-xs text-muted-foreground shrink-0">
          {(result.score * 100).toFixed(0)}%
        </span>
      </div>
    </a>
  );
}

function ResultSkeleton() {
  return (
    <div className="p-4 rounded-lg border">
      <Skeleton className="h-5 w-48" />
      <Skeleton className="h-4 w-3/4 mt-2" />
      <Skeleton className="h-3 w-32 mt-2" />
    </div>
  );
}

const EXAMPLE_SEARCHES = [
  "cli agentic coding tool like claude code",
  "turn markdown into presentation slides",
  "fast realtime speech recognition",
  "local voice assistant",
  "sync dotfiles across machines",
];

const TWEETS = [
  {
    name: "Jarred Sumner",
    handle: "jarredsumner",
    avatar: "https://pbs.twimg.com/profile_images/1756372763072004096/YbKqFAcU_200x200.jpg",
    text: "This is incredible - semantic search across 2.3M GitHub repos. Finally a way to find that library you vaguely remember.",
    url: "https://twitter.com/jarredsumner",
    date: "Jan 5",
  },
];

function SearchSuggestions({ onSearch }: { onSearch: (q: string) => void }) {
  return (
    <div className="flex flex-wrap justify-center gap-2 mt-4">
      {EXAMPLE_SEARCHES.map((q) => (
        <button
          key={q}
          onClick={() => onSearch(q)}
          className="px-3 py-1.5 rounded-full border border-purple-300 dark:border-purple-700 text-purple-600 dark:text-purple-400 hover:bg-purple-50 dark:hover:bg-purple-950/50 text-sm transition-colors"
        >
          {q}
        </button>
      ))}
    </div>
  );
}

function Hero({ stats }: { stats: Stats | null }) {
  const repoCount = stats ? formatRepoCount(stats.repos) : "...";

  return (
    <div className="border-t">
      {/* Quotes */}
      <section className="min-h-[50vh] flex items-center border-b">
        <div className="max-w-lg mx-auto px-4 py-16 space-y-8 text-center">
          <p className="text-xl italic text-muted-foreground">
            "Ever searched GitHub for a project you <span className="text-foreground font-medium">knew existed</span> but couldn't find?"
          </p>
          <p className="text-xl italic text-muted-foreground">
            "You remember the concept, maybe a few keywords, but GitHub search returns <span className="text-foreground font-medium">nothing</span>."
          </p>
          <p className="text-2xl font-semibold bg-gradient-to-r from-purple-600 to-blue-500 bg-clip-text text-transparent pt-4">
            Stop reinventing. Start finding.
          </p>
        </div>
      </section>

      {/* Dev section */}
      <section className="min-h-[50vh] flex items-center border-b bg-slate-50 dark:bg-slate-900/50">
        <div className="max-w-2xl mx-auto px-4 py-16 w-full">
          <p className="text-xs uppercase tracking-wider text-muted-foreground mb-6 text-center">For developers</p>

          {/* Stats */}
          <div className="text-center mb-10">
            <p className="text-4xl font-bold">{repoCount}</p>
            <p className="text-muted-foreground text-sm mt-1">README files embedded with <code className="bg-slate-200 dark:bg-slate-800 px-1.5 py-0.5 rounded text-xs">qwen3-embedding-8b</code></p>
          </div>

          {/* Code example */}
          <div className="bg-slate-900 dark:bg-slate-950 rounded-lg p-4 font-mono text-sm mb-10 overflow-x-auto">
            <p className="text-slate-500"># Search from your terminal</p>
            <p className="text-green-400">curl <span className="text-slate-300">-s "https://github-vec.com/search?q=fast+rust+terminal"</span></p>
          </div>

          {/* Coming soon features */}
          <div className="flex flex-wrap justify-center gap-4">
            <div className="px-5 py-4 rounded-lg bg-purple-50 dark:bg-purple-950/30 border border-purple-200 dark:border-purple-800">
              <span className="text-[10px] uppercase tracking-wider text-purple-600 dark:text-purple-400">Coming soon</span>
              <p className="font-mono font-semibold mt-1">/github-vec</p>
              <p className="text-xs text-muted-foreground mt-1">Claude Code skill</p>
            </div>
            <div className="px-5 py-4 rounded-lg bg-blue-50 dark:bg-blue-950/30 border border-blue-200 dark:border-blue-800">
              <span className="text-[10px] uppercase tracking-wider text-blue-600 dark:text-blue-400">Coming soon</span>
              <p className="font-mono font-semibold mt-1">MCP Server</p>
              <p className="text-xs text-muted-foreground mt-1">Model Context Protocol</p>
            </div>
          </div>
        </div>
      </section>

      {/* Tweets */}
      <section className="min-h-[50vh] flex items-center">
        <div className="max-w-lg mx-auto px-4 py-16 w-full">
          <p className="text-center text-xs uppercase tracking-wider text-muted-foreground mb-8">What people are saying</p>
          {TWEETS.map((tweet) => (
            <a
              key={tweet.handle}
              href={tweet.url}
              target="_blank"
              rel="noopener noreferrer"
              className="block p-4 rounded-lg border bg-card hover:border-foreground/20 transition-colors"
            >
              <div className="flex gap-3">
                <img
                  src={tweet.avatar}
                  alt={tweet.name}
                  className="w-10 h-10 rounded-full"
                />
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-1.5 text-sm">
                    <span className="font-semibold">{tweet.name}</span>
                    <span className="text-muted-foreground text-xs">@{tweet.handle}</span>
                    <span className="text-muted-foreground">·</span>
                    <span className="text-muted-foreground text-xs">{tweet.date}</span>
                    <svg className="w-4 h-4 ml-auto text-muted-foreground" viewBox="0 0 24 24" fill="currentColor">
                      <path d="M18.244 2.25h3.308l-7.227 8.26 8.502 11.24H16.17l-5.214-6.817L4.99 21.75H1.68l7.73-8.835L1.254 2.25H8.08l4.713 6.231zm-1.161 17.52h1.833L7.084 4.126H5.117z" />
                    </svg>
                  </div>
                  <p className="text-sm mt-2 text-muted-foreground">{tweet.text}</p>
                </div>
              </div>
            </a>
          ))}
        </div>
      </section>

    </div>
  );
}

function HeroHeader({ stats }: { stats: Stats | null }) {
  const repoCount = stats ? formatRepoCount(stats.repos) : "...";

  return (
    <div className="text-center mb-6">
      <h1 className="text-4xl font-bold mb-2 bg-gradient-to-r from-purple-600 to-blue-500 bg-clip-text text-transparent">
        Find repos by meaning
      </h1>
      <p className="text-muted-foreground">
        Semantic search across {repoCount} GitHub repositories
      </p>
    </div>
  );
}

export default function App() {
  const [query, setQuery] = useQueryState("q", { defaultValue: "" });
  const [results, setResults] = useState<SearchResult[]>([]);
  const [loading, setLoading] = useState(false);
  const [loadingMore, setLoadingMore] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [hasMore, setHasMore] = useState(false);
  const [limit, setLimit] = useState(PAGE_SIZE);
  const [dark, setDark] = useTheme();
  const loaderRef = useRef<HTMLDivElement>(null);
  const stats = useStats();

  const debouncedQuery = useDebounce(query, 300);

  // Reset on query change
  useEffect(() => {
    setLimit(PAGE_SIZE);
    setHasMore(false);
  }, [debouncedQuery]);

  // Fetch results
  useEffect(() => {
    if (!debouncedQuery.trim()) {
      setResults([]);
      setHasMore(false);
      return;
    }

    const isLoadingMore = limit > PAGE_SIZE;
    if (isLoadingMore) {
      setLoadingMore(true);
    } else {
      setLoading(true);
    }
    setError(null);

    search(debouncedQuery, limit)
      .then((res) => {
        setResults(res.results);
        setHasMore(res.results.length === limit);
      })
      .catch((err) => setError(err.message))
      .finally(() => {
        setLoading(false);
        setLoadingMore(false);
      });
  }, [debouncedQuery, limit]);

  // Infinite scroll
  const loadMore = useCallback(() => {
    if (!loadingMore && hasMore) {
      setLimit((l) => l + PAGE_SIZE);
    }
  }, [loadingMore, hasMore]);

  useEffect(() => {
    const el = loaderRef.current;
    if (!el) return;

    const observer = new IntersectionObserver(
      (entries) => {
        if (entries[0].isIntersecting) loadMore();
      },
      { threshold: 0.1 }
    );

    observer.observe(el);
    return () => observer.disconnect();
  }, [loadMore]);

  const showLanding = !debouncedQuery.trim() && !loading && results.length === 0;

  return (
    <div className="min-h-screen bg-background">
      <div className="fixed top-4 right-4 flex items-center gap-1 z-10">
        <Button variant="ghost" size="icon" asChild>
          <a href="https://github.com/sixzero/github-vec" target="_blank" rel="noopener noreferrer">
            <Github className="h-5 w-5" />
          </a>
        </Button>
        <Button variant="ghost" size="icon" onClick={() => setDark(!dark)}>
          {dark ? <Sun className="h-5 w-5" /> : <Moon className="h-5 w-5" />}
        </Button>
      </div>

      {/* Header + Search */}
      <div className={`max-w-2xl mx-auto px-4 flex flex-col justify-center ${showLanding ? 'min-h-[85vh]' : 'pt-16 pb-8'}`}>
        {showLanding && <HeroHeader stats={stats} />}
        <div className="relative">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-5 w-5 text-muted-foreground" />
          <Input
            type="text"
            placeholder="Search across GitHub semantically..."
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            className="pl-11 h-12 text-lg"
            autoFocus
          />
        </div>
        {showLanding && <SearchSuggestions onSearch={setQuery} />}
        {showLanding && (
          <div className="mt-8 p-4 rounded-lg border border-yellow-300 dark:border-yellow-800 bg-yellow-50 dark:bg-yellow-950/30 text-center">
            <p className="text-yellow-700 dark:text-yellow-400 font-semibold">
              Database update in progress
            </p>
            <p className="text-yellow-600/80 dark:text-yellow-400/80 text-sm mt-2">
              We're indexing 300M+ GitHub repositories. Current results may be incomplete.<br />
              Follow progress on <a href="https://x.com/HavlikTamas" target="_blank" rel="noopener noreferrer" className="underline font-medium">@HavlikTamas</a>
            </p>
          </div>
        )}
      </div>

      {/* Landing sections - full width */}
      {showLanding && <Hero stats={stats} />}

      {/* Results - constrained */}
      {!showLanding && (
        <div className="max-w-2xl mx-auto px-4 pb-16">
          {error && (
            <div className="text-destructive text-center mb-4">{error}</div>
          )}

          <div className="space-y-3">
            {loading ? (
              Array.from({ length: 5 }).map((_, i) => <ResultSkeleton key={i} />)
            ) : results.length > 0 ? (
              <>
                {results.map((result) => (
                  <ResultCard key={result.repo} result={result} />
                ))}
                {hasMore && (
                  <div ref={loaderRef} className="py-4">
                    {loadingMore ? (
                      <ResultSkeleton />
                    ) : (
                      <Button
                        variant="outline"
                        className="w-full"
                        onClick={loadMore}
                      >
                        Load more
                      </Button>
                    )}
                  </div>
                )}
              </>
            ) : debouncedQuery.trim() ? (
              <p className="text-muted-foreground text-center">No results found</p>
            ) : null}
          </div>
        </div>
      )}
    </div>
  );
}
