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

const X_EMBED_IDS = [
  "2015532814075064540", // @HavlikTamas: Just created @GithubVec...
];

function SearchSuggestions({ onSearch }: { onSearch: (q: string) => void }) {
  return (
    <div className="flex flex-wrap justify-center gap-2 mt-6">
      {EXAMPLE_SEARCHES.map((q) => (
        <button
          key={q}
          onClick={() => onSearch(q)}
          className="px-4 py-2 rounded-full border border-purple-200 dark:border-purple-800 text-purple-600 dark:text-purple-400 hover:bg-purple-100 dark:hover:bg-purple-950/50 hover:border-purple-400 dark:hover:border-purple-600 text-sm transition-all duration-200 hover:shadow-sm"
        >
          {q}
        </button>
      ))}
    </div>
  );
}

declare global {
  interface Window {
    twttr?: {
      widgets: {
        load: (element?: HTMLElement) => void;
      };
    };
  }
}

function XEmbeds({ ids }: { ids: string[] }) {
  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    // Load Twitter widgets script
    const script = document.createElement("script");
    script.src = "https://platform.twitter.com/widgets.js";
    script.async = true;
    script.onload = () => {
      if (window.twttr && containerRef.current) {
        window.twttr.widgets.load(containerRef.current);
      }
    };
    document.body.appendChild(script);

    return () => {
      document.body.removeChild(script);
    };
  }, []);

  return (
    <div ref={containerRef} className="space-y-4 flex flex-col items-center [&_twitter-widget]:!max-w-full">
      {ids.map((id) => (
        <blockquote key={id} className="twitter-tweet" data-theme="dark">
          <a href={`https://twitter.com/x/status/${id}`}>Loading...</a>
        </blockquote>
      ))}
    </div>
  );
}

function Hero({ stats }: { stats: Stats | null }) {
  // 5x multiplier accounts for forks and duplicated READMEs
  const repoCount = stats ? formatRepoCount(stats.repos * 5) : "...";

  return (
    <div className="border-t">
      {/* Quotes */}
      <section className="min-h-[50vh] flex items-center border-b bg-gradient-to-b from-purple-50/50 to-transparent dark:from-purple-950/20 dark:to-transparent">
        <div className="max-w-xl mx-auto px-4 py-20 space-y-6 text-center">
          <div className="space-y-4">
            <p className="text-xl italic text-muted-foreground leading-relaxed">
              "Ever searched GitHub for a project you <span className="text-foreground font-medium">knew existed</span> but couldn't find?"
            </p>
            <p className="text-xl italic text-muted-foreground leading-relaxed">
              "You remember the concept, maybe a few keywords, but GitHub search returns <span className="text-foreground font-medium">nothing</span>."
            </p>
          </div>
          <div className="pt-6 border-t border-border/50">
            <p className="text-base text-foreground/80 leading-relaxed">
              I faced the same problem, so I created a vectorized GitHub search. I believe we can find many great ideas and hidden gems — projects that aren't famous yet but already have proper READMEs.
            </p>
            <p className="text-sm text-muted-foreground mt-4">— Tamas</p>
          </div>
        </div>
      </section>

      {/* Dev section */}
      <section className="min-h-[50vh] flex items-center border-b bg-slate-50 dark:bg-slate-900/50">
        <div className="max-w-2xl mx-auto px-4 py-20 w-full">
          <p className="text-xs uppercase tracking-widest text-purple-600 dark:text-purple-400 mb-8 text-center font-medium">For developers</p>

          {/* Stats */}
          <div className="text-center mb-12">
            <p className="text-5xl font-bold bg-gradient-to-r from-purple-600 to-blue-500 bg-clip-text text-transparent">{repoCount}</p>
            <p className="text-muted-foreground text-sm mt-2">README files embedded with <code className="bg-slate-200 dark:bg-slate-800 px-1.5 py-0.5 rounded text-xs font-medium">qwen3-embedding-8b</code></p>
          </div>

          {/* MCP install */}
          <div className="space-y-3 mb-8">
            <div className="bg-slate-900 dark:bg-slate-950 rounded-xl p-4 font-mono text-sm overflow-x-auto shadow-lg">
              <p className="text-slate-500 text-xs mb-1"># Add to Claude Code</p>
              <p className="text-green-400">claude mcp add github-vec -- <span className="text-slate-300">npx -y github:todoforai/github-vec-mcp</span></p>
            </div>
            <div className="bg-slate-900 dark:bg-slate-950 rounded-xl p-4 font-mono text-sm overflow-x-auto shadow-lg">
              <p className="text-slate-500 text-xs mb-1"># Add to OpenCode</p>
              <p className="text-green-400">opencode mcp add github-vec -- <span className="text-slate-300">npx -y github:todoforai/github-vec-mcp</span></p>
            </div>
          </div>

          {/* curl example */}
          <div className="bg-slate-900 dark:bg-slate-950 rounded-xl p-4 font-mono text-sm overflow-x-auto shadow-lg">
            <p className="text-slate-500 text-xs mb-1"># Or search from your terminal</p>
            <p className="text-green-400">curl <span className="text-slate-300">-s "https://github-vec.com/search?q=fast+rust+terminal"</span></p>
          </div>
        </div>
      </section>

      {/* X Mentions */}
      <section className="min-h-[50vh] flex items-center">
        <div className="max-w-lg mx-auto px-4 py-16 w-full">
          <p className="text-center text-xs uppercase tracking-wider text-muted-foreground mb-8">What people are saying</p>
          <XEmbeds ids={X_EMBED_IDS} />
        </div>
      </section>

    </div>
  );
}

function HeroHeader({ stats }: { stats: Stats | null }) {
  // 5x multiplier accounts for forks and duplicated READMEs
  const repoCount = stats ? formatRepoCount(stats.repos * 5) : "...";

  return (
    <div className="text-center mb-8">
      <h1 className="text-5xl font-bold mb-3 bg-gradient-to-r from-purple-600 via-blue-500 to-purple-600 bg-clip-text text-transparent">
        Find repos by meaning
      </h1>
      <p className="text-lg text-muted-foreground">
        Semantic search across <span className="font-semibold text-foreground">{repoCount}</span> GitHub repositories
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
          <a href="https://github.com/todoforai/github-vec" target="_blank" rel="noopener noreferrer">
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
        <div className="relative group">
          <Search className="absolute left-4 top-1/2 -translate-y-1/2 h-5 w-5 text-muted-foreground group-focus-within:text-purple-500 transition-colors" />
          <Input
            type="text"
            placeholder="Describe what you're looking for..."
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            className="pl-12 h-14 text-lg rounded-xl border-2 focus:border-purple-500 focus:ring-purple-500/20 shadow-sm"
            autoFocus
          />
        </div>
        {showLanding && <SearchSuggestions onSearch={setQuery} />}
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
