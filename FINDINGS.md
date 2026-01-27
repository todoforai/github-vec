# Findings

## Minimal Bandwidth Git Commit Date Fetch

**Problem:** Need to check if a repo had commits in the past 2 years without cloning or hitting API rate limits.

**Solution:** `git fetch --depth=1 --filter=blob:none`

```bash
git init -q /tmp/r
git -C /tmp/r fetch --depth=1 --filter=blob:none https://github.com/OWNER/REPO.git HEAD
git -C /tmp/r show -s --format=%ci FETCH_HEAD
```

**What gets fetched:**
- 1 commit object
- Tree objects (directory structure only)
- **NO blobs** (file contents)

**Bandwidth:** ~20-30KB per repo

**Why this works:**
- `--depth=1` = no parent commits (no history)
- `--filter=blob:none` = skip file contents, only metadata
- Commit date lives in commit object, which we do fetch

**Comparison:**
| Method | Bandwidth | Rate Limited |
|--------|-----------|--------------|
| `git fetch --depth=1` (no filter) | ~1MB+ | No |
| `git fetch --depth=1 --filter=blob:none` | ~20-30KB | No |
| GitHub API `/repos/{owner}/{repo}` | ~5KB | Yes (5K/hr) |
| `git ls-remote` | ~1KB | No |

`ls-remote` is cheapest but gives no dates. Filter fetch is the sweet spot for date checking without API limits.
