# github-vec MCP Server

Semantic search across millions of GitHub repositories.

## Installation

### Claude Code

```bash
claude mcp add github-vec -- bunx --bun github:todoforai/github-vec/mcp
```

### Claude Desktop

Add to `~/.config/claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "github-vec": {
      "command": "bunx",
      "args": ["--bun", "github:todoforai/github-vec/mcp"]
    }
  }
}
```

## Tools

### search_github_repos

Search GitHub repositories by meaning/concept.

**Parameters:**
- `query` (required): What you're looking for
- `limit` (optional): Number of results (default: 10, max: 50)

**Example:**
```
"Find a fast Rust terminal emulator"
"CLI tool for managing kubernetes"
"React component library with accessibility"
```

## Environment Variables

- `GITHUB_VEC_URL` - API endpoint (default: https://github-vec.com)
