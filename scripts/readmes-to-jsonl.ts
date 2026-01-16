import { Glob } from "bun";
import { createHash } from "crypto";

const DATA_DIR = process.env.DATA_DIR || "/home/root/data";
const outputPath = `${DATA_DIR}/readmes.jsonl`;
const readmesDir = `${DATA_DIR}/readmes`;

const glob = new Glob("*");
const files = await Array.fromAsync(glob.scan(readmesDir));

console.log(`Converting ${files.length} READMEs to JSONL...`);

const lines: string[] = [];

for (const file of files) {
  const content = await Bun.file(`${readmesDir}/${file}`).text();

  // Parse filename: owner_repo_branch_README.md
  // Find repo name by removing branch and filename suffix
  const parts = file.split("_");
  // Last two parts are branch and filename (e.g., "main_README.md")
  // Everything before is owner_repo (but repo can have underscores)

  // Find branch by looking for known branches
  let branchIdx = parts.findIndex(p => p === "main" || p === "master" || p === "default");
  if (branchIdx === -1) branchIdx = parts.length - 2; // fallback

  const owner = parts[0];
  const repo = parts.slice(1, branchIdx).join("_");
  const repoName = `${owner}/${repo}`;

  const contentHash = createHash("sha1").update(content).digest("hex");

  lines.push(JSON.stringify({
    content_hash: contentHash,
    repo_name: repoName,
    content: content,
  }));
}

await Bun.write(outputPath, lines.join("\n") + "\n");
console.log(`Written ${lines.length} entries to ${outputPath}`);
