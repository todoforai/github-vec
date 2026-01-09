import { BigQuery } from "@google-cloud/bigquery";

// BigQuery public dataset: 21,750,725 README files total
// After dedup by content hash: ~2.3M unique READMEs (~4.9 GB)

const QUERY = `
  SELECT
    c.id as content_hash,
    ANY_VALUE(f.repo_name) as repo_name,
    ANY_VALUE(c.content) as content,
    MAX(cm.committer.date.seconds) as last_commit
  FROM \`bigquery-public-data.github_repos.files\` f
  JOIN \`bigquery-public-data.github_repos.contents\` c ON f.id = c.id
  LEFT JOIN \`bigquery-public-data.github_repos.commits\` cm ON f.repo_name IN UNNEST(cm.repo_name)
  WHERE LOWER(f.path) = 'readme.md'
    AND c.content IS NOT NULL
    AND c.binary = false
  GROUP BY c.id
  ORDER BY last_commit DESC NULLS LAST
`;

const dest = process.argv[2] || ".";
const outFile = `${dest}/readmes.jsonl`;

console.log(`Streaming to ${outFile}...`);

const bigquery = new BigQuery();
const [job] = await bigquery.createQueryJob({ query: QUERY });
console.log(`Job ID: ${job.id}`);

const writer = Bun.file(outFile).writer();
let count = 0;
let bytes = 0;
const start = Date.now();

for await (const row of job.getQueryResultsStream()) {
  const line = JSON.stringify(row) + "\n";
  writer.write(line);
  bytes += line.length;
  count++;
  console.log(`${count}: ${row.repo_name}`);
  if (count % 10000 === 0) {
    const elapsed = (Date.now() - start) / 1000;
    const mbps = (bytes / 1024 / 1024 / elapsed).toFixed(2);
    console.log(`--- ${count.toLocaleString()} rows, ${(bytes / 1024 / 1024).toFixed(0)} MB, ${mbps} MB/s ---`);
  }
}

await writer.end();
console.log(`Done! ${count.toLocaleString()} rows, ${(bytes / 1024 / 1024 / 1024).toFixed(2)} GB`);
