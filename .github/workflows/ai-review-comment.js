// Posts the ai-review PR comment: the format step's comment.md, or the raw per-check reports
// when that step failed. Creates the comment or updates our earlier one.
// Called from ai-review.yml through actions/github-script; `reportsDir` holds <check>.md files.
module.exports = async ({ github, context, reportsDir }) => {
  const fs = require("fs");
  // Same order as the matrix; a missing file means that check failed or timed out.
  const checks = ["security", "logic", "protocol", "best-practices", "tests", "perf"];
  const title = (c) => c[0].toUpperCase() + c.slice(1).replace("-", " ");
  const run = `${context.serverUrl}/${context.repo.owner}/${context.repo.repo}/actions/runs/${context.runId}`;
  // GitHub caps a comment at 65536 characters; leave room for the table.
  const perReport = Math.floor(60000 / checks.length);

  const reports = checks.map((c) => {
    const path = `${reportsDir}/${c}.md`;
    if (!fs.existsSync(path)) return { c, summary: `did not run ([logs](${run}))`, body: "" };
    const text = fs.readFileSync(path, "utf8").trim();
    const [first, ...rest] = text.split("\n");
    const summary = first.startsWith("SUMMARY:") ? first.slice(8).trim() : "no summary line";
    let body = (first.startsWith("SUMMARY:") ? rest.join("\n") : text).trim();
    if (body.length > perReport) body = body.slice(0, perReport) + `\n\n_Truncated; see the ${c} artifact in the [run](${run})._`;
    return { c, summary, body };
  });

  // The comment body the format step wrote; without it, fall back to the raw reports.
  const formattedPath = `${reportsDir}/comment.md`;
  const formatted = fs.existsSync(formattedPath) ? fs.readFileSync(formattedPath, "utf8").trim() : "";

  const marker = "<!-- ai-review -->";
  const sha = context.payload.pull_request.head.sha.slice(0, 8);
  const footer = `<sub>AI-generated (${process.env.PROVIDER || "unknown provider"}); findings are unverified. [Run logs](${run}).</sub>`;
  const lines = formatted
    ? [marker, `## AI review of \`${sha}\``, "", formatted, "", footer]
    : [
        marker,
        `## AI review of \`${sha}\``,
        "",
        "| Check | Result |",
        "|---|---|",
        ...reports.map((r) => `| ${title(r.c)} | ${r.summary.replace(/\|/g, "\\|")} |`),
        "",
        ...reports.filter((r) => r.body).flatMap((r) => [
          "<details>",
          `<summary><b>${title(r.c)}</b>: ${r.summary}</summary>`,
          "",
          r.body,
          "",
          "</details>",
          "",
        ]),
        footer,
      ];
  const body = lines.join("\n");

  // Update our earlier comment instead of adding a new one per push.
  const { owner, repo } = context.repo;
  const issue_number = context.payload.pull_request.number;
  const comments = await github.paginate(github.rest.issues.listComments, { owner, repo, issue_number });
  const ours = comments.find((c) => c.user.type === "Bot" && c.body.startsWith(marker));
  if (ours) {
    await github.rest.issues.updateComment({ owner, repo, comment_id: ours.id, body });
  } else {
    await github.rest.issues.createComment({ owner, repo, issue_number, body });
  }
};
