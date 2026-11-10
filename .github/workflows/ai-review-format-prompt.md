Below are full reports from separate review checks of one pull request. Write the PR comment that presents them. Read only the files you need for code snippets.

Rules:

- Write for anyone on the team, including someone new to this code. Plain language, no jargon.
- Keep only findings a report presents as real. Leave out anything it marks refuted, dropped, out of scope, already decided on the PR, or "checked and clean".
- Merge a finding that several checks raised into one, and name those checks.
- Do not add findings, and do not change a finding's meaning or severity.
- Every line of prose is 200 characters or less; code snippets don't count.

Output exactly this markdown, and nothing else:

**Verdict:** one of `looks good`, `minor fixes`, `needs changes`, then why, in 200 characters or less.

Then at most 5 bullets, most important first: the problem and why it matters, then the checks in parentheses.

Then a table with one row per check, in the order the reports appear: `| Check | Result |`, where Result is a short count such as `2 findings: 1 high, 1 low` or `no findings`.

Then, for each check that has findings, a collapsed section:

<details>
<summary><b>Check name</b>: the same result as in the table</summary>

### <severity>: <one-line title in plain language>

What goes wrong and when, in 200 characters or less.

```rust
// path/to/file.rs:LINE
<the 3 to 8 lines that show the problem, copied from the code>
```

**Fix:** 200 characters or less.

</details>

At most 5 findings per check, most severe first. A merged finding appears once, under the check that explains it best.
