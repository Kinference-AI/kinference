"""Renders the summary comment and the inline comments.

Every model-provided string reaches this module already sanitized.
Subjects and job names from the pull request are sanitized here.
"""

from . import state as st
from .filters import sanitize

READY_HINT = "Push a fix, or comment `/ai-review ready` to hand the pull request to a maintainer now."


def _where(f, subjects):
    if f.get("inline"):
        return f"`{f['file']}:{f['line']}`"
    subject = sanitize(subjects.get(f["commit"], ""), 80)
    return f"Commit {f['commit'][:12]} ({subject})"


def _items(findings, subjects):
    return "\n".join(f"- {_where(f, subjects)}: {f['message']}" for f in findings)


def iteration(state, cap, subjects):
    blocking = st.open_blocking(state)
    return "\n".join(
        [
            st.encode(state),
            f"The automated review found {len(blocking)} blocking problems in round {state['round']} of {cap}. {READY_HINT}",
            "",
            _items(blocking, subjects),
        ]
    )


def ci_failed(state, sha, jobs):
    rows = []
    for job in jobs:
        name = sanitize(job["name"], 120)
        url = job.get("html_url") or ""
        rows.append(f"- [{name}]({url})" if url.startswith("https://github.com/") else f"- {name}")
    return "\n".join(
        [
            st.encode(state),
            f"CI or the e2e suite failed on {sha[:12]}, so the automated review waits for a push that passes both. The failing checks:",
            "",
            *rows,
        ]
    )


def handoff(state, reason, size, floor, floor_glob, warnings, subjects, error=None):
    a = state.get("assessment") or {}
    lines = [st.encode(state), f"The automated review handed this pull request to the maintainers. Reason: {reason}.", ""]
    if error:
        lines += [f"The review failed: {error}. A maintainer can comment `/ai-review` to run it again.", ""]
    if a:
        lines.append(f"Risk: {a['risk']}. Complexity: {a['complexity']}. Size: {size}.")
        if floor != "low" and floor_glob:
            lines.append(f"The risk is at least {floor} because the pull request changes files that match `{floor_glob}`.")
        lines += ["", a["maintainer_summary"], ""]
    elif size:
        lines += [f"Size: {size}.", ""]
    blocking = st.open_blocking(state)
    if blocking:
        lines += ["Unresolved blocking findings:", "", _items(blocking, subjects), ""]
    if warnings:
        lines += ["Warnings:", "", *[f"- {w}" for w in warnings], ""]
    advisory = st.advisory(state)
    if advisory:
        lines += [
            f"<details><summary>Advisory findings ({len(advisory)})</summary>",
            "",
            _items(advisory, subjects),
            "",
            "</details>",
        ]
    dismissed = len(st.dismissed(state))
    if dismissed:
        lines.append(f"Maintainers dismissed {dismissed} findings.")
    return "\n".join(lines).rstrip() + "\n"


def inline(f):
    body = [f["message"]]
    if f.get("suggestion"):
        body += ["", "```suggestion", f["suggestion"].rstrip("\n"), "```"]
    body += ["", "<sub>Blocking. A maintainer can reply `/dismiss` to drop this finding.</sub>", st.KEY_MARKER.format(f["key"])]
    return "\n".join(body)
