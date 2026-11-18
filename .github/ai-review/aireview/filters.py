"""Deterministic checks and the post-filter that every model finding passes."""

import fnmatch
import re

LEVELS = ["low", "medium", "high"]
MESSAGE_LIMIT = 400
SUMMARY_LIMIT = 1200
SUGGESTION_LIMIT = 2000

_IMAGE = re.compile(r"!\[[^\]]*\]\([^)]*\)")
_LINK = re.compile(r"\[([^\]]*)\]\([^)]*\)")
_URL = re.compile(r"\b[a-z][a-z0-9+.-]*://\S+", re.I)
_MENTION = re.compile(r"@(?=[A-Za-z0-9])")
# GitHub turns #123 and owner/repo#123 into references that show up on
# the other issue's timeline.
_ISSUE_REF = re.compile(r"#(?=\d)")
_HUNK = re.compile(r"^@@ -\d+(?:,\d+)? \+(\d+)(?:,\d+)? @@")


def sanitize(text, limit, single_line=True):
    """Strips images, links, URLs, mentions, issue references and HTML."""
    text = _IMAGE.sub("", text or "")
    text = _LINK.sub(r"\1", text)
    text = _URL.sub("", text)
    text = _MENTION.sub("", text)
    text = _ISSUE_REF.sub("#​", text)
    text = text.replace("<", "&lt;").replace(">", "&gt;")
    if single_line:
        text = " ".join(text.split())
    if len(text) > limit:
        text = text[: limit - 1].rstrip() + "…"
    return text


def sanitize_code(text):
    """Keeps a suggestion's lines and drops anything that would end its fence."""
    if not text:
        return None
    text = text.replace("```", "")
    if len(text) > SUGGESTION_LIMIT or not text.strip():
        return None
    return text


def normalize_key(key):
    key = re.sub(r"[^a-z0-9-]+", "-", (key or "").lower()).strip("-")
    return key[:64] or None


def added_lines(diff):
    """Maps each path in a unified diff to the line numbers that it adds."""
    added, path, line = {}, None, 0
    for raw in diff.splitlines():
        if raw.startswith("diff --git "):
            path = None
        elif raw.startswith("+++ "):
            target = raw[4:]
            path = None if target == "/dev/null" else target[2:] if target.startswith("b/") else target
            if path is not None:
                added.setdefault(path, set())
        elif raw.startswith("@@"):
            m = _HUNK.match(raw)
            line = int(m.group(1)) if m else 0
        elif path is None:
            continue
        elif raw.startswith("+"):
            added[path].add(line)
            line += 1
        elif raw.startswith(" "):
            line += 1
    return added


def parse_risk_floor(text):
    rules = []
    for raw in text.splitlines():
        raw = raw.strip()
        if not raw or raw.startswith("#"):
            continue
        glob, level = raw.split()
        if level not in LEVELS:
            raise ValueError(f"risk-floor: unknown level {level!r}")
        rules.append((glob, level))
    return rules


def risk_floor(paths, rules):
    """Returns the highest level among the rules that match a path, and that rule's glob."""
    best, why = "low", None
    for path in paths:
        for glob, level in rules:
            if fnmatch.fnmatchcase(path, glob) and LEVELS.index(level) > LEVELS.index(best):
                best, why = level, glob
    return best, why


def max_level(a, b):
    return a if LEVELS.index(a) >= LEVELS.index(b) else b


def size_label(lines, files, config):
    if lines > config["max_changed_lines"] or files > config["max_changed_files"]:
        return "xl"
    for name, limit in config["size_buckets"].items():
        if lines <= limit:
            return name
    return "xl"


_TEST_ATTR = re.compile(r"^\+\s*#\[(tokio::)?test")
_API_PATHS = ("charts/kinference-crds/", "crates/kinference-router/proto/", "charts/kinference/values.yaml")


def heuristics(paths, diff):
    """Advisory warnings for the handoff summary. They never block."""
    warnings = []
    rust = [p for p in paths if p.endswith(".rs") and "/tests/" not in p and not p.startswith("tests/")]
    tests = any(p.startswith("tests/") or "/tests/" in p for p in paths) or any(
        _TEST_ATTR.match(line) for line in diff.splitlines()
    )
    if rust and not tests:
        warnings.append("Rust source changed, and no test changed.")
    api = any(p.startswith(_API_PATHS) for p in paths)
    docs = any(p.startswith("docs/") or p.endswith(".md") for p in paths)
    if api and not docs:
        warnings.append("A CRD, the picker protocol or the chart values changed, and no doc changed.")
    return warnings


def _resolve_commit(commit, series):
    commit = (commit or "").strip().lower()
    if len(commit) < 7:
        return None
    matches = [sha for sha in series if sha.startswith(commit)]
    return matches[0] if len(matches) == 1 else None


def filter_findings(findings, added, series, dismissed):
    """Applies the post-filter. Returns the findings that survive and one reason per dropped finding.

    A finding survives when its line is one that the final diff adds, or
    when it names a commit of the autosquashed series. Each survivor gets
    `inline` set to whether it can anchor to a line.
    """
    kept, dropped, seen = [], [], set()
    for f in findings:
        key = normalize_key(f.get("key"))
        if key is None or key in seen:
            dropped.append(f"duplicate or empty key {f.get('key')!r}")
            continue
        if key in dismissed:
            dropped.append(f"{key}: dismissed by a maintainer")
            continue
        commit = _resolve_commit(f.get("commit"), series)
        path, line = f.get("file"), f.get("line")
        inline = bool(path and isinstance(line, int) and line in added.get(path, ()))
        if not inline and commit is None:
            dropped.append(f"{key}: not on an added line and names no commit of the series")
            continue
        seen.add(key)
        kept.append(
            {
                "key": key,
                "severity": f["severity"],
                "category": f["category"],
                "commit": commit,
                "file": path if inline else None,
                "line": line if inline else None,
                "inline": inline,
                "message": sanitize(f.get("message"), MESSAGE_LIMIT),
                "suggestion": sanitize_code(f.get("suggestion")) if inline else None,
            }
        )
    return kept, dropped
