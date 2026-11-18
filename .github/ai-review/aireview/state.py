"""The review state that the bot keeps in a hidden marker of its summary comment."""

import base64
import json
import re

MARKER = "<!-- kinference-ai-review -->"
_STATE = re.compile(r"<!-- ai-review-state: ([A-Za-z0-9+/=]+) -->")
KEY_MARKER = "<!-- ai-review-key: {} -->"
_KEY = re.compile(r"<!-- ai-review-key: ([a-z0-9-]+) -->")


def empty():
    return {"round": 0, "head": None, "handed_off": False, "assessment": None, "findings": []}


def encode(state):
    # Base64 keeps model text out of the HTML comment, so no value can close it.
    blob = base64.b64encode(json.dumps(state, sort_keys=True).encode()).decode()
    return f"{MARKER}\n<!-- ai-review-state: {blob} -->"


def decode(body):
    m = _STATE.search(body or "")
    if not m:
        return empty()
    try:
        state = json.loads(base64.b64decode(m.group(1)))
    except ValueError:
        return empty()
    return {**empty(), **state}


def finding_key(body):
    m = _KEY.search(body or "")
    return m.group(1) if m else None


def dismissed(state):
    return {f["key"] for f in state["findings"] if f["status"] == "dismissed"}


def dismiss(state, keys):
    for f in state["findings"]:
        if f["key"] in keys:
            f["status"] = "dismissed"


def merge(state, kept, previous, round_):
    """Folds one round's findings into the state and returns the findings that are new.

    The agent's `previous_findings` can only move an open finding to
    addressed. A previous finding that the agent doesn't mention stays as
    it was. A finding that the agent reports again keeps its comment.
    """
    by_key = {f["key"]: f for f in state["findings"]}
    for p in previous:
        f = by_key.get(p.get("key"))
        if f and f["status"] == "open" and p.get("status") == "addressed":
            f["status"] = "addressed"
    new = []
    for k in kept:
        f = by_key.get(k["key"])
        if f is None:
            f = {**k, "status": "open", "round": round_, "comment_id": None}
            state["findings"].append(f)
            by_key[k["key"]] = f
            new.append(f)
        elif f["status"] != "dismissed":
            f.update({x: k[x] for x in ("severity", "category", "commit", "message")})
            if k["inline"]:
                f.update(file=k["file"], line=k["line"], inline=True, suggestion=k["suggestion"])
            f["status"] = "open"
    return new


def open_blocking(state):
    return [f for f in state["findings"] if f["status"] == "open" and f["severity"] == "blocking"]


def advisory(state):
    return [f for f in state["findings"] if f["status"] == "open" and f["severity"] == "advisory"]
