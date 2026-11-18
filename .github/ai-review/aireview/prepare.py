"""Decides what the run does and gathers its inputs. This step never writes to GitHub."""

import json
import os
import re

from . import filters, gitio
from . import state as st
from .gh import BOT

_ISSUE_REF = re.compile(r"(?:closes|fixes|resolves|part of) #(\d+)", re.I)


def none(reason):
    return {"action": "none", "reason": reason}


def _command(body):
    first = (body or "").strip().splitlines()[0].strip() if (body or "").strip() else ""
    if first == "/ai-review ready":
        return "ready"
    if first == "/ai-review":
        return "rerun"
    return None


def _pr_for_sha(gh, sha):
    # A check run on a fork's commit lists no pull requests, so match the head.
    for pr in gh.paginate("pulls?state=open"):
        if pr["head"]["sha"] == sha:
            return pr["number"], "auto", None
    return none("no open pull request has the run's head")


def event_sha(event_name, event):
    """The head that a CI or e2e completion reports on, or None for other events."""
    if event_name == "workflow_run":
        return event["workflow_run"]["head_sha"]
    if event_name == "check_run":
        return event["check_run"]["head_sha"]
    return None


def resolve_trigger(gh, event_name, event, config):
    """Returns (pull request number, trigger, comment) or a `none` plan."""
    if event_name == "workflow_run":
        if event["workflow_run"]["event"] != "pull_request":
            return none("the CI run is not for a pull request")
        return _pr_for_sha(gh, event_sha(event_name, event))
    if event_name == "check_run":
        if event["check_run"]["name"] != config["e2e_check"]:
            return none("the check run is not the e2e check")
        return _pr_for_sha(gh, event_sha(event_name, event))
    if event_name == "pull_request_target":
        return event["pull_request"]["number"], "auto", None
    if event_name == "issue_comment":
        if "pull_request" not in event["issue"]:
            return none("the comment is not on a pull request")
        command = _command(event["comment"]["body"])
        if command is None:
            return none("the comment holds no command")
        return event["issue"]["number"], command, event["comment"]
    if event_name == "workflow_dispatch":
        return int(event["inputs"]["pr"]), "rerun", None
    return none(f"unsupported event {event_name}")


def _authorized(gh, pr, trigger, comment):
    if comment is None:
        return True
    login, assoc = comment["user"]["login"], comment.get("author_association")
    if trigger == "ready" and login == pr["user"]["login"]:
        return True
    return gh.is_maintainer(login, assoc)


def _policy_passed(gh, sha, name):
    runs = gh.repo_get(f"commits/{sha}/check-runs?check_name={name.replace(' ', '%20')}")["check_runs"]
    return bool(runs) and runs[0]["conclusion"] == "success"


def _ci(gh, sha, workflow):
    runs = gh.repo_get(f"actions/workflows/{workflow}/runs?head_sha={sha}&event=pull_request")["workflow_runs"]
    if not runs:
        return "pending", []
    run = max(runs, key=lambda r: r["run_number"])
    if run["status"] != "completed":
        return "pending", []
    if run["conclusion"] == "success":
        return "success", []
    if run["conclusion"] not in ("failure", "timed_out"):
        return "pending", []
    jobs = gh.repo_get(f"actions/runs/{run['id']}/jobs?filter=latest&per_page=100")["jobs"]
    failed = [{"name": j["name"], "html_url": j["html_url"]} for j in jobs if j["conclusion"] in ("failure", "timed_out")]
    return "failure", failed


def _e2e(gh, sha, name):
    runs = gh.repo_get(f"commits/{sha}/check-runs?check_name={name}&filter=latest")["check_runs"]
    if not runs:
        return "pending", []
    run = max(runs, key=lambda r: r["id"])
    if run["status"] != "completed":
        return "pending", []
    if run["conclusion"] == "success":
        return "success", []
    # A cancelled run means a newer dispatch replaced it.
    if run["conclusion"] not in ("failure", "timed_out"):
        return "pending", []
    return "failure", [{"name": name, "html_url": run["html_url"]}]


def checks(gh, sha, config):
    """Combines CI and the e2e check into one of pending, success or failure."""
    results = [_ci(gh, sha, config["ci_workflow"]), _e2e(gh, sha, config["e2e_check"])]
    failed = [job for status, jobs in results if status == "failure" for job in jobs]
    if any(status == "failure" for status, _ in results):
        return "failure", failed
    if any(status == "pending" for status, _ in results):
        return "pending", []
    return "success", []


def _dismissals(gh, number):
    """Keys of bot findings that a maintainer answered with /dismiss."""
    comments = gh.paginate(f"pulls/{number}/comments")
    keys = {c["id"]: st.finding_key(c["body"]) for c in comments if c["user"]["login"] == BOT}
    out = set()
    for c in comments:
        key = keys.get(c.get("in_reply_to_id"))
        if key and c["body"].strip().startswith("/dismiss") and gh.is_maintainer(c["user"]["login"], c.get("author_association")):
            out.add(key)
    return out


def _linked_issue(gh, pr):
    data = gh.graphql(
        """query($owner: String!, $name: String!, $number: Int!) {
          repository(owner: $owner, name: $name) {
            pullRequest(number: $number) { closingIssuesReferences(first: 1) { nodes { number } } }
          }
        }""",
        number=pr["number"],
    )
    nodes = data["repository"]["pullRequest"]["closingIssuesReferences"]["nodes"]
    number = nodes[0]["number"] if nodes else None
    if number is None:
        m = _ISSUE_REF.search(pr.get("body") or "")
        number = int(m.group(1)) if m else None
    if number is None:
        return None
    issue = gh.repo_get(f"issues/{number}")
    return {"number": number, "title": issue["title"], "body": issue.get("body") or ""}


def plan(gh, config, env, work, base_dir):
    with open(env["GITHUB_EVENT_PATH"]) as f:
        event = json.load(f)
    resolved = resolve_trigger(gh, env["GITHUB_EVENT_NAME"], event, config)
    if isinstance(resolved, dict):
        return resolved
    number, trigger, comment = resolved
    pr = gh.repo_get(f"pulls/{number}")
    if pr["state"] != "open":
        return none("the pull request is closed")
    if not _authorized(gh, pr, trigger, comment):
        return none("the commenter may not run this command")
    auto = trigger == "auto"
    labels = {label["name"] for label in pr["labels"]}
    if auto and pr["draft"]:
        return none("the pull request is a draft")
    if auto and pr["user"]["type"] == "Bot":
        return none("a bot opened the pull request")
    if auto and "ai-review/skip" in labels:
        return none("the pull request has ai-review/skip")

    existing = gh.bot_comment(number, st.MARKER)
    state = st.decode(existing["body"] if existing else "")
    st.dismiss(state, _dismissals(gh, number))
    if auto and state["handed_off"]:
        return none("the review already handed off")

    sha = pr["head"]["sha"]
    reported = event_sha(env["GITHUB_EVENT_NAME"], event)
    if auto and reported and reported != sha:
        return none("the run is for an older head")
    if auto and state["head"] == sha:
        return none("the review already covered this head")
    lines, files = pr["additions"] + pr["deletions"], pr["changed_files"]
    author = pr["user"]["login"]
    base = {
        "number": number,
        "head_sha": sha,
        "trigger": trigger,
        "comment_id": comment["id"] if comment else None,
        "author_is_maintainer": gh.is_maintainer(author, pr.get("author_association")),
        "labels": sorted(labels),
        "summary_comment": existing["id"] if existing else None,
        "state": state,
        "size": filters.size_label(lines, files, config),
    }
    if trigger == "ready":
        return {**base, "action": "handoff"}
    if base["size"] == "xl":
        return {**base, "action": "skip_large"}
    if auto:
        if not _policy_passed(gh, sha, config["policy_check"]):
            return none("the pull request policy check has not passed")
        status, jobs = checks(gh, sha, config)
        if status == "pending":
            return none("CI or the e2e check has not finished")
        if status == "failure":
            return {**base, "action": "ci_failed", "jobs": jobs}

    gitdir = os.path.join(work, "git")
    gitio.fetch(gitdir, gh.repo, number, pr["base"]["ref"])
    if gitio.git(gitdir, "rev-parse", "refs/remotes/pr").strip() != sha:
        return none("the pull request moved during the run")
    mb = gitio.merge_base(gitdir, sha)
    diff = gitio.diff(gitdir, mb, sha)
    paths = [p for p, _ in gitio.numstat(gitdir, mb, sha)]
    with open(os.path.join(base_dir, ".github/ai-review/risk-floor")) as f:
        floor, floor_glob = filters.risk_floor(paths, filters.parse_risk_floor(f.read()))
    series, autosquashed = gitio.autosquash(gitdir, mb, sha)
    since_last = None
    if state["head"] and state["head"] != sha and gitio.has_commit(gitdir, state["head"]):
        since_last = gitio.diff(gitdir, state["head"], sha)
    gitio.extract(gitdir, sha, os.path.join(work, "tree"))

    agent_input = {
        "title": pr["title"],
        "description": pr.get("body") or "",
        "issue": _linked_issue(gh, pr),
        "diff": diff,
        "series": [{"sha": c, "show": gitio.show(gitdir, c)} for c in series],
        "autosquashed": autosquashed,
        "since_last": since_last,
        "previous": [f for f in state["findings"] if f["status"] != "dismissed"],
    }
    with open(os.path.join(work, "input.json"), "w") as f:
        json.dump(agent_input, f)
    return {
        **base,
        "action": "review",
        "added": {p: sorted(v) for p, v in filters.added_lines(diff).items()},
        "subjects": {c: gitio.subject(gitdir, c) for c in series},
        "floor": floor,
        "floor_glob": floor_glob,
        "warnings": filters.heuristics(paths, diff),
    }
