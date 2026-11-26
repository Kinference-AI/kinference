#!/usr/bin/env python3
"""Entry point for the ai-review workflow: `review.py prepare|agent|decide`.

The three steps share files in $AI_REVIEW_WORK. Only `prepare` and
`decide` see the GitHub token, and only `agent` sees the model API key.
"""

import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
BASE_DIR = os.path.dirname(os.path.dirname(HERE))
PHASES = ("shadow", "handoff", "full")


def _load(name):
    with open(os.path.join(HERE, name)) as f:
        return json.load(f) if name.endswith(".json") else f.read()


def _work(name):
    return os.path.join(os.environ["AI_REVIEW_WORK"], name)


def _summary(text):
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if path:
        with open(path, "a") as f:
            f.write(text + "\n")


def _gh():
    from aireview.gh import GitHub

    return GitHub(os.environ["GH_TOKEN"], os.environ["GITHUB_REPOSITORY"])


def prepare():
    from aireview import prepare as p

    os.makedirs(os.environ["AI_REVIEW_WORK"], exist_ok=True)
    plan = p.plan(_gh(), _load("config.json"), os.environ, os.environ["AI_REVIEW_WORK"], BASE_DIR)
    with open(_work("plan.json"), "w") as f:
        json.dump(plan, f)
    reason = f": {plan['reason']}" if plan.get("reason") else ""
    number = plan.get("number")
    if number is None:
        print(f"pr: none, action: {plan['action']}{reason}")
        _summary(f"No pull request, action {plan['action']}{reason}.")
    else:
        url = f"{os.environ['GITHUB_SERVER_URL']}/{os.environ['GITHUB_REPOSITORY']}/pull/{number}"
        print(f"pr: {number}, action: {plan['action']}{reason}")
        _summary(f"Pull request [#{number}]({url}), action {plan['action']}{reason}.")
    with open(os.environ["GITHUB_OUTPUT"], "a") as f:
        f.write(f"action={plan['action']}\n")


def agent():
    from aireview import agent as a

    with open(_work("input.json")) as f:
        inp = json.load(f)
    result = a.review(_load("config.json"), _load("schema.json"), _load("prompt.md"), BASE_DIR, _work("tree"), inp)
    with open(_work("result.json"), "w") as f:
        json.dump(result, f)
    u = result["usage"]
    _summary(f"Model tokens: {u['input']} input, {u['cache_read']} read from cache, {u['output']} output.")
    if "error" in result:
        print(f"::warning::review failed: {result['error']}")


def decide():
    from aireview import decide as d
    from aireview.gh import Writer

    phase = os.environ.get("AI_REVIEW_PHASE", "")
    if phase not in PHASES:
        sys.exit(f"AI_REVIEW_PHASE must be one of {', '.join(PHASES)}")
    with open(_work("plan.json")) as f:
        plan = json.load(f)
    result = {}
    if os.path.exists(_work("result.json")):
        with open(_work("result.json")) as f:
            result = json.load(f)
    elif plan["action"] == "review":
        result = {"error": "the model step did not run"}
    gh = _gh()
    writer = Writer(gh, plan.get("number"), dry=phase == "shadow")
    if plan.get("comment_id"):
        writer.react(plan["comment_id"])
    body = d.apply(plan, result, writer, phase, _load("config.json"))
    lines = [f"Phase {phase}, action {plan['action']}{': ' + plan['reason'] if plan.get('reason') else ''}.", ""]
    if phase == "shadow":
        lines += ["Writes that the full flow would make:", ""] + [f"- {what}" for what, _ in writer.log]
        for what, payload in writer.log:
            if what.startswith("post") and payload and payload.get("comments"):
                lines += ["", "Inline findings:", ""] + [f"- `{c['path']}:{c['line']}`: {c['body'].splitlines()[0]}" for c in payload["comments"]]
    if body:
        lines += ["", "Summary comment:", "", "````markdown", body, "````"]
    _summary("\n".join(lines))


if __name__ == "__main__":
    {"prepare": prepare, "agent": agent, "decide": decide}[sys.argv[1]]()
