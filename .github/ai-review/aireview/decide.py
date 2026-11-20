"""Turns the plan and the model output into labels, comments and a review request."""

from . import filters, render
from . import state as st

FAMILIES = ("ai-review/", "size/", "risk/", "complexity/")
KEEP = {"ai-review/skip"}


def _labels(status, size, assessment=None):
    out = {f"ai-review/{status}"}
    if size:
        out.add(f"size/{size}")
    if assessment:
        out |= {f"risk/{assessment['risk']}", f"complexity/{assessment['complexity']}"}
    return out


def _set_labels(writer, plan, wanted):
    current = set(plan["labels"]) - KEEP
    writer.set_labels(current, wanted, FAMILIES)


def _handoff(writer, plan, config, reason, error=None):
    state = plan["state"]
    state["handed_off"] = True
    status = "error" if error else "skipped" if plan["action"] == "skip_large" else "ready"
    _set_labels(writer, plan, _labels(status, plan.get("size"), state.get("assessment")))
    body = render.handoff(
        state, reason, plan.get("size"), plan.get("floor", "low"), plan.get("floor_glob"),
        plan.get("warnings", []), plan.get("subjects", {}), error,
    )
    writer.comment(plan.get("summary_comment") and {"id": plan["summary_comment"]}, body)
    writer.request_review(config["maintainers_team"])
    return body


def apply(plan, result, writer, phase, config):
    """Applies one run. Returns the summary comment body that it wrote, or None."""
    action = plan["action"]
    if action == "none":
        return None
    state = plan["state"]
    # Shadow mode rehearses the full flow and writes nothing.
    iterate = phase in ("full", "shadow") and not plan["author_is_maintainer"]
    existing = plan.get("summary_comment") and {"id": plan["summary_comment"]}

    if action == "ci_failed":
        if not iterate:
            return None
        _set_labels(writer, plan, _labels("changes-requested", plan["size"]))
        body = render.ci_failed(state, plan["head_sha"], plan["jobs"])
        writer.comment(existing, body)
        return body
    if action == "skip_large":
        reason = f"the pull request changes more than {config['max_changed_lines']} lines or {config['max_changed_files']} files"
        return _handoff(writer, plan, config, reason)
    if action == "handoff":
        return _handoff(writer, plan, config, "the author asked for a handoff")

    if "error" in result:
        return _handoff(writer, plan, config, "the review failed", error=result["error"])

    output = result["output"]
    added = {p: set(v) for p, v in plan["added"].items()}
    kept, dropped = filters.filter_findings(output["findings"], added, plan["subjects"], st.dismissed(state))
    for reason in dropped:
        print(f"dropped finding: {reason}")
    state["round"] += 1
    state["head"] = plan["head_sha"]
    model = output["assessment"]
    state["assessment"] = {
        "risk": filters.max_level(plan["floor"], model["risk"]),
        "complexity": model["complexity"],
        "maintainer_summary": filters.sanitize(model["maintainer_summary"], filters.SUMMARY_LIMIT),
    }
    new = st.merge(state, kept, output["previous_findings"], state["round"])

    cap = config["max_rounds"]
    blocking = st.open_blocking(state)
    if iterate and plan["trigger"] == "auto" and blocking and state["round"] < cap:
        inline = [f for f in new if f["severity"] == "blocking" and f["inline"]]
        if inline:
            comments = [{"path": f["file"], "line": f["line"], "side": "RIGHT", "body": render.inline(f)} for f in inline]
            writer.review(plan["head_sha"], comments)
        _set_labels(writer, plan, _labels("changes-requested", plan["size"], state["assessment"]))
        body = render.iteration(state, cap, plan["subjects"])
        writer.comment(existing, body)
        return body

    if not blocking:
        reason = "no blocking finding remains"
    elif plan["trigger"] == "rerun":
        reason = "a maintainer asked for a new review"
    elif plan["author_is_maintainer"]:
        reason = "a maintainer wrote the pull request"
    elif not iterate:
        reason = "the review runs without the contributor loop"
    else:
        reason = f"the review reached its limit of {cap} rounds"
    return _handoff(writer, plan, config, reason)
