import copy
import unittest

from aireview import decide, state

CONFIG = {"max_rounds": 3, "max_changed_lines": 1000, "max_changed_files": 30, "maintainers_team": "maintainers"}
SHA = "f" * 40
SERIES = {"a" * 40: "Add a"}


class FakeWriter:
    def __init__(self):
        self.labels, self.comments, self.reviews, self.requested = None, [], [], []

    def set_labels(self, current, wanted, families):
        self.labels = wanted

    def comment(self, existing, body):
        self.comments.append(body)

    def review(self, sha, comments):
        self.reviews.append(comments)

    def request_review(self, team):
        self.requested.append(team)


def make_plan(**kw):
    plan = {
        "action": "review",
        "number": 7,
        "head_sha": SHA,
        "trigger": "auto",
        "comment_id": None,
        "author_is_maintainer": False,
        "labels": ["ai-review/skip-not", "size/m"],
        "summary_comment": None,
        "state": state.empty(),
        "size": "s",
        "added": {"src/a.rs": [2, 3]},
        "subjects": SERIES,
        "floor": "medium",
        "floor_glob": "src/config/*",
        "warnings": [],
    }
    return {**plan, **kw}


def output(findings=(), previous=(), risk="low"):
    return {
        "output": {
            "assessment": {"risk": risk, "complexity": "low", "maintainer_summary": "Look at @bob's lock.", "ready_for_maintainer": not findings},
            "findings": list(findings),
            "previous_findings": list(previous),
        }
    }


def blocking(key="bug", line=2, severity="blocking"):
    return {"key": key, "severity": severity, "category": "correctness", "commit": None, "file": "src/a.rs", "line": line, "message": "Breaks.", "suggestion": None}


class DecideTest(unittest.TestCase):
    def apply(self, plan, result, phase="full"):
        w = FakeWriter()
        body = decide.apply(plan, result, w, phase, CONFIG)
        return w, body

    def test_blocking_finding_asks_the_contributor(self):
        plan = make_plan()
        w, body = self.apply(plan, output([blocking()]))
        self.assertIn("ai-review/changes-requested", w.labels)
        self.assertIn("risk/medium", w.labels)
        self.assertEqual(len(w.reviews[0]), 1)
        self.assertEqual(w.requested, [])
        self.assertEqual(state.decode(body)["round"], 1)

    def test_no_blocking_finding_hands_off(self):
        w, body = self.apply(make_plan(), output([blocking(severity="advisory")]))
        self.assertIn("ai-review/ready", w.labels)
        self.assertEqual(w.reviews, [])
        self.assertEqual(w.requested, ["maintainers"])
        self.assertIn("Advisory findings (1)", body)
        self.assertNotIn("@bob", body)
        self.assertTrue(state.decode(body)["handed_off"])

    def test_round_cap_hands_off_with_open_findings(self):
        s = state.empty()
        s["round"] = 2
        w, body = self.apply(make_plan(state=s), output([blocking()]))
        self.assertIn("ai-review/ready", w.labels)
        self.assertIn("limit of 3 rounds", body)
        self.assertIn("Unresolved blocking findings", body)

    def test_reported_again_finding_is_not_posted_twice(self):
        w, body = self.apply(make_plan(), output([blocking()]))
        plan = make_plan(state=state.decode(body))
        w, body = self.apply(plan, output([blocking(line=3), blocking(key="other")]))
        self.assertEqual([c["line"] for c in w.reviews[0]], [2])
        self.assertEqual(state.decode(body)["round"], 2)

    def test_addressed_finding_closes(self):
        _, body = self.apply(make_plan(), output([blocking()]))
        w, body = self.apply(make_plan(state=state.decode(body)), output(previous=[{"key": "bug", "status": "addressed"}]))
        self.assertIn("ai-review/ready", w.labels)

    def test_dismissed_finding_is_never_raised_again(self):
        _, body = self.apply(make_plan(), output([blocking()]))
        s = state.decode(body)
        state.dismiss(s, {"bug"})
        w, body = self.apply(make_plan(state=s), output([blocking()]))
        self.assertIn("ai-review/ready", w.labels)
        self.assertIn("dismissed 1", body)

    def test_maintainer_author_gets_no_loop(self):
        w, _ = self.apply(make_plan(author_is_maintainer=True), output([blocking()]))
        self.assertEqual(w.reviews, [])
        self.assertIn("ai-review/ready", w.labels)

    def test_handoff_phase_has_no_loop(self):
        w, body = self.apply(make_plan(), output([blocking()]), phase="handoff")
        self.assertEqual(w.reviews, [])
        self.assertIn("without the contributor loop", body)

    def test_model_error_hands_off(self):
        w, body = self.apply(make_plan(), {"error": "the model declined the review"})
        self.assertIn("ai-review/error", w.labels)
        self.assertIn("declined", body)

    def test_ci_failure_lists_jobs_without_a_review(self):
        jobs = [{"name": "lint @x", "html_url": "https://github.com/o/r/actions/runs/1/job/2"}, {"name": "x", "html_url": "https://evil.example"}]
        w, body = self.apply(make_plan(action="ci_failed", jobs=jobs), {})
        self.assertIn("ai-review/changes-requested", w.labels)
        self.assertIn("[lint x](https://github.com/", body)
        self.assertNotIn("evil", body)

    def test_ci_failure_is_silent_in_handoff_phase(self):
        w, body = self.apply(make_plan(action="ci_failed", jobs=[]), {}, phase="handoff")
        self.assertIsNone(body)

    def test_state_round_trips(self):
        s = state.empty()
        s["findings"].append({"key": "k", "message": "--> <!--"})
        self.assertEqual(state.decode(state.encode(copy.deepcopy(s))), s)


if __name__ == "__main__":
    unittest.main()
