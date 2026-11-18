import unittest

from aireview import prepare

CONFIG = {"ci_workflow": "ci.yml", "e2e_check": "e2e"}
SHA = "f" * 40


class FakeGitHub:
    def __init__(self, ci=None, e2e=None, jobs=()):
        self.ci, self.e2e, self.jobs = ci, e2e, list(jobs)

    def repo_get(self, path):
        if path.startswith("actions/workflows/"):
            return {"workflow_runs": [self.ci] if self.ci else []}
        if path.startswith("actions/runs/"):
            return {"jobs": self.jobs}
        if path.startswith(f"commits/{SHA}/check-runs"):
            return {"check_runs": self.e2e or []}
        raise AssertionError(path)

    def paginate(self, path):
        return [{"number": 7, "head": {"sha": SHA}}]


def ci(conclusion, status="completed"):
    return {"id": 1, "run_number": 1, "status": status, "conclusion": conclusion}


def check(conclusion, status="completed", id_=1):
    return {"id": id_, "status": status, "conclusion": conclusion, "html_url": f"https://github.com/o/r/runs/{id_}"}


class ChecksTest(unittest.TestCase):
    def status(self, gh):
        return prepare.checks(gh, SHA, CONFIG)

    def test_both_pass(self):
        self.assertEqual(self.status(FakeGitHub(ci("success"), [check("success")])), ("success", []))

    def test_waits_for_a_missing_e2e_check(self):
        self.assertEqual(self.status(FakeGitHub(ci("success"), [])), ("pending", []))

    def test_waits_for_a_queued_e2e_check(self):
        self.assertEqual(self.status(FakeGitHub(ci("success"), [check(None, "queued")]))[0], "pending")

    def test_waits_for_ci(self):
        self.assertEqual(self.status(FakeGitHub(ci(None, "in_progress"), [check("success")]))[0], "pending")

    def test_e2e_failure_lists_the_check(self):
        status, failed = self.status(FakeGitHub(ci("success"), [check("success", id_=1), check("failure", id_=2)]))
        self.assertEqual(status, "failure")
        self.assertEqual(failed, [{"name": "e2e", "html_url": "https://github.com/o/r/runs/2"}])

    def test_cancelled_e2e_check_waits(self):
        self.assertEqual(self.status(FakeGitHub(ci("success"), [check("cancelled")]))[0], "pending")

    def test_ci_failure_does_not_wait_for_e2e(self):
        jobs = [{"name": "lint", "html_url": "https://github.com/x", "conclusion": "failure"}]
        status, failed = self.status(FakeGitHub(ci("failure"), [], jobs))
        self.assertEqual((status, [j["name"] for j in failed]), ("failure", ["lint"]))


class TriggerTest(unittest.TestCase):
    def test_e2e_check_run_resolves_by_head(self):
        event = {"check_run": {"name": "e2e", "head_sha": SHA, "pull_requests": []}}
        self.assertEqual(prepare.resolve_trigger(FakeGitHub(), "check_run", event, CONFIG), (7, "auto", None))

    def test_other_check_runs_do_nothing(self):
        event = {"check_run": {"name": "DCO", "head_sha": SHA}}
        self.assertEqual(prepare.resolve_trigger(FakeGitHub(), "check_run", event, CONFIG)["action"], "none")


if __name__ == "__main__":
    unittest.main()
