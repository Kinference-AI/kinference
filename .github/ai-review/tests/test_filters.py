import json
import os
import unittest

from aireview import filters, schema

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DIFF = """diff --git a/src/a.rs b/src/a.rs
index 1..2 100644
--- a/src/a.rs
+++ b/src/a.rs
@@ -1,3 +1,4 @@ fn x()
 one
-two
+two changed
+three
 four
@@ -10,2 +11,2 @@
 ten
+eleven
diff --git a/old.rs b/old.rs
deleted file mode 100644
--- a/old.rs
+++ /dev/null
@@ -1 +0,0 @@
-gone
"""

SERIES = {"a" * 40: "Add a", "b" * 40: "Add b"}


def finding(**kw):
    base = {
        "key": "k",
        "severity": "blocking",
        "category": "correctness",
        "commit": None,
        "file": "src/a.rs",
        "line": 2,
        "message": "m",
        "suggestion": None,
    }
    return {**base, **kw}


class AddedLinesTest(unittest.TestCase):
    def test_tracks_new_line_numbers(self):
        self.assertEqual(filters.added_lines(DIFF), {"src/a.rs": {2, 3, 12}})


class SanitizeTest(unittest.TestCase):
    def test_strips_mentions_links_images_and_html(self):
        text = "@alice see [docs](https://x.y) ![i](https://x.y/i.png) https://evil.example <b>-->"
        out = filters.sanitize(text, 400)
        self.assertNotIn("@", out)
        self.assertNotIn("http", out)
        self.assertNotIn("<", out)
        self.assertIn("docs", out)

    def test_breaks_issue_references(self):
        self.assertNotIn("#12", filters.sanitize("see #12", 400))

    def test_truncates(self):
        self.assertEqual(len(filters.sanitize("x" * 500, 400)), 400)

    def test_suggestion_cannot_close_its_fence(self):
        self.assertEqual(filters.sanitize_code("a```b"), "ab")


class RiskFloorTest(unittest.TestCase):
    def test_highest_matching_rule_wins(self):
        with open(os.path.join(HERE, "risk-floor")) as f:
            rules = filters.parse_risk_floor(f.read())
        self.assertEqual(filters.risk_floor(["docs/x.md"], rules), ("low", None))
        self.assertEqual(filters.risk_floor(["src/config/a.rs", ".github/workflows/ci.yml"], rules)[0], "high")
        self.assertEqual(filters.risk_floor(["crates/kinference-router/Cargo.toml"], rules)[0], "high")

    def test_model_cannot_lower_the_floor(self):
        self.assertEqual(filters.max_level("high", "low"), "high")
        self.assertEqual(filters.max_level("low", "medium"), "medium")


class SizeTest(unittest.TestCase):
    def test_buckets(self):
        with open(os.path.join(HERE, "config.json")) as f:
            config = json.load(f)
        self.assertEqual(filters.size_label(10, 1, config), "s")
        self.assertEqual(filters.size_label(300, 5, config), "m")
        self.assertEqual(filters.size_label(1001, 5, config), "xl")
        self.assertEqual(filters.size_label(10, 31, config), "xl")


class HeuristicsTest(unittest.TestCase):
    def test_source_without_tests(self):
        self.assertTrue(filters.heuristics(["src/a.rs"], "+fn a() {}"))
        self.assertFalse(filters.heuristics(["src/a.rs"], "+    #[tokio::test]"))
        self.assertFalse(filters.heuristics(["src/a.rs", "tests/e2e/smoke.sh"], ""))

    def test_api_without_docs(self):
        self.assertTrue(filters.heuristics(["charts/kinference-crds/templates/x.yaml"], ""))
        self.assertFalse(filters.heuristics(["charts/kinference-crds/templates/x.yaml", "docs/a.md"], ""))


class FilterFindingsTest(unittest.TestCase):
    added = {"src/a.rs": {2, 3}}

    def run_filter(self, findings, dismissed=()):
        return filters.filter_findings(findings, self.added, SERIES, set(dismissed))

    def test_keeps_a_finding_on_an_added_line(self):
        kept, dropped = self.run_filter([finding()])
        self.assertEqual([k["key"] for k in kept], ["k"])
        self.assertTrue(kept[0]["inline"])
        self.assertEqual(dropped, [])

    def test_drops_a_finding_on_an_unchanged_line(self):
        kept, _ = self.run_filter([finding(line=1)])
        self.assertEqual(kept, [])

    def test_keeps_a_commit_finding_with_a_known_sha(self):
        kept, _ = self.run_filter([finding(file=None, line=None, commit="aaaaaaa")])
        self.assertEqual(kept[0]["commit"], "a" * 40)
        self.assertFalse(kept[0]["inline"])

    def test_drops_a_commit_finding_with_an_unknown_sha(self):
        kept, _ = self.run_filter([finding(file=None, line=None, commit="cccccccc")])
        self.assertEqual(kept, [])

    def test_drops_dismissed_and_duplicate_keys(self):
        kept, _ = self.run_filter([finding(key="Gone Key"), finding(key="dup"), finding(key="dup", line=3)], ["gone-key"])
        self.assertEqual([k["key"] for k in kept], ["dup"])


class SchemaTest(unittest.TestCase):
    def setUp(self):
        with open(os.path.join(HERE, "schema.json")) as f:
            self.schema = json.load(f)
        self.valid = {
            "assessment": {"risk": "low", "complexity": "low", "maintainer_summary": "", "ready_for_maintainer": True},
            "findings": [finding()],
            "previous_findings": [{"key": "k", "status": "open"}],
        }

    def test_accepts_valid_output(self):
        self.assertEqual(schema.validate(self.valid, self.schema), [])

    def test_rejects_bad_enum_extra_field_and_bool_line(self):
        self.valid["assessment"]["risk"] = "none"
        self.valid["findings"][0]["line"] = True
        self.valid["extra"] = 1
        self.assertEqual(len(schema.validate(self.valid, self.schema)), 3)


if __name__ == "__main__":
    unittest.main()
