import os
import tempfile
import unittest

from aireview.tools import ToolError, Tools


class ToolsTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = os.path.join(self.tmp.name, "tree")
        os.makedirs(os.path.join(root, "src"))
        with open(os.path.join(root, "src", "a.rs"), "w") as f:
            f.write("fn a() {}\nfn b() {}\n")
        with open(os.path.join(self.tmp.name, "secret"), "w") as f:
            f.write("token")
        os.symlink(os.path.join(self.tmp.name, "secret"), os.path.join(root, "link"))
        os.symlink("/proc/self", os.path.join(root, "src", "proc"))
        self.tools = Tools(root)

    def tearDown(self):
        self.tmp.cleanup()

    def test_reads_numbered_lines(self):
        self.assertEqual(self.tools.read_file("src/a.rs", 2, None), "2\tfn b() {}")

    def test_refuses_paths_outside_the_root(self):
        for path in ("../secret", "/etc/passwd", "src/../../secret"):
            with self.assertRaises(ToolError):
                self.tools.read_file(path)

    def test_refuses_symlinks(self):
        with self.assertRaises(ToolError):
            self.tools.read_file("link")
        with self.assertRaises(ToolError):
            self.tools.list_dir("src/proc")
        with self.assertRaises(ToolError):
            self.tools.read_file("src/proc/environ")

    def test_grep_skips_symlinks(self):
        out = self.tools.grep("token|fn b", None, None)
        self.assertEqual(out, "src/a.rs:2: fn b() {}")

    def test_lists_a_directory(self):
        self.assertEqual(self.tools.list_dir("src"), "a.rs")

    def test_run_rejects_unknown_tools(self):
        with self.assertRaises(ToolError):
            self.tools.run("bash", {})


if __name__ == "__main__":
    unittest.main()
