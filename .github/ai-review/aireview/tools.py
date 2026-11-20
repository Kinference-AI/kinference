"""Read-only file tools that stay inside the pull request checkout.

A path resolves inside the root or the tool refuses it. The tools never
follow a symlink, so a pull request can't point one at a runner file.
"""

import fnmatch
import os
import re

MAX_FILE_BYTES = 1_000_000
MAX_LINES = 2000
MAX_MATCHES = 200
MAX_ENTRIES = 500


class ToolError(Exception):
    pass


def _nullable(kind, description):
    return {"type": [kind, "null"], "description": description}


DEFINITIONS = [
    {
        "name": "read_file",
        "description": "Read a file of the repository at the pull request head. Returns numbered lines.",
        "strict": True,
        "input_schema": {
            "type": "object",
            "properties": {
                "path": {"type": "string", "description": "Path relative to the repository root."},
                "start_line": _nullable("integer", "First line, 1-based. Null reads from the start."),
                "end_line": _nullable("integer", "Last line, inclusive. Null reads to the end."),
            },
            "required": ["path", "start_line", "end_line"],
            "additionalProperties": False,
        },
    },
    {
        "name": "grep",
        "description": "Search the repository at the pull request head with a Python regular expression.",
        "strict": True,
        "input_schema": {
            "type": "object",
            "properties": {
                "pattern": {"type": "string"},
                "path": _nullable("string", "Directory or file to search. Null searches the whole repository."),
                "glob": _nullable("string", "Only search files whose path matches this glob, such as *.rs."),
            },
            "required": ["pattern", "path", "glob"],
            "additionalProperties": False,
        },
    },
    {
        "name": "list_dir",
        "description": "List a directory of the repository at the pull request head. Directories end with /.",
        "strict": True,
        "input_schema": {
            "type": "object",
            "properties": {"path": {"type": "string", "description": "Use . for the root."}},
            "required": ["path"],
            "additionalProperties": False,
        },
    },
]


class Tools:
    def __init__(self, root):
        self.root = os.path.realpath(root)

    def resolve(self, rel):
        if not isinstance(rel, str) or os.path.isabs(rel) or "\0" in rel:
            raise ToolError("path must be relative to the repository root")
        path = os.path.join(self.root, os.path.normpath(rel))
        real = os.path.realpath(path)
        if real != self.root and not real.startswith(self.root + os.sep):
            raise ToolError("path is outside the repository")
        probe = real
        while probe != self.root:
            if os.path.islink(probe):
                raise ToolError("path goes through a symlink")
            probe = os.path.dirname(probe)
        if not os.path.exists(real):
            raise ToolError("no such file or directory")
        return real

    def _rel(self, path):
        return os.path.relpath(path, self.root)

    def run(self, name, args):
        fn = {"read_file": self.read_file, "grep": self.grep, "list_dir": self.list_dir}.get(name)
        if fn is None:
            raise ToolError(f"unknown tool {name}")
        return fn(**args)

    def read_file(self, path, start_line=None, end_line=None):
        real = self.resolve(path)
        if not os.path.isfile(real):
            raise ToolError("not a file")
        if os.path.getsize(real) > MAX_FILE_BYTES:
            raise ToolError("file is larger than 1 MB")
        with open(real, "rb") as f:
            lines = f.read().decode("utf-8", errors="replace").splitlines()
        start = max(1, start_line or 1)
        end = min(len(lines), end_line or len(lines), start + MAX_LINES - 1)
        out = [f"{i}\t{lines[i - 1]}" for i in range(start, end + 1)]
        if end < len(lines):
            out.append(f"[{len(lines) - end} more lines; read on with start_line={end + 1}]")
        return "\n".join(out) or "[empty file]"

    def list_dir(self, path):
        real = self.resolve(path)
        if not os.path.isdir(real):
            raise ToolError("not a directory")
        names = sorted(n for n in os.listdir(real) if not os.path.islink(os.path.join(real, n)))
        out = [n + "/" if os.path.isdir(os.path.join(real, n)) else n for n in names[:MAX_ENTRIES]]
        if len(names) > MAX_ENTRIES:
            out.append(f"[{len(names) - MAX_ENTRIES} more entries]")
        return "\n".join(out) or "[empty directory]"

    def _files(self, real):
        if os.path.isfile(real):
            yield real
            return
        for dirpath, dirnames, filenames in os.walk(real, followlinks=False):
            dirnames.sort()
            for name in sorted(filenames):
                yield os.path.join(dirpath, name)

    def grep(self, pattern, path=None, glob=None):
        try:
            regex = re.compile(pattern)
        except re.error as err:
            raise ToolError(f"bad pattern: {err}") from None
        real = self.resolve(path or ".")
        out = []
        for file in self._files(real):
            rel = self._rel(file)
            if glob and not fnmatch.fnmatch(rel, glob) and not fnmatch.fnmatch(os.path.basename(rel), glob):
                continue
            if os.path.islink(file) or os.path.getsize(file) > MAX_FILE_BYTES:
                continue
            with open(file, "rb") as f:
                raw = f.read()
            if b"\0" in raw[:8192]:
                continue
            for i, line in enumerate(raw.decode("utf-8", errors="replace").splitlines(), 1):
                if regex.search(line):
                    out.append(f"{rel}:{i}: {line[:300]}")
                    if len(out) >= MAX_MATCHES:
                        out.append("[stopped at 200 matches]")
                        return "\n".join(out)
        return "\n".join(out) or "[no matches]"
