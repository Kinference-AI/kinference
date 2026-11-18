"""Fetches a pull request as data. Nothing from the pull request ever runs.

Every git call runs with an empty global config, hooks pointed at
/dev/null and a repository that this module created, so the pull
request can't supply hooks, filters or diff drivers.
"""

import io
import os
import subprocess
import tarfile

_ENV = {
    "GIT_CONFIG_NOSYSTEM": "1",
    "GIT_CONFIG_GLOBAL": "/dev/null",
    "GIT_TERMINAL_PROMPT": "0",
    "GIT_SEQUENCE_EDITOR": ":",
    "GIT_EDITOR": ":",
    "GIT_AUTHOR_NAME": "ai-review",
    "GIT_AUTHOR_EMAIL": "ai-review@invalid",
    "GIT_COMMITTER_NAME": "ai-review",
    "GIT_COMMITTER_EMAIL": "ai-review@invalid",
    "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
}
_SAFE = ["-c", "core.hooksPath=/dev/null", "-c", "core.quotePath=false", "-c", "commit.gpgsign=false"]


def git(gitdir, *args, check=True, text=True):
    out = subprocess.run(
        ["git", f"--git-dir={gitdir}", *_SAFE, *args],
        env=_ENV,
        capture_output=True,
        check=check,
        text=text,
    )
    return out.stdout


def fetch(gitdir, repo, number, base_ref):
    """Fetches the base branch and the pull request head into a bare repository."""
    subprocess.run(["git", "init", "-q", "--bare", gitdir], env=_ENV, check=True)
    url = f"https://github.com/{repo}.git"
    git(
        gitdir, "fetch", "-q", "--no-tags", url,
        f"+refs/heads/{base_ref}:refs/remotes/base",
        f"+refs/pull/{number}/head:refs/remotes/pr",
    )


def has_commit(gitdir, sha):
    if not sha:
        return False
    proc = subprocess.run(
        ["git", f"--git-dir={gitdir}", "cat-file", "-e", f"{sha}^{{commit}}"],
        env=_ENV, capture_output=True,
    )
    return proc.returncode == 0


def merge_base(gitdir, head):
    return git(gitdir, "merge-base", "refs/remotes/base", head).strip()


def diff(gitdir, a, b):
    return git(gitdir, "diff", "--no-ext-diff", "--no-textconv", "--no-color", a, b)


def numstat(gitdir, a, b):
    """Returns (path, added + deleted lines) per changed path. Binary files count as zero lines."""
    rows = []
    for line in git(gitdir, "diff", "--numstat", "-z", "--no-renames", a, b).split("\0"):
        if not line:
            continue
        add, delete, path = line.split("\t", 2)
        rows.append((path, (int(add) if add.isdigit() else 0) + (int(delete) if delete.isdigit() else 0)))
    return rows


def autosquash(gitdir, base, head):
    """Returns the commit series as it will land, oldest first, and whether the autosquash worked.

    The rebase runs in a scratch worktree that only git touches. When the
    fixups don't apply cleanly, the series falls back to the commits as
    pushed.
    """
    work = os.path.join(os.path.dirname(gitdir), "squash")
    git(gitdir, "worktree", "add", "-q", "--detach", work, head)
    proc = subprocess.run(
        ["git", "-C", work, *_SAFE, "rebase", "-q", "-i", "--autosquash", "--no-verify", base],
        env=_ENV, capture_output=True, text=True,
    )
    ok = proc.returncode == 0
    if ok:
        tip = subprocess.run(
            ["git", "-C", work, "rev-parse", "HEAD"], env=_ENV, capture_output=True, text=True, check=True
        ).stdout.strip()
    else:
        subprocess.run(["git", "-C", work, *_SAFE, "rebase", "--abort"], env=_ENV, capture_output=True)
        tip = head
    shas = git(gitdir, "rev-list", "--reverse", "--no-merges", f"{base}..{tip}").split()
    return shas, ok


def show(gitdir, sha):
    return git(gitdir, "show", "--no-ext-diff", "--no-textconv", "--no-color", "--stat", "--patch", "--format=commit %H%n%n%B", sha)


def subject(gitdir, sha):
    return git(gitdir, "log", "-1", "--format=%s", sha).strip()


def extract(gitdir, sha, dest):
    """Writes the tree at `sha` into `dest` as regular files only. Symlinks and other entries are skipped."""
    raw = git(gitdir, "archive", "--format=tar", sha, text=False)
    with tarfile.open(fileobj=io.BytesIO(raw)) as tar:
        regular = [m for m in tar.getmembers() if m.isreg() or m.isdir()]
        tar.extractall(dest, members=regular, filter="data")
