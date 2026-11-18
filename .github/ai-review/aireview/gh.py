"""A small GitHub API client. Every write goes through `Writer`."""

import json
import os
import urllib.error
import urllib.parse
import urllib.request

API = os.environ.get("GITHUB_API_URL", "https://api.github.com")
BOT = "github-actions[bot]"
WRITE_PERMISSIONS = {"admin", "maintain", "write"}


class GitHub:
    def __init__(self, token, repo):
        self.token, self.repo = token, repo
        self._maintainers = {}

    def request(self, method, path, body=None, accept="application/vnd.github+json"):
        url = path if path.startswith("https://") else f"{API}/{path}"
        data = None if body is None else json.dumps(body).encode()
        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("Accept", accept)
        req.add_header("Authorization", f"Bearer {self.token}")
        req.add_header("X-GitHub-Api-Version", "2022-11-28")
        if data is not None:
            req.add_header("Content-Type", "application/json")
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read()
            link = resp.headers.get("Link", "")
        if accept != "application/vnd.github+json":
            return raw.decode(errors="replace"), link
        return (json.loads(raw) if raw else None), link

    def get(self, path, accept="application/vnd.github+json"):
        return self.request("GET", path, accept=accept)[0]

    def repo_get(self, path):
        return self.get(f"repos/{self.repo}/{path}")

    def paginate(self, path):
        sep = "&" if "?" in path else "?"
        url, items = f"{API}/repos/{self.repo}/{path}{sep}per_page=100", []
        while url:
            page, link = self.request("GET", url)
            items += page if isinstance(page, list) else []
            url = next(
                (p.split(";")[0].strip("<> ") for p in link.split(",") if 'rel="next"' in p),
                None,
            )
        return items

    def graphql(self, query, **variables):
        owner, name = self.repo.split("/")
        body = {"query": query, "variables": {"owner": owner, "name": name, **variables}}
        data, _ = self.request("POST", "graphql", body)
        if data.get("errors"):
            raise RuntimeError(f"graphql: {data['errors']}")
        return data["data"]

    def is_maintainer(self, login, association=None):
        """A maintainer holds write access to the repository."""
        if login not in self._maintainers:
            try:
                perm = self.repo_get(f"collaborators/{urllib.parse.quote(login)}/permission")
                self._maintainers[login] = perm.get("permission") in WRITE_PERMISSIONS
            except urllib.error.HTTPError:
                self._maintainers[login] = association in ("OWNER", "MEMBER")
        return self._maintainers[login]

    def bot_comment(self, number, marker):
        for c in self.paginate(f"issues/{number}/comments"):
            if c["user"]["login"] == BOT and c["body"].startswith(marker):
                return c
        return None


class Writer:
    """Applies writes, or records them when `dry` is set for shadow mode."""

    def __init__(self, gh, number, dry):
        self.gh, self.number, self.dry = gh, number, dry
        self.log = []

    def _send(self, what, method, path, body=None):
        self.log.append((what, body))
        if not self.dry:
            self.gh.request(method, f"repos/{self.gh.repo}/{path}", body)

    def set_labels(self, current, wanted, families):
        """Adds `wanted` and removes the other labels of the same families."""
        for name in sorted(current - wanted):
            if any(name.startswith(f) for f in families):
                self._send(f"remove label {name}", "DELETE", f"issues/{self.number}/labels/{urllib.parse.quote(name, safe='')}")
        add = sorted(wanted - current)
        if add:
            self._send(f"add labels {', '.join(add)}", "POST", f"issues/{self.number}/labels", {"labels": add})

    def comment(self, existing, body):
        if existing:
            self._send("update summary comment", "PATCH", f"issues/comments/{existing['id']}", {"body": body})
        else:
            self._send("post summary comment", "POST", f"issues/{self.number}/comments", {"body": body})

    def review(self, sha, comments):
        body = {"commit_id": sha, "event": "COMMENT", "body": "", "comments": comments}
        try:
            self._send(f"post {len(comments)} inline findings", "POST", f"pulls/{self.number}/reviews", body)
        except urllib.error.HTTPError as err:
            # The summary comment still lists every open finding.
            print(f"::warning::inline review failed: {err.code} {err.reason}")

    def react(self, comment_id):
        self._send("react to the command", "POST", f"issues/comments/{comment_id}/reactions", {"content": "+1"})

    def request_review(self, team):
        body = {"team_reviewers": [team]}
        try:
            self._send(f"request review from the {team} team", "POST", f"pulls/{self.number}/requested_reviewers", body)
        except urllib.error.HTTPError as err:
            # A failed request still leaves the handoff label and summary.
            print(f"::warning::review request failed: {err.code} {err.reason}")
