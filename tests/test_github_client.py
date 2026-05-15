from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from github_analytics.github_client import GitHubClient


class FakeResponse:
    def __init__(self, payload, links=None, status_code=200):
        self._payload = payload
        self.links = links or {}
        self.status_code = status_code

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"status={self.status_code}")


class FakeSession:
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []

    def get(self, url, *, params=None, headers=None, timeout=None):
        self.calls.append(
            {
                "url": url,
                "params": params,
                "headers": headers,
                "timeout": timeout,
            }
        )
        return self._responses.pop(0)


def test_paginate_collects_all_pages_and_uses_auth_headers():
    session = FakeSession(
        [
            FakeResponse(
                payload=[{"id": 1}],
                links={"next": {"url": "https://api.github.com/resource?page=2"}},
            ),
            FakeResponse(payload=[{"id": 2}]),
        ]
    )
    client = GitHubClient(token="secret-token", session=session)

    pages = list(
        client.paginate(
            "https://api.github.com/resource",
            params={"per_page": 100, "type": "public"},
        )
    )

    assert [page.items for page in pages] == [[{"id": 1}], [{"id": 2}]]
    assert pages[0].request_url == "https://api.github.com/resource"
    assert pages[0].page_number == 1
    assert pages[1].request_url == "https://api.github.com/resource?page=2"
    assert pages[1].page_number == 2
    assert session.calls[0]["headers"]["Authorization"] == "Bearer secret-token"
    assert session.calls[0]["headers"]["Accept"] == "application/vnd.github+json"
    assert session.calls[0]["headers"]["X-GitHub-Api-Version"] == "2022-11-28"
    assert session.calls[0]["params"] == {"per_page": 100, "type": "public"}
    assert session.calls[1]["params"] is None
