from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import requests


@dataclass(frozen=True)
class PageResult:
    items: list[dict[str, Any]]
    page_number: int
    request_url: str


class GitHubClient:
    def __init__(self, *, token: str, session: requests.Session | None = None, timeout: int = 30):
        self._token = token
        self._session = session or requests.Session()
        self._timeout = timeout

    def paginate(self, url: str, *, params: dict[str, Any] | None = None):
        next_url = url
        next_params = params
        page_number = 1

        while next_url:
            response = self._session.get(
                next_url,
                params=next_params,
                headers={
                    "Accept": "application/vnd.github+json",
                    "Authorization": f"Bearer {self._token}",
                    "X-GitHub-Api-Version": "2022-11-28",
                },
                timeout=self._timeout,
            )
            response.raise_for_status()
            payload = response.json()
            yield PageResult(items=payload, page_number=page_number, request_url=next_url)

            page_number += 1
            next_url = response.links.get("next", {}).get("url")
            next_params = None
