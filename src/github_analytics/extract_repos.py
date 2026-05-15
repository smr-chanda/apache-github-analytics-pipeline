from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
import json
from pathlib import Path
from typing import Any

from github_analytics.config import load_config
from github_analytics.file_layout import build_raw_output_path
from github_analytics.github_client import GitHubClient


DEFAULT_ORGANIZATION = "apache"
DEFAULT_REPOSITORY_LIMIT = 200


@dataclass(frozen=True)
class RepositoryExtractionResult:
    output_paths: list[Path]
    selected_repositories: list[dict[str, Any]]


def select_repositories(repositories: list[dict[str, Any]], *, limit: int = DEFAULT_REPOSITORY_LIMIT):
    active_repositories = [
        repo for repo in repositories if not repo.get("archived") and not repo.get("disabled")
    ]
    return sorted(
        active_repositories,
        key=lambda repo: repo.get("pushed_at") or "",
        reverse=True,
    )[:limit]


def extract_repositories(
    *,
    output_root: Path,
    client: GitHubClient,
    organization: str = DEFAULT_ORGANIZATION,
    extract_date: date | None = None,
    repository_limit: int = DEFAULT_REPOSITORY_LIMIT,
) -> RepositoryExtractionResult:
    extract_date = extract_date or datetime.now(UTC).date()
    all_repositories: list[dict[str, Any]] = []

    for page in client.paginate(
        f"https://api.github.com/orgs/{organization}/repos",
        params={"per_page": 100, "type": "public", "sort": "pushed", "direction": "desc"},
    ):
        all_repositories.extend(page.items)
        if len(select_repositories(all_repositories, limit=repository_limit)) >= repository_limit:
            break

    selected = select_repositories(all_repositories, limit=repository_limit)
    saved_paths: list[Path] = []
    page_size = 100
    selected_pages = [
        selected[index : index + page_size]
        for index in range(0, len(selected), page_size)
    ]

    for page_number, payload in enumerate(selected_pages, start=1):
        target_path = build_raw_output_path(
            root=output_root,
            organization=organization,
            extract_date=extract_date,
            source_name="repos",
            page_number=page_number,
        )
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        saved_paths.append(target_path)

    return RepositoryExtractionResult(output_paths=saved_paths, selected_repositories=selected)


def main() -> None:
    config = load_config()
    result = extract_repositories(
        output_root=Path("raw"),
        client=GitHubClient(token=str(config["github_token"])),
        organization=str(config["organization"]),
        repository_limit=int(config["repo_metadata_limit"]),
    )
    print(f"Saved {len(result.output_paths)} repository page(s).")
    print(f"Selected {len(result.selected_repositories)} active repositories.")


if __name__ == "__main__":
    main()
