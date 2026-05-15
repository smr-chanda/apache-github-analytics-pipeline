from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
import json
from pathlib import Path

from github_analytics.config import load_config
from github_analytics.file_layout import build_raw_output_path
from github_analytics.github_client import GitHubClient


DEFAULT_ORGANIZATION = "apache"
DEFAULT_DEEP_DIVE_REPOSITORIES = ["spark", "kafka", "flink", "airflow", "iceberg"]
DEFAULT_DAYS_BACK = 30


@dataclass(frozen=True)
class CommitExtractionResult:
    output_paths: list[Path]
    commit_counts_by_repo: dict[str, int]


def default_since_timestamp(*, now: datetime | None = None, days_back: int = DEFAULT_DAYS_BACK) -> str:
    current_time = now or datetime.now(UTC)
    return (current_time - timedelta(days=days_back)).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def extract_commits(
    *,
    output_root: Path,
    client: GitHubClient,
    repositories: list[str] | None = None,
    organization: str = DEFAULT_ORGANIZATION,
    extract_date: date | None = None,
    since: str | None = None,
) -> CommitExtractionResult:
    extract_date = extract_date or datetime.now(UTC).date()
    repositories = repositories or list(DEFAULT_DEEP_DIVE_REPOSITORIES)
    since = since or default_since_timestamp()

    output_paths: list[Path] = []
    commit_counts_by_repo: dict[str, int] = {}

    for repo_name in repositories:
        repo_count = 0
        for page in client.paginate(
            f"https://api.github.com/repos/{organization}/{repo_name}/commits",
            params={"per_page": 100, "since": since},
        ):
            target_path = build_raw_output_path(
                root=output_root,
                organization=organization,
                extract_date=extract_date,
                source_name="commits",
                page_number=page.page_number,
                repo_name=repo_name,
            )
            target_path.parent.mkdir(parents=True, exist_ok=True)
            target_path.write_text(json.dumps(page.items, indent=2), encoding="utf-8")
            output_paths.append(target_path)
            repo_count += len(page.items)
        commit_counts_by_repo[repo_name] = repo_count

    return CommitExtractionResult(output_paths=output_paths, commit_counts_by_repo=commit_counts_by_repo)
def main() -> None:
    config = load_config()
    result = extract_commits(
        output_root=Path("raw"),
        client=GitHubClient(token=str(config["github_token"])),
        repositories=list(config["deep_dive_repos"]),
        organization=str(config["organization"]),
        since=default_since_timestamp(days_back=int(config["commit_window_days"])),
    )
    for repo_name, count in result.commit_counts_by_repo.items():
        print(f"{repo_name}: {count} commits")


if __name__ == "__main__":
    main()
