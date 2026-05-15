from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
import json
from pathlib import Path

from github_analytics.config import load_config
from github_analytics.file_layout import build_raw_output_path
from github_analytics.github_client import GitHubClient


DEFAULT_ORGANIZATION = "apache"
DEFAULT_DEEP_DIVE_REPOSITORIES = ["spark", "kafka", "flink", "airflow", "iceberg"]


@dataclass(frozen=True)
class ContributorExtractionResult:
    output_paths: list[Path]
    contributor_counts_by_repo: dict[str, int]
    anonymous_contributors_by_repo: dict[str, int]


def extract_contributors(
    *,
    output_root: Path,
    client: GitHubClient,
    repositories: list[str] | None = None,
    organization: str = DEFAULT_ORGANIZATION,
    extract_date: date | None = None,
) -> ContributorExtractionResult:
    extract_date = extract_date or datetime.now(UTC).date()
    repositories = repositories or list(DEFAULT_DEEP_DIVE_REPOSITORIES)

    output_paths: list[Path] = []
    contributor_counts_by_repo: dict[str, int] = {}
    anonymous_contributors_by_repo: dict[str, int] = {}

    for repo_name in repositories:
        repo_count = 0
        anonymous_count = 0
        for page in client.paginate(
            f"https://api.github.com/repos/{organization}/{repo_name}/contributors",
            params={"per_page": 100, "anon": "true"},
        ):
            target_path = build_raw_output_path(
                root=output_root,
                organization=organization,
                extract_date=extract_date,
                source_name="contributors",
                page_number=page.page_number,
                repo_name=repo_name,
            )
            target_path.parent.mkdir(parents=True, exist_ok=True)
            target_path.write_text(json.dumps(page.items, indent=2), encoding="utf-8")
            output_paths.append(target_path)
            repo_count += len(page.items)
            anonymous_count += sum(1 for contributor in page.items if contributor.get("type") == "Anonymous")
        contributor_counts_by_repo[repo_name] = repo_count
        anonymous_contributors_by_repo[repo_name] = anonymous_count

    return ContributorExtractionResult(
        output_paths=output_paths,
        contributor_counts_by_repo=contributor_counts_by_repo,
        anonymous_contributors_by_repo=anonymous_contributors_by_repo,
    )
def main() -> None:
    config = load_config()
    result = extract_contributors(
        output_root=Path("raw"),
        client=GitHubClient(token=str(config["github_token"])),
        repositories=list(config["deep_dive_repos"]),
        organization=str(config["organization"]),
    )
    for repo_name, count in result.contributor_counts_by_repo.items():
        anonymous = result.anonymous_contributors_by_repo.get(repo_name, 0)
        print(f"{repo_name}: {count} contributors ({anonymous} anonymous)")


if __name__ == "__main__":
    main()
