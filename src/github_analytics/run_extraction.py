from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

from github_analytics.config import load_config
from github_analytics.extract_commits import (
    default_since_timestamp,
    extract_commits,
)
from github_analytics.extract_contributors import extract_contributors
from github_analytics.extract_repos import extract_repositories
from github_analytics.github_client import GitHubClient
from github_analytics.summary import render_summary, summarize_extraction_root


def run_extraction(
    *,
    output_root: Path,
    client,
    repositories: list[str] | None = None,
    organization: str | None = None,
    extract_date: date | None = None,
    since: str | None = None,
    repository_limit: int = 200,
) -> str:
    extract_date = extract_date or datetime.now(UTC).date()
    config = load_config()
    organization = organization or str(config["organization"])
    repositories = repositories or list(config["deep_dive_repos"])
    since = since or default_since_timestamp()

    extract_repositories(
        output_root=output_root,
        client=client,
        organization=organization,
        extract_date=extract_date,
        repository_limit=repository_limit,
    )
    extract_commits(
        output_root=output_root,
        client=client,
        repositories=repositories,
        organization=organization,
        extract_date=extract_date,
        since=since,
    )
    extract_contributors(
        output_root=output_root,
        client=client,
        repositories=repositories,
        organization=organization,
        extract_date=extract_date,
    )
    return render_summary(
        summarize_extraction_root(
            output_root,
            organization=organization,
            extract_date=extract_date.isoformat(),
        )
    )


def main() -> None:
    config = load_config()
    summary_text = run_extraction(
        output_root=Path("raw"),
        client=GitHubClient(token=str(config["github_token"])),
        repositories=list(config["deep_dive_repos"]),
        organization=str(config["organization"]),
        repository_limit=int(config["repo_metadata_limit"]),
        since=default_since_timestamp(days_back=int(config["commit_window_days"])),
    )
    print(summary_text)


if __name__ == "__main__":
    main()
