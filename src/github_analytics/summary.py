from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
import json
from pathlib import Path


@dataclass(frozen=True)
class ExtractionSummary:
    organization: str
    extract_date: str
    repo_items: int
    commit_items_by_repo: dict[str, int]
    contributor_items_by_repo: dict[str, int]
    anonymous_contributors_by_repo: dict[str, int]


def summarize_extraction_root(
    root: Path,
    *,
    organization: str | None = None,
    extract_date: str | None = None,
) -> ExtractionSummary:
    github_root = root / "github"
    org_dir = _resolve_org_dir(github_root, organization)
    extract_dir = _resolve_extract_dir(org_dir, extract_date)

    repo_items = 0
    commit_items_by_repo: dict[str, int] = defaultdict(int)
    contributor_items_by_repo: dict[str, int] = defaultdict(int)
    anonymous_contributors_by_repo: dict[str, int] = defaultdict(int)

    for path in sorted(extract_dir.rglob("page=*.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        source_name = next(part.split("=", 1)[1] for part in path.parts if part.startswith("source="))
        repo_name = next(
            (part.split("=", 1)[1] for part in path.parts if part.startswith("repo=")),
            None,
        )

        if source_name == "repos":
            repo_items += len(payload)
        elif source_name == "commits" and repo_name:
            commit_items_by_repo[repo_name] += len(payload)
        elif source_name == "contributors" and repo_name:
            contributor_items_by_repo[repo_name] += len(payload)
            anonymous_contributors_by_repo[repo_name] += sum(
                1 for contributor in payload if contributor.get("type") == "Anonymous"
            )

    return ExtractionSummary(
        organization=org_dir.name.split("=", 1)[1],
        extract_date=extract_dir.name.split("=", 1)[1],
        repo_items=repo_items,
        commit_items_by_repo=dict(commit_items_by_repo),
        contributor_items_by_repo=dict(contributor_items_by_repo),
        anonymous_contributors_by_repo=dict(anonymous_contributors_by_repo),
    )


def render_summary(summary: ExtractionSummary) -> str:
    lines = [
        f"Organization: {summary.organization}",
        f"Extract date: {summary.extract_date}",
        f"Repositories saved: {summary.repo_items}",
    ]
    if summary.commit_items_by_repo:
        lines.append("Commit pages:")
        lines.extend(
            f"- {repo_name}: {count} commits"
            for repo_name, count in sorted(summary.commit_items_by_repo.items())
        )
    if summary.contributor_items_by_repo:
        lines.append("Contributor pages:")
        lines.extend(
            f"- {repo_name}: {summary.contributor_items_by_repo[repo_name]} contributors"
            + (
                f" ({summary.anonymous_contributors_by_repo.get(repo_name, 0)} anonymous)"
                if summary.anonymous_contributors_by_repo.get(repo_name, 0)
                else ""
            )
            for repo_name in sorted(summary.contributor_items_by_repo)
        )
    return "\n".join(lines)


def _resolve_org_dir(github_root: Path, organization: str | None) -> Path:
    if organization:
        org_dir = github_root / f"org={organization}"
        if not org_dir.exists():
            raise FileNotFoundError(f"No extraction found for organization {organization!r}.")
        return org_dir

    org_dirs = sorted(path for path in github_root.glob("org=*") if path.is_dir())
    if not org_dirs:
        raise FileNotFoundError("No organization directories found under raw extraction root.")
    return org_dirs[-1]


def _resolve_extract_dir(org_dir: Path, extract_date: str | None) -> Path:
    if extract_date:
        extract_dir = org_dir / f"extract_date={extract_date}"
        if not extract_dir.exists():
            raise FileNotFoundError(f"No extraction found for date {extract_date!r}.")
        return extract_dir

    extract_dirs = sorted(path for path in org_dir.glob("extract_date=*") if path.is_dir())
    if not extract_dirs:
        raise FileNotFoundError("No extraction dates found under the organization directory.")
    return extract_dirs[-1]
