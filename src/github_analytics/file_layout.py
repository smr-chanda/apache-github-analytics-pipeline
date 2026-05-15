from __future__ import annotations

from datetime import date
from pathlib import Path


def build_raw_output_path(
    *,
    root: Path,
    organization: str,
    extract_date: date,
    source_name: str,
    page_number: int,
    repo_name: str | None = None,
) -> Path:
    path = (
        root
        / "github"
        / f"org={organization}"
        / f"extract_date={extract_date.isoformat()}"
        / f"source={source_name}"
    )
    if repo_name:
        path = path / f"repo={repo_name}"
    return path / f"page={page_number:03d}.json"
