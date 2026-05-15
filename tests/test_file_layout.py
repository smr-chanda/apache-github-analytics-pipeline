from datetime import date
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from github_analytics.file_layout import build_raw_output_path


def test_build_raw_output_path_for_commit_page():
    path = build_raw_output_path(
        root=Path("raw"),
        organization="apache",
        extract_date=date(2026, 5, 15),
        source_name="commits",
        page_number=1,
        repo_name="spark",
    )

    assert (
        path.as_posix()
        == "raw/github/org=apache/extract_date=2026-05-15/source=commits/repo=spark/page=001.json"
    )
