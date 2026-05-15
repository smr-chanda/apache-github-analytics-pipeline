from datetime import date
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from github_analytics.file_layout import build_raw_output_path
from github_analytics.extract_repos import extract_repositories, select_repositories
from github_analytics.extract_commits import extract_commits
from github_analytics.extract_contributors import extract_contributors
from github_analytics.run_extraction import run_extraction
from github_analytics.summary import render_summary, summarize_extraction_root


FIXTURES = ROOT / "tests" / "fixtures" / "github"


def _write_fixture(root: Path, source_name: str, fixture_name: str, repo_name: str | None = None):
    target = build_raw_output_path(
        root=root,
        organization="apache",
        extract_date=date(2026, 5, 15),
        source_name=source_name,
        page_number=1,
        repo_name=repo_name,
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text((FIXTURES / fixture_name).read_text(), encoding="utf-8")


def test_summarize_extraction_root_counts_saved_items(tmp_path: Path):
    _write_fixture(tmp_path, "repos", "repos_page_1.json")
    _write_fixture(tmp_path, "commits", "commits_page_1.json", repo_name="spark")
    _write_fixture(tmp_path, "contributors", "contributors_page_1.json", repo_name="spark")

    summary = summarize_extraction_root(tmp_path)

    assert summary.organization == "apache"
    assert summary.extract_date == "2026-05-15"
    assert summary.repo_items == 2
    assert summary.commit_items_by_repo == {"spark": 2}
    assert summary.contributor_items_by_repo == {"spark": 2}
    assert summary.anonymous_contributors_by_repo == {"spark": 1}


def test_render_summary_reports_totals_in_plain_text(tmp_path: Path):
    _write_fixture(tmp_path, "repos", "repos_page_1.json")
    _write_fixture(tmp_path, "commits", "commits_page_1.json", repo_name="spark")

    rendered = render_summary(summarize_extraction_root(tmp_path))

    assert "Organization: apache" in rendered
    assert "Extract date: 2026-05-15" in rendered
    assert "Repositories saved: 2" in rendered
    assert "spark: 2 commits" in rendered


def test_select_repositories_filters_archived_and_limits_results():
    payload = json.loads((FIXTURES / "repos_page_1.json").read_text(encoding="utf-8"))
    payload.append(
        {
            "id": 3,
            "name": "airflow",
            "full_name": "apache/airflow",
            "archived": False,
            "disabled": False,
            "pushed_at": "2026-05-15T08:00:00Z",
        }
    )

    selected = select_repositories(payload, limit=1)

    assert [repo["name"] for repo in selected] == ["airflow"]


def test_extract_repositories_persists_only_selected_active_scope(tmp_path: Path):
    payload = json.loads((FIXTURES / "repos_page_1.json").read_text(encoding="utf-8"))
    payload.extend(
        [
            {
                "id": 3,
                "name": "airflow",
                "full_name": "apache/airflow",
                "archived": False,
                "disabled": False,
                "pushed_at": "2026-05-15T08:00:00Z",
            },
            {
                "id": 4,
                "name": "old-project",
                "full_name": "apache/old-project",
                "archived": True,
                "disabled": False,
                "pushed_at": "2026-05-15T07:00:00Z",
            },
        ]
    )
    client = type(
        "RepoClient",
        (),
        {
            "paginate": lambda self, url, params=None: iter(
                [type("Page", (), {"items": payload, "page_number": 1, "request_url": url})()]
            )
        },
    )()

    result = extract_repositories(
        output_root=tmp_path,
        client=client,
        extract_date=date(2026, 5, 15),
        repository_limit=2,
    )

    saved_payload = json.loads(result.output_paths[0].read_text(encoding="utf-8"))

    assert [repo["name"] for repo in saved_payload] == ["airflow", "spark"]
    assert [repo["name"] for repo in result.selected_repositories] == ["airflow", "spark"]


class RecordingClient:
    def __init__(self, payload_by_repo):
        self.payload_by_repo = payload_by_repo
        self.calls = []

    def paginate(self, url, *, params=None):
        repo_name = url.rsplit("/", 2)[1]
        self.calls.append({"url": url, "params": params})
        yield type(
            "Page",
            (),
            {
                "items": self.payload_by_repo[repo_name],
                "page_number": 1,
                "request_url": url,
            },
        )()


class RoutingClient:
    def __init__(self, repo_payload, commit_payload, contributor_payload):
        self.repo_payload = repo_payload
        self.commit_payload = commit_payload
        self.contributor_payload = contributor_payload

    def paginate(self, url, *, params=None):
        if "/orgs/" in url:
            payload = self.repo_payload
        elif url.endswith("/commits"):
            repo_name = url.rsplit("/", 2)[1]
            payload = self.commit_payload[repo_name]
        else:
            repo_name = url.rsplit("/", 2)[1]
            payload = self.contributor_payload[repo_name]
        yield type("Page", (), {"items": payload, "page_number": 1, "request_url": url})()


def test_extract_commits_writes_one_page_per_repo(tmp_path: Path):
    payload = json.loads((FIXTURES / "commits_page_1.json").read_text(encoding="utf-8"))
    client = RecordingClient({"spark": payload, "kafka": payload})

    result = extract_commits(
        output_root=tmp_path,
        client=client,
        repositories=["spark", "kafka"],
        extract_date=date(2026, 5, 15),
        since="2026-04-15T00:00:00Z",
    )

    assert result.commit_counts_by_repo == {"spark": 2, "kafka": 2}
    assert len(result.output_paths) == 2
    assert client.calls[0]["params"]["since"] == "2026-04-15T00:00:00Z"
    assert result.output_paths[0].as_posix().endswith("source=commits/repo=spark/page=001.json")


def test_extract_contributors_keeps_anonymous_snapshot_data(tmp_path: Path):
    payload = json.loads((FIXTURES / "contributors_page_1.json").read_text(encoding="utf-8"))
    client = RecordingClient({"spark": payload})

    result = extract_contributors(
        output_root=tmp_path,
        client=client,
        repositories=["spark"],
        extract_date=date(2026, 5, 15),
    )

    assert result.contributor_counts_by_repo == {"spark": 2}
    assert result.anonymous_contributors_by_repo == {"spark": 1}
    assert client.calls[0]["params"]["anon"] == "true"


def test_run_extraction_returns_summary_for_all_sources(tmp_path: Path):
    repo_payload = json.loads((FIXTURES / "repos_page_1.json").read_text(encoding="utf-8"))
    commit_payload = json.loads((FIXTURES / "commits_page_1.json").read_text(encoding="utf-8"))
    contributor_payload = json.loads((FIXTURES / "contributors_page_1.json").read_text(encoding="utf-8"))
    client = RoutingClient(
        repo_payload=repo_payload,
        commit_payload={"spark": commit_payload},
        contributor_payload={"spark": contributor_payload},
    )

    summary_text = run_extraction(
        output_root=tmp_path,
        client=client,
        repositories=["spark"],
        extract_date=date(2026, 5, 15),
        since="2026-04-15T00:00:00Z",
        repository_limit=10,
    )

    assert "Repositories saved: 1" in summary_text
    assert "spark: 2 commits" in summary_text
    assert "spark: 2 contributors (1 anonymous)" in summary_text


def test_summarize_extraction_root_uses_requested_extract_date(tmp_path: Path):
    _write_fixture(tmp_path, "repos", "repos_page_1.json")

    newer_path = build_raw_output_path(
        root=tmp_path,
        organization="apache",
        extract_date=date(2026, 5, 16),
        source_name="repos",
        page_number=1,
    )
    newer_path.parent.mkdir(parents=True, exist_ok=True)
    newer_path.write_text("[]", encoding="utf-8")

    summary = summarize_extraction_root(tmp_path, organization="apache", extract_date="2026-05-15")

    assert summary.extract_date == "2026-05-15"
    assert summary.repo_items == 2
