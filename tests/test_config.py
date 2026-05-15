from pathlib import Path
import sys

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = PROJECT_ROOT / "src"

if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))


def test_load_config_requires_github_token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)

    from github_analytics import config as config_module

    monkeypatch.setattr(config_module, "_read_windows_user_env", lambda name: "")

    with pytest.raises(config_module.ConfigurationError, match="GITHUB_TOKEN"):
        config_module.load_config()


def test_load_config_uses_project_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GITHUB_TOKEN", "test-token")
    monkeypatch.delenv("GITHUB_ORG", raising=False)
    monkeypatch.delenv("REPO_METADATA_LIMIT", raising=False)
    monkeypatch.delenv("DEEP_DIVE_REPOS", raising=False)
    monkeypatch.delenv("COMMIT_WINDOW_DAYS", raising=False)

    from github_analytics.config import load_config

    config = load_config()

    assert config["organization"] == "apache"
    assert config["repo_metadata_limit"] == 200
    assert config["deep_dive_repos"] == (
        "spark",
        "kafka",
        "flink",
        "airflow",
        "iceberg",
    )
    assert config["commit_window_days"] == 30


def test_load_config_uses_windows_user_environment_when_shell_env_is_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)

    from github_analytics import config as config_module

    monkeypatch.setattr(config_module, "_read_windows_user_env", lambda name: "registry-token")

    config = config_module.load_config()

    assert config["github_token"] == "registry-token"


def test_load_config_applies_environment_overrides(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GITHUB_TOKEN", "test-token")
    monkeypatch.setenv("GITHUB_ORG", "apache-incubator")
    monkeypatch.setenv("REPO_METADATA_LIMIT", "25")
    monkeypatch.setenv("DEEP_DIVE_REPOS", "beam,calcite")
    monkeypatch.setenv("COMMIT_WINDOW_DAYS", "14")

    from github_analytics.config import load_config

    config = load_config()

    assert config["organization"] == "apache-incubator"
    assert config["repo_metadata_limit"] == 25
    assert config["deep_dive_repos"] == ("beam", "calcite")
    assert config["commit_window_days"] == 14
