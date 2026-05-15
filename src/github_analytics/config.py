from __future__ import annotations

import os
from typing import Final


DEFAULT_ORGANIZATION: Final[str] = "apache"
DEFAULT_REPO_METADATA_LIMIT: Final[int] = 200
DEFAULT_COMMIT_WINDOW_DAYS: Final[int] = 30


class ConfigurationError(ValueError):
    """Raised when required project configuration is missing or invalid."""


DEFAULT_DEEP_DIVE_REPOS = ("spark", "kafka", "flink", "airflow", "iceberg")


def load_config() -> dict[str, object]:
    github_token = _load_token()
    if not github_token:
        msg = "GITHUB_TOKEN is required."
        raise ConfigurationError(msg)

    return {
        "github_token": github_token,
        "organization": os.getenv("GITHUB_ORG", DEFAULT_ORGANIZATION).strip() or DEFAULT_ORGANIZATION,
        "repo_metadata_limit": int(os.getenv("REPO_METADATA_LIMIT", str(DEFAULT_REPO_METADATA_LIMIT))),
        "deep_dive_repos": _load_deep_dive_repos(),
        "commit_window_days": int(os.getenv("COMMIT_WINDOW_DAYS", str(DEFAULT_COMMIT_WINDOW_DAYS))),
    }


def _load_deep_dive_repos() -> tuple[str, ...]:
    raw_value = os.getenv("DEEP_DIVE_REPOS", "")
    if not raw_value.strip():
        return DEFAULT_DEEP_DIVE_REPOS

    repos = tuple(part.strip() for part in raw_value.split(",") if part.strip())
    return repos or DEFAULT_DEEP_DIVE_REPOS


def _load_token() -> str:
    direct_token = os.getenv("GITHUB_TOKEN", "").strip()
    if direct_token:
        return direct_token

    return _read_windows_user_env("GITHUB_TOKEN").strip()


def _read_windows_user_env(name: str) -> str:
    if os.name != "nt":
        return ""

    try:
        import winreg
    except ImportError:
        return ""

    try:
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, "Environment") as key:
            value, _ = winreg.QueryValueEx(key, name)
    except OSError:
        return ""

    return str(value)
