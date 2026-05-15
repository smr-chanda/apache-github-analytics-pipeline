# Apache GitHub Analytics Pipeline

This project implements an end-to-end ELT pipeline for Apache GitHub activity using Python for extraction and Databricks/PySpark for Bronze-Silver-Gold processing.

Public repository: `https://github.com/smr-chanda/apache-github-analytics-pipeline`

## What This Project Does

- Extracts raw GitHub REST API data for the Apache organization.
- Preserves raw JSON in a Bronze-style landing layout.
- Cleans and joins repository, commit, and contributor data into Silver-style tables.
- Produces Gold-style analytics for:
  - repository activity
  - weekly and monthly commit trends
  - language distribution
  - contributor counts
  - stars versus recent activity
  - contributor behavior over time

## Scope

- Organization: `apache`
- Repository metadata scope: top `200` active public repositories
- Deep-dive repositories: `spark`, `kafka`, `flink`, `airflow`, `iceberg`
- Commit window: latest `30` days

## Repository Layout

- `src/github_analytics/`: extraction pipeline and shared configuration
- `tests/`: unit tests for config, file layout, GitHub client behavior, and summaries
- `notebooks/submission_notebook.py`: simplified Databricks notebook for final submission
- `notebooks/01_github_analytics_pipeline.py`: fuller notebook version
- `docs/report-content.md`: final report draft content
- `docs/screenshots-checklist.md`: screenshot checklist for submission
- `docs/plans/`: implementation plan

## Prerequisites

- Python `3.11+`
- A GitHub personal access token stored as `GITHUB_TOKEN`
- Databricks Free Edition or another Databricks workspace

## Local Setup

1. Create and activate a virtual environment.
2. Install the package and dev dependencies:
   ```powershell
   python -m pip install -e .[dev]
   ```
3. Set your GitHub token:
   ```powershell
   setx GITHUB_TOKEN "your-token-here"
   ```
   Then open a new PowerShell window.
4. Confirm the token is visible:
   ```powershell
   echo $env:GITHUB_TOKEN
   ```

## Configuration

The project supports these environment variables:

- `GITHUB_TOKEN`: required
- `GITHUB_ORG`: defaults to `apache`
- `REPO_METADATA_LIMIT`: defaults to `200`
- `DEEP_DIVE_REPOS`: comma-separated repo names
- `COMMIT_WINDOW_DAYS`: defaults to `30`

Defaults are defined in `src/github_analytics/config.py`.

## Run Local Extraction

Run the end-to-end extractor:

```powershell
python -m github_analytics.run_extraction
```

Expected result:

- raw files are written under `raw/github/...`
- a summary is printed for repositories, commits, and contributors

## Run Tests

```powershell
python -m pytest
```

## Load Data into Databricks

1. Upload the contents of `raw/github` into a Databricks Volume.
2. Keep the folder structure unchanged so the volume contains `org=apache/...` under the chosen base path.
3. In Databricks, import or open `notebooks/submission_notebook.py`.
4. Set:
   ```python
   RAW_BASE_PATH = "/Volumes/<catalog>/<schema>/<volume>/github"
   ```
   The path must stop at the folder directly above `org=apache`.
5. If using Unity Catalog, use `_metadata.file_path` instead of `input_file_name()` when reading source file paths.
6. Run the notebook top to bottom.

## How To Recreate The Final Submission

1. Run local extraction.
2. Upload raw JSON to a Databricks Volume.
3. Run `notebooks/submission_notebook.py` in Databricks.
4. Capture screenshots listed in `docs/screenshots-checklist.md`.
5. Use `docs/report-content.md` as the written report base.
6. Submit:
   - the Databricks notebook
   - the written report
   - the output screenshots

## Verification

Minimum local verification:

```powershell
python -m pytest
python -m github_analytics.run_extraction
```

Databricks verification:

- Bronze row counts load successfully
- Silver tables build successfully
- Gold analytics render successfully
- validation output reports zero duplicate commits and zero null commit timestamps
