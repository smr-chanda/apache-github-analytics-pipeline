# Apache GitHub Analytics Pipeline

End-to-end ELT pipeline for Apache GitHub activity using the **GitHub REST API**, local Python extraction, and Databricks/PySpark Bronze-Silver-Gold processing.

## Scope

- Organization: `apache`
- Repository metadata: top `200` active public repositories
- Deep-dive repositories: `spark`, `kafka`, `flink`, `airflow`, `iceberg`
- Commit window: latest `30` days

## Repo Map

```text
.
|-- .env.example
|-- .gitignore
|-- README.md
|-- docs/
|   `-- Sai_Mani_Raj_Chanda_Big_Data_Project_Report.pdf
|-- notebooks/
|   `-- submission_notebook.ipynb
|-- pyproject.toml
|-- requirements.txt
|-- src/
|   `-- github_analytics/
|       |-- config.py
|       |-- extract_commits.py
|       |-- extract_contributors.py
|       |-- extract_repos.py
|       |-- file_layout.py
|       |-- github_client.py
|       |-- run_extraction.py
|       `-- summary.py
`-- tests/
    |-- fixtures/github/
    |-- test_config.py
    |-- test_file_layout.py
    |-- test_github_client.py
    `-- test_summary.py
```

## Prerequisites

- Python `3.11+`
- Databricks Free Edition or another Databricks workspace
- A **GitHub personal access token (PAT)** for authenticated GitHub API access

## Configuration

Required:

- `GITHUB_TOKEN`: your GitHub personal access token

Optional:

- `GITHUB_ORG`: defaults to `apache`
- `REPO_METADATA_LIMIT`: defaults to `200`
- `DEEP_DIVE_REPOS`: defaults to `spark,kafka,flink,airflow,iceberg`
- `COMMIT_WINDOW_DAYS`: defaults to `30`

Example:

```env
GITHUB_TOKEN=your_github_personal_access_token
GITHUB_ORG=apache
REPO_METADATA_LIMIT=200
DEEP_DIVE_REPOS=spark,kafka,flink,airflow,iceberg
COMMIT_WINDOW_DAYS=30
```

## Local Setup

1. Create and activate a virtual environment.
2. Install dependencies:
   ```powershell
   python -m pip install -e .[dev]
   ```
3. Set your GitHub personal access token:
   ```powershell
   setx GITHUB_TOKEN "your-token-here"
   ```
4. Open a new PowerShell window and verify:
   ```powershell
   echo $env:GITHUB_TOKEN
   ```

## Run Local Extraction

```powershell
python -m github_analytics.run_extraction
```

This writes raw GitHub API JSON under `raw/github/...` for:

- repository metadata
- commit history for the five deep-dive repositories
- contributor snapshots for the same five repositories

## Run Tests

```powershell
python -m pytest
```

## Run In Databricks

1. Upload the contents of `raw/github` into a Databricks Volume.
2. Keep the folder structure unchanged.
3. Import `notebooks/submission_notebook.ipynb` into Databricks.
4. Set:
   ```python
   RAW_BASE_PATH = "/Volumes/<catalog>/<schema>/<volume>/github"
   ```
5. Make sure `RAW_BASE_PATH` stops at the folder directly above `org=apache`.
6. If you are using Unity Catalog, use `_metadata.file_path` instead of `input_file_name()` when reading source paths.
7. Run the notebook from top to bottom.

## Recreate The Submission

1. Run local extraction.
2. Upload the raw JSON to Databricks.
3. Run `notebooks/submission_notebook.ipynb`.
4. Capture notebook outputs for screenshots.
5. Submit the notebook, report, and screenshots.

## Included Submission Artifact

- Final report PDF: `docs/Sai_Mani_Raj_Chanda_Big_Data_Project_Report.pdf`

## Verification

Local:

```powershell
python -m pytest
python -m github_analytics.run_extraction
```

Databricks:

- Bronze loads successfully
- Silver tables build successfully
- Gold outputs render successfully
- validation shows zero duplicate commits and zero null commit timestamps

## Data Source Note

This project uses the **GitHub REST API**. API usage is subject to GitHub's API terms:

- [GitHub REST API docs](https://docs.github.com/en/rest?ref=public-apis)
- [GitHub API authentication](https://docs.github.com/v3/auth)
- [GitHub personal access tokens](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens?source=post_page-----64ee8bb11630---------------------------------------)
- [GitHub Terms of Service](https://docs.github.com/terms-of-service)
