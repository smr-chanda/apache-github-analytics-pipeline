# Apache GitHub Analytics Pipeline Implementation Plan

**Goal:** Build an end-to-end Apache GitHub analytics pipeline that extracts public GitHub data locally with Python, processes it in Databricks with PySpark using Bronze-Silver-Gold layers, and produces submission-ready analytics, screenshots, and report content for the Big Data Management final project.

**Architecture:** Use a split execution model. Python runs locally for GitHub REST API extraction and stores raw JSON snapshots in a deterministic folder layout; Databricks ingests those snapshots into Bronze tables, transforms them into Silver relational tables, and produces Gold analytics with Spark SQL and DataFrames. This keeps ingestion reliable under Databricks Free Edition constraints while still making the notebook the primary graded artifact.

**Scope:** In scope are project scaffolding, local extraction for Apache repository data, Databricks Bronze-Silver-Gold processing, contributor behavior analysis with Spark window functions, screenshots, and a written report. Out of scope are streaming, GH Archive ingestion, anomaly detection, dashboards outside Databricks notebook visuals, and analysis of the full Apache organization history.

**Verification Strategy:** Verify each layer independently. Local extraction is checked with deterministic file output and summary counts; Databricks transformations are checked with schema, row-count, and dedup validations; Gold outputs are checked with targeted queries and screenshot-ready tables; the final submission is checked against the course rubric and required deliverables.

---

## Planned File Structure

### Files to create
- `README.md`
  - Project overview, setup, execution order, and submission artifact list.
- `.gitignore`
  - Ignore local environment files, extracted raw data, and notebook exports if needed.
- `requirements.txt`
  - Python dependencies for local extraction and lightweight validation.
- `.env.example`
  - Example environment variables for GitHub token and extraction parameters.
- `src/github_analytics/__init__.py`
  - Package marker for local extraction utilities.
- `src/github_analytics/config.py`
  - Centralized configuration for GitHub token, target organization, repo scope, and date window.
- `src/github_analytics/github_client.py`
  - Authenticated GitHub REST client, pagination, headers, and request helpers.
- `src/github_analytics/extract_repos.py`
  - Extract and save Apache repository metadata.
- `src/github_analytics/extract_commits.py`
  - Extract and save commit history for the deep-dive repository set.
- `src/github_analytics/extract_contributors.py`
  - Extract and save contributor snapshots for the deep-dive repository set.
- `src/github_analytics/run_extraction.py`
  - Orchestrates the full local extraction run and prints a final summary.
- `src/github_analytics/file_layout.py`
  - Path builders and file-writing helpers for deterministic raw output locations.
- `src/github_analytics/summary.py`
  - Produces extraction summaries for validation and report support.
- `tests/test_config.py`
  - Validates configuration parsing and required inputs.
- `tests/test_file_layout.py`
  - Validates raw folder and file naming conventions.
- `tests/test_github_client.py`
  - Validates pagination and request handling with mocked API responses.
- `tests/test_summary.py`
  - Validates extraction summary generation from sample raw outputs.
- `tests/fixtures/github/repos_page_1.json`
  - Sample repository payload for tests.
- `tests/fixtures/github/commits_page_1.json`
  - Sample commit payload for tests.
- `tests/fixtures/github/contributors_page_1.json`
  - Sample contributor payload for tests.
- `notebooks/01_github_analytics_pipeline.py`
  - Databricks notebook source containing architecture notes, Bronze-Silver-Gold logic, analytics, and conclusions.
- `docs/report-outline.md`
  - Structured draft for the 2-4 page report aligned to the grading rubric.
- `docs/screenshots-checklist.md`
  - Exact screenshots to capture, what each should show, and where each belongs in submission materials.
- `docs/plans/2026-05-15-apache-github-analytics-pipeline.md`
  - This implementation plan.

### Files expected to be modified during execution
- `README.md`
  - Update once the final run commands and artifact paths are confirmed.
- `notebooks/01_github_analytics_pipeline.py`
  - Expand from Bronze load through Gold analytics as tasks are completed.
- `docs/report-outline.md`
  - Refine language after the final findings are known.
- `docs/screenshots-checklist.md`
  - Mark screenshots completed and update any exact notebook section names if they shift.

## Fixed Project Decisions

- Use `Python` for local extraction.
- Use `Databricks Free Edition` for Spark transformation and analytics.
- Use `ELT`, not ETL.
- Use explicit `Bronze -> Silver -> Gold` layer naming in the notebook and report.
- Use `apache` as the GitHub organization.
- Use a GitHub personal access token for authenticated API access.
- Extract repository metadata for the top `200` Apache public repositories sorted by recent push activity.
- Deep-dive commit and contributor analysis is limited to `spark`, `kafka`, `flink`, `airflow`, and `iceberg`.
- Commit analysis window is the most recent `30 days` at run time.
- Use GitHub REST API as the only required source for v1.
- Keep the implementation batch-only.
- All coding subagents must use the `tdd` skill with a vertical `red -> green -> refactor` workflow and behavior-focused tests through public interfaces, without exceptions.

### Task 1: Scaffold the Project Structure

**Outcome:** The repository has a clean Python-first structure with setup instructions, dependency tracking, environment configuration, and folders that support the rest of the implementation.

**Files to inspect:**
- `README.md`
- `.gitignore`
- `requirements.txt`

**Files to change:**
- Create: `README.md`
- Create: `.gitignore`
- Create: `requirements.txt`
- Create: `.env.example`
- Create: `src/github_analytics/__init__.py`
- Create: `src/github_analytics/config.py`
- Create: `tests/test_config.py`

**Tests / verification:**
- Add or update: `tests/test_config.py`
- Run: `python -m pytest tests/test_config.py`
- Expected: `Configuration tests pass and confirm required environment variables and default scope values are parsed correctly.`

**Notes / risks:**
- The config module must fail clearly when `GITHUB_TOKEN` is missing so later extraction failures are obvious.
- Keep dependency count small: `requests`, `python-dotenv`, and `pytest` are sufficient for the local workflow.

- [ ] Step 1: Write `tests/test_config.py` to cover token handling, default organization, deep-dive repo list, and 30-day window configuration.
- [ ] Step 2: Run `python -m pytest tests/test_config.py` and confirm the initial failure because implementation files do not exist yet.
- [ ] Step 3: Create `README.md`, `.gitignore`, `requirements.txt`, `.env.example`, package structure, and `config.py` with the minimum logic needed to satisfy the test.
- [ ] Step 4: Run `python -m pytest tests/test_config.py` again.
- [ ] Step 5: Stop and report the created structure and any config decisions that affect later tasks.

### Task 2: Implement Deterministic Raw File Layout

**Outcome:** Extraction code can write raw GitHub payloads to deterministic, partitioned locations that encode source, extraction date, repo name when applicable, and page number.

**Files to inspect:**
- `src/github_analytics/config.py`
- `README.md`

**Files to change:**
- Create: `src/github_analytics/file_layout.py`
- Create: `tests/test_file_layout.py`
- Modify: `README.md`

**Tests / verification:**
- Add or update: `tests/test_file_layout.py`
- Run: `python -m pytest tests/test_file_layout.py`
- Expected: `Layout tests pass and confirm exact raw paths such as raw/github/org=apache/extract_date=YYYY-MM-DD/source=commits/repo=spark/page=001.json.`

**Notes / risks:**
- Raw output paths must stay stable across reruns for easy Databricks upload and notebook references.
- Do not mix repository metadata and commit payloads in the same folder level.

- [ ] Step 1: Write `tests/test_file_layout.py` covering repo, commit, and contributor path generation plus zero-padded page numbering.
- [ ] Step 2: Run `python -m pytest tests/test_file_layout.py` and confirm failure.
- [ ] Step 3: Implement `file_layout.py` with explicit helper functions for repo, commit, and contributor output paths and add a short path-layout section to `README.md`.
- [ ] Step 4: Run `python -m pytest tests/test_file_layout.py`.
- [ ] Step 5: Stop and report the final raw layout examples.

### Task 3: Build the GitHub API Client

**Outcome:** The project has a reusable authenticated client that sends the required headers, follows pagination using the `Link` header, and returns JSON payloads with request metadata needed by the extractors.

**Files to inspect:**
- `src/github_analytics/config.py`
- `src/github_analytics/file_layout.py`

**Files to change:**
- Create: `src/github_analytics/github_client.py`
- Create: `tests/test_github_client.py`

**Tests / verification:**
- Add or update: `tests/test_github_client.py`
- Run: `python -m pytest tests/test_github_client.py`
- Expected: `Client tests pass and confirm authenticated requests, pagination traversal, and preservation of request URL and page metadata.`

**Notes / risks:**
- The client must use `per_page=100` wherever pagination applies to reduce total requests.
- Pagination parsing should read `rel="next"` from the `Link` header rather than guessing page counts.

- [ ] Step 1: Write `tests/test_github_client.py` with mocked responses for single-page and multi-page API calls.
- [ ] Step 2: Run `python -m pytest tests/test_github_client.py` and confirm failure.
- [ ] Step 3: Implement `github_client.py` with token auth, required GitHub headers, JSON parsing, and pagination helpers.
- [ ] Step 4: Run `python -m pytest tests/test_github_client.py`.
- [ ] Step 5: Stop and report the supported request patterns and any rate-limit handling that was added.

### Task 4: Implement Repository Metadata Extraction

**Outcome:** A local extraction module fetches and saves the top 200 Apache public repositories sorted by recent push activity, excluding archived and disabled repositories.

**Files to inspect:**
- `src/github_analytics/config.py`
- `src/github_analytics/github_client.py`
- `src/github_analytics/file_layout.py`

**Files to change:**
- Create: `src/github_analytics/extract_repos.py`
- Create: `tests/fixtures/github/repos_page_1.json`
- Modify: `README.md`

**Tests / verification:**
- Add or update: `tests/test_summary.py`
- Run: `python -m pytest tests/test_summary.py`
- Expected: `Summary tests can confirm repo extraction counts and filtered output behavior from sample payloads.`
- Run: `python -m src.github_analytics.extract_repos`
- Expected: `Raw repository JSON files are created locally and the output summary shows 200 retained repositories or fewer if the live filtered set is smaller.`

**Notes / risks:**
- Filtering archived and disabled repositories must happen before the final top-200 selection is persisted.
- Preserve the full raw JSON objects rather than flattening at extraction time.

- [ ] Step 1: Extend `tests/test_summary.py` or create it to validate filtering and count summaries against sample repository payloads.
- [ ] Step 2: Run `python -m pytest tests/test_summary.py` and confirm failure if the summary module is not implemented yet.
- [ ] Step 3: Implement `extract_repos.py` to fetch, filter, and save raw repository metadata and update `README.md` with the run command.
- [ ] Step 4: Run `python -m pytest tests/test_summary.py` and then run `python -m src.github_analytics.extract_repos`.
- [ ] Step 5: Stop and report the repo count, output path, and any live API constraints observed.

### Task 5: Implement Commit Extraction for the Deep-Dive Repository Set

**Outcome:** A local extraction module fetches commit history for `spark`, `kafka`, `flink`, `airflow`, and `iceberg` over the last 30 days and writes the raw pages into deterministic per-repo folders.

**Files to inspect:**
- `src/github_analytics/config.py`
- `src/github_analytics/github_client.py`
- `src/github_analytics/file_layout.py`

**Files to change:**
- Create: `src/github_analytics/extract_commits.py`
- Create: `tests/fixtures/github/commits_page_1.json`
- Modify: `tests/test_summary.py`

**Tests / verification:**
- Add or update: `tests/test_summary.py`
- Run: `python -m pytest tests/test_summary.py`
- Expected: `Summary tests confirm commit counts are computed per repo and per extraction date from sample raw data.`
- Run: `python -m src.github_analytics.extract_commits`
- Expected: `Raw commit JSON files exist for all five deep-dive repos and the summary prints non-zero commit counts for at least one repository.`

**Notes / risks:**
- The extractor must pass a `since` timestamp based on the configured 30-day window.
- Keep the repository list centralized in `config.py` so later notebook sections and report text stay aligned.

- [ ] Step 1: Expand `tests/test_summary.py` to validate commit count summaries from sample commit payloads.
- [ ] Step 2: Run `python -m pytest tests/test_summary.py` and confirm failure for the unimplemented commit path.
- [ ] Step 3: Implement `extract_commits.py` to fetch paginated commit payloads for the five deep-dive repos and save each page under the agreed layout.
- [ ] Step 4: Run `python -m pytest tests/test_summary.py` and then run `python -m src.github_analytics.extract_commits`.
- [ ] Step 5: Stop and report per-repo commit totals and any repos with unexpectedly low activity.

### Task 6: Implement Contributor Snapshot Extraction

**Outcome:** A local extraction module fetches contributor snapshots, including anonymous contributors, for the same five deep-dive repositories and saves them into deterministic raw paths.

**Files to inspect:**
- `src/github_analytics/config.py`
- `src/github_analytics/github_client.py`
- `src/github_analytics/file_layout.py`

**Files to change:**
- Create: `src/github_analytics/extract_contributors.py`
- Create: `tests/fixtures/github/contributors_page_1.json`
- Modify: `tests/test_summary.py`

**Tests / verification:**
- Add or update: `tests/test_summary.py`
- Run: `python -m pytest tests/test_summary.py`
- Expected: `Summary tests confirm contributor counts and anonymous-contributor handling from sample data.`
- Run: `python -m src.github_analytics.extract_contributors`
- Expected: `Raw contributor JSON files exist for all five deep-dive repos and the summary shows contributor counts per repo.`

**Notes / risks:**
- The contributor endpoint can be cached by GitHub, so the report should describe it as a snapshot, not a full event history.
- Preserve anonymous contributors instead of discarding them, since they matter for realistic counts.

- [ ] Step 1: Expand `tests/test_summary.py` to validate contributor summaries and anonymous-contributor counting.
- [ ] Step 2: Run `python -m pytest tests/test_summary.py` and confirm failure for the missing contributor logic.
- [ ] Step 3: Implement `extract_contributors.py` to fetch contributor snapshots with `anon=true` and write raw pages to disk.
- [ ] Step 4: Run `python -m pytest tests/test_summary.py` and then run `python -m src.github_analytics.extract_contributors`.
- [ ] Step 5: Stop and report contributor totals and any repos where anonymous contributor records appear.

### Task 7: Add End-to-End Extraction Orchestration and Summary Output

**Outcome:** One command runs all extractors in the correct order and produces a compact extraction summary suitable for notebook setup and report reference.

**Files to inspect:**
- `src/github_analytics/extract_repos.py`
- `src/github_analytics/extract_commits.py`
- `src/github_analytics/extract_contributors.py`

**Files to change:**
- Create: `src/github_analytics/summary.py`
- Create: `src/github_analytics/run_extraction.py`
- Modify: `README.md`
- Modify: `tests/test_summary.py`

**Tests / verification:**
- Add or update: `tests/test_summary.py`
- Run: `python -m pytest tests/test_summary.py`
- Expected: `Summary tests pass for repository, commit, and contributor extraction outputs.`
- Run: `python -m src.github_analytics.run_extraction`
- Expected: `A full extraction run completes and prints a per-source and per-repo summary without manual steps.`

**Notes / risks:**
- The summary should print totals that can later be compared with Bronze and Silver row counts.
- Keep orchestration linear and simple; no concurrency is needed for this project.

- [ ] Step 1: Write or extend `tests/test_summary.py` to verify end-to-end summary formatting and total calculations from fixture-backed sample outputs.
- [ ] Step 2: Run `python -m pytest tests/test_summary.py` and confirm failure.
- [ ] Step 3: Implement `summary.py`, `run_extraction.py`, and update `README.md` with the full extraction command sequence.
- [ ] Step 4: Run `python -m pytest tests/test_summary.py` and then run `python -m src.github_analytics.run_extraction`.
- [ ] Step 5: Stop and report the final extraction summary and any setup instructions needed before Databricks upload.

### Task 8: Create the Databricks Notebook Skeleton and Bronze Load

**Outcome:** The Databricks notebook exists as a source-controlled `.py` notebook script with project overview, architecture explanation, data scope notes, and Bronze ingestion logic for uploaded raw JSON files.

**Files to inspect:**
- `README.md`
- `src/github_analytics/file_layout.py`
- `src/github_analytics/summary.py`

**Files to change:**
- Create: `notebooks/01_github_analytics_pipeline.py`
- Modify: `README.md`

**Tests / verification:**
- Run: `python -m py_compile notebooks/01_github_analytics_pipeline.py`
- Expected: `The notebook source compiles as valid Python.`
- Run: `Databricks notebook cell execution for Bronze section`
- Expected: `Bronze DataFrames or temp views load raw repository, commit, and contributor JSON successfully from the uploaded paths.`

**Notes / risks:**
- Use clear markdown cells or comment banners so the notebook visibly maps to the course rubric.
- Bronze logic should add ingestion metadata columns and preserve nested JSON.

- [ ] Step 1: Create the notebook skeleton with architecture, scope, and setup sections plus placeholder Bronze code blocks driven by the planned raw paths.
- [ ] Step 2: Run `python -m py_compile notebooks/01_github_analytics_pipeline.py` and confirm any syntax issues before Databricks import.
- [ ] Step 3: Implement Bronze load cells for repository, commit, and contributor raw JSON plus basic preview queries.
- [ ] Step 4: Execute the Bronze notebook section in Databricks and confirm data loads from uploaded raw files.
- [ ] Step 5: Stop and report the Bronze row counts and any path adjustments required in Databricks.

### Task 9: Implement Silver Repository and Commit Transformations

**Outcome:** The notebook transforms raw repository and commit JSON into typed, deduplicated Silver tables that are ready for joins and time-based analysis.

**Files to inspect:**
- `notebooks/01_github_analytics_pipeline.py`
- `src/github_analytics/summary.py`

**Files to change:**
- Modify: `notebooks/01_github_analytics_pipeline.py`

**Tests / verification:**
- Run: `Databricks notebook cell execution for Silver repositories and commits`
- Expected: `silver_repositories and silver_commits are created with parsed timestamps, stable repository fields, and no duplicate (repo_id, sha) pairs.`
- Run: `Silver validation SQL in notebook`
- Expected: `Duplicate-count checks return zero and null timestamp counts are either zero or intentionally explained.`

**Notes / risks:**
- Keep `repo_id` and `repo_name` in both Silver tables to simplify later joins and report language.
- Commit author identity can be partially null for some records; preserve those rows and distinguish GitHub user identity from commit metadata identity.

- [ ] Step 1: Add notebook validation queries that should fail or show empty output until Silver logic is implemented.
- [ ] Step 2: Execute those validation queries in Databricks and confirm the expected pre-implementation failure or empty state.
- [ ] Step 3: Implement repository and commit parsing, typing, and dedup logic in the notebook.
- [ ] Step 4: Re-run the Silver notebook section and validation SQL.
- [ ] Step 5: Stop and report Silver schemas, row counts, and duplicate-check results.

### Task 10: Implement Silver Contributor Transformations

**Outcome:** The notebook transforms raw contributor JSON into a typed contributor snapshot table that preserves anonymous contributors and can be joined to repository context.

**Files to inspect:**
- `notebooks/01_github_analytics_pipeline.py`
- `src/github_analytics/summary.py`

**Files to change:**
- Modify: `notebooks/01_github_analytics_pipeline.py`

**Tests / verification:**
- Run: `Databricks notebook cell execution for Silver contributors`
- Expected: `silver_contributors_snapshot is created with snapshot date, contributor identity fields, anonymous-contributor marker, and stable repo context.`
- Run: `Silver contributor validation SQL in notebook`
- Expected: `Counts by repo match the extraction summary within the expected snapshot semantics.`

**Notes / risks:**
- Contributor snapshots are not a time series by themselves; the notebook should state that time-based contributor behavior comes from commit history.
- Anonymous contributors need an explicit boolean flag instead of implicit null interpretation.

- [ ] Step 1: Add validation queries for contributor snapshot counts and anonymous-contributor flags.
- [ ] Step 2: Execute those validations in Databricks and confirm they fail or remain incomplete before the implementation.
- [ ] Step 3: Implement contributor parsing, typing, and dedup logic in the notebook.
- [ ] Step 4: Re-run the Silver contributor section and validation SQL.
- [ ] Step 5: Stop and report contributor row counts and anonymous-contributor handling results.

### Task 11: Implement Core Gold Analytics

**Outcome:** The notebook produces Gold outputs for repository activity, commit trends, language distribution, contributor counts, and stars-versus-activity comparisons.

**Files to inspect:**
- `notebooks/01_github_analytics_pipeline.py`
- `docs/report-outline.md`

**Files to change:**
- Modify: `notebooks/01_github_analytics_pipeline.py`
- Create: `docs/report-outline.md`

**Tests / verification:**
- Run: `Databricks notebook cell execution for Gold analytics`
- Expected: `Gold tables or temp views exist for repo activity summary, weekly trends, monthly trends, language distribution, contributor counts, and popularity-versus-activity comparison.`
- Run: `Gold validation SQL in notebook`
- Expected: `Aggregate counts are internally consistent with Silver inputs and the outputs are readable enough for screenshots.`

**Notes / risks:**
- Build language distribution from the broad 200-repo metadata slice, not from the five deep-dive repos only.
- Keep Gold output names stable because the report and screenshot checklist depend on them.

- [ ] Step 1: Add report-outline sections for the planned analytics and write notebook validation queries for each Gold output.
- [ ] Step 2: Execute the notebook validation queries and confirm the expected pre-implementation gap.
- [ ] Step 3: Implement the Gold analytics tables and preview outputs in the notebook.
- [ ] Step 4: Re-run the Gold section and validation queries.
- [ ] Step 5: Stop and report the final Gold outputs and any findings strong enough to mention in the report.

### Task 12: Implement Contributor Behavior Analysis with Window Functions

**Outcome:** The notebook includes one advanced analytical component that shows contributor behavior over time using Spark window functions and supports the professor’s feedback directly.

**Files to inspect:**
- `notebooks/01_github_analytics_pipeline.py`
- `docs/report-outline.md`

**Files to change:**
- Modify: `notebooks/01_github_analytics_pipeline.py`
- Modify: `docs/report-outline.md`

**Tests / verification:**
- Run: `Databricks notebook cell execution for contributor behavior analysis`
- Expected: `The notebook produces first-seen versus returning contributor logic, contributor ranking or concentration metrics, and at least one window-based output that can be explained clearly in the report.`
- Run: `Contributor behavior validation SQL in notebook`
- Expected: `Sample contributor timelines and ranking outputs are coherent and free of obvious duplication.`

**Notes / risks:**
- Use commit history, not contributor snapshots, as the source for time-based contributor behavior.
- Keep the advanced analysis interpretable; simple, correct window logic is better than overcomplicated metrics.

- [ ] Step 1: Add validation queries and report-outline notes for first-seen contributors, returning contributors, and contributor concentration.
- [ ] Step 2: Execute the notebook section before implementation and confirm missing outputs.
- [ ] Step 3: Implement the contributor behavior analysis with explicit window functions.
- [ ] Step 4: Re-run the analysis and validation SQL in Databricks.
- [ ] Step 5: Stop and report the final advanced-analysis outputs and the exact window functions used.

### Task 13: Create Submission Checklist, Screenshot Plan, and Final Narrative Cleanup

**Outcome:** The project has submission-ready supporting documents, a screenshot checklist tied to notebook outputs, and a notebook narrative that maps cleanly to the grading rubric.

**Files to inspect:**
- `notebooks/01_github_analytics_pipeline.py`
- `docs/report-outline.md`
- `README.md`

**Files to change:**
- Create: `docs/screenshots-checklist.md`
- Modify: `docs/report-outline.md`
- Modify: `notebooks/01_github_analytics_pipeline.py`
- Modify: `README.md`

**Tests / verification:**
- Run: `Manual rubric walkthrough using notebook, report outline, and screenshot checklist`
- Expected: `Every required deliverable and grading category has one explicit artifact or notebook section that satisfies it.`

**Notes / risks:**
- The final notebook should read like a polished submission, not an internal engineering log.
- Screenshot targets must be concrete so the final packaging step is fast.

- [ ] Step 1: Create `docs/screenshots-checklist.md` with exact notebook sections and output names to capture.
- [ ] Step 2: Review the notebook and report outline against the project requirements and professor feedback, noting any gaps.
- [ ] Step 3: Tighten notebook comments, section headers, and report-outline language so architecture, ELT, Spark usage, and tradeoffs are explicit.
- [ ] Step 4: Perform the manual rubric walkthrough and confirm each category is covered.
- [ ] Step 5: Stop and report any remaining submission risks or polish work.

## Verification Commands Summary

- `python -m pytest tests/test_config.py`
- `python -m pytest tests/test_file_layout.py`
- `python -m pytest tests/test_github_client.py`
- `python -m pytest tests/test_summary.py`
- `python -m src.github_analytics.extract_repos`
- `python -m src.github_analytics.extract_commits`
- `python -m src.github_analytics.extract_contributors`
- `python -m src.github_analytics.run_extraction`
- `python -m py_compile notebooks/01_github_analytics_pipeline.py`
- `Databricks notebook execution for Bronze, Silver, Gold, and contributor-behavior sections`

## Self-Review

- Spec coverage: The plan covers ingestion, storage, transformation, querying, outputs, architecture explanation, report, and screenshots.
- Placeholder scan: No `TBD`, `TODO`, or deferred validation placeholders are present.
- Naming consistency: Bronze, Silver, Gold, `apache`, deep-dive repo list, and 30-day window are consistent throughout.
- Task boundaries: Each task has one main outcome, a small file set, and a clear verification path.
- Low-context execution: The file map, task sequence, and command list are explicit enough for a low-context coding agent to execute safely.
