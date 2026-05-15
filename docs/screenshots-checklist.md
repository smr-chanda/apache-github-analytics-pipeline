# Apache GitHub Analytics Screenshot Checklist

## Required Screenshots

### 1. Notebook overview and architecture
- Capture the title and the opening markdown section.
- Make sure Bronze, Silver, Gold, ELT, and Databricks are visible in the notebook narrative.
- Use this near the start of the report or presentation to show the project structure.

### 2. Bronze raw repositories preview
- Capture the `bronze_repos_raw` preview output.
- Show `id`, `name`, `language`, `extract_date`, and `source_file`.
- This proves raw JSON landed in Databricks with ingestion metadata preserved.

### 3. Bronze commit or contributor validation
- Capture one Bronze validation result, preferably commit row counts by repository.
- This shows that the uploaded raw files were partitioned and loaded correctly.

### 4. Silver repositories preview
- Capture the `silver_repositories` preview output.
- Show typed repository fields such as `repo_id`, `repo_name`, `language`, `stargazers_count`, and `pushed_at`.
- This demonstrates that raw JSON was converted into a clean relational table.

### 5. Silver commits validation
- Capture the duplicate-check or timestamp validation output for `silver_commits`.
- Prefer the query that shows no duplicate `(repo_id, sha)` rows.
- This is the best proof that the Silver layer is doing real cleaning and quality control.

### 6. Gold repository activity output
- Capture `gold_repo_activity_summary`.
- Show repositories ordered by `commit_count_30d`.
- Use this as one of the main findings in the report.

### 7. Gold time-trend output
- Capture either `gold_commit_trends_weekly` or `gold_commit_trends_monthly`.
- If Databricks charting looks clean, use a line or bar chart instead of a table.
- This supports the time-based analytics requirement from the proposal.

### 8. Gold language distribution output
- Capture `gold_language_distribution`.
- Prefer a chart if it is readable; otherwise use the ordered table.
- This supports the broader organization metadata analysis.

### 9. Gold popularity versus activity output
- Capture `gold_repo_popularity_vs_activity`.
- Show `repo_name`, `stargazers_count`, `forks_count`, and `commit_count_30d`.
- This supports comparison between popularity and recent engineering work.

### 10. Advanced contributor behavior output
- Capture `gold_contributor_behavior`.
- Include `contributor_status`, `contributor_rank`, and `contribution_share`.
- This directly addresses the professor feedback asking for a more advanced analytical component.

### 11. Final dataset summary
- Capture the `final_dataset_summary` output.
- Use this as the closing proof that the full Bronze, Silver, and Gold pipeline executed end to end.

## Final Packaging Check
- Make sure each screenshot comes from the final notebook version, not an intermediate run.
- Keep notebook cell names and table names consistent with the report.
- Avoid screenshots with truncated columns when the point is data quality or architecture evidence.
