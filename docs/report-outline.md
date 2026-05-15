# Apache GitHub Analytics Report Outline

## 1. Project Goal
- State that the project analyzes Apache GitHub activity with a batch ELT pipeline built for Databricks.
- Frame the value in practical terms: understanding repository activity, language mix, contributor participation, and popularity versus engineering activity.

## 2. Architecture Choice
- Explain the split workflow: local Python extraction for GitHub REST API data, then Databricks for Spark processing and analytics.
- State why this is an ELT pipeline: raw JSON lands first, transformations happen inside Spark.
- Name the medallion structure directly: Bronze for raw payloads, Silver for cleaned relational tables, Gold for analytics outputs.

## 3. Data Scope
- Apache organization metadata covers the top 200 recently pushed public repositories.
- Deep-dive activity analysis covers `spark`, `kafka`, `flink`, `airflow`, and `iceberg`.
- Commit history is limited to the latest 30-day window to keep scope realistic and aligned with the project plan.

## 4. Data Engineering Workflow
- Describe Bronze ingestion of uploaded raw JSON with extraction metadata from file paths.
- Describe Silver parsing of repositories, commits, and contributors with typed timestamps, deduplication, and joinable keys.
- Describe Gold analytics as the submission-facing layer with activity, language, contributor, and popularity outputs.

## 5. Spark Work Used
- PySpark DataFrames for JSON ingestion, normalization, deduplication, and aggregation.
- Spark SQL validation queries for row counts, duplicate checks, and screenshot-ready outputs.
- Window functions for contributor behavior over time, including first-seen versus returning contributors and contributor ranking within repositories.

## 6. Main Findings to Fill In After Execution
- Most active repositories by 30-day commit volume.
- Weekly and monthly commit trend highlights.
- Top languages across the broader Apache repository set.
- Contributor snapshot patterns by repository.
- Relationship between stars and recent engineering activity.
- Contributor concentration and participation patterns from the advanced analysis.

## 7. Tradeoffs and Limitations
- GitHub REST API contributor snapshots are point-in-time and not a historical event stream.
- Commit analysis is intentionally scoped to five deep-dive repositories and a 30-day window.
- Databricks notebook execution depends on uploaded raw files and workspace path configuration.

## 8. Conclusion
- Re-state that the project demonstrates a full big-data pipeline, not just static analysis.
- Tie the conclusion back to the course goals: ingestion, semi-structured data handling, Spark transformations, joins, analytics, and a clear architecture.
