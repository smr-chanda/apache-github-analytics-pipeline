# Apache GitHub Activity Analytics with PySpark

**Student:** Sai Mani Raj Chanda  
**Student ID:** 008735877  
**Course:** CSE 6320 - BIG DATA MANAGEMENT  
**Professor:** Dr. Said Ngobi  
**Submission Date:** 15/05/2026  

## Project Overview

This project builds an end-to-end big data pipeline for analyzing public GitHub activity from the Apache organization. The goal was to use a real semi-structured data source, process it through a clear lakehouse pipeline, and produce meaningful analytics about repository activity, language usage, contributor participation, and popularity versus recent engineering activity.

The scope was intentionally controlled to keep the work practical and technically sound. Repository metadata was collected for 200 active Apache repositories, while commit and contributor analysis focused on five repositories: Airflow, Spark, Iceberg, Kafka, and Flink. The commit window was limited to the latest 30 days so that the pipeline remained realistic for Databricks Free Edition while still showing current repository behavior.

Project repository: `https://github.com/smr-chanda/apache-github-analytics-pipeline`

## Architecture and Processing

The pipeline follows an ELT design implemented through a Bronze-Silver-Gold lakehouse structure. This was the right architecture for the project because the GitHub REST API returns nested JSON rather than clean relational tables. Instead of flattening data before loading it, the pipeline first preserves the raw payloads and then uses Spark to transform them into structured analytical datasets. That choice keeps the pipeline simpler, preserves lineage back to the source, and makes it easier to inspect or reprocess the data when needed.

The ingestion path is split into two stages. First, Python extracts repository, commit, and contributor data from the GitHub API and saves the responses as raw JSON files. Second, those files are uploaded into Databricks storage, where Spark reads them directly for downstream processing. This separation is practical for Databricks Free Edition, where local extraction is easier for API access and Databricks is better used as the transformation and analytics engine.

The Bronze layer acts as the raw landing zone. It stores the original repository, commit, and contributor payloads with minimal change, along with extraction context from the file layout. The purpose of Bronze is not analysis; it is preservation, traceability, and replayability. If a downstream result looks wrong, the raw records can still be inspected without losing any of the original API structure.

The Silver layer converts the raw JSON into typed, reliable tables. Repository metadata is cleaned and deduplicated, timestamps are normalized, and commit and contributor records are joined to repository context. This layer is where the data engineering work becomes visible: semi-structured records are turned into stable entities that support accurate filtering, grouping, and joins.

The Gold layer contains the final outputs used for interpretation. These outputs include most active repositories, weekly and monthly commit trends, language distribution across the broader Apache set, contributor counts by repository, popularity-versus-activity comparisons, and contributor behavior over time. Structuring the notebook this way makes the flow explicit: Bronze preserves raw source data, Silver creates trustworthy analytical tables, and Gold delivers the business-facing results.

Spark is the core processing engine throughout Silver and Gold. PySpark DataFrames were used for JSON ingestion, normalization, joins, deduplication, and aggregation. Window functions were used in the advanced analytical component to identify each contributor's first active week, distinguish new versus returning contributors, and rank contributors within repositories based on their overall activity. That advanced step strengthens the project beyond static counts and shows how behavior changes over time.

## Results

The final results showed that Airflow was the most active repository in the 30-day deep-dive with 776 commits, followed by Spark with 335. Iceberg, Kafka, and Flink also showed meaningful development activity, but at lower levels. This indicates that the pipeline successfully captured recent engineering behavior rather than only static repository metadata.

Across the 200-repository metadata slice, Java was the dominant language with 95 repositories. HTML and Python followed at much lower levels, which reflects the strong Java orientation of the Apache ecosystem. This broader metadata view complements the deeper repository-level commit analysis by showing what kinds of technologies dominate the organization.

The contributor outputs added another layer of insight. Airflow and Spark had the highest contributor snapshot counts, which aligned with their high recent commit volumes. Weekly trends showed that activity was not evenly distributed across the 30-day window, and the popularity-versus-activity output showed that heavily starred repositories are not always the ones with the highest recent engineering activity. That distinction helps separate long-term visibility from current development intensity.

The advanced contributor-behavior analysis was the strongest analytical component. By identifying each contributor's first active week and ranking contributors by total activity within each repository, the project moved beyond summary counts into behavioral analysis. This makes the pipeline more analytically meaningful and addresses the requirement for a stronger advanced component.

## Validation and Trade-offs

The final run produced 200 repositories, 1,537 commits, and 12,099 contributor rows after Silver-layer cleanup. Validation checks confirmed zero duplicate commits and zero null commit timestamps, which indicates that the core transformation logic behaved correctly on the final dataset. These checks were important because the project depended on joins across multiple semi-structured datasets, and incorrect parsing or duplication would have reduced confidence in the final outputs.

The main trade-off was scope control. Apache contains far more repositories than could be deeply analyzed within the available time and platform limits, especially in Databricks Free Edition. The project therefore used a broad metadata slice for organization-level analysis and a narrower deep-dive subset for commits and contributors. This trade-off kept the pipeline feasible without weakening the quality of the final analysis.

Another limitation is that GitHub contributor snapshots are not a historical event stream. They describe contributor totals at extraction time rather than a full sequence of historical contributor events. For that reason, the time-based contributor analysis was derived primarily from commit history. In addition, a large share of contributor rows were anonymous, which limits identity-level precision in some contributor comparisons even though the overall activity analysis remains valid.

A final trade-off was the choice of batch processing rather than streaming. Streaming was optional for the course, and it was intentionally excluded so the project could focus on a stronger batch implementation with reliable ingestion, clear transformations, and reproducible final outputs.

## Conclusion

Overall, the project meets the course goal of building a realistic end-to-end big data pipeline. It demonstrates practical ingestion from a real API source, storage of semi-structured data, Spark-based transformation, multi-table joins, and meaningful analytical outputs.

More importantly, the project shows real data engineering thinking. It does not rely on flat files or shallow descriptive analysis alone. Instead, it preserves raw JSON, stages data through explicit Bronze-Silver-Gold layers, performs structured transformations in Spark, and adds a time-based contributor analysis using window functions. The final result is a technically sound, well-scoped GitHub analytics pipeline that aligns closely with the objectives of Big Data Management.
