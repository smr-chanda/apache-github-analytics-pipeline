# Databricks notebook source
# Apache GitHub Analytics Pipeline
# This notebook is designed to be imported into Databricks as a source notebook.

# COMMAND ----------
# MAGIC %md
# MAGIC # Apache GitHub Analytics Pipeline
# MAGIC
# MAGIC **Goal:** Analyze Apache GitHub activity with a batch ELT pipeline that lands raw JSON in Bronze, standardizes it in Silver, and produces analytics-ready Gold outputs.
# MAGIC
# MAGIC **Architecture:** Raw GitHub REST API payloads are extracted locally with Python and uploaded to Databricks storage. This notebook performs the Databricks portion of the project using PySpark and Spark SQL with explicit Bronze, Silver, and Gold sections.
# MAGIC
# MAGIC **Scope:** Apache organization metadata for the top 200 active repositories, plus a deep-dive analysis of `spark`, `kafka`, `flink`, `airflow`, and `iceberg` over the most recent 30-day commit window.

# COMMAND ----------
# Configuration cell

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


ORG_NAME = "apache"
DEEP_DIVE_REPOS = ["spark", "kafka", "flink", "airflow", "iceberg"]
COMMIT_WINDOW_DAYS = 30
REPO_METADATA_LIMIT = 200

# Replace this with the uploaded Databricks volume or workspace path before running.
RAW_BASE_PATH = "/Volumes/workspace/default/github/raw/github"

# The raw layout matches the local extractor output:
# raw/github/org=apache/extract_date=YYYY-MM-DD/source=repos/page=001.json
# raw/github/org=apache/extract_date=YYYY-MM-DD/source=commits/repo=spark/page=001.json
# raw/github/org=apache/extract_date=YYYY-MM-DD/source=contributors/repo=spark/page=001.json


def display_frame(frame: DataFrame, limit: int = 20) -> None:
    """Use Databricks display() when available and fall back to show()."""
    if "display" in globals():
        display(frame.limit(limit))
    else:
        frame.show(limit, truncate=False)


def run_validation(name: str, query: str) -> None:
    """Execute a named validation query so notebook checks are easy to rerun."""
    print(f"\n=== {name} ===")
    display_frame(spark.sql(query))


print(
    {
        "org_name": ORG_NAME,
        "deep_dive_repos": DEEP_DIVE_REPOS,
        "commit_window_days": COMMIT_WINDOW_DAYS,
        "raw_base_path": RAW_BASE_PATH,
    }
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bronze Layer
# MAGIC
# MAGIC Bronze preserves the raw JSON shape with minimal normalization. Each read adds ingestion metadata from the uploaded file path so later validation can reconcile Databricks counts against the local extraction summary.

# COMMAND ----------


def bronze_path(source_name: str) -> str:
    return f"{RAW_BASE_PATH}/org={ORG_NAME}/*/source={source_name}"


bronze_repos_raw = (
    spark.read.option("multiLine", True)
    .json(f"{bronze_path('repos')}/*.json")
    .withColumn("source_file", F.input_file_name())
    .withColumn("extract_date", F.regexp_extract("source_file", r"extract_date=([0-9-]+)", 1))
    .withColumn("source_name", F.lit("repos"))
)

bronze_commits_raw = (
    spark.read.option("multiLine", True)
    .json(f"{bronze_path('commits')}/repo=*/*.json")
    .withColumn("source_file", F.input_file_name())
    .withColumn("extract_date", F.regexp_extract("source_file", r"extract_date=([0-9-]+)", 1))
    .withColumn("repo_name", F.regexp_extract("source_file", r"repo=([^/]+)", 1))
    .withColumn("source_name", F.lit("commits"))
)

bronze_contributors_raw = (
    spark.read.option("multiLine", True)
    .json(f"{bronze_path('contributors')}/repo=*/*.json")
    .withColumn("source_file", F.input_file_name())
    .withColumn("extract_date", F.regexp_extract("source_file", r"extract_date=([0-9-]+)", 1))
    .withColumn("repo_name", F.regexp_extract("source_file", r"repo=([^/]+)", 1))
    .withColumn("source_name", F.lit("contributors"))
)

bronze_repos_raw.createOrReplaceTempView("bronze_repos_raw")
bronze_commits_raw.createOrReplaceTempView("bronze_commits_raw")
bronze_contributors_raw.createOrReplaceTempView("bronze_contributors_raw")

display_frame(bronze_repos_raw.select("id", "name", "language", "extract_date", "source_file"))
display_frame(bronze_commits_raw.select("sha", "repo_name", "extract_date", "source_file"))
display_frame(bronze_contributors_raw.select("login", "repo_name", "extract_date", "source_file"))

# COMMAND ----------
# Bronze validation queries

BRONZE_VALIDATIONS = {
    "bronze_repo_row_count": """
        SELECT source_name, extract_date, COUNT(*) AS row_count
        FROM bronze_repos_raw
        GROUP BY source_name, extract_date
        ORDER BY extract_date DESC
    """,
    "bronze_commit_row_count_by_repo": """
        SELECT repo_name, extract_date, COUNT(*) AS row_count
        FROM bronze_commits_raw
        GROUP BY repo_name, extract_date
        ORDER BY repo_name, extract_date DESC
    """,
    "bronze_contributor_row_count_by_repo": """
        SELECT repo_name, extract_date, COUNT(*) AS row_count
        FROM bronze_contributors_raw
        GROUP BY repo_name, extract_date
        ORDER BY repo_name, extract_date DESC
    """,
}

for validation_name, validation_query in BRONZE_VALIDATIONS.items():
    run_validation(validation_name, validation_query)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Silver Layer
# MAGIC
# MAGIC Silver converts the semi-structured payloads into typed, deduplicated tables that are stable for joins and time-based analysis.

# COMMAND ----------

silver_repositories_ranked = (
    bronze_repos_raw.select(
        F.col("id").alias("repo_id"),
        F.col("name").alias("repo_name"),
        F.col("full_name"),
        F.col("language"),
        F.col("stargazers_count"),
        F.col("forks_count"),
        F.col("open_issues_count"),
        F.col("archived"),
        F.col("disabled"),
        F.to_timestamp("created_at").alias("created_at"),
        F.to_timestamp("updated_at").alias("updated_at"),
        F.to_timestamp("pushed_at").alias("pushed_at"),
        F.col("extract_date"),
        F.row_number().over(
            Window.partitionBy("id").orderBy(F.col("extract_date").desc(), F.col("updated_at").desc())
        ).alias("repo_version_rank"),
    )
)

silver_repositories = (
    silver_repositories_ranked.filter(F.col("repo_version_rank") == 1)
    .filter(~F.col("archived") & ~F.col("disabled"))
    .withColumn(
        "activity_rank",
        F.row_number().over(Window.orderBy(F.col("pushed_at").desc(), F.col("repo_name").asc())),
    )
    .filter(F.col("activity_rank") <= REPO_METADATA_LIMIT)
    .drop("repo_version_rank")
    .drop("activity_rank", "archived", "disabled")
)

silver_commits = (
    bronze_commits_raw.select(
        F.col("repo_name"),
        F.col("sha"),
        F.col("author.login").alias("author_login"),
        F.col("author.id").alias("author_id"),
        F.col("commit.author.name").alias("commit_author_name"),
        F.col("commit.author.email").alias("commit_author_email"),
        F.to_timestamp(F.col("commit.author.date")).alias("commit_timestamp"),
        F.col("commit.message").alias("message"),
        F.col("extract_date"),
    )
    .join(
        silver_repositories.select("repo_id", "repo_name"),
        on="repo_name",
        how="left",
    )
    .dropDuplicates(["repo_id", "sha"])
)

silver_contributors_snapshot = (
    bronze_contributors_raw.select(
        F.col("repo_name"),
        F.col("login").alias("contributor_login"),
        F.col("id").alias("contributor_id"),
        F.col("name").alias("contributor_name"),
        F.col("contributions"),
        F.col("type"),
        F.col("extract_date").alias("snapshot_date"),
        F.when(F.col("login").isNull(), F.lit(True)).otherwise(F.lit(False)).alias("is_anonymous"),
        F.coalesce(
            F.col("login"),
            F.concat_ws(":", F.lit("anonymous"), F.col("name"), F.col("contributions").cast("string")),
        ).alias("contributor_key"),
    )
    .join(
        silver_repositories.select("repo_id", "repo_name"),
        on="repo_name",
        how="left",
    )
    .dropDuplicates(["repo_id", "contributor_key", "snapshot_date", "is_anonymous"])
    .drop("contributor_key")
)

silver_repositories.createOrReplaceTempView("silver_repositories")
silver_commits.createOrReplaceTempView("silver_commits")
silver_contributors_snapshot.createOrReplaceTempView("silver_contributors_snapshot")

display_frame(silver_repositories)
display_frame(silver_commits)
display_frame(silver_contributors_snapshot)

# COMMAND ----------
# Silver validation queries

SILVER_VALIDATIONS = {
    "silver_repository_null_key_check": """
        SELECT
            SUM(CASE WHEN repo_id IS NULL THEN 1 ELSE 0 END) AS null_repo_ids,
            SUM(CASE WHEN repo_name IS NULL THEN 1 ELSE 0 END) AS null_repo_names
        FROM silver_repositories
    """,
    "silver_commit_duplicate_check": """
        SELECT repo_id, sha, COUNT(*) AS duplicate_count
        FROM silver_commits
        GROUP BY repo_id, sha
        HAVING COUNT(*) > 1
    """,
    "silver_commit_timestamp_check": """
        SELECT
            SUM(CASE WHEN commit_timestamp IS NULL THEN 1 ELSE 0 END) AS null_commit_timestamps
        FROM silver_commits
    """,
    "silver_contributor_anonymous_check": """
        SELECT repo_name, is_anonymous, COUNT(*) AS contributor_rows
        FROM silver_contributors_snapshot
        GROUP BY repo_name, is_anonymous
        ORDER BY repo_name, is_anonymous DESC
    """,
}

for validation_name, validation_query in SILVER_VALIDATIONS.items():
    run_validation(validation_name, validation_query)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Gold Layer
# MAGIC
# MAGIC Gold contains the final analytics used in the proposal, report, and screenshots. These outputs are designed to be readable as submission artifacts, not just intermediate engineering checks.

# COMMAND ----------

gold_repo_activity_summary = (
    silver_commits.groupBy("repo_id", "repo_name")
    .agg(F.count("*").alias("commit_count_30d"))
    .orderBy(F.col("commit_count_30d").desc(), F.col("repo_name").asc())
)

gold_commit_trends_weekly = (
    silver_commits.withColumn("activity_week", F.date_trunc("week", F.col("commit_timestamp")))
    .groupBy("repo_name", "activity_week")
    .agg(F.count("*").alias("commit_count"))
    .orderBy("repo_name", "activity_week")
)

gold_commit_trends_monthly = (
    silver_commits.withColumn("activity_month", F.date_trunc("month", F.col("commit_timestamp")))
    .groupBy("repo_name", "activity_month")
    .agg(F.count("*").alias("commit_count"))
    .orderBy("repo_name", "activity_month")
)

gold_language_distribution = (
    silver_repositories.groupBy("language")
    .agg(F.count("*").alias("repository_count"))
    .fillna({"language": "Unknown"})
    .orderBy(F.col("repository_count").desc(), F.col("language").asc())
)

gold_repo_contributor_counts = (
    silver_contributors_snapshot.groupBy("repo_id", "repo_name")
    .agg(F.count("*").alias("contributor_count_snapshot"))
    .orderBy(F.col("contributor_count_snapshot").desc(), F.col("repo_name").asc())
)

gold_repo_popularity_vs_activity = (
    silver_repositories.select("repo_id", "repo_name", "stargazers_count", "forks_count")
    .join(gold_repo_activity_summary, on=["repo_id", "repo_name"], how="left")
    .fillna({"commit_count_30d": 0})
    .orderBy(F.col("stargazers_count").desc(), F.col("commit_count_30d").desc())
)

contributor_activity_base = (
    silver_commits.filter(F.col("author_login").isNotNull())
    .withColumn("activity_week", F.date_trunc("week", F.col("commit_timestamp")))
    .groupBy("repo_name", "author_login", "activity_week")
    .agg(F.count("*").alias("weekly_commit_count"))
)

first_seen_window = Window.partitionBy("repo_name", "author_login")
concentration_window = Window.partitionBy("repo_name").orderBy(F.col("total_commits").desc(), F.col("author_login").asc())

contributor_first_seen = (
    silver_commits.filter(F.col("author_login").isNotNull())
    .withColumn("activity_week", F.date_trunc("week", F.col("commit_timestamp")))
    .groupBy("repo_name", "author_login", "activity_week")
    .agg(F.count("*").alias("weekly_commit_count"))
    .withColumn("first_active_week", F.min("activity_week").over(first_seen_window))
    .withColumn(
        "contributor_status",
        F.when(F.col("activity_week") == F.col("first_active_week"), F.lit("new")).otherwise(F.lit("returning")),
    )
)

contributor_concentration = (
    silver_commits.filter(F.col("author_login").isNotNull())
    .groupBy("repo_name", "author_login")
    .agg(F.count("*").alias("total_commits"))
    .withColumn("contributor_rank", F.row_number().over(concentration_window))
    .withColumn("repo_commit_total", F.sum("total_commits").over(Window.partitionBy("repo_name")))
    .withColumn(
        "contribution_share",
        F.when(F.col("repo_commit_total") == 0, F.lit(0.0)).otherwise(F.col("total_commits") / F.col("repo_commit_total")),
    )
)

gold_contributor_behavior = (
    contributor_first_seen.alias("timeline")
    .join(
        contributor_concentration.select(
            "repo_name",
            "author_login",
            "contributor_rank",
            "total_commits",
            "repo_commit_total",
            "contribution_share",
        ).alias("concentration"),
        on=["repo_name", "author_login"],
        how="left",
    )
    .orderBy("repo_name", "activity_week", "contributor_rank")
)

gold_repo_activity_summary.createOrReplaceTempView("gold_repo_activity_summary")
gold_commit_trends_weekly.createOrReplaceTempView("gold_commit_trends_weekly")
gold_commit_trends_monthly.createOrReplaceTempView("gold_commit_trends_monthly")
gold_language_distribution.createOrReplaceTempView("gold_language_distribution")
gold_repo_contributor_counts.createOrReplaceTempView("gold_repo_contributor_counts")
gold_repo_popularity_vs_activity.createOrReplaceTempView("gold_repo_popularity_vs_activity")
gold_contributor_behavior.createOrReplaceTempView("gold_contributor_behavior")

display_frame(gold_repo_activity_summary)
display_frame(gold_commit_trends_weekly)
display_frame(gold_commit_trends_monthly)
display_frame(gold_language_distribution)
display_frame(gold_repo_contributor_counts)
display_frame(gold_repo_popularity_vs_activity)
display_frame(gold_contributor_behavior)

# COMMAND ----------
# Gold validation queries

GOLD_VALIDATIONS = {
    "gold_repo_activity_check": """
        SELECT repo_name, commit_count_30d
        FROM gold_repo_activity_summary
        ORDER BY commit_count_30d DESC, repo_name
    """,
    "gold_language_distribution_check": """
        SELECT language, repository_count
        FROM gold_language_distribution
        ORDER BY repository_count DESC, language
    """,
    "gold_popularity_vs_activity_check": """
        SELECT repo_name, stargazers_count, forks_count, commit_count_30d
        FROM gold_repo_popularity_vs_activity
        ORDER BY stargazers_count DESC, commit_count_30d DESC
    """,
    "gold_contributor_behavior_check": """
        SELECT
            repo_name,
            author_login,
            activity_week,
            contributor_status,
            contributor_rank,
            total_commits,
            ROUND(contribution_share, 4) AS contribution_share
        FROM gold_contributor_behavior
        ORDER BY repo_name, activity_week, contributor_rank
    """,
}

for validation_name, validation_query in GOLD_VALIDATIONS.items():
    run_validation(validation_name, validation_query)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Report and Screenshot Guidance
# MAGIC
# MAGIC The final report should explain why ELT and the medallion architecture fit this project, how the Spark transformations work, and what the analytics reveal about Apache repository activity. The screenshot checklist should capture one strong example from Bronze, Silver, and each major Gold output so the submission clearly shows the full pipeline.

# COMMAND ----------

final_summary_query = """
    SELECT 'bronze_repos_raw' AS dataset, COUNT(*) AS row_count FROM bronze_repos_raw
    UNION ALL
    SELECT 'bronze_commits_raw' AS dataset, COUNT(*) AS row_count FROM bronze_commits_raw
    UNION ALL
    SELECT 'bronze_contributors_raw' AS dataset, COUNT(*) AS row_count FROM bronze_contributors_raw
    UNION ALL
    SELECT 'silver_repositories' AS dataset, COUNT(*) AS row_count FROM silver_repositories
    UNION ALL
    SELECT 'silver_commits' AS dataset, COUNT(*) AS row_count FROM silver_commits
    UNION ALL
    SELECT 'silver_contributors_snapshot' AS dataset, COUNT(*) AS row_count FROM silver_contributors_snapshot
    UNION ALL
    SELECT 'gold_repo_activity_summary' AS dataset, COUNT(*) AS row_count FROM gold_repo_activity_summary
    UNION ALL
    SELECT 'gold_commit_trends_weekly' AS dataset, COUNT(*) AS row_count FROM gold_commit_trends_weekly
    UNION ALL
    SELECT 'gold_commit_trends_monthly' AS dataset, COUNT(*) AS row_count FROM gold_commit_trends_monthly
    UNION ALL
    SELECT 'gold_language_distribution' AS dataset, COUNT(*) AS row_count FROM gold_language_distribution
    UNION ALL
    SELECT 'gold_repo_contributor_counts' AS dataset, COUNT(*) AS row_count FROM gold_repo_contributor_counts
    UNION ALL
    SELECT 'gold_repo_popularity_vs_activity' AS dataset, COUNT(*) AS row_count FROM gold_repo_popularity_vs_activity
    UNION ALL
    SELECT 'gold_contributor_behavior' AS dataset, COUNT(*) AS row_count FROM gold_contributor_behavior
"""

run_validation("final_dataset_summary", final_summary_query)
