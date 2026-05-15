# Databricks notebook source
# Apache GitHub Analytics Pipeline - Submission Notebook

# COMMAND ----------
# MAGIC %md
# MAGIC # Apache GitHub Analytics Pipeline
# MAGIC
# MAGIC This notebook reads raw GitHub JSON data from a Databricks volume, cleans it with PySpark, and produces final analytics for the project submission.

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F


RAW_BASE_PATH = "/Volumes/workspace/github_project/github_raw/github"
ORG_NAME = "apache"
REPO_LIMIT = 200


def show_df(df, rows=20):
    if "display" in globals():
        display(df.limit(rows))
    else:
        df.show(rows, truncate=False)


# COMMAND ----------
# MAGIC %md
# MAGIC ## Bronze: Read Raw JSON

# COMMAND ----------

repos_raw = (
    spark.read.option("multiLine", True)
    .json(f"{RAW_BASE_PATH}/org={ORG_NAME}/*/source=repos/*.json")
    .withColumn("source_file", F.input_file_name())
    .withColumn("extract_date", F.regexp_extract("source_file", r"extract_date=([0-9-]+)", 1))
)

commits_raw = (
    spark.read.option("multiLine", True)
    .json(f"{RAW_BASE_PATH}/org={ORG_NAME}/*/source=commits/repo=*/*.json")
    .withColumn("source_file", F.input_file_name())
    .withColumn("extract_date", F.regexp_extract("source_file", r"extract_date=([0-9-]+)", 1))
    .withColumn("repo_name", F.regexp_extract("source_file", r"repo=([^/]+)", 1))
)

contributors_raw = (
    spark.read.option("multiLine", True)
    .json(f"{RAW_BASE_PATH}/org={ORG_NAME}/*/source=contributors/repo=*/*.json")
    .withColumn("source_file", F.input_file_name())
    .withColumn("extract_date", F.regexp_extract("source_file", r"extract_date=([0-9-]+)", 1))
    .withColumn("repo_name", F.regexp_extract("source_file", r"repo=([^/]+)", 1))
)

print("Bronze loaded")
print("Repos:", repos_raw.count())
print("Commits:", commits_raw.count())
print("Contributors:", contributors_raw.count())

show_df(repos_raw.select("id", "name", "language", "extract_date"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Silver: Clean and Structure Data

# COMMAND ----------

repo_rank_window = Window.partitionBy("id").orderBy(F.col("extract_date").desc(), F.col("pushed_at").desc())

repositories = (
    repos_raw.select(
        F.col("id").alias("repo_id"),
        F.col("name").alias("repo_name"),
        "full_name",
        "language",
        "stargazers_count",
        "forks_count",
        "open_issues_count",
        "archived",
        "disabled",
        F.to_timestamp("created_at").alias("created_at"),
        F.to_timestamp("updated_at").alias("updated_at"),
        F.to_timestamp("pushed_at").alias("pushed_at"),
        "extract_date",
    )
    .withColumn("repo_rank", F.row_number().over(repo_rank_window))
    .filter(F.col("repo_rank") == 1)
    .filter(~F.col("archived") & ~F.col("disabled"))
    .withColumn("activity_rank", F.row_number().over(Window.orderBy(F.col("pushed_at").desc(), F.col("repo_name").asc())))
    .filter(F.col("activity_rank") <= REPO_LIMIT)
    .drop("repo_rank", "activity_rank", "archived", "disabled")
)

commits = (
    commits_raw.select(
        "repo_name",
        "sha",
        F.col("author.login").alias("author_login"),
        F.col("author.id").alias("author_id"),
        F.col("commit.author.name").alias("commit_author_name"),
        F.col("commit.author.email").alias("commit_author_email"),
        F.to_timestamp(F.col("commit.author.date")).alias("commit_timestamp"),
        F.col("commit.message").alias("message"),
        "extract_date",
    )
    .join(repositories.select("repo_id", "repo_name"), on="repo_name", how="left")
    .dropDuplicates(["repo_id", "sha"])
)

contributors = (
    contributors_raw.select(
        "repo_name",
        F.col("login").alias("contributor_login"),
        F.col("id").alias("contributor_id"),
        F.col("name").alias("contributor_name"),
        "contributions",
        "type",
        F.col("extract_date").alias("snapshot_date"),
        F.when(F.col("login").isNull(), F.lit(True)).otherwise(F.lit(False)).alias("is_anonymous"),
        F.coalesce(
            F.col("login"),
            F.concat_ws(":", F.lit("anonymous"), F.col("name"), F.col("contributions").cast("string")),
        ).alias("contributor_key"),
    )
    .join(repositories.select("repo_id", "repo_name"), on="repo_name", how="left")
    .dropDuplicates(["repo_id", "contributor_key", "snapshot_date", "is_anonymous"])
    .drop("contributor_key")
)

print("Silver created")
print("Repositories:", repositories.count())
print("Commits:", commits.count())
print("Contributors:", contributors.count())

show_df(repositories.select("repo_name", "language", "stargazers_count", "forks_count"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Gold: Final Analytics

# COMMAND ----------

repo_activity = (
    commits.groupBy("repo_name")
    .agg(F.count("*").alias("commit_count_30d"))
    .orderBy(F.col("commit_count_30d").desc())
)

weekly_trends = (
    commits.withColumn("activity_week", F.date_trunc("week", F.col("commit_timestamp")))
    .groupBy("repo_name", "activity_week")
    .agg(F.count("*").alias("commit_count"))
    .orderBy("repo_name", "activity_week")
)

monthly_trends = (
    commits.withColumn("activity_month", F.date_trunc("month", F.col("commit_timestamp")))
    .groupBy("repo_name", "activity_month")
    .agg(F.count("*").alias("commit_count"))
    .orderBy("repo_name", "activity_month")
)

language_distribution = (
    repositories.groupBy("language")
    .agg(F.count("*").alias("repository_count"))
    .fillna({"language": "Unknown"})
    .orderBy(F.col("repository_count").desc())
)

contributor_counts = (
    contributors.groupBy("repo_name")
    .agg(F.count("*").alias("contributor_count_snapshot"))
    .orderBy(F.col("contributor_count_snapshot").desc())
)

popularity_vs_activity = (
    repositories.select("repo_name", "stargazers_count", "forks_count")
    .join(repo_activity, on="repo_name", how="left")
    .fillna({"commit_count_30d": 0})
    .orderBy(F.col("stargazers_count").desc())
)

first_seen_window = Window.partitionBy("repo_name", "author_login")
rank_window = Window.partitionBy("repo_name").orderBy(F.col("total_commits").desc(), F.col("author_login").asc())

contributor_timeline = (
    commits.filter(F.col("author_login").isNotNull())
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
    commits.filter(F.col("author_login").isNotNull())
    .groupBy("repo_name", "author_login")
    .agg(F.count("*").alias("total_commits"))
    .withColumn("contributor_rank", F.row_number().over(rank_window))
    .withColumn("repo_commit_total", F.sum("total_commits").over(Window.partitionBy("repo_name")))
    .withColumn("contribution_share", F.col("total_commits") / F.col("repo_commit_total"))
)

contributor_behavior = (
    contributor_timeline.join(
        contributor_concentration.select(
            "repo_name",
            "author_login",
            "contributor_rank",
            "total_commits",
            "repo_commit_total",
            "contribution_share",
        ),
        on=["repo_name", "author_login"],
        how="left",
    )
    .orderBy("repo_name", "activity_week", "contributor_rank")
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Results to Screenshot

# COMMAND ----------

print("1. Most active repositories")
show_df(repo_activity)

print("2. Weekly trends")
show_df(weekly_trends)

print("3. Language distribution")
show_df(language_distribution)

print("4. Contributor counts")
show_df(contributor_counts)

print("5. Stars vs activity")
show_df(popularity_vs_activity)

print("6. Contributor behavior over time")
show_df(contributor_behavior)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Final Checks

# COMMAND ----------

print("Duplicate commits:", commits.groupBy("repo_id", "sha").count().filter("count > 1").count())
print("Null commit timestamps:", commits.filter(F.col("commit_timestamp").isNull()).count())
print("Anonymous contributors:", contributors.filter(F.col("is_anonymous")).count())

summary_df = spark.createDataFrame(
    [
        ("repositories", repositories.count()),
        ("commits", commits.count()),
        ("contributors", contributors.count()),
        ("repo_activity", repo_activity.count()),
        ("weekly_trends", weekly_trends.count()),
        ("monthly_trends", monthly_trends.count()),
        ("language_distribution", language_distribution.count()),
        ("contributor_counts", contributor_counts.count()),
        ("popularity_vs_activity", popularity_vs_activity.count()),
        ("contributor_behavior", contributor_behavior.count()),
    ],
    ["dataset", "row_count"],
)

show_df(summary_df)
