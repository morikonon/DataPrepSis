"""Cleaning and transformation of the validated records with pandas and NumPy."""

import html
import logging
import unicodedata

import numpy as np
import pandas as pd

from src import config

log = logging.getLogger("cleaner")

TOOL_NAMES = {"pandas": "pandas", "apache-kafka": "Kafka", "airflow": "Airflow", "sqlite": "SQLite",
              "selenium-webdriver": "Selenium", "web-scraping": "Web scraping"}
TIMESTAMP_COLUMNS = ["creation_date", "last_activity_date", "last_edit_date", "closed_date", "first_answer_date",
                     "accepted_answer_date"]
INT_COLUMNS = ["question_id", "score", "view_count", "answer_count"]
NULLABLE_INT_COLUMNS = ["accepted_answer_id", "owner_user_id", "owner_reputation"]
STATUS_ORDER = ["Accepted", "Answered, not accepted", "Unanswered", "Closed"]
REPUTATION_EDGES = [10, 100, 1000]
REPUTATION_BANDS = ["1-9", "10-99", "100-999", "1000+"]
WEEKDAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
# owner_accept_rate is shown only for users with enough questions (80 % empty); every present content_license is
# "CC BY-SA 4.0", so the column carries no information.
DROPPED_COLUMNS = ["owner_accept_rate", "content_license", "query_tag"]


def missing_value_report(df: pd.DataFrame) -> pd.DataFrame:
    missing = df.isna().sum()
    report = pd.DataFrame({"missing": missing, "percent": (missing / len(df) * 100).round(2)})
    return report[report["missing"] > 0].sort_values("missing", ascending=False)


def remove_duplicates(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """Keep one row per question; the newest message wins.

    Exact copies come from Kafka redelivery. The same question also arrives once per tracked tag it carries
    (a question tagged pandas and sqlite is returned by both API queries). Messages are keyed by question_id,
    so all copies sit in one partition and the highest offset is the most recent one.
    """
    content_columns = [col for col in df.columns if not col.startswith("kafka_")]
    exact = int(df.duplicated(subset=content_columns).sum())
    by_key = int(df.duplicated(subset="question_id").sum())
    deduplicated = (df.sort_values(["kafka_partition", "kafka_offset"])
                      .drop_duplicates(subset="question_id", keep="last"))
    return deduplicated, {"exact_duplicates": exact, "same_question_other_tag": by_key - exact,
                          "removed_duplicates": by_key}


def normalize_title(title: pd.Series) -> pd.Series:
    """'it&#39;s  &quot;NaN&quot;' -> 'it's "NaN"': decode HTML entities, unify Unicode forms, collapse whitespace."""
    decoded = title.map(lambda text: unicodedata.normalize("NFKC", html.unescape(text)))
    return decoded.str.replace(r"\s+", " ", regex=True).str.strip()


def normalize_text(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    df = df.copy()
    normalized_title = normalize_title(df["title"])
    changed_titles = int((normalized_title != df["title"]).sum())
    df["title"] = normalized_title

    tag_lists = df["tags"].str.lower().str.split(";").map(lambda tags: [tag.strip() for tag in tags])
    df["tags"] = tag_lists.str.join(";")
    df["tag_count"] = tag_lists.str.len()
    tracked = tag_lists.map(lambda tags: [TOOL_NAMES[tag] for tag in TOOL_NAMES if tag in tags])
    df["tools"] = tracked.str.join(";")
    df["tool_count"] = tracked.str.len()
    df["owner_user_type"] = df["owner_user_type"].str.strip().str.lower()
    df["closed_reason"] = df["closed_reason"].str.strip()
    return df, changed_titles


def convert_types(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df[INT_COLUMNS] = df[INT_COLUMNS].astype("int64")
    df[NULLABLE_INT_COLUMNS] = df[NULLABLE_INT_COLUMNS].apply(pd.to_numeric).astype("Int64")
    df["is_answered"] = df["is_answered"].map({"True": True, "False": False}).astype(bool)
    for column in TIMESTAMP_COLUMNS:  # Unix seconds -> timezone-aware UTC timestamps
        df[column] = pd.to_datetime(pd.to_numeric(df[column]), unit="s", utc=True)
    df["collected_at"] = pd.to_datetime(df["collected_at"], utc=True)
    return df.rename(columns={column: column.replace("_date", "_at") for column in TIMESTAMP_COLUMNS})


def hours_between(later: pd.Series, earlier: pd.Series) -> pd.Series:
    return np.round((later - earlier).dt.total_seconds() / 3600, 2)


def add_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["is_edited"] = df["last_edit_at"].notna()
    df["is_closed"] = df["closed_at"].notna()
    df["has_accepted_answer"] = df["accepted_answer_at"].notna()

    df["hours_to_first_answer"] = hours_between(df["first_answer_at"], df["creation_at"])
    df["hours_to_accepted_answer"] = hours_between(df["accepted_answer_at"], df["creation_at"])
    df["answered_within_24h"] = df["hours_to_first_answer"].le(24)

    age_days = (df["collected_at"] - df["creation_at"]).dt.days.to_numpy()
    df["views_per_day"] = np.round(df["view_count"].to_numpy() / np.maximum(age_days, 1), 3)
    df["log_views"] = np.round(np.log1p(df["view_count"].to_numpy()), 3)

    df["status"] = np.select(
        [df["is_closed"], df["has_accepted_answer"], df["answer_count"] > 0],
        ["Closed", "Accepted", "Answered, not accepted"],
        default="Unanswered",
    )
    reputation = df["owner_reputation"].astype("float64").to_numpy()
    band_index = np.digitize(np.nan_to_num(reputation, nan=0), REPUTATION_EDGES)
    df["reputation_band"] = np.where(np.isnan(reputation), "Deleted account", np.array(REPUTATION_BANDS)[band_index])

    df["creation_month"] = df["creation_at"].dt.strftime("%Y-%m")
    df["creation_weekday"] = pd.Categorical(df["creation_at"].dt.day_name(), categories=WEEKDAYS, ordered=True)
    df["creation_hour_utc"] = df["creation_at"].dt.hour
    df["title_length"] = df["title"].str.len()
    return df


def explode_tools(df: pd.DataFrame) -> pd.DataFrame:
    """One row per question and tracked tool, for comparisons between tools."""
    return df.assign(tool=df["tools"].str.split(";")).explode("tool")


def summarize_tools(df: pd.DataFrame) -> pd.DataFrame:
    per_tool = explode_tools(df).assign(has_answer=lambda d: d["answer_count"] > 0)
    return (per_tool.groupby("tool")
                    .agg(questions=("question_id", "count"),
                         answered_rate=("has_answer", "mean"),
                         accepted_rate=("has_accepted_answer", "mean"),
                         closed_rate=("is_closed", "mean"),
                         answered_24h_rate=("answered_within_24h", "mean"),
                         median_hours_to_answer=("hours_to_first_answer", "median"),
                         median_views=("view_count", "median"),
                         mean_score=("score", "mean"))
                    .round(3)
                    .sort_values("questions", ascending=False))


def reshape(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    per_tool = explode_tools(df)
    by_month = per_tool.pivot_table(index="creation_month", columns="tool", values="question_id",
                                    aggfunc="count", fill_value=0)
    by_month_long = (by_month.reset_index()
                             .melt(id_vars="creation_month", var_name="tool", value_name="questions"))
    status_by_tool = (per_tool.groupby(["tool", "status"]).size()
                              .unstack(fill_value=0)
                              .reindex(columns=STATUS_ORDER, fill_value=0))
    return {"questions_by_month": by_month, "questions_by_month_long": by_month_long,
            "status_by_tool": status_by_tool}


def clean(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    stats = {"input_records": len(df)}
    stats["missing_before"] = missing_value_report(df)["missing"].to_dict()

    df, duplicate_stats = remove_duplicates(df)
    stats.update(duplicate_stats)

    df, stats["normalized_titles"] = normalize_text(df)
    df = add_features(convert_types(df)).drop(columns=DROPPED_COLUMNS)

    # The remaining gaps are structural (no edit, not closed, no answer, deleted account) and stay NULL:
    # filling them would invent dates that never happened.
    stats["missing_after"] = missing_value_report(df)["missing"].to_dict()
    stats["output_records"] = len(df)
    return df.drop(columns=["kafka_partition", "kafka_offset"]).sort_values("question_id").reset_index(drop=True), stats


def run() -> dict:
    validated = pd.read_csv(config.VALID_CSV_PATH, dtype=str)
    validated[["kafka_partition", "kafka_offset"]] = validated[["kafka_partition", "kafka_offset"]].astype(int)
    log.info("Missing values in validated data:\n%s", missing_value_report(validated).to_string())

    clean_df, stats = clean(validated)
    clean_df.to_csv(config.CLEAN_CSV_PATH, index=False)

    popular_threshold = np.percentile(clean_df["view_count"], 75)
    unanswered_popular = clean_df[(clean_df["status"] == "Unanswered")
                                  & (clean_df["view_count"] >= popular_threshold)
                                  & (clean_df["score"] >= 0)]
    stats["popular_views_threshold"] = float(popular_threshold)
    stats["unanswered_popular"] = len(unanswered_popular)

    config.AGGREGATES_DIR.mkdir(exist_ok=True)
    tables = {"tool_summary": summarize_tools(clean_df),
              "unanswered_popular": unanswered_popular[["question_id", "title", "tools", "view_count", "score",
                                                        "creation_at"]].sort_values("view_count", ascending=False),
              **reshape(clean_df)}
    for name, table in tables.items():
        table.to_csv(config.AGGREGATES_DIR / f"{name}.csv",
                     index=name in ("tool_summary", "questions_by_month", "status_by_tool"))

    log.info("Duplicates: %d exact Kafka copies, %d same question under another tag -> %d removed",
             stats["exact_duplicates"], stats["same_question_other_tag"], stats["removed_duplicates"])
    log.info("Titles changed by text normalisation: %d", stats["normalized_titles"])
    log.info("Cleaned %d -> %d records, %d columns -> %s", stats["input_records"], stats["output_records"],
             clean_df.shape[1], config.CLEAN_CSV_PATH.name)
    log.info("Missing values after cleaning (structural, kept as NULL):\n%s",
             missing_value_report(clean_df).to_string())
    log.info("Unanswered questions with >= %.0f views and score >= 0 (multi-condition filter): %d",
             popular_threshold, stats["unanswered_popular"])
    log.info("Per-tool summary (GroupBy):\n%s", tables["tool_summary"].to_string())
    log.info("Questions per month and tool (pivot_table):\n%s", tables["questions_by_month"].to_string())
    return stats


if __name__ == "__main__":
    config.setup_logging()
    run()
