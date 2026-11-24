"""Loads the cleaned records into SQLite with idempotent upserts and verifies the stored data."""

import argparse
import hashlib
import logging
import sqlite3

import pandas as pd

from src import config
from src.cleaner import TIMESTAMP_COLUMNS, TOOL_NAMES

log = logging.getLogger("loader")

TIMESTAMPS = [column.replace("_date", "_at") for column in TIMESTAMP_COLUMNS] + ["collected_at"]
FLAGS = ["is_answered", "has_accepted_answer", "is_edited", "is_closed", "answered_within_24h"]
NULLABLE_INT_COLUMNS = ["accepted_answer_id", "owner_user_id", "owner_reputation"]
QUESTION_COLUMNS = [
    "question_id", "title", "link", "score", "view_count", "answer_count", "is_answered", "has_accepted_answer",
    "is_edited", "is_closed", "status", "closed_reason", "accepted_answer_id", "creation_at", "last_activity_at",
    "last_edit_at", "closed_at", "first_answer_at", "accepted_answer_at", "hours_to_first_answer",
    "hours_to_accepted_answer", "answered_within_24h", "views_per_day", "log_views", "owner_user_id",
    "owner_user_type", "owner_reputation", "reputation_band", "creation_month", "creation_weekday",
    "creation_hour_utc", "title_length", "tag_count", "tool_count", "collected_at",
]

VERIFICATION_QUERIES = {
    "rows_per_table": """
        SELECT 'questions' AS table_name, COUNT(*) AS row_count FROM questions
        UNION ALL SELECT 'question_tags', COUNT(*) FROM question_tags
        UNION ALL SELECT 'v_question_tools', COUNT(*) FROM v_question_tools""",
    "questions_without_tool": """
        SELECT q.question_id FROM questions q
        WHERE NOT EXISTS (SELECT 1 FROM question_tags t WHERE t.question_id = q.question_id AND t.tool IS NOT NULL)""",
    "tag_count_mismatch": """
        SELECT q.question_id, q.tag_count, COUNT(t.tag) AS stored_tags FROM questions q
        LEFT JOIN question_tags t ON t.question_id = q.question_id
        GROUP BY q.question_id HAVING COUNT(t.tag) <> q.tag_count""",
    "inconsistent_answers": """
        SELECT question_id, answer_count, is_answered, has_accepted_answer FROM questions
        WHERE answer_count = 0 AND (is_answered = 1 OR has_accepted_answer = 1 OR first_answer_at IS NOT NULL)""",
    "questions_per_tool": """
        SELECT tool, COUNT(*) AS questions,
               ROUND(100.0 * AVG(answer_count > 0), 1) AS answered_pct,
               ROUND(100.0 * AVG(has_accepted_answer), 1) AS accepted_pct,
               ROUND(100.0 * AVG(answered_within_24h), 1) AS answered_24h_pct,
               ROUND(AVG(view_count)) AS avg_views
        FROM v_question_tools GROUP BY tool ORDER BY questions DESC""",
    "questions_per_quarter": """
        SELECT substr(creation_month, 1, 4) || '-Q' || ((CAST(substr(creation_month, 6, 2) AS INTEGER) + 2) / 3)
                   AS quarter,
               COUNT(*) AS questions, SUM(status = 'Unanswered') AS unanswered
        FROM questions GROUP BY quarter ORDER BY quarter""",
    "most_viewed": """
        SELECT q.question_id, substr(q.title, 1, 60) AS title, q.view_count, q.score, q.status
        FROM questions q ORDER BY q.view_count DESC LIMIT 5""",
}


def connect(db_path=config.DB_PATH) -> sqlite3.Connection:
    connection = sqlite3.connect(db_path)
    connection.execute("PRAGMA foreign_keys = ON")
    connection.executescript(config.SCHEMA_SQL_PATH.read_text(encoding="utf-8"))
    return connection


def read_clean_data(path=config.CLEAN_CSV_PATH) -> pd.DataFrame:
    df = pd.read_csv(path, dtype={column: "Int64" for column in NULLABLE_INT_COLUMNS})
    for column in TIMESTAMPS:
        df[column] = pd.to_datetime(df[column], utc=True).dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    df[FLAGS] = df[FLAGS].astype(int)
    return df


def tag_rows(df: pd.DataFrame) -> pd.DataFrame:
    """One row per question and tag, with the tag's position and the tool name of tracked tags."""
    tags = df[["question_id", "tags"]].assign(tag=df["tags"].str.split(";")).explode("tag")
    tags["tag_position"] = tags.groupby("question_id").cumcount() + 1
    tags["tool"] = tags["tag"].map(TOOL_NAMES)
    return tags[["question_id", "tag", "tag_position", "tool"]]


def to_rows(df: pd.DataFrame) -> list[dict]:
    """DataFrame -> list of dicts with plain Python values (NaN / <NA> -> None) that sqlite3 can bind."""
    return df.astype(object).where(df.notna(), None).to_dict("records")


def upsert(connection: sqlite3.Connection, table: str, key: list[str], rows: pd.DataFrame) -> dict:
    """INSERT new keys; UPDATE an existing row only if one of its values changed; otherwise do nothing."""
    columns = list(rows.columns)
    non_key = [column for column in columns if column not in key]
    sql = (
        f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(':' + c for c in columns)}) "
        f"ON CONFLICT ({', '.join(key)}) DO UPDATE SET {', '.join(f'{c} = excluded.{c}' for c in non_key)} "
        f"WHERE {' OR '.join(f'{table}.{c} IS NOT excluded.{c}' for c in non_key)}"
    )
    before = connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    changes_before = connection.total_changes
    connection.executemany(sql, to_rows(rows))
    after = connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

    inserted = after - before
    updated = connection.total_changes - changes_before - inserted
    return {"input": len(rows), "inserted": inserted, "updated": updated,
            "unchanged": len(rows) - inserted - updated, "rows_in_table": after}


def delete_stale_tags(connection: sqlite3.Connection, tags: pd.DataFrame) -> int:
    """Remove tags that a re-collected question no longer has (a question can be retagged between runs)."""
    connection.execute("CREATE TEMP TABLE incoming_tags (question_id INTEGER, tag TEXT)")
    connection.executemany("INSERT INTO incoming_tags VALUES (?, ?)", tags[["question_id", "tag"]].itertuples(index=False))
    deleted = connection.execute("""
        DELETE FROM question_tags
        WHERE question_id IN (SELECT question_id FROM incoming_tags)
          AND (question_id, tag) NOT IN (SELECT question_id, tag FROM incoming_tags)""").rowcount
    connection.execute("DROP TABLE incoming_tags")
    return deleted


def table_fingerprint(connection: sqlite3.Connection, table: str) -> str:
    """SHA-256 of the whole table in key order: equal fingerprints mean identical stored data."""
    digest = hashlib.sha256()
    for row in connection.execute(f"SELECT * FROM {table} ORDER BY 1, 2"):
        digest.update(repr(row).encode())
    return digest.hexdigest()[:16]


def load(df: pd.DataFrame, db_path=config.DB_PATH) -> dict:
    """Load both tables in one transaction (questions first, so foreign keys always resolve)."""
    tags = tag_rows(df)
    with connect(db_path) as connection:
        stats = {"questions": upsert(connection, "questions", ["question_id"], df[QUESTION_COLUMNS]),
                 "question_tags": upsert(connection, "question_tags", ["question_id", "tag"], tags)}
        stats["question_tags"]["deleted"] = delete_stale_tags(connection, tags)
        stats["fingerprint"] = "/".join(table_fingerprint(connection, table) for table in ("questions", "question_tags"))
    connection.close()
    return stats


def verify(db_path=config.DB_PATH) -> dict[str, pd.DataFrame]:
    with sqlite3.connect(db_path) as connection:
        results = {name: pd.read_sql_query(query, connection) for name, query in VERIFICATION_QUERIES.items()}
    connection.close()
    for name, result in results.items():
        log.info("SQL check '%s':\n%s", name, result.to_string(index=False) if not result.empty else "  (no rows)")
    return results


def run(repeat: int = 2) -> dict:
    """Load the same dataset `repeat` times to prove the load is idempotent, then run the SQL checks."""
    df = read_clean_data()
    runs = []
    for attempt in range(1, repeat + 1):
        stats = load(df)
        runs.append(stats)
        for table in ("questions", "question_tags"):
            log.info("Load #%d %-13s inserted=%d updated=%d unchanged=%d, rows in table=%d",
                     attempt, table, stats[table]["inserted"], stats[table]["updated"], stats[table]["unchanged"],
                     stats[table]["rows_in_table"])
        log.info("Load #%d fingerprint=%s", attempt, stats["fingerprint"])

    checks = verify()
    stored = int(checks["rows_per_table"].set_index("table_name").loc["questions", "row_count"])
    summary = {
        "processed_records": len(df),
        "stored_records": stored,
        "loads": runs,
        "idempotent": len({run["fingerprint"] for run in runs}) == 1 and all(
            run[table]["inserted"] == 0 and run[table]["updated"] == 0
            for run in runs[1:] for table in ("questions", "question_tags")),
        "questions_without_tool": len(checks["questions_without_tool"]),
        "tag_count_mismatch": len(checks["tag_count_mismatch"]),
        "inconsistent_answers": len(checks["inconsistent_answers"]),
    }
    log.info("Processed records: %d, stored in SQLite: %d, idempotent: %s",
             summary["processed_records"], summary["stored_records"], summary["idempotent"])
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repeat", type=int, default=2, help="how many times to load the same data (default 2)")
    parser.add_argument("--verify-only", action="store_true", help="only run the SQL verification queries")
    args = parser.parse_args()
    config.setup_logging()
    if args.verify_only:
        verify()
    else:
        run(args.repeat)


if __name__ == "__main__":
    main()
