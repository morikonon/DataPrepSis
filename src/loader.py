"""Loads the cleaned records into SQLite with idempotent upserts and verifies the stored data."""

import argparse
import hashlib
import logging
import sqlite3

import pandas as pd

from src import config

log = logging.getLogger("loader")

TABLES = {
    # table: (primary key, {table column: clean_data column})
    "races": ("race_id", {
        "race_id": "race_id", "season": "season", "round": "round", "race_name": "race_name",
        "race_date": "race_date", "race_datetime_utc": "race_datetime_utc", "circuit_id": "circuit_id",
        "circuit_name": "circuit_name", "locality": "locality", "country": "country"}),
    "drivers": ("driver_id", {
        "driver_id": "driver_id", "driver_code": "driver_code", "driver_number": "driver_number",
        "driver_name": "driver_name", "nationality": "driver_nationality", "date_of_birth": "driver_dob"}),
    "constructors": ("constructor_id", {
        "constructor_id": "constructor_id", "constructor_name": "constructor_name",
        "nationality": "constructor_nationality", "team_id": "team_id", "team_name": "team_name"}),
    "results": ("result_id", {
        "result_id": "record_id", "race_id": "race_id", "driver_id": "driver_id", "constructor_id": "constructor_id",
        "grid": "grid", "grid_effective": "grid_effective", "position": "position", "position_text": "position_text",
        "is_classified": "is_classified", "points": "points", "laps": "laps", "status": "status",
        "status_group": "status_group", "result_category": "result_category", "positions_gained": "positions_gained",
        "race_time_ms": "race_time_ms", "gap_to_winner_s": "gap_to_winner_s", "fastest_lap_rank": "fastest_lap_rank",
        "fastest_lap_time": "fastest_lap_time", "fastest_lap_s": "fastest_lap_s",
        "lap_pace_gap_pct": "lap_pace_gap_pct", "driver_age": "driver_age"}),
}

NULLABLE_INT_COLUMNS = ["driver_number", "positions_gained", "race_time_ms", "fastest_lap_rank"]

VERIFICATION_QUERIES = {
    "rows_per_table": """
        SELECT 'races' AS table_name, COUNT(*) AS row_count FROM races
        UNION ALL SELECT 'drivers', COUNT(*) FROM drivers
        UNION ALL SELECT 'constructors', COUNT(*) FROM constructors
        UNION ALL SELECT 'results', COUNT(*) FROM results""",
    "duplicate_results": """
        SELECT race_id, driver_id, COUNT(*) AS copies FROM results
        GROUP BY race_id, driver_id HAVING COUNT(*) > 1""",
    "orphan_results": """
        SELECT r.result_id FROM results r
        LEFT JOIN races ra ON ra.race_id = r.race_id
        LEFT JOIN drivers d ON d.driver_id = r.driver_id
        LEFT JOIN constructors c ON c.constructor_id = r.constructor_id
        WHERE ra.race_id IS NULL OR d.driver_id IS NULL OR c.constructor_id IS NULL""",
    "races_without_single_winner": """
        SELECT race_id, SUM(position = 1) AS winners FROM results
        GROUP BY race_id HAVING SUM(position = 1) <> 1""",
    "results_per_season": """
        SELECT season, COUNT(DISTINCT round) AS races, COUNT(*) AS results, SUM(points) AS total_points
        FROM v_results GROUP BY season ORDER BY season""",
    "top_drivers_per_season": """
        SELECT season, driver_name, team_name, points FROM (
            SELECT season, driver_name, GROUP_CONCAT(DISTINCT team_name) AS team_name, SUM(points) AS points,
                   ROW_NUMBER() OVER (PARTITION BY season ORDER BY SUM(points) DESC) AS rank_in_season
            FROM v_results GROUP BY season, driver_name)
        WHERE rank_in_season <= 3 ORDER BY season, points DESC""",
}


def connect(db_path=config.DB_PATH) -> sqlite3.Connection:
    connection = sqlite3.connect(db_path)
    connection.execute("PRAGMA foreign_keys = ON")
    connection.executescript(config.SCHEMA_SQL_PATH.read_text(encoding="utf-8"))
    return connection


def read_clean_data(path=config.CLEAN_CSV_PATH) -> pd.DataFrame:
    df = pd.read_csv(path, dtype={column: "Int64" for column in NULLABLE_INT_COLUMNS})
    df["race_id"] = df["season"].astype(str) + "_" + df["round"].astype(str).str.zfill(2)
    return df


def to_rows(df: pd.DataFrame) -> list[dict]:
    """DataFrame -> list of dicts with plain Python values (NaN / <NA> -> None) that sqlite3 can bind."""
    return df.astype(object).where(df.notna(), None).to_dict("records")


def upsert(connection: sqlite3.Connection, table: str, key: str, rows: pd.DataFrame) -> dict:
    """INSERT new keys; UPDATE an existing row only if one of its values changed; otherwise do nothing."""
    columns = list(rows.columns)
    non_key = [column for column in columns if column != key]
    sql = (
        f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(':' + c for c in columns)}) "
        f"ON CONFLICT ({key}) DO UPDATE SET {', '.join(f'{c} = excluded.{c}' for c in non_key)} "
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


def table_fingerprint(connection: sqlite3.Connection, table: str) -> str:
    """SHA-256 of the whole table in key order: equal fingerprints mean identical stored data."""
    digest = hashlib.sha256()
    for row in connection.execute(f"SELECT * FROM {table} ORDER BY 1"):
        digest.update(repr(row).encode())
    return digest.hexdigest()[:16]


def load(df: pd.DataFrame, db_path=config.DB_PATH) -> dict:
    """Load all four tables in one transaction (dimensions first, so foreign keys always resolve)."""
    stats = {}
    with connect(db_path) as connection:
        for table, (key, column_map) in TABLES.items():
            rows = df[list(column_map.values())].set_axis(list(column_map), axis=1).drop_duplicates(subset=key)
            stats[table] = upsert(connection, table, key, rows)
        stats["fingerprint"] = table_fingerprint(connection, "results")
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
        results = stats["results"]
        log.info("Load #%d: results inserted=%d updated=%d unchanged=%d, rows in table=%d, fingerprint=%s",
                 attempt, results["inserted"], results["updated"], results["unchanged"],
                 results["rows_in_table"], stats["fingerprint"])

    checks = verify()
    stored = int(checks["rows_per_table"].set_index("table_name").loc["results", "row_count"])
    summary = {
        "processed_records": len(df),
        "stored_records": stored,
        "loads": runs,
        "idempotent": len({run["fingerprint"] for run in runs}) == 1 and all(
            run["results"]["inserted"] == 0 and run["results"]["updated"] == 0 for run in runs[1:]),
        "duplicate_results": len(checks["duplicate_results"]),
        "orphan_results": len(checks["orphan_results"]),
        "races_without_single_winner": len(checks["races_without_single_winner"]),
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
