"""Cleaning and transformation of the validated records with pandas and NumPy."""

import logging

import numpy as np
import pandas as pd

from src import config

log = logging.getLogger("cleaner")

COUNTRY_NAMES = {"UK": "United Kingdom", "USA": "United States", "UAE": "United Arab Emirates"}

# Statuses not listed here (Retired, Accident, Collision damage, Undertray, ...) are retirements.
STATUS_GROUPS = {
    "Finished": "Finished",
    "Lapped": "Lapped",
    "Disqualified": "DSQ",
    "Did not start": "DNS",
    "Withdrew": "DNS",
}

# The same team entry renamed between seasons: Alfa Romeo -> Sauber, AlphaTauri -> RB.
TEAM_LINEAGE = {"alfa": "sauber", "alphatauri": "rb"}

INT_COLUMNS = ["season", "round", "grid", "position", "laps"]
NULLABLE_INT_COLUMNS = ["driver_number", "race_time_ms", "fastest_lap_rank", "fastest_lap_number"]
CATEGORY_COLUMNS = ["country", "driver_nationality", "constructor_name", "team_name", "status", "status_group",
                    "result_category"]


def missing_value_report(df: pd.DataFrame) -> pd.DataFrame:
    missing = df.isna().sum()
    report = pd.DataFrame({"missing": missing, "percent": (missing / len(df) * 100).round(2)})
    return report[report["missing"] > 0].sort_values("missing", ascending=False)


def remove_duplicates(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """Drop repeated deliveries of the same result; the newest message wins.

    The producer keys messages by record_id, so all copies of a record sit in one partition
    and the highest offset is the most recent one.
    """
    content_columns = [col for col in df.columns if not col.startswith("kafka_")]
    exact = int(df.duplicated(subset=content_columns).sum())
    by_key = int(df.duplicated(subset="record_id").sum())
    deduplicated = (df.sort_values(["kafka_partition", "kafka_offset"])
                      .drop_duplicates(subset="record_id", keep="last"))
    return deduplicated, {"exact_duplicates": exact, "duplicate_record_ids": by_key}


def normalize_text(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    text_columns = df.select_dtypes(include="object").columns
    df[text_columns] = df[text_columns].apply(lambda col: col.str.strip().str.replace(r"\s+", " ", regex=True))

    df["driver_code"] = df["driver_code"].str.upper()
    df["driver_name"] = df["given_name"] + " " + df["family_name"]
    df["country"] = df["country"].replace(COUNTRY_NAMES)
    df["constructor_name"] = df["constructor_name"].str.replace(r"\s+F1 Team$", "", regex=True)
    df["status"] = df["status"].str.capitalize()
    df["status_group"] = df["status"].map(STATUS_GROUPS).fillna("DNF")

    df["team_id"] = df["constructor_id"].replace(TEAM_LINEAGE)
    latest_name = df.sort_values("season").groupby("team_id")["constructor_name"].last()
    df["team_name"] = df["team_id"].map(latest_name)
    return df


def convert_types(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df[INT_COLUMNS] = df[INT_COLUMNS].astype("int64")
    df[NULLABLE_INT_COLUMNS] = df[NULLABLE_INT_COLUMNS].apply(pd.to_numeric).astype("Int64")
    df["points"] = df["points"].astype("float64")
    df["race_date"] = pd.to_datetime(df["race_date"], format="%Y-%m-%d")
    df["driver_dob"] = pd.to_datetime(df["driver_dob"], format="%Y-%m-%d")
    df["race_datetime_utc"] = pd.to_datetime(df["race_date"].dt.strftime("%Y-%m-%d") + " " + df["race_start_time"],
                                             format="%Y-%m-%d %H:%M:%SZ", utc=True)
    df["collected_at"] = pd.to_datetime(df["collected_at"], utc=True)
    return df.drop(columns=["race_start_time", "given_name", "family_name"])


def lap_time_to_seconds(lap_time: pd.Series) -> pd.Series:
    """'1:32.608' -> 92.608 (vectorised; missing stays NaN)."""
    parts = lap_time.str.split(":", expand=True).astype(float)
    return pd.Series(np.round(parts[0].to_numpy() * 60 + parts[1].to_numpy(), 3), index=lap_time.index)


def add_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    race = df.groupby(["season", "round"])

    starters = race["grid"].transform("size")
    df["grid_effective"] = np.where(df["grid"] == 0, starters, df["grid"])  # pit-lane start = back of the grid
    df["is_classified"] = df["position_text"].str.isdigit()
    # Only meaningful for classified finishers: a retired car's "position" is just its retirement order.
    df["positions_gained"] = (df["grid_effective"] - df["position"]).where(df["is_classified"]).astype("Int64")

    race_time_s = df["race_time_ms"].astype("float64") / 1000
    df["race_time_s"] = race_time_s.round(3)
    df["gap_to_winner_s"] = (race_time_s - race_time_s.groupby([df["season"], df["round"]]).transform("min")).round(3)

    df["fastest_lap_s"] = lap_time_to_seconds(df["fastest_lap_time"])
    best_lap = df.groupby(["season", "round"])["fastest_lap_s"].transform("min")
    df["lap_pace_gap_pct"] = np.round((df["fastest_lap_s"] / best_lap - 1) * 100, 3)  # comparable across circuits

    df["driver_age"] = np.floor((df["race_date"] - df["driver_dob"]).dt.days / 365.25).astype("int64")

    df["result_category"] = np.select(
        [df["position"] == 1, df["position"] <= 3, df["points"] > 0, df["status_group"].isin(["Finished", "Lapped"])],
        ["Win", "Podium", "Points", "No points"],
        default="DNF/DNS/DSQ",
    )
    return df


def summarize_teams(df: pd.DataFrame) -> pd.DataFrame:
    flags = df.assign(win=df["position"].eq(1), podium=df["position"].le(3), dnf=df["status_group"].eq("DNF"))
    summary = (flags.groupby(["season", "team_name"], observed=True)
                    .agg(entries=("record_id", "count"),
                         points=("points", "sum"),
                         wins=("win", "sum"),
                         podiums=("podium", "sum"),
                         dnf_rate=("dnf", "mean"),
                         avg_grid=("grid_effective", "mean"),
                         avg_finish=("position", "mean"),
                         median_lap_gap_pct=("lap_pace_gap_pct", "median"))
                    .round(3)
                    .reset_index()
                    .sort_values(["season", "points"], ascending=[True, False]))
    return summary


def summarize_drivers(df: pd.DataFrame) -> pd.DataFrame:
    flags = df.assign(win=df["position"].eq(1), podium=df["position"].le(3), dnf=df["status_group"].eq("DNF"))
    return (flags.groupby(["season", "driver_code", "driver_name"], observed=True)
                 .agg(team=("team_name", lambda teams: " / ".join(teams.astype(str).unique())),
                      races=("round", "nunique"),
                      points=("points", "sum"),
                      wins=("win", "sum"),
                      podiums=("podium", "sum"),
                      dnfs=("dnf", "sum"),
                      avg_positions_gained=("positions_gained", "mean"))
                 .round(2)
                 .reset_index()
                 .sort_values(["season", "points"], ascending=[True, False]))


def reshape(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    team_points = df.pivot_table(index="team_name", columns="season", values="points",
                                 aggfunc="sum", observed=True).sort_values(config.SEASONS[-1], ascending=False)

    status_by_team = (df.groupby(["team_name", "status_group"], observed=True).size()
                        .unstack(fill_value=0))

    per_round = df.pivot_table(index=["season", "round"], columns="driver_code", values="points",
                               aggfunc="sum", fill_value=0)
    cumulative = per_round.groupby(level="season").cumsum()
    championship_progress = (cumulative.reset_index()
                             .melt(id_vars=["season", "round"], var_name="driver_code", value_name="cumulative_points"))
    championship_progress = championship_progress.merge(
        df[["season", "driver_code"]].drop_duplicates(), on=["season", "driver_code"])  # keep drivers who raced that season

    return {"team_points_by_season": team_points, "status_by_team": status_by_team,
            "championship_progress": championship_progress}


def clean(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    stats = {"input_records": len(df)}
    stats["missing_before"] = missing_value_report(df)["missing"].to_dict()

    df, duplicate_stats = remove_duplicates(df)
    stats.update(duplicate_stats)

    df = add_features(convert_types(normalize_text(df)))
    df[CATEGORY_COLUMNS] = df[CATEGORY_COLUMNS].astype("category")

    # Structural missingness is kept as NaN: a car that retired has no race time and imputing one would invent data.
    stats["missing_after"] = missing_value_report(df)["missing"].to_dict()
    stats["output_records"] = len(df)
    return df.drop(columns=["kafka_partition", "kafka_offset"]).sort_values("record_id").reset_index(drop=True), stats


def run() -> dict:
    validated = pd.read_csv(config.VALID_CSV_PATH, dtype=str)
    validated[["kafka_partition", "kafka_offset"]] = validated[["kafka_partition", "kafka_offset"]].astype(int)
    log.info("Missing values in validated data:\n%s", missing_value_report(validated).to_string())

    clean_df, stats = clean(validated)
    clean_df.to_csv(config.CLEAN_CSV_PATH, index=False)

    comeback_drives = clean_df[clean_df["is_classified"] & (clean_df["position"] <= 3) & (clean_df["grid_effective"] >= 8)]
    stats["comeback_drives"] = len(comeback_drives)

    config.AGGREGATES_DIR.mkdir(exist_ok=True)
    tables = {"team_season_summary": summarize_teams(clean_df), "driver_season_summary": summarize_drivers(clean_df),
              "comeback_drives": comeback_drives[["record_id", "driver_name", "team_name", "race_name",
                                                  "grid_effective", "position", "positions_gained"]],
              **reshape(clean_df)}
    for name, table in tables.items():
        table.to_csv(config.AGGREGATES_DIR / f"{name}.csv", index=name in ("team_points_by_season", "status_by_team"))

    log.info("Duplicates: %d exact, %d repeated record_id -> removed", stats["exact_duplicates"],
             stats["duplicate_record_ids"])
    log.info("Cleaned %d -> %d records, %d columns -> %s", stats["input_records"], stats["output_records"],
             clean_df.shape[1], config.CLEAN_CSV_PATH.name)
    log.info("Podium finishes from grid 8 or lower (multi-condition filter): %d", stats["comeback_drives"])
    log.info("Team points by season (pivot_table):\n%s", tables["team_points_by_season"].to_string())
    return stats


if __name__ == "__main__":
    config.setup_logging()
    run()
