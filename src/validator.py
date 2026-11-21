"""Pandera validation of the consumed records before cleaning: invalid rows are quarantined, not repaired."""

import logging
from dataclasses import dataclass

import pandas as pd
import pandera.pandas as pa
from pandera.errors import SchemaErrors

from src import config

log = logging.getLogger("validator")

FIRST_F1_RACE = pd.Timestamp("1950-05-13")
MAX_CARS = 26
NAME = pa.Column(str, pa.Check.str_length(min_value=1))


def points_only_for_top_10(df: pd.DataFrame) -> pd.Series:
    return (df["position"] <= 10) | (df["points"] == 0)


def race_date_in_season(df: pd.DataFrame) -> pd.Series:
    return df["race_date"].dt.year == df["season"]


def position_text_matches_position(df: pd.DataFrame) -> pd.Series:
    is_classified = df["position_text"].str.isdigit()
    return ~is_classified | (pd.to_numeric(df["position_text"], errors="coerce") == df["position"])


def finisher_has_race_time(df: pd.DataFrame) -> pd.Series:
    return ~df["status"].isin(["Finished", "Lapped"]) | df["race_time_ms"].notna()


def driver_age_plausible(df: pd.DataFrame) -> pd.Series:
    age_years = (df["race_date"] - df["driver_dob"]).dt.days / 365.25
    return age_years.between(16, 60)


def one_driver_per_position(df: pd.DataFrame) -> pd.Series:
    """A finishing position belongs to one driver; an exact redelivered copy passes (the cleaner drops it)."""
    first_driver = df.groupby(["season", "round", "position"])["driver_id"].transform("first")
    return df["driver_id"] == first_driver


RULES_SCHEMA = pa.DataFrameSchema(
    columns={
        "record_id": pa.Column(str, pa.Check.str_matches(r"^\d{4}_\d{2}_[a-z_]+$")),
        "season": pa.Column(int, pa.Check.isin(config.SEASONS)),
        "round": pa.Column(int, pa.Check.in_range(1, 24)),
        "race_name": pa.Column(str, pa.Check.str_matches(r".+ Grand Prix$")),
        "race_date": pa.Column("datetime64[ns]", pa.Check.in_range(FIRST_F1_RACE, pd.Timestamp.today().normalize())),
        "race_start_time": pa.Column(str, pa.Check.str_matches(r"^([01]\d|2[0-3]):[0-5]\d:[0-5]\dZ$")),
        "circuit_id": NAME,
        "country": NAME,
        "driver_id": NAME,
        "driver_code": pa.Column(str, pa.Check.str_matches(r"^[A-Z]{3}$")),
        "given_name": NAME,
        "family_name": NAME,
        "driver_dob": pa.Column("datetime64[ns]"),
        "constructor_id": NAME,
        "constructor_name": NAME,
        "grid": pa.Column(int, pa.Check.in_range(0, MAX_CARS)),  # 0 = pit-lane start
        "position": pa.Column(int, pa.Check.in_range(1, MAX_CARS)),
        "position_text": pa.Column(str, pa.Check.str_matches(r"^(\d+|[RDEWFN])$")),
        "points": pa.Column(float, pa.Check.in_range(0, 26)),  # 25 for a win + 1 for the fastest lap
        "laps": pa.Column(int, pa.Check.in_range(0, 100)),
        "status": NAME,
        "race_time_ms": pa.Column(float, pa.Check.gt(0), nullable=True),
        "fastest_lap_rank": pa.Column(float, pa.Check.in_range(1, MAX_CARS), nullable=True),
        "fastest_lap_time": pa.Column(str, pa.Check.str_matches(r"^\d+:\d{2}\.\d{3}$"), nullable=True),
    },
    checks=[
        pa.Check(points_only_for_top_10, name="points_only_for_top_10"),
        pa.Check(race_date_in_season, name="race_date_in_season"),
        pa.Check(position_text_matches_position, name="position_text_matches_position"),
        pa.Check(finisher_has_race_time, name="finisher_has_race_time"),
        pa.Check(driver_age_plausible, name="driver_age_plausible"),
        pa.Check(one_driver_per_position, name="one_driver_per_position"),
    ],
    coerce=True,
    strict=False,
)

# Same columns without value checks: a value that cannot be cast would make every range check on its
# column fail for all rows, so types are checked first and only well-typed rows reach the rules.
TYPES_SCHEMA = RULES_SCHEMA.update_columns({name: {"checks": []} for name in RULES_SCHEMA.columns})
TYPES_SCHEMA.checks = []


@dataclass
class ValidationResult:
    valid: pd.DataFrame
    invalid: pd.DataFrame
    errors: pd.DataFrame


def build_test_records(df: pd.DataFrame) -> pd.DataFrame:
    """Deliberately invalid copies of a real record; each one targets different rules."""
    template = df.sort_values("record_id").iloc[0].to_dict()
    cases = {
        "wrong_types": {"points": "twenty-five", "grid": "P1", "race_date": "2024-02-30"},
        "missing_required": {"constructor_name": None, "driver_code": None, "status": None},
        "out_of_range": {"position": "35", "laps": "-5", "grid": "40", "points": "18"},
        "bad_category_and_date": {"season": "1949", "race_date": "2031-07-06", "driver_code": "verstappen"},
        "conflicting_position": {"driver_id": "test_driver", "position": "1", "position_text": "1"},
    }
    rows = []
    for case, overrides in cases.items():
        record_id = f"{template['season']}_{template['round'].zfill(2)}_test_{case}"
        rows.append({**template, **overrides, "record_id": record_id, "is_test_record": True, "test_case": case})
    return pd.DataFrame(rows)


def collect_failures(schema: pa.DataFrameSchema, df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Validate lazily; return (coerced rows that passed, one row per failed check per record)."""
    try:
        return schema.validate(df, lazy=True), pd.DataFrame()
    except SchemaErrors as exc:
        failures = exc.failure_cases
    if failures["index"].isna().any():
        dataset_level = failures[failures["index"].isna()]
        raise ValueError(f"Dataset-level schema errors (missing columns?):\n{dataset_level.to_string()}")

    # With coerce=True a failed cast is reported twice (coerce_dtype and dtype); keep one.
    failures = failures[~failures["check"].astype(str).str.startswith("dtype(")].copy()
    row_level = failures["schema_context"] == "DataFrameSchema"
    failures.loc[row_level, ["column", "failure_case"]] = ["(row)", "rule violated"]
    failures = failures.drop_duplicates(subset=["index", "column", "check"])
    failures = failures[["index", "column", "check", "failure_case"]].astype({"index": int})

    passed = df.drop(index=failures["index"].unique())
    return schema.validate(passed), failures


def validate(df: pd.DataFrame) -> ValidationResult:
    """Type check -> rule check on well-typed rows -> split into valid and quarantined records."""
    typed, type_errors = collect_failures(TYPES_SCHEMA, df)
    _, rule_errors = collect_failures(RULES_SCHEMA, typed)
    errors = pd.concat([type_errors, rule_errors], ignore_index=True)

    if errors.empty:
        invalid_index = pd.Index([])
    else:
        errors.insert(0, "record_id", df.loc[errors["index"], "record_id"].to_numpy())
        invalid_index = pd.Index(errors["index"].unique())

    invalid = df.loc[invalid_index].copy()
    if not invalid.empty:
        messages = errors.assign(message=errors["column"] + ": " + errors["check"].astype(str))
        invalid["validation_errors"] = messages.groupby("index")["message"].agg("; ".join)

    valid = df.drop(index=invalid_index)
    leaked_tests = valid["is_test_record"].sum()
    if leaked_tests:
        log.warning("%d test record(s) passed validation and are removed anyway", leaked_tests)
    valid = valid[~valid["is_test_record"]]
    return ValidationResult(valid=valid, invalid=invalid, errors=errors.drop(columns="index", errors="ignore"))


def run() -> dict:
    consumed = pd.read_csv(config.CONSUMED_CSV_PATH, dtype=str)
    consumed["is_test_record"] = False
    consumed["test_case"] = None
    test_records = build_test_records(consumed)
    to_validate = pd.concat([consumed, test_records], ignore_index=True)

    result = validate(to_validate)
    result.valid.drop(columns=["is_test_record", "test_case"]).to_csv(config.VALID_CSV_PATH, index=False)
    result.invalid.to_csv(config.INVALID_CSV_PATH, index=False)
    result.errors.to_csv(config.VALIDATION_ERRORS_CSV_PATH, index=False)

    invalid_tests = int(result.invalid["is_test_record"].sum()) if not result.invalid.empty else 0
    stats = {
        "input_records": len(to_validate),
        "real_records": len(consumed),
        "test_records": len(test_records),
        "valid": len(result.valid),
        "invalid": len(result.invalid),
        "invalid_real": len(result.invalid) - invalid_tests,
        "invalid_test": invalid_tests,
        "failed_checks": len(result.errors),
        "errors_by_check": result.errors["check"].astype(str).value_counts().to_dict() if not result.errors.empty else {},
    }
    log.info("Validated %d records (%d real + %d test): %d valid, %d invalid (%d real, %d test), %d failed checks",
             stats["input_records"], stats["real_records"], stats["test_records"], stats["valid"],
             stats["invalid"], stats["invalid_real"], stats["invalid_test"], stats["failed_checks"])
    if not result.invalid.empty:
        for record_id, messages in result.invalid[["record_id", "validation_errors"]].itertuples(index=False):
            log.info("  INVALID %-32s %s", record_id, messages)
    return stats


if __name__ == "__main__":
    config.setup_logging()
    run()
