"""Pandera validation of the consumed records before cleaning: invalid rows are quarantined, not repaired."""

import logging
from dataclasses import dataclass

import pandas as pd
import pandera.pandas as pa
from pandera.errors import SchemaErrors

from src import config

log = logging.getLogger("validator")

FROM_TS, TO_TS = config.unix_range()
NOW_TS = int(pd.Timestamp.now(tz="UTC").timestamp())
CLOSE_REASONS = ["Duplicate", "Needs details or clarity", "Needs more focus", "Not suitable for this site",
                 "Opinion-based"]
USER_TYPES = ["registered", "unregistered", "does_not_exist"]
LICENSES = ["CC BY-SA 2.5", "CC BY-SA 3.0", "CC BY-SA 4.0"]
TEST_ID_BASE = 900_000_000  # far above real question ids, so test records are easy to recognise
OPTIONAL_TIMESTAMP = pa.Column("Int64", pa.Check.le(NOW_TS), nullable=True)


def not_before(later: pd.Series, earlier: pd.Series) -> pd.Series:
    """True where `later` is missing or not earlier than `earlier`."""
    return (later.isna() | (later >= earlier)).fillna(False).astype(bool)


def query_tag_in_tags(df: pd.DataFrame) -> pd.Series:
    return pd.Series([tag in tags.split(";") for tag, tags in zip(df["query_tag"], df["tags"])], index=df.index)


def link_matches_question_id(df: pd.DataFrame) -> pd.Series:
    return df["link"].str.extract(r"/questions/(\d+)/", expand=False) == df["question_id"].astype(str)


def activity_after_creation(df: pd.DataFrame) -> pd.Series:
    return (not_before(df["last_activity_date"], df["creation_date"])
            & not_before(df["last_edit_date"], df["creation_date"])
            & not_before(df["closed_date"], df["creation_date"]))


def answers_after_question(df: pd.DataFrame) -> pd.Series:
    return (not_before(df["first_answer_date"], df["creation_date"])
            & not_before(df["accepted_answer_date"], df["first_answer_date"]))


def answer_fields_consistent(df: pd.DataFrame) -> pd.Series:
    """Answers exist exactly when a first-answer date exists; 'answered' needs at least one answer."""
    has_answers = df["answer_count"] > 0
    is_answered = df["is_answered"] == "True"
    return (has_answers == df["first_answer_date"].notna()) & (~is_answered | has_answers)


def accepted_answer_consistent(df: pd.DataFrame) -> pd.Series:
    return df["accepted_answer_id"].notna() == df["accepted_answer_date"].notna()


def closed_fields_consistent(df: pd.DataFrame) -> pd.Series:
    return df["closed_date"].notna() == df["closed_reason"].notna()


RULES_SCHEMA = pa.DataFrameSchema(
    columns={
        "question_id": pa.Column(int, pa.Check.gt(0)),
        "query_tag": pa.Column(str, pa.Check.isin(config.TAGS)),
        # Stack Overflow requires at least 15 characters; HTML entities (&quot;) make raw titles longer than 150.
        "title": pa.Column(str, pa.Check.str_length(15, 300)),
        "tags": pa.Column(str, pa.Check.str_matches(r"^[a-z0-9+#.-]+(;[a-z0-9+#.-]+){0,4}$")),  # 1-5 tags
        "link": pa.Column(str, pa.Check.str_matches(r"^https://stackoverflow\.com/questions/\d+/")),
        "score": pa.Column(int, pa.Check.in_range(-100, 100_000)),
        "view_count": pa.Column(int, pa.Check.ge(0)),
        "answer_count": pa.Column(int, pa.Check.in_range(0, 100)),
        "is_answered": pa.Column(str, pa.Check.isin(["True", "False"])),
        "accepted_answer_id": pa.Column("Int64", pa.Check.gt(0), nullable=True),
        "creation_date": pa.Column(int, pa.Check.in_range(FROM_TS, TO_TS)),
        "last_activity_date": pa.Column(int, pa.Check.le(NOW_TS)),
        "last_edit_date": OPTIONAL_TIMESTAMP,
        "closed_date": OPTIONAL_TIMESTAMP,
        "closed_reason": pa.Column(str, pa.Check.isin(CLOSE_REASONS), nullable=True),
        "content_license": pa.Column(str, pa.Check.isin(LICENSES), nullable=True),
        "owner_user_type": pa.Column(str, pa.Check.isin(USER_TYPES)),
        "owner_reputation": pa.Column("Int64", pa.Check.ge(1), nullable=True),
        "first_answer_date": OPTIONAL_TIMESTAMP,
        "accepted_answer_date": OPTIONAL_TIMESTAMP,
    },
    checks=[
        pa.Check(query_tag_in_tags, name="query_tag_in_tags"),
        pa.Check(link_matches_question_id, name="link_matches_question_id"),
        pa.Check(activity_after_creation, name="activity_after_creation"),
        pa.Check(answers_after_question, name="answers_after_question"),
        pa.Check(answer_fields_consistent, name="answer_fields_consistent"),
        pa.Check(accepted_answer_consistent, name="accepted_answer_consistent"),
        pa.Check(closed_fields_consistent, name="closed_fields_consistent"),
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
    template = df.sort_values("question_id").iloc[0].to_dict()
    created = int(template["creation_date"])
    cases = {
        "wrong_types": {"score": "high", "view_count": "1.2k", "creation_date": "2025-13-01"},
        "missing_required": {"title": None, "tags": None, "owner_user_type": None},
        "out_of_range": {"title": "help", "answer_count": "-1", "creation_date": "1577836800"},
        "bad_categories": {"query_tag": "excel", "is_answered": "yes", "owner_user_type": "moderator",
                           "closed_reason": "Spam", "closed_date": str(created + 60)},
        "inconsistent": {"is_answered": "True", "answer_count": "0", "first_answer_date": str(created - 3600),
                         "last_activity_date": str(created - 60), "link": "https://stackoverflow.com/questions/1/x"},
    }
    rows = []
    for number, (case, overrides) in enumerate(cases.items(), start=1):
        question_id = str(TEST_ID_BASE + number)
        link = f"https://stackoverflow.com/questions/{question_id}/test-{case.replace('_', '-')}"
        rows.append({**template, "question_id": question_id, "link": link, **overrides,
                     "is_test_record": True, "test_case": case})
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
        errors.insert(0, "query_tag", df.loc[errors["index"], "query_tag"].to_numpy())
        errors.insert(0, "question_id", df.loc[errors["index"], "question_id"].to_numpy())
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
        for question_id, case, messages in result.invalid[["question_id", "test_case", "validation_errors"]].itertuples(
                index=False):
            log.info("  INVALID %s %-16s %s", question_id, case or "(real record)", messages)
    return stats


if __name__ == "__main__":
    config.setup_logging()
    run()
