"""Project settings: file paths, API parameters and Kafka connection (overridable via environment variables)."""

import logging
import os
from datetime import date, datetime, time, timezone
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / "data"
FIGURES_DIR = ROOT_DIR / "figures"
LOGS_DIR = ROOT_DIR / "logs"
SCHEMA_SQL_PATH = ROOT_DIR / "sql" / "schema.sql"

RAW_JSON_PATH = DATA_DIR / "raw_api_responses.json"
RAW_CSV_PATH = DATA_DIR / "raw_data.csv"
CONSUMED_CSV_PATH = DATA_DIR / "consumed_data.csv"
VALID_CSV_PATH = DATA_DIR / "validated_data.csv"
INVALID_CSV_PATH = DATA_DIR / "invalid_records.csv"
VALIDATION_ERRORS_CSV_PATH = DATA_DIR / "validation_errors.csv"
CLEAN_CSV_PATH = DATA_DIR / "clean_data.csv"
AGGREGATES_DIR = DATA_DIR / "aggregates"
DB_PATH = DATA_DIR / "output.db"
SUMMARY_JSON_PATH = DATA_DIR / "pipeline_summary.json"

API_BASE_URL = os.getenv("SE_API_BASE_URL", "https://api.stackexchange.com/2.3")
API_SITE = "stackoverflow"
API_KEY = os.getenv("STACKEXCHANGE_KEY")  # optional: raises the daily quota per IP from 300 to 10,000 requests
TAGS = os.getenv("SO_TAGS", "pandas,apache-kafka,airflow,sqlite,selenium-webdriver,web-scraping").split(",")
FROM_DATE = date.fromisoformat(os.getenv("SO_FROM_DATE", "2025-01-01"))
TO_DATE = date.fromisoformat(os.getenv("SO_TO_DATE", "2025-12-31"))  # inclusive
API_PAGE_SIZE = 100  # maximum page size accepted by the API
API_MAX_PAGES_PER_TAG = 25
API_TIMEOUT_S = 20
API_MAX_RETRIES = 4
API_REQUEST_DELAY_S = 0.2  # the API allows at most 30 requests/s per IP

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "so-questions")
KAFKA_PARTITIONS = 3
KAFKA_REPLICATION_FACTOR = 1
KAFKA_RETENTION_MS = 7 * 24 * 60 * 60 * 1000
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "so-pipeline-consumer")
CONSUMER_IDLE_TIMEOUT_S = 10


def unix_range(first: date = FROM_DATE, last: date = TO_DATE) -> tuple[int, int]:
    """First and last second (UTC) of the collection window as Unix timestamps."""
    start = datetime.combine(first, time.min, tzinfo=timezone.utc)
    end = datetime.combine(last, time.max, tzinfo=timezone.utc)
    return int(start.timestamp()), int(end.timestamp())


def setup_logging(level: int = logging.INFO) -> None:
    """Log to the console and to logs/pipeline.log with one shared format."""
    LOGS_DIR.mkdir(exist_ok=True)
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-7s | %(name)-12s | %(message)s",
        datefmt="%H:%M:%S",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(LOGS_DIR / "pipeline.log", encoding="utf-8"),
        ],
        force=True,
    )
    logging.getLogger("matplotlib").setLevel(logging.WARNING)
