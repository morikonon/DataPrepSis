"""Project settings: file paths, API parameters and Kafka connection (overridable via environment variables)."""

import logging
import os
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

API_BASE_URL = os.getenv("F1_API_BASE_URL", "https://api.jolpi.ca/ergast/f1")
SEASONS = [int(season) for season in os.getenv("F1_SEASONS", "2023,2024,2025").split(",")]
API_PAGE_LIMIT = 100  # maximum page size accepted by the API
API_TIMEOUT_S = 15
API_MAX_RETRIES = 4
API_REQUEST_DELAY_S = 0.35  # Jolpica rate limit: 4 requests/s burst, 500 requests/hour

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "f1-race-results")
KAFKA_PARTITIONS = 3
KAFKA_REPLICATION_FACTOR = 1
KAFKA_RETENTION_MS = 7 * 24 * 60 * 60 * 1000
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "f1-pipeline-consumer")
CONSUMER_IDLE_TIMEOUT_S = 10


def setup_logging(level: int = logging.INFO) -> None:
    """Log to the console and to logs/pipeline.log with one shared format."""
    LOGS_DIR.mkdir(exist_ok=True)
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-7s | %(name)-16s | %(message)s",
        datefmt="%H:%M:%S",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(LOGS_DIR / "pipeline.log", encoding="utf-8"),
        ],
        force=True,
    )
    logging.getLogger("matplotlib").setLevel(logging.WARNING)
