"""Runs the full pipeline: API -> Kafka producer -> Kafka consumer -> Pandera -> cleaning -> SQLite -> EDA."""

import argparse
import json
import logging
import sys
import time

import pandas as pd

from src import analysis, api_client, cleaner, config, consumer, loader, producer, validator
from src.kafka_admin import BrokerUnavailableError

log = logging.getLogger("main")


def explain_counts(counts: dict, validation: dict, cleaning: dict) -> list[str]:
    notes = []
    if counts["published"] != counts["collected"]:
        notes.append(f"{counts['collected'] - counts['published']} record(s) could not be published to Kafka.")
    if counts["consumed"] != counts["published"]:
        notes.append("Consumed != published: the consumer stopped on its idle timeout or read older messages.")
    notes.append(f"Validation input = {validation['real_records']} consumed + {validation['test_records']} "
                 "deliberately invalid test records.")
    notes.append(f"Invalid = {validation['invalid_real']} real + {validation['invalid_test']} test records "
                 "(quarantined in data/invalid_records.csv, never cleaned or stored).")
    removed = cleaning["input_records"] - cleaning["output_records"]
    notes.append(f"Cleaning removed {removed} duplicate record(s) "
                 f"({cleaning['duplicate_record_ids']} repeated record_id).")
    if counts["stored"] == counts["cleaned"]:
        notes.append("Every cleaned record is stored exactly once in SQLite (results table).")
    return notes


def run_pipeline(skip_collect: bool, seasons: list[int]) -> dict:
    started = time.perf_counter()

    if skip_collect:
        collected = len(pd.read_csv(config.RAW_CSV_PATH, usecols=["record_id"]))
        log.info("Step 1/7 skipped: reusing %d collected records from %s", collected, config.RAW_CSV_PATH.name)
    else:
        log.info("Step 1/7: collecting data from the API")
        collected = len(api_client.collect(seasons))

    log.info("Step 2/7: publishing to Kafka")
    production = producer.run()
    log.info("Step 3/7: consuming from Kafka")
    consumption = consumer.run(expected=production["published"])
    log.info("Step 4/7: validating with Pandera")
    validation = validator.run()
    log.info("Step 5/7: cleaning and transforming")
    cleaning = cleaner.run()
    log.info("Step 6/7: loading into SQLite (twice, to prove idempotency)")
    loading = loader.run(repeat=2)
    log.info("Step 7/7: exploratory data analysis")
    eda = analysis.run()

    counts = {
        "collected": collected,
        "published": production["published"],
        "consumed": consumption["consumed"],
        "validation_input": validation["input_records"],
        "valid": validation["valid"],
        "invalid": validation["invalid"],
        "cleaned": cleaning["output_records"],
        "stored": loading["stored_records"],
    }
    summary = {
        "counts": counts,
        "notes": explain_counts(counts, validation, cleaning),
        "kafka": {"producer": production, "consumer": consumption},
        "validation": validation,
        "cleaning": cleaning,
        "loading": loading,
        "eda": eda,
        "runtime_s": round(time.perf_counter() - started, 1),
    }
    with open(config.SUMMARY_JSON_PATH, "w", encoding="utf-8") as file:
        json.dump(summary, file, indent=2, default=lambda value: value.item() if hasattr(value, "item") else str(value))
    return summary


def log_summary(summary: dict) -> None:
    labels = {
        "collected": "Collected from API", "published": "Published to Kafka", "consumed": "Consumed from Kafka",
        "validation_input": "Validation input", "valid": "Valid records", "invalid": "Invalid records",
        "cleaned": "Records after cleaning", "stored": "Stored in SQLite",
    }
    table = "\n".join(f"  {labels[key]:<24}{value:>6}" for key, value in summary["counts"].items())
    notes = "\n".join(f"  - {note}" for note in summary["notes"])
    log.info("PIPELINE SUMMARY (%.1fs)\n%s\n%s\n  Idempotent load: %s",
             summary["runtime_s"], table, notes, summary["loading"]["idempotent"])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--skip-collect", action="store_true", help="reuse data/raw_data.csv instead of calling the API")
    parser.add_argument("--seasons", type=int, nargs="+", default=config.SEASONS)
    args = parser.parse_args()
    config.setup_logging()

    try:
        summary = run_pipeline(args.skip_collect, args.seasons)
    except (BrokerUnavailableError, api_client.ApiError, FileNotFoundError, RuntimeError) as exc:
        log.error("Pipeline stopped: %s", exc)
        sys.exit(1)
    log_summary(summary)


if __name__ == "__main__":
    main()
