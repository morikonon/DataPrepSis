"""Kafka producer: publishes the collected API records to the topic as JSON messages."""

import json
import logging
import sys
from collections import Counter

import pandas as pd
from confluent_kafka import KafkaException, Producer

from src import config
from src.kafka_admin import BrokerUnavailableError, check_broker, ensure_topic

log = logging.getLogger("producer")


def load_raw_records(path=config.RAW_CSV_PATH) -> list[dict]:
    """Read the collected records exactly as stored (all values as text, empty cells -> None)."""
    raw_df = pd.read_csv(path, dtype=str)
    return raw_df.astype(object).where(raw_df.notna(), None).to_dict("records")


def serialize(record: dict) -> bytes:
    return json.dumps(record, ensure_ascii=False).encode("utf-8")


class DeliveryReport:
    """Delivery callback: counts acknowledged and failed messages per partition."""

    def __init__(self) -> None:
        self.delivered_per_partition: Counter = Counter()
        self.failed = 0

    def __call__(self, error, message) -> None:
        if error is not None:
            self.failed += 1
            log.error("Delivery failed for key=%s: %s", message.key(), error)
        else:
            self.delivered_per_partition[message.partition()] += 1

    @property
    def delivered(self) -> int:
        return sum(self.delivered_per_partition.values())


def publish(records: list[dict], topic: str = config.KAFKA_TOPIC) -> dict:
    """Send every record (key = record_id) and wait until the broker acknowledges all of them."""
    producer = Producer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
        "acks": "all",
        "enable.idempotence": True,  # broker discards retried duplicates, so a retry never doubles a message
        "linger.ms": 10,
        "logger": log,
    })
    report = DeliveryReport()
    not_sent = 0

    for record in records:
        try:
            value = serialize(record)
        except (TypeError, ValueError) as exc:
            not_sent += 1
            log.error("Cannot serialize record %s: %s", record.get("record_id"), exc)
            continue
        while True:
            try:
                producer.produce(topic, key=record["record_id"], value=value,
                                 headers={"source": "jolpica-f1-api"}, on_delivery=report)
                break
            except BufferError:
                producer.poll(1)  # local queue is full: serve delivery callbacks, then retry
            except KafkaException as exc:
                not_sent += 1
                log.error("Cannot produce record %s: %s", record["record_id"], exc)
                break
        producer.poll(0)

    unacknowledged = producer.flush(30)
    stats = {
        "attempted": len(records),
        "published": report.delivered,
        "failed": report.failed + not_sent + unacknowledged,
        "per_partition": dict(sorted(report.delivered_per_partition.items())),
    }
    log.info("Published %d/%d messages to '%s' (failed: %d), per partition: %s",
             stats["published"], stats["attempted"], topic, stats["failed"], stats["per_partition"])
    return stats


def run() -> dict:
    check_broker()
    ensure_topic()
    records = load_raw_records()
    log.info("Loaded %d raw records from %s", len(records), config.RAW_CSV_PATH.name)
    return publish(records)


if __name__ == "__main__":
    config.setup_logging()
    try:
        run()
    except (BrokerUnavailableError, FileNotFoundError) as exc:
        log.error("%s", exc)
        sys.exit(1)
