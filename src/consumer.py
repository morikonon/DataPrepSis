"""Kafka consumer: reads JSON messages from the topic until a stop condition is met and saves them."""

import argparse
import json
import logging
import sys
import time

import pandas as pd
from confluent_kafka import Consumer, KafkaException

from src import config
from src.kafka_admin import BrokerUnavailableError, check_broker, topic_exists

log = logging.getLogger("consumer")


def deserialize(raw_value: bytes) -> dict:
    record = json.loads(raw_value.decode("utf-8"))
    if not isinstance(record, dict):
        raise ValueError(f"expected a JSON object, got {type(record).__name__}")
    return record


def consume(expected: int | None = None, idle_timeout_s: float = config.CONSUMER_IDLE_TIMEOUT_S,
            topic: str = config.KAFKA_TOPIC, group_id: str = config.KAFKA_GROUP_ID) -> tuple[list[dict], dict]:
    """Poll until `expected` messages are read or no message arrives for `idle_timeout_s` seconds.

    Offsets are committed only after the whole batch has been read, so a crash before that
    makes the batch be re-read (at-least-once delivery; duplicates are removed by the cleaner).
    """
    consumer = Consumer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "logger": log,
    })
    consumer.subscribe([topic])

    records, malformed, broker_errors = [], 0, 0
    stop_reason = "expected message count reached"
    last_message_at = time.monotonic()
    try:
        while expected is None or len(records) + malformed < expected:
            message = consumer.poll(1.0)
            if message is None:
                if time.monotonic() - last_message_at >= idle_timeout_s:
                    stop_reason = f"no new messages for {idle_timeout_s:.0f}s"
                    break
                continue
            last_message_at = time.monotonic()

            if message.error():
                if message.error().fatal():
                    raise KafkaException(message.error())
                broker_errors += 1
                log.warning("Broker error: %s", message.error())
                continue
            try:
                record = deserialize(message.value())
            except (UnicodeDecodeError, ValueError) as exc:  # JSONDecodeError is a ValueError
                malformed += 1
                log.warning("Skipping malformed message at partition %d offset %d: %s",
                            message.partition(), message.offset(), exc)
                continue
            record["kafka_partition"] = message.partition()
            record["kafka_offset"] = message.offset()
            records.append(record)

        if records or malformed:
            consumer.commit(asynchronous=False)
    finally:
        consumer.close()

    stats = {"consumed": len(records), "malformed": malformed, "broker_errors": broker_errors, "stop_reason": stop_reason}
    log.info("Consumed %d messages from '%s' (malformed: %d, broker errors: %d); stopped because: %s",
             len(records), topic, malformed, broker_errors, stop_reason)
    return records, stats


def run(expected: int | None = None, idle_timeout_s: float = config.CONSUMER_IDLE_TIMEOUT_S) -> dict:
    check_broker()
    if not topic_exists():
        raise RuntimeError(f"Topic '{config.KAFKA_TOPIC}' does not exist yet: run the producer first")
    records, stats = consume(expected=expected, idle_timeout_s=idle_timeout_s)
    if not records:
        log.warning("Nothing new in the topic for group '%s'; %s was not overwritten",
                    config.KAFKA_GROUP_ID, config.CONSUMED_CSV_PATH.name)
        return stats
    pd.DataFrame(records).to_csv(config.CONSUMED_CSV_PATH, index=False)
    log.info("Saved consumed records -> %s", config.CONSUMED_CSV_PATH.name)
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected", type=int, help="stop after this many messages (default: stop when idle)")
    parser.add_argument("--idle-timeout", type=float, default=config.CONSUMER_IDLE_TIMEOUT_S,
                        help="stop after this many seconds without a new message")
    args = parser.parse_args()
    config.setup_logging()
    try:
        run(args.expected, args.idle_timeout)
    except (BrokerUnavailableError, RuntimeError) as exc:
        log.error("%s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
