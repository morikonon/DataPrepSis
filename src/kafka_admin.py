"""Kafka broker health check and topic management (create / describe)."""

import argparse
import logging
import sys

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from src import config

log = logging.getLogger("kafka_admin")


class BrokerUnavailableError(RuntimeError):
    """The Kafka broker cannot be reached."""


def admin_client() -> AdminClient:
    # Connection failures are reported by check_broker(), so librdkafka's own retry messages are muted.
    return AdminClient({"bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS, "logger": log, "log_level": 2})


def check_broker(timeout_s: float = 10) -> None:
    """Fail fast with a clear message if the broker is down."""
    try:
        metadata = admin_client().list_topics(timeout=timeout_s)
    except KafkaException as exc:
        raise BrokerUnavailableError(
            f"Kafka broker is not reachable at {config.KAFKA_BOOTSTRAP_SERVERS}. "
            "Start it with: docker compose up -d"
        ) from exc
    log.info("Connected to Kafka at %s (%d broker(s))", config.KAFKA_BOOTSTRAP_SERVERS, len(metadata.brokers))


def ensure_topic(topic: str = config.KAFKA_TOPIC) -> None:
    """Create the topic if it does not exist yet; an existing topic is left unchanged."""
    new_topic = NewTopic(
        topic,
        num_partitions=config.KAFKA_PARTITIONS,
        replication_factor=config.KAFKA_REPLICATION_FACTOR,
        config={"retention.ms": str(config.KAFKA_RETENTION_MS), "cleanup.policy": "delete"},
    )
    admin = admin_client()  # keep a reference: the future fails if the client is garbage-collected
    try:
        admin.create_topics([new_topic])[topic].result()
        log.info("Created topic '%s' (%d partitions, replication factor %d)",
                 topic, config.KAFKA_PARTITIONS, config.KAFKA_REPLICATION_FACTOR)
    except KafkaException as exc:
        if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
            raise
        log.info("Topic '%s' already exists", topic)


def topic_exists(topic: str = config.KAFKA_TOPIC) -> bool:
    return topic in admin_client().list_topics(timeout=10).topics


def describe_topic(topic: str = config.KAFKA_TOPIC) -> dict[int, int]:
    """Return {partition: number of messages currently stored} using the low/high watermarks."""
    consumer = Consumer({"bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS, "group.id": "so-topic-inspector"})
    try:
        topic_metadata = consumer.list_topics(topic, timeout=10).topics[topic]
        if topic_metadata.error is not None:
            raise KafkaException(topic_metadata.error)
        partitions = topic_metadata.partitions
        sizes = {}
        for partition_id in sorted(partitions):
            low, high = consumer.get_watermark_offsets(TopicPartition(topic, partition_id), timeout=10)
            sizes[partition_id] = high - low
    finally:
        consumer.close()
    log.info("Topic '%s': %s messages per partition, %d in total", topic, sizes, sum(sizes.values()))
    return sizes


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["create", "describe"])
    args = parser.parse_args()
    config.setup_logging()
    try:
        check_broker()
    except BrokerUnavailableError as exc:
        log.error("%s", exc)
        sys.exit(1)
    if args.action == "create":
        ensure_topic()
    describe_topic()


if __name__ == "__main__":
    main()
