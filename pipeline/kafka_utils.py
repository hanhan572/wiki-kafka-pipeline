"""Small helpers around confluent-kafka: connection config, topic creation, producer/consumer factories."""
import logging
import time

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from . import config

log = logging.getLogger(__name__)


def base_conf() -> dict:
    conf = {"bootstrap.servers": config.KAFKA_BOOTSTRAP}
    # Optional SASL settings so the same code works against a hosted Kafka
    if config.env("KAFKA_SECURITY_PROTOCOL"):
        conf["security.protocol"] = config.env("KAFKA_SECURITY_PROTOCOL")
    if config.env("KAFKA_SASL_MECHANISM"):
        conf["sasl.mechanism"] = config.env("KAFKA_SASL_MECHANISM")
        conf["sasl.username"] = config.env("KAFKA_USERNAME")
        conf["sasl.password"] = config.env("KAFKA_PASSWORD")
    return conf


def ensure_topics(topics: list[str], retries: int = 30) -> None:
    """Create topics if missing (idempotent). Waits for the broker to come up."""
    admin = AdminClient(base_conf())
    for attempt in range(retries):
        try:
            existing = set(admin.list_topics(timeout=5).topics)
            break
        except Exception as exc:  # broker not ready yet
            log.warning("waiting for Kafka (%s)", exc)
            time.sleep(2)
    else:
        raise RuntimeError("Kafka not reachable")
    missing = [t for t in topics if t not in existing]
    if not missing:
        return
    futures = admin.create_topics([NewTopic(t, num_partitions=config.TOPIC_PARTITIONS, replication_factor=1) for t in missing])
    for topic, fut in futures.items():
        try:
            fut.result()
            log.info("created topic %s (%d partitions)", topic, config.TOPIC_PARTITIONS)
        except Exception as exc:
            if "TOPIC_ALREADY_EXISTS" not in str(exc):
                raise


def make_producer(**extra) -> Producer:
    return Producer({**base_conf(), "linger.ms": 50, "compression.type": "lz4", "enable.idempotence": True, **extra})


def make_consumer(group_id: str, topics: list[str], **extra) -> Consumer:
    consumer = Consumer({
        **base_conf(),
        "group.id": group_id,
        "auto.offset.reset": "earliest",   # a new group replays whatever the broker still holds
        "enable.auto.commit": False,       # we commit manually, AFTER the work is safely stored
        "topic.metadata.refresh.interval.ms": 10000,   # notice quickly if a topic was (re)created
        **extra,
    })
    consumer.subscribe(topics)
    return consumer


def safe_commit(consumer: Consumer) -> None:
    """Synchronously commit the consumer's current positions; no-op when nothing new was consumed."""
    try:
        consumer.commit(asynchronous=False)
    except KafkaException as exc:
        if exc.args[0].code() != KafkaError._NO_OFFSET:
            raise
