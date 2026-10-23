"""Contract-checked Kafka publishing.

Valid records go to the contract's topic. Records that fail validation go to
`<topic>.dlq` with the validation errors in a header, so bad data is visible
instead of silently dropped or silently landed.
"""

from __future__ import annotations

import json
import logging
from collections import Counter
from collections.abc import Iterable
from typing import Any, Protocol

from grainflow.contracts import Contract

log = logging.getLogger(__name__)


class Producer(Protocol):
    """The slice of confluent_kafka.Producer we use. Lets tests pass a fake."""

    def produce(
        self, topic: str, value: bytes, key: bytes | None = None, **kwargs: Any
    ) -> None: ...

    def poll(self, timeout: float) -> int: ...

    def flush(self, timeout: float = ...) -> int: ...


def kafka_producer(bootstrap: str) -> Producer:
    from confluent_kafka import Producer as KafkaProducer

    return KafkaProducer(
        {
            "bootstrap.servers": bootstrap,
            "enable.idempotence": True,
            "acks": "all",
            "linger.ms": 50,
            "compression.type": "zstd",
        }
    )


def ensure_topics(
    bootstrap: str, contracts: Iterable[Contract], partitions: int = 3, admin: Any = None
) -> None:
    """Create each contract's topic and its DLQ if they don't exist yet. Safe to rerun."""
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = admin or AdminClient({"bootstrap.servers": bootstrap})
    existing = set(admin.list_topics(timeout=10).topics)
    wanted = []
    for contract in contracts:
        if not contract.is_streaming:
            continue
        for topic in (contract.topic, contract.dlq_topic):
            if topic not in existing:
                wanted.append(NewTopic(topic, num_partitions=partitions, replication_factor=1))

    if not wanted:
        log.info("all topics already exist")
        return

    for topic, future in admin.create_topics(wanted).items():
        future.result()
        log.info("created topic %s", topic)


class Publisher:
    def __init__(self, contract: Contract, producer: Producer):
        if not contract.is_streaming:
            raise ValueError(f"{contract.name} is not a Kafka dataset")
        self.contract = contract
        self.producer = producer
        self.validator = contract.validator()
        self.counts: Counter[str] = Counter()

    def publish(self, record: dict[str, Any]) -> bool:
        """Send one record. Returns False if it was routed to the DLQ."""
        value = json.dumps(record, separators=(",", ":")).encode()
        key = self._key(record)
        errors = self.validator.errors(record)

        if errors:
            self.producer.produce(
                self.contract.dlq_topic,
                value=value,
                key=key,
                headers={
                    "contract": f"{self.contract.name}.v{self.contract.version}",
                    "errors": json.dumps(errors).encode(),
                },
            )
            self.counts["rejected"] += 1
            log.warning("rejected %s record: %s", self.contract.name, "; ".join(errors))
        else:
            self.producer.produce(self.contract.topic, value=value, key=key)
            self.counts["published"] += 1

        # Serve delivery callbacks so the local queue never fills up.
        self.producer.poll(0)
        return not errors

    def publish_all(self, records: Iterable[dict[str, Any]]) -> Counter[str]:
        for record in records:
            self.publish(record)
        self.flush()
        return self.counts

    def flush(self) -> None:
        remaining = self.producer.flush(30)
        if remaining:
            raise RuntimeError(f"{remaining} messages still undelivered after 30s")

    def _key(self, record: dict[str, Any]) -> bytes | None:
        field = self.contract.key_field
        if field is None or record.get(field) is None:
            return None
        return str(record[field]).encode()
