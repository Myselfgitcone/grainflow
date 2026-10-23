import json

import pytest

from grainflow.publisher import Publisher, ensure_topics


class FakeProducer:
    def __init__(self):
        self.sent = []

    def produce(self, topic, value, key=None, headers=None):
        self.sent.append(
            {"topic": topic, "value": json.loads(value), "key": key, "headers": headers}
        )

    def poll(self, timeout):
        return 0

    def flush(self, timeout=None):
        return 0


@pytest.fixture
def producer():
    return FakeProducer()


def test_valid_record_goes_to_the_topic_keyed_by_contract_key(contracts, producer, price_bar):
    publisher = Publisher(contracts["price_bar"], producer)

    assert publisher.publish(price_bar) is True

    [message] = producer.sent
    assert message["topic"] == "market.price_bars.v1"
    assert message["key"] == b"ZC=F"
    assert message["value"] == price_bar


def test_invalid_record_goes_to_the_dlq_with_reasons(contracts, producer, price_bar):
    publisher = Publisher(contracts["price_bar"], producer)

    assert publisher.publish({**price_bar, "high": -3}) is False

    [message] = producer.sent
    assert message["topic"] == "market.price_bars.v1.dlq"
    assert message["headers"]["contract"] == "price_bar.v1"
    assert "high" in json.loads(message["headers"]["errors"])[0]


def test_publish_all_counts_outcomes(contracts, producer, price_bar):
    publisher = Publisher(contracts["price_bar"], producer)
    counts = publisher.publish_all([price_bar, price_bar, {**price_bar, "symbol": None}])
    assert counts == {"published": 2, "rejected": 1}


def test_batch_contracts_cannot_be_published(contracts, producer):
    with pytest.raises(ValueError, match="not a Kafka dataset"):
        Publisher(contracts["crop_condition"], producer)


class FakeAdmin:
    def __init__(self, existing):
        self.existing = existing
        self.created = None

    def list_topics(self, timeout):
        return type("Metadata", (), {"topics": dict.fromkeys(self.existing)})()

    def create_topics(self, topics):
        self.created = [t.topic for t in topics]
        return {}


def test_ensure_topics_creates_only_missing_topics(contracts):
    admin = FakeAdmin(existing={"market.price_bars.v1"})

    ensure_topics("unused:9092", contracts.values(), admin=admin)

    assert admin.created == [
        "market.price_bars.v1.dlq",
        "weather.observations.v1",
        "weather.observations.v1.dlq",
    ]


def test_ensure_topics_is_a_no_op_when_everything_exists(contracts):
    topics = {t for c in contracts.values() if c.is_streaming for t in (c.topic, c.dlq_topic)}
    admin = FakeAdmin(existing=topics)

    ensure_topics("unused:9092", contracts.values(), admin=admin)

    assert admin.created is None
