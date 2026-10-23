import json

import pytest

from grainflow.publisher import Publisher


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
