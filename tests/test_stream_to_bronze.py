"""Checks the Spark decode/validate/split logic on a static DataFrame shaped like Kafka rows."""

import json
import shutil
from datetime import datetime, timezone

import pytest

pytestmark = pytest.mark.spark

if shutil.which("java") is None:
    pytest.skip("PySpark tests need Java", allow_module_level=True)

import stream_to_bronze as job  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("grainflow-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def kafka_rows(spark, payloads):
    rows = [
        (
            p if isinstance(p, bytes) else json.dumps(p).encode(),
            0,
            offset,
            datetime(2026, 7, 14, tzinfo=timezone.utc),
        )
        for offset, p in enumerate(payloads)
    ]
    return spark.createDataFrame(
        rows, "value binary, partition int, offset long, timestamp timestamp"
    )


def test_good_rows_land_with_typed_columns(spark, contracts, price_bar):
    contract = contracts["price_bar"]
    decoded = job.decode(kafka_rows(spark, [price_bar]), contract)

    good, bad = job.split(decoded, contract)

    [row] = good.collect()
    assert bad.count() == 0
    # collect() hands back naive local-time datetimes; compare in UTC.
    assert row.bar_start.astimezone(timezone.utc) == datetime(
        2026, 7, 14, 14, 31, tzinfo=timezone.utc
    )
    assert row.volume == 1830
    assert row._kafka_offset == 0
    assert good.columns[-4:] == [
        "_kafka_partition",
        "_kafka_offset",
        "_kafka_timestamp",
        "_ingested_at",
    ]


def test_bad_rows_are_quarantined_with_every_reason(spark, contracts, price_bar):
    contract = contracts["price_bar"]
    broken = {**price_bar, "symbol": "BTC", "low": -1.0, "bar_start": "not a time"}
    payloads = [broken, {**price_bar, "close": None}, b"{not json"]

    good, bad = job.split(job.decode(kafka_rows(spark, payloads), contract), contract)

    reasons = [r.reason for r in bad.orderBy("kafka_offset").collect()]
    assert good.count() == 0
    assert reasons[0] == "symbol not allowed; bar_start is not a valid timestamp; low below 0"
    assert reasons[1] == "close is required"
    assert "symbol is required" in reasons[2]


def test_optional_nulls_are_fine(spark, contracts, weather_observation):
    contract = contracts["weather_observation"]
    weather_observation.update(temperature_c=None, description=None)

    good, bad = job.split(job.decode(kafka_rows(spark, [weather_observation]), contract), contract)

    assert (good.count(), bad.count()) == (1, 0)
