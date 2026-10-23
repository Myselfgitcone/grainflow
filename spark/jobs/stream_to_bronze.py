"""Stream every Kafka-backed contract into its Iceberg bronze table.

One streaming query per topic. Each micro-batch is checked against the
contract again (producers we don't own may write to these topics too). Rows
that pass land in the bronze table; rows that fail land in bronze.quarantine
with the raw payload and the reason, so nothing disappears silently.

Delivery is at-least-once: foreachBatch can replay a batch after a crash.
The silver dbt models dedupe on each dataset's natural key, which is cheaper
and simpler than idempotent sinks here.
"""

from __future__ import annotations

import argparse
import logging
import os

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from grainflow.contracts import Contract, load_contracts

log = logging.getLogger("stream_to_bronze")

CATALOG = "lake"
QUARANTINE_TABLE = f"{CATALOG}.bronze.quarantine"

# Timestamps and dates are read as strings and cast afterwards: a failed cast
# becomes a named violation instead of an anonymous null from from_json.
_WIRE_TYPES = {
    "string": T.StringType(),
    "double": T.DoubleType(),
    "long": T.LongType(),
    "int": T.IntegerType(),
    "timestamp": T.StringType(),
    "date": T.StringType(),
}
_CASTS = {"timestamp": "timestamp", "date": "date"}


def wire_schema(contract: Contract) -> T.StructType:
    return T.StructType([T.StructField(f.name, _WIRE_TYPES[f.type], True) for f in contract.fields])


def decode(raw: DataFrame, contract: Contract) -> DataFrame:
    """Kafka rows -> one column per contract field, plus lineage columns."""
    payload = F.col("value").cast("string")
    parsed = raw.select(
        payload.alias("_raw"),
        F.from_json(payload, wire_schema(contract)).alias("record"),
        F.col("partition").alias("_kafka_partition"),
        F.col("offset").alias("_kafka_offset"),
        F.col("timestamp").alias("_kafka_timestamp"),
    )

    columns = []
    for field in contract.fields:
        columns.append(F.col(f"record.{field.name}").alias(f"_wire_{field.name}"))
        if field.type in _CASTS:
            # try_cast: a malformed value becomes null (and a violation) instead of
            # failing the whole micro-batch under ANSI mode.
            value = F.expr(f"try_cast(record.`{field.name}` AS {_CASTS[field.type]})")
        else:
            value = F.col(f"record.{field.name}")
        columns.append(value.alias(field.name))

    return parsed.select("_raw", "_kafka_partition", "_kafka_offset", "_kafka_timestamp", *columns)


def violations(contract: Contract) -> Column:
    """A single string column listing every rule a row breaks, or null if none."""
    checks: list[tuple[Column, str]] = []
    for field in contract.fields:
        value = F.col(field.name)
        wire = F.col(f"_wire_{field.name}")

        if field.required:
            checks.append((wire.isNull(), f"{field.name} is required"))
        if field.type in _CASTS:
            checks.append(
                (wire.isNotNull() & value.isNull(), f"{field.name} is not a valid {field.type}")
            )
        if field.allowed is not None:
            checks.append(
                (value.isNotNull() & ~value.isin(*field.allowed), f"{field.name} not allowed")
            )
        if field.minimum is not None:
            checks.append((value < F.lit(field.minimum), f"{field.name} below {field.minimum}"))
        if field.maximum is not None:
            checks.append((value > F.lit(field.maximum), f"{field.name} above {field.maximum}"))

    messages = [F.when(condition, F.lit(message)) for condition, message in checks]
    joined = F.concat_ws("; ", *messages)
    return F.when(joined == "", None).otherwise(joined)


def split(decoded: DataFrame, contract: Contract) -> tuple[DataFrame, DataFrame]:
    """Return (rows for the bronze table, rows for quarantine)."""
    checked = decoded.withColumn("_reason", violations(contract)).withColumn(
        "_ingested_at", F.current_timestamp()
    )
    lineage = ["_kafka_partition", "_kafka_offset", "_kafka_timestamp", "_ingested_at"]

    good = checked.where(F.col("_reason").isNull()).select(*contract.field_names, *lineage)
    bad = checked.where(F.col("_reason").isNotNull()).select(
        F.lit(contract.name).alias("contract"),
        F.lit(contract.topic).alias("topic"),
        F.col("_kafka_partition").alias("kafka_partition"),
        F.col("_kafka_offset").alias("kafka_offset"),
        F.col("_raw").alias("raw_value"),
        F.col("_reason").alias("reason"),
        "_ingested_at",
    )
    return good, bad


def batch_writer(contract: Contract):
    table = f"{CATALOG}.{contract.table}"

    def write(batch: DataFrame, batch_id: int) -> None:
        decoded = decode(batch, contract).persist()
        try:
            good, bad = split(decoded, contract)
            if not good.isEmpty():
                good.writeTo(table).append()
            if not bad.isEmpty():
                bad.writeTo(QUARANTINE_TABLE).append()
        finally:
            decoded.unpersist()
        log.info("%s batch %d committed", contract.name, batch_id)

    return write


def start_stream(spark: SparkSession, contract: Contract, args: argparse.Namespace):
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", contract.topic)
        .option("startingOffsets", args.starting_offsets)
        .option("maxOffsetsPerTrigger", args.max_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )
    return (
        raw.writeStream.queryName(f"bronze_{contract.name}")
        .foreachBatch(batch_writer(contract))
        .option("checkpointLocation", f"{args.checkpoints}/{contract.name}")
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .start()
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "redpanda:9092"))
    parser.add_argument("--checkpoints", default=os.environ.get("CHECKPOINT_ROOT", "/checkpoints"))
    parser.add_argument("--trigger-seconds", type=int, default=30)
    parser.add_argument("--max-offsets", type=int, default=50_000)
    parser.add_argument("--starting-offsets", default="earliest")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args(argv)
    spark = SparkSession.builder.appName("grainflow-bronze").getOrCreate()

    streaming = [c for c in load_contracts().values() if c.is_streaming]
    for contract in streaming:
        start_stream(spark, contract, args)
        log.info("streaming %s -> %s", contract.topic, contract.table)

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
