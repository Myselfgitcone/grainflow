"""Iceberg tables for the bronze layer.

Tables are created here, from the contracts, and nowhere else. Spark only
appends to them, which keeps schema and partitioning decisions in one place.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.expressions import EqualTo
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform, IdentityTransform, MonthTransform
from pyiceberg.types import (
    DateType,
    DoubleType,
    IcebergType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
)

from grainflow.contracts import Contract
from grainflow.settings import Settings, get_settings

log = logging.getLogger(__name__)

QUARANTINE_TABLE = "bronze.quarantine"

_ICEBERG_TYPES: dict[str, type[IcebergType]] = {
    "string": StringType,
    "double": DoubleType,
    "long": LongType,
    "int": IntegerType,
    "timestamp": TimestamptzType,
    "date": DateType,
}

_TRANSFORMS = {"identity": IdentityTransform, "day": DayTransform, "month": MonthTransform}

# Lineage columns added to every bronze table. Kafka ones stay null for batch loads.
_METADATA_COLUMNS: list[tuple[str, IcebergType]] = [
    ("_kafka_partition", IntegerType()),
    ("_kafka_offset", LongType()),
    ("_kafka_timestamp", TimestamptzType()),
    ("_ingested_at", TimestamptzType()),
]

QUARANTINE_SCHEMA = Schema(
    NestedField(1, "contract", StringType()),
    NestedField(2, "topic", StringType()),
    NestedField(3, "kafka_partition", IntegerType()),
    NestedField(4, "kafka_offset", LongType()),
    NestedField(5, "raw_value", StringType()),
    NestedField(6, "reason", StringType()),
    NestedField(7, "_ingested_at", TimestamptzType()),
)


@dataclass
class LoadResult:
    written: int
    rejected: int


def catalog(settings: Settings | None = None) -> Catalog:
    s = settings or get_settings()
    properties = {
        "uri": s.catalog_uri,
        "warehouse": s.warehouse,
        "s3.access-key-id": s.s3_access_key,
        "s3.secret-access-key": s.s3_secret_key,
        "s3.region": s.s3_region,
    }
    if s.s3_endpoint:
        properties["s3.endpoint"] = s.s3_endpoint
    return SqlCatalog(s.catalog_name, **properties)


def iceberg_schema(contract: Contract) -> Schema:
    columns = [(f.name, _ICEBERG_TYPES[f.type]()) for f in contract.fields]
    columns += _METADATA_COLUMNS
    # Everything is optional in bronze. Contracts are enforced before the write,
    # and a nullable column never blocks landing a record we want to inspect.
    return Schema(
        *(NestedField(i, name, kind, required=False) for i, (name, kind) in enumerate(columns, 1))
    )


def partition_spec(contract: Contract, schema: Schema) -> PartitionSpec:
    source = schema.find_field(contract.partition_field)
    transform = _TRANSFORMS[contract.partition_transform]()
    suffix = (
        "" if contract.partition_transform == "identity" else f"_{contract.partition_transform}"
    )
    return PartitionSpec(
        PartitionField(
            source_id=source.field_id,
            field_id=1000,
            transform=transform,
            name=f"{contract.partition_field}{suffix}",
        )
    )


def ensure_tables(lake: Catalog, contracts: Iterable[Contract]) -> None:
    """Create namespaces and bronze tables that don't exist yet. Safe to rerun."""
    contracts = list(contracts)
    for namespace in sorted({c.namespace for c in contracts} | {"silver", "gold"}):
        lake.create_namespace_if_not_exists(namespace)

    for contract in contracts:
        schema = iceberg_schema(contract)
        lake.create_table_if_not_exists(
            contract.table,
            schema=schema,
            partition_spec=partition_spec(contract, schema),
            properties={
                "format-version": "2",
                "write.metadata.delete-after-commit.enabled": "true",
            },
        )
        log.info("table ready: %s", contract.table)

    lake.create_table_if_not_exists(QUARANTINE_TABLE, schema=QUARANTINE_SCHEMA)


def load_batch(
    lake: Catalog,
    contract: Contract,
    records: list[dict[str, Any]],
    replace_where: tuple[str, Any] | None = None,
) -> LoadResult:
    """Validate records and write them to the contract's bronze table.

    `replace_where=(column, value)` swaps out everything matching that filter in
    one Iceberg commit, so reloading the same slice is idempotent.
    """
    validator = contract.validator()
    now = datetime.now(timezone.utc)
    good, bad = [], []
    for record in records:
        errors = validator.errors(record)
        if errors:
            bad.append((record, "; ".join(errors)))
        else:
            good.append(record)

    table = lake.load_table(contract.table)
    frame = _to_arrow(contract, table.schema(), good, now)
    if not good:
        # Never let an empty API response wipe a slice we already have.
        log.warning("%s: nothing valid to write, leaving table untouched", contract.table)
    elif replace_where:
        column, value = replace_where
        table.overwrite(frame, overwrite_filter=EqualTo(column, value))
    else:
        table.append(frame)

    if bad:
        _quarantine(lake, contract, bad, now)

    log.info("%s: wrote %d, quarantined %d", contract.table, len(good), len(bad))
    return LoadResult(written=len(good), rejected=len(bad))


def _to_arrow(
    contract: Contract, schema: Schema, records: list[dict[str, Any]], ingested_at: datetime
) -> pa.Table:
    converters = {f.name: _CONVERTERS.get(f.type, _identity) for f in contract.fields}
    rows = []
    for record in records:
        row = {name: convert(record.get(name)) for name, convert in converters.items()}
        row["_ingested_at"] = ingested_at
        rows.append(row)
    return pa.Table.from_pylist(rows, schema=schema.as_arrow())


def _quarantine(
    lake: Catalog, contract: Contract, rejects: list[tuple[dict[str, Any], str]], now: datetime
) -> None:
    rows = [
        {
            "contract": contract.name,
            "raw_value": json.dumps(record, default=str),
            "reason": reason,
            "_ingested_at": now,
        }
        for record, reason in rejects
    ]
    table = lake.load_table(QUARANTINE_TABLE)
    table.append(pa.Table.from_pylist(rows, schema=table.schema().as_arrow()))


def _identity(value: Any) -> Any:
    return value


def _timestamp(value: Any) -> datetime | None:
    if value is None or isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def _date(value: Any) -> date | None:
    if value is None or isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


_CONVERTERS = {"timestamp": _timestamp, "date": _date}
