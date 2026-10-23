"""Generate the dbt sources file from the data contracts.

Column docs, not_null / accepted_values tests and freshness thresholds all come
from the contracts, so dbt can never drift from what producers promise. CI runs
`grainflow contracts dbt-sources --check` to catch a stale file.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import yaml

from grainflow.contracts import Contract

SOURCES_FILE = (
    Path(__file__).resolve().parents[2] / "transform" / "models" / "staging" / "_sources.yml"
)

HEADER = (
    "# Generated from contracts/*.yaml by `grainflow contracts dbt-sources`.\n"
    "# Do not edit by hand: change the contract and regenerate.\n"
)

QUARANTINE = {
    "name": "quarantine",
    "description": "Records that failed their contract at ingestion, with the raw payload.",
    "config": {"loaded_at_field": "_ingested_at"},
    "columns": [
        {"name": "contract", "data_tests": ["not_null"]},
        {"name": "reason", "data_tests": ["not_null"]},
        {"name": "raw_value"},
    ],
}


def render_sources(contracts: Iterable[Contract]) -> str:
    by_namespace: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for contract in sorted(contracts, key=lambda c: c.table):
        by_namespace[contract.namespace].append(_table(contract))
    by_namespace["bronze"].append(QUARANTINE)

    document = {
        "version": 2,
        "sources": [
            {
                "name": namespace,
                "database": "{{ target.database }}",
                "schema": namespace,
                "tables": tables,
            }
            for namespace, tables in sorted(by_namespace.items())
        ],
    }
    return HEADER + yaml.safe_dump(document, sort_keys=False, width=100)


def _table(contract: Contract) -> dict[str, Any]:
    minutes = contract.freshness_minutes
    return {
        "name": contract.table_name,
        "description": contract.description,
        "config": {
            "loaded_at_field": "_ingested_at",
            "freshness": {
                "warn_after": {"count": minutes, "period": "minute"},
                "error_after": {"count": minutes * 2, "period": "minute"},
            },
            "meta": {
                "contract": contract.name,
                "version": contract.version,
                "owner": contract.owner,
            },
        },
        "columns": [_column(f) for f in contract.fields],
    }


def _column(field) -> dict[str, Any]:
    column: dict[str, Any] = {"name": field.name}
    if field.description:
        column["description"] = field.description
    tests: list[Any] = []
    if field.required:
        tests.append("not_null")
    if field.allowed is not None:
        tests.append({"accepted_values": {"arguments": {"values": list(field.allowed)}}})
    if tests:
        column["data_tests"] = tests
    return column
