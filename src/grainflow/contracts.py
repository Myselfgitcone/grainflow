"""Data contracts.

Every dataset is described once, in contracts/<name>.yaml. Producers validate
against it, the Spark job derives its read schema from it, the lake module
creates Iceberg tables from it, and Dagster reads freshness targets from it.

Keep this module light on imports: the Spark image loads it with nothing but
PyYAML installed.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from typing import Any

import yaml

FIELD_TYPES = ("string", "double", "long", "int", "timestamp", "date")
PARTITION_TRANSFORMS = ("identity", "day", "month")

# Widening a column is safe for every engine we use; anything else is not.
SAFE_TYPE_CHANGES = {("int", "long")}


def default_contracts_dir() -> Path:
    override = os.environ.get("GRAINFLOW_CONTRACTS_DIR")
    if override:
        return Path(override)
    return Path(__file__).resolve().parents[2] / "contracts"


class ContractError(ValueError):
    pass


@dataclass(frozen=True)
class Field:
    name: str
    type: str
    required: bool = False
    description: str = ""
    allowed: tuple[Any, ...] | None = None
    minimum: float | None = None
    maximum: float | None = None


@dataclass(frozen=True)
class Contract:
    name: str
    version: int
    owner: str
    description: str
    transport: dict[str, Any]
    table: str
    partition_field: str
    partition_transform: str
    freshness_minutes: int
    fields: tuple[Field, ...]

    @property
    def is_streaming(self) -> bool:
        return self.transport.get("kind") == "kafka"

    @property
    def topic(self) -> str:
        if not self.is_streaming:
            raise ContractError(f"{self.name} is a batch dataset and has no topic")
        return self.transport["topic"]

    @property
    def dlq_topic(self) -> str:
        return f"{self.topic}.dlq"

    @property
    def key_field(self) -> str | None:
        return self.transport.get("key")

    @property
    def namespace(self) -> str:
        return self.table.split(".")[0]

    @property
    def table_name(self) -> str:
        return self.table.split(".")[1]

    @property
    def field_names(self) -> list[str]:
        return [f.name for f in self.fields]

    @property
    def required_fields(self) -> list[str]:
        return [f.name for f in self.fields if f.required]

    def field(self, name: str) -> Field:
        for f in self.fields:
            if f.name == name:
                return f
        raise KeyError(name)

    @cached_property
    def json_schema(self) -> dict[str, Any]:
        return _json_schema(self)

    def validator(self) -> RecordValidator:
        return RecordValidator(self)


class RecordValidator:
    """Checks plain dict records against a contract's JSON Schema."""

    def __init__(self, contract: Contract):
        from jsonschema import Draft202012Validator, FormatChecker

        self._validator = Draft202012Validator(contract.json_schema, format_checker=FormatChecker())

    def errors(self, record: dict[str, Any]) -> list[str]:
        problems = []
        for error in sorted(self._validator.iter_errors(record), key=lambda e: list(e.path)):
            location = ".".join(str(p) for p in error.path) or "record"
            problems.append(f"{location}: {error.message}")
        return problems


def load_contract(path: Path) -> Contract:
    return parse_contract(path.read_text(), source=path.name)


def parse_contract(text: str, source: str = "<string>") -> Contract:
    raw = yaml.safe_load(text)
    try:
        fields = tuple(_parse_field(f) for f in raw["fields"])
        partition = raw["storage"]["partition"]
        contract = Contract(
            name=raw["name"],
            version=int(raw["version"]),
            owner=raw["owner"],
            description=raw.get("description", "").strip(),
            transport=raw["transport"],
            table=raw["storage"]["table"],
            partition_field=partition["field"],
            partition_transform=partition.get("transform", "identity"),
            freshness_minutes=int(raw["freshness_minutes"]),
            fields=fields,
        )
    except KeyError as missing:
        raise ContractError(f"{source}: missing key {missing}") from None

    _check(contract, source)
    return contract


def load_contracts(directory: Path | None = None) -> dict[str, Contract]:
    directory = directory or default_contracts_dir()
    contracts = {}
    for path in sorted(directory.glob("*.yaml")):
        contract = load_contract(path)
        if contract.name in contracts:
            raise ContractError(f"duplicate contract name {contract.name!r}")
        contracts[contract.name] = contract
    if not contracts:
        raise ContractError(f"no contracts found in {directory}")
    return contracts


def breaking_changes(old: Contract, new: Contract) -> list[str]:
    """List the changes in `new` that would break existing consumers of `old`."""
    problems = []
    new_fields = {f.name: f for f in new.fields}

    for before in old.fields:
        after = new_fields.get(before.name)
        if after is None:
            problems.append(f"field {before.name!r} was removed")
            continue
        if before.type != after.type and (before.type, after.type) not in SAFE_TYPE_CHANGES:
            problems.append(f"field {before.name!r} changed type {before.type} -> {after.type}")
        if after.required and not before.required:
            problems.append(f"field {before.name!r} became required")
        if before.allowed is not None:
            dropped = set(before.allowed) - set(after.allowed or before.allowed)
            if dropped:
                problems.append(f"field {before.name!r} no longer allows {sorted(dropped)}")

    old_names = set(old.field_names)
    for after in new.fields:
        if after.name not in old_names and after.required:
            problems.append(f"new field {after.name!r} is required")

    return problems


def _parse_field(raw: dict[str, Any]) -> Field:
    allowed = raw.get("allowed")
    return Field(
        name=raw["name"],
        type=raw["type"],
        required=bool(raw.get("required", False)),
        description=raw.get("description", ""),
        allowed=tuple(allowed) if allowed is not None else None,
        minimum=raw.get("minimum"),
        maximum=raw.get("maximum"),
    )


def _check(contract: Contract, source: str) -> None:
    names = contract.field_names
    if len(names) != len(set(names)):
        raise ContractError(f"{source}: duplicate field names")

    for f in contract.fields:
        if f.type not in FIELD_TYPES:
            raise ContractError(f"{source}: field {f.name!r} has unknown type {f.type!r}")
        if f.name.startswith("_"):
            raise ContractError(f"{source}: leading underscore is reserved for lake metadata")

    if contract.partition_field not in names:
        raise ContractError(f"{source}: partition field {contract.partition_field!r} not defined")
    if contract.partition_transform not in PARTITION_TRANSFORMS:
        raise ContractError(f"{source}: unknown transform {contract.partition_transform!r}")
    if contract.table.count(".") != 1:
        raise ContractError(f"{source}: storage.table must look like namespace.table")

    if contract.is_streaming:
        if "topic" not in contract.transport:
            raise ContractError(f"{source}: kafka transport needs a topic")
        if contract.key_field and contract.key_field not in names:
            raise ContractError(f"{source}: key field {contract.key_field!r} not defined")


_JSON_TYPES = {
    "string": {"type": "string"},
    "double": {"type": "number"},
    "long": {"type": "integer"},
    "int": {"type": "integer"},
    "timestamp": {"type": "string", "format": "date-time"},
    "date": {"type": "string", "format": "date"},
}


def _json_schema(contract: Contract) -> dict[str, Any]:
    properties = {}
    for f in contract.fields:
        prop = dict(_JSON_TYPES[f.type])
        if not f.required:
            prop["type"] = [prop["type"], "null"]
        if f.allowed is not None:
            prop["enum"] = list(f.allowed) + ([] if f.required else [None])
        if f.minimum is not None:
            prop["minimum"] = f.minimum
        if f.maximum is not None:
            prop["maximum"] = f.maximum
        if f.description:
            prop["description"] = f.description
        properties[f.name] = prop

    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": contract.name,
        "description": contract.description,
        "type": "object",
        "properties": properties,
        "required": contract.required_fields,
        "additionalProperties": False,
    }
