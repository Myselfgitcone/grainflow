import pytest

from grainflow.contracts import ContractError, breaking_changes, parse_contract

BASE = """
name: thing
version: 1
owner: test
transport: {kind: kafka, topic: things.v1, key: id}
storage: {table: bronze.things, partition: {field: seen_at, transform: day}}
freshness_minutes: 10
fields:
  - {name: id, type: string, required: true}
  - {name: seen_at, type: timestamp, required: true}
  - {name: color, type: string, allowed: [red, blue]}
  - {name: count, type: int}
"""


def with_fields(*lines: str, version: int = 1) -> str:
    head = BASE.split("fields:")[0].replace("version: 1", f"version: {version}")
    return head + "fields:\n" + "\n".join(f"  - {line}" for line in lines)


def test_all_repo_contracts_load(contracts):
    assert set(contracts) == {"price_bar", "weather_observation", "crop_condition"}
    assert contracts["price_bar"].topic == "market.price_bars.v1"
    assert contracts["price_bar"].dlq_topic == "market.price_bars.v1.dlq"
    assert not contracts["crop_condition"].is_streaming


def test_valid_record_has_no_errors(contracts, price_bar):
    assert contracts["price_bar"].validator().errors(price_bar) == []


@pytest.mark.parametrize(
    ("change", "expected"),
    [
        ({"symbol": "ZZ=F"}, "symbol"),
        ({"close": -1.0}, "close"),
        ({"bar_start": "yesterday"}, "bar_start"),
        ({"volume": 12.5}, "volume"),
        ({"surprise": 1}, "Additional properties"),
    ],
)
def test_invalid_records_are_explained(contracts, price_bar, change, expected):
    errors = contracts["price_bar"].validator().errors({**price_bar, **change})
    assert any(expected in e for e in errors), errors


def test_missing_required_field(contracts, price_bar):
    price_bar.pop("close")
    errors = contracts["price_bar"].validator().errors(price_bar)
    assert errors == ["record: 'close' is a required property"]


def test_optional_fields_accept_null(contracts, weather_observation):
    weather_observation["temperature_c"] = None
    assert contracts["weather_observation"].validator().errors(weather_observation) == []


def test_unknown_type_is_rejected():
    with pytest.raises(ContractError, match="unknown type"):
        parse_contract(with_fields("{name: id, type: uuid}", "{name: seen_at, type: timestamp}"))


def test_partition_field_must_exist():
    with pytest.raises(ContractError, match="partition field"):
        parse_contract(with_fields("{name: id, type: string}"))


def test_adding_an_optional_field_is_not_breaking():
    old = parse_contract(BASE)
    new = parse_contract(BASE + "  - {name: note, type: string}\n")
    assert breaking_changes(old, new) == []


def test_widening_int_to_long_is_not_breaking():
    old = parse_contract(BASE)
    new = parse_contract(BASE.replace("{name: count, type: int}", "{name: count, type: long}"))
    assert breaking_changes(old, new) == []


def test_breaking_changes_are_listed():
    old = parse_contract(BASE)
    new = parse_contract(
        with_fields(
            "{name: id, type: long, required: true}",
            "{name: seen_at, type: timestamp, required: true}",
            "{name: color, type: string, allowed: [red]}",
            "{name: region, type: string, required: true}",
        )
    )
    assert breaking_changes(old, new) == [
        "field 'id' changed type string -> long",
        "field 'color' no longer allows ['blue']",
        "field 'count' was removed",
        "new field 'region' is required",
    ]
