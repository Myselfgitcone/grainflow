"""Runs the real pyiceberg code against a SQLite catalog and a local warehouse."""

import pytest

from grainflow import lake
from grainflow.settings import Settings


@pytest.fixture
def local_lake(tmp_path, contracts):
    settings = Settings(
        catalog_uri=f"sqlite:///{tmp_path / 'catalog.db'}",
        warehouse=f"file://{tmp_path / 'warehouse'}",
        s3_endpoint=None,
    )
    catalog = lake.catalog(settings)
    lake.ensure_tables(catalog, contracts.values())
    return catalog


def crop_rows(state: str, pct: float = 50.0):
    return [
        {
            "commodity": "CORN",
            "state_alpha": state,
            "week_ending": "2026-07-12",
            "condition_class": cls,
            "pct": pct,
            "nass_load_time": "2026-07-13T16:00:42+00:00",
        }
        for cls in ("GOOD", "EXCELLENT")
    ]


def test_tables_follow_contracts(local_lake, contracts):
    table = local_lake.load_table("bronze.price_bars")

    columns = [f.name for f in table.schema().fields]
    assert columns[: len(contracts["price_bar"].fields)] == contracts["price_bar"].field_names
    assert columns[-1] == "_ingested_at"
    assert str(table.spec().fields[0].transform) == "day"


def test_ensure_tables_is_idempotent(local_lake, contracts):
    lake.ensure_tables(local_lake, contracts.values())
    assert ("bronze", "quarantine") in local_lake.list_tables("bronze")


def test_load_batch_replaces_the_slice(local_lake, contracts):
    contract = contracts["crop_condition"]

    lake.load_batch(local_lake, contract, crop_rows("IA") + crop_rows("IL"), ("commodity", "CORN"))
    result = lake.load_batch(local_lake, contract, crop_rows("IA", 60.0), ("commodity", "CORN"))

    rows = local_lake.load_table("bronze.crop_condition").scan().to_arrow().to_pylist()
    assert result.written == 2
    assert {(r["state_alpha"], r["pct"]) for r in rows} == {("IA", 60.0)}


def test_bad_rows_are_quarantined_not_dropped(local_lake, contracts):
    rows = crop_rows("IA")
    rows[1]["pct"] = 140.0

    result = lake.load_batch(local_lake, contracts["crop_condition"], rows)

    quarantined = local_lake.load_table("bronze.quarantine").scan().to_arrow().to_pylist()
    assert (result.written, result.rejected) == (1, 1)
    assert quarantined[0]["contract"] == "crop_condition"
    assert "pct" in quarantined[0]["reason"]


def test_empty_response_does_not_wipe_existing_data(local_lake, contracts):
    contract = contracts["crop_condition"]
    lake.load_batch(local_lake, contract, crop_rows("IA"), ("commodity", "CORN"))

    lake.load_batch(local_lake, contract, [], ("commodity", "CORN"))

    assert local_lake.load_table("bronze.crop_condition").scan().to_arrow().num_rows == 2
