import yaml

from grainflow.dbt_sources import SOURCES_FILE, render_sources


def test_sources_take_tests_and_freshness_from_contracts(contracts):
    document = yaml.safe_load(render_sources(contracts.values()))
    [bronze] = document["sources"]
    tables = {t["name"]: t for t in bronze["tables"]}

    price_bars = tables["price_bars"]
    assert price_bars["config"]["freshness"]["warn_after"] == {"count": 30, "period": "minute"}
    symbol = next(c for c in price_bars["columns"] if c["name"] == "symbol")
    assert symbol["data_tests"][0] == "not_null"
    assert symbol["data_tests"][1]["accepted_values"]["arguments"]["values"] == [
        "ZC=F",
        "ZS=F",
        "ZW=F",
        "KE=F",
    ]
    assert "quarantine" in tables


def test_committed_sources_file_is_current(contracts):
    assert SOURCES_FILE.read_text() == render_sources(contracts.values())
