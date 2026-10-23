"""Loads the Dagster definitions against a freshly parsed Trino-target manifest."""

import os
import subprocess
import sys
from pathlib import Path

import pytest

TRANSFORM = Path(__file__).resolve().parents[1] / "transform"


@pytest.fixture(scope="module")
def defs(tmp_path_factory):
    os.environ["DBT_TARGET"] = "trino"
    os.environ.setdefault("DAGSTER_HOME", str(tmp_path_factory.mktemp("dagster")))
    dbt = Path(sys.executable).with_name("dbt")
    subprocess.run(
        [
            dbt,
            "parse",
            "--project-dir",
            TRANSFORM,
            "--profiles-dir",
            TRANSFORM,
            "--target",
            "trino",
        ],
        check=True,
        capture_output=True,
    )
    from grainflow.orchestration.definitions import defs

    return defs


def test_bronze_feeds_silver_feeds_gold(defs):
    graph = defs.resolve_asset_graph()

    def parents(key: str) -> set[str]:
        node = graph.get(next(k for k in graph.get_all_asset_keys() if k.to_user_string() == key))
        return {p.to_user_string() for p in node.parent_keys}

    assert parents("silver/stg_price_bars") == {"bronze/price_bars"}
    assert parents("silver/stg_crop_condition") == {"bronze/crop_condition"}
    assert parents("gold/mart_corn_belt_weekly") == {
        "gold/fct_price_daily",
        "gold/fct_weather_state_daily",
        "gold/fct_crop_condition_weekly",
        "silver/ref_crop_progress_states",
    }


def test_every_contract_gets_a_freshness_check(defs, contracts):
    checks = {k.to_user_string() for k in defs.resolve_asset_graph().asset_check_keys}
    for contract in contracts.values():
        assert f"{contract.table.replace('.', '/')}:fresh_per_contract" in checks


def test_jobs_and_schedules_are_registered(defs):
    for name in ("transform", "load_crop_condition", "check_freshness", "lake_maintenance"):
        assert defs.resolve_job_def(name) is not None
    assert len(defs.schedules) == 4
