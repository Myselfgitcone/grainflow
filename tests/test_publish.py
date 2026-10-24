import json

import pytest

from grainflow import publish


@pytest.fixture
def con(tmp_path, contracts):
    connection = publish.connect(tmp_path / "publish.duckdb")
    publish.create_bronze(connection, contracts.values())
    yield connection
    connection.close()


def test_bronze_tables_match_the_contracts(con, contracts):
    columns = [r[0] for r in con.execute("describe bronze.price_bars").fetchall()]
    assert columns == [
        *contracts["price_bar"].field_names,
        "_kafka_partition",
        "_kafka_offset",
        "_kafka_timestamp",
        "_ingested_at",
    ]


def test_bad_records_are_quarantined(con, contracts, price_bar):
    result = publish.load_records(
        con, contracts["price_bar"], [price_bar, {**price_bar, "close": -1}]
    )

    assert (result.written, result.rejected) == (1, 1)
    stored = con.execute("select symbol, close, bar_start from bronze.price_bars").fetchone()
    assert stored[:2] == ("ZC=F", 412.5)
    reason = con.execute("select reason from bronze.quarantine").fetchone()[0]
    assert "close" in reason


def test_significance_matches_a_hand_calculation():
    stats = publish._significance(-0.161, 257)
    assert round(stats["t_stat"], 2) == -2.6
    assert 0.008 < stats["p_value"] < 0.011
    assert publish._significance(None, 10) == {"t_stat": None, "p_value": None}


def test_export_writes_the_dashboard_files(tmp_path, con):
    con.execute("create schema silver; create schema gold")
    con.execute(
        """
        create table gold.mart_corn_belt_weekly as
        select * from (values
            (date '2025-06-02', 450.0, 0.01, 70.0, -2.0, 0.02, 120.0, 18),
            (date '2025-06-09', 459.0, 0.02, 68.0, -2.0, -0.01, 130.0, 18),
            (date '2025-06-16', 454.0, -0.01, 70.0, 2.0, -0.02, 110.0, 18),
            (date '2025-06-23', 445.0, -0.02, 72.0, 2.0, 0.01, 115.0, 18)
        ) t(week_start, corn_close, corn_return, pct_good_excellent, good_excellent_change,
            next_week_corn_return, gdd_f, states_reporting)
        """
    )
    con.execute(
        """
        create table gold.fct_crop_condition_weekly as
        select 'CORN' as commodity, 'IA' as state_alpha, date '2025-06-22' as week_ending,
               72.0 as pct_good_excellent, 2.0 as good_excellent_change, 3.9 as condition_index
        """
    )
    con.execute(
        "create table silver.ref_crop_progress_states as "
        "select 'CORN' as commodity, 'IA' as state_alpha"
    )
    con.execute("create table gold.fct_price_daily as select date '2025-06-27' as trade_date")
    con.execute(
        "create table silver.stg_weather_observations as "
        "select timestamptz '2025-06-27 12:00:00+00' as observed_at"
    )
    con.close()

    run_results = tmp_path / "run_results.json"
    run_results.write_text(
        json.dumps(
            {
                "results": [
                    {"unique_id": "model.grainflow.a", "status": "success"},
                    {"unique_id": "test.grainflow.b", "status": "pass"},
                    {"unique_id": "test.grainflow.c", "status": "fail"},
                ]
            }
        )
    )

    summary = publish.export_site_data(tmp_path / "publish.duckdb", tmp_path / "site", run_results)

    assert summary["signal"]["weeks"] == 4
    assert summary["signal"]["fell_weeks"] == 2
    assert summary["dbt"] == {"models": 1, "tests": 2, "tests_passed": 1}
    states = json.loads((tmp_path / "site" / "data" / "states.json").read_text())
    assert states[0]["state_alpha"] == "IA"
    weekly = json.loads((tmp_path / "site" / "data" / "weekly.json").read_text())
    assert weekly[0]["week_start"] == "2025-06-02"
