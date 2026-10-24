"""The public dashboard build: same contracts and dbt models, no servers.

The streaming stack (Kafka, Spark, Iceberg, Trino, Dagster) needs a machine
running all day. The public site is rebuilt once a day instead:

1. `load` pulls the sources into DuckDB tables shaped exactly like bronze,
   validating every record against its contract on the way in.
2. `dbt build --target publish` runs the unchanged silver and gold models.
3. `export` writes the gold results and run stats to JSON for the static page.
"""

from __future__ import annotations

import json
import logging
import math
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa

from grainflow.contracts import Contract, coerce, load_contracts
from grainflow.settings import Settings

log = logging.getLogger(__name__)

_DUCK_TYPES = {
    "string": "VARCHAR",
    "double": "DOUBLE",
    "long": "BIGINT",
    "int": "INTEGER",
    "timestamp": "TIMESTAMPTZ",
    "date": "DATE",
}
_ARROW_TYPES = {
    "string": pa.string(),
    "double": pa.float64(),
    "long": pa.int64(),
    "int": pa.int32(),
    "timestamp": pa.timestamp("us", tz="UTC"),
    "date": pa.date32(),
}

# Same lineage columns as the Iceberg bronze tables; Kafka ones stay null here.
_METADATA_COLUMNS = [
    ("_kafka_partition", "INTEGER"),
    ("_kafka_offset", "BIGINT"),
    ("_kafka_timestamp", "TIMESTAMPTZ"),
    ("_ingested_at", "TIMESTAMPTZ"),
]

PRICE_SYMBOLS = ("ZC=F", "ZS=F", "ZW=F")
CROP_COMMODITIES = ("CORN", "SOYBEANS")


@dataclass
class LoadResult:
    written: int
    rejected: int


def connect(db_path: Path):
    import duckdb

    con = duckdb.connect(str(db_path))
    con.execute("SET TimeZone = 'UTC'")
    return con


def create_bronze(con, contracts: Iterable[Contract]) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    for contract in contracts:
        columns = [f"{f.name} {_DUCK_TYPES[f.type]}" for f in contract.fields]
        columns += [f"{name} {kind}" for name, kind in _METADATA_COLUMNS]
        con.execute(f"CREATE OR REPLACE TABLE {contract.table} ({', '.join(columns)})")
    con.execute(
        """
        CREATE OR REPLACE TABLE bronze.quarantine (
            contract VARCHAR, topic VARCHAR, kafka_partition INTEGER, kafka_offset BIGINT,
            raw_value VARCHAR, reason VARCHAR, _ingested_at TIMESTAMPTZ
        )
        """
    )


def load_records(con, contract: Contract, records: list[dict[str, Any]]) -> LoadResult:
    """Validate and insert records; anything that breaks the contract goes to quarantine."""
    validator = contract.validator()
    now = datetime.now(timezone.utc)
    good, bad = [], []
    for record in records:
        errors = validator.errors(record)
        if errors:
            bad.append((record, "; ".join(errors)))
        else:
            good.append(record)

    if good:
        schema = pa.schema(
            [(f.name, _ARROW_TYPES[f.type]) for f in contract.fields]
            + [("_ingested_at", pa.timestamp("us", tz="UTC"))]
        )
        rows = [
            {
                **{f.name: coerce(r.get(f.name), f.type) for f in contract.fields},
                "_ingested_at": now,
            }
            for r in good
        ]
        con.register("incoming", pa.Table.from_pylist(rows, schema=schema))
        con.execute(f"INSERT INTO {contract.table} BY NAME SELECT * FROM incoming")
        con.unregister("incoming")

    for record, reason in bad:
        con.execute(
            "INSERT INTO bronze.quarantine (contract, raw_value, reason, _ingested_at) "
            "VALUES (?, ?, ?, ?)",
            [contract.name, json.dumps(record, default=str), reason, now],
        )

    log.info("%s: %d loaded, %d quarantined", contract.table, len(good), len(bad))
    return LoadResult(written=len(good), rejected=len(bad))


def load_sources(db_path: Path, settings: Settings, price_period: str = "10y") -> dict[str, Any]:
    """Fetch every source into a fresh DuckDB file shaped like bronze."""
    from grainflow.sources import crops, prices, weather

    if not settings.nass_api_key:
        raise RuntimeError("GRAINFLOW_NASS_API_KEY is not set")

    contracts = load_contracts()
    con = connect(db_path)
    create_bronze(con, contracts.values())

    bars = [b for s in PRICE_SYMBOLS for b in prices.fetch_bars(s, "1d", price_period)]
    if not bars:
        raise RuntimeError("no price bars returned; refusing to publish an empty site")
    stats = {"price_bar": load_records(con, contracts["price_bar"], bars)}

    client = weather.WeatherClient(settings.noaa_user_agent)
    observations = []
    for station in weather.CORN_BELT_STATIONS:
        try:
            observations += client.recent_observations(station, limit=500)
        except Exception as error:  # one flaky station shouldn't sink the build
            log.warning("skipping weather station %s: %s", station, error)
    stats["weather_observation"] = load_records(con, contracts["weather_observation"], observations)

    conditions = [
        r for c in CROP_COMMODITIES for r in crops.fetch_condition(settings.nass_api_key, c, 2018)
    ]
    stats["crop_condition"] = load_records(con, contracts["crop_condition"], conditions)

    con.close()
    return {name: vars(result) for name, result in stats.items()}


def export_site_data(
    db_path: Path, out_dir: Path, run_results: Path | None = None, video_url: str = ""
) -> dict[str, Any]:
    """Write the JSON the dashboard reads. Returns the summary for logging."""
    con = connect(db_path)
    data_dir = out_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    signal = _rows(
        con,
        """
        select
            count(*) as weeks,
            corr(good_excellent_change, next_week_corn_return) as correlation,
            count_if(good_excellent_change <= -1.5) as fell_weeks,
            avg(case when good_excellent_change <= -1.5 then next_week_corn_return end) as fell_avg,
            count_if(good_excellent_change >= 1.5) as rose_weeks,
            avg(case when good_excellent_change >= 1.5 then next_week_corn_return end) as rose_avg,
            min(week_start) as first_week,
            max(week_start) as last_week
        from gold.mart_corn_belt_weekly
        where good_excellent_change is not null and next_week_corn_return is not null
        """,
    )[0]
    signal.update(_significance(signal["correlation"], signal["weeks"]))

    weekly = _rows(
        con,
        """
        select week_start, corn_close, corn_return, pct_good_excellent, good_excellent_change,
               next_week_corn_return, gdd_f, states_reporting
        from gold.mart_corn_belt_weekly
        order by week_start
        """,
    )

    states = _rows(
        con,
        """
        with latest as (
            select max(week_ending) as week_ending
            from gold.fct_crop_condition_weekly where commodity = 'CORN'
        )
        select c.state_alpha, c.week_ending, c.pct_good_excellent, c.good_excellent_change,
               c.condition_index
        from gold.fct_crop_condition_weekly as c
        join latest using (week_ending)
        join silver.ref_crop_progress_states as major
            on c.commodity = major.commodity and c.state_alpha = major.state_alpha
        where c.commodity = 'CORN'
        order by c.pct_good_excellent desc
        """,
    )

    freshness = _rows(
        con,
        """
        select
            (select max(trade_date) from gold.fct_price_daily) as latest_price_date,
            (select max(week_ending) from gold.fct_crop_condition_weekly) as latest_crop_week,
            (select max(observed_at) from silver.stg_weather_observations) as latest_weather_obs
        """,
    )[0]

    volumes = _rows(
        con,
        """
        select 'price_bars' as dataset, count(*) as rows from bronze.price_bars
        union all select 'weather_observations', count(*) from bronze.weather_observations
        union all select 'crop_condition', count(*) from bronze.crop_condition
        union all select 'quarantine', count(*) from bronze.quarantine
        """,
    )
    con.close()

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "signal": signal,
        "freshness": freshness,
        "volumes": {v["dataset"]: v["rows"] for v in volumes},
        "dbt": _dbt_stats(run_results) if run_results else None,
        "video_url": video_url or None,
    }
    _write(data_dir / "summary.json", summary)
    _write(data_dir / "weekly.json", weekly)
    _write(data_dir / "states.json", states)
    return summary


def _significance(r: float | None, n: int) -> dict[str, float | None]:
    """t statistic and a normal-approximation p-value for a Pearson correlation."""
    if r is None or n < 3 or abs(r) >= 1:
        return {"t_stat": None, "p_value": None}
    t = r * math.sqrt(n - 2) / math.sqrt(1 - r * r)
    return {"t_stat": t, "p_value": math.erfc(abs(t) / math.sqrt(2))}


def _dbt_stats(run_results: Path) -> dict[str, int] | None:
    if not run_results.exists():
        return None
    results = json.loads(run_results.read_text())["results"]
    tests = [r for r in results if r["unique_id"].startswith("test.")]
    models = [r for r in results if r["unique_id"].startswith("model.")]
    return {
        "models": len(models),
        "tests": len(tests),
        "tests_passed": sum(r["status"] == "pass" for r in tests),
    }


def _rows(con, sql: str) -> list[dict[str, Any]]:
    cursor = con.execute(sql)
    names = [d[0] for d in cursor.description]
    return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]


def _write(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, default=_json_default, separators=(",", ":")))


def _json_default(value: Any) -> Any:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, float) and math.isnan(value):
        return None
    return str(value)
