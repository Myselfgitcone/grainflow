"""Assets: what exists in the lake and how each piece gets there.

Streaming bronze tables are declared as external assets because Spark, not
Dagster, writes them. Declaring them still buys lineage (dbt models hang off
them) and lets freshness checks run against them.
"""

import os
from datetime import datetime, timezone
from pathlib import Path

from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetExecutionContext,
    AssetKey,
    AssetSpec,
    Config,
    MaterializeResult,
    asset,
    asset_check,
)
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

from grainflow import lake
from grainflow.contracts import Contract, load_contracts
from grainflow.orchestration.resources import TrinoResource
from grainflow.settings import get_settings

CONTRACTS = load_contracts()
TRANSFORM_DIR = Path(__file__).resolve().parents[3] / "transform"

dbt_project = DbtProject(
    project_dir=TRANSFORM_DIR,
    profiles_dir=TRANSFORM_DIR,
    target=os.environ.get("DBT_TARGET", "trino"),
)
dbt_project.prepare_if_dev()


def bronze_key(contract: Contract) -> AssetKey:
    # Matches dagster-dbt's key for source('bronze', <table>), which wires up lineage.
    return AssetKey([contract.namespace, contract.table_name])


streaming_bronze = [
    AssetSpec(
        bronze_key(c),
        group_name="bronze",
        description=c.description,
        kinds={"kafka", "spark", "iceberg"},
        owners=[f"team:{c.owner}"],
        metadata={"topic": c.topic, "contract_version": c.version},
    )
    for c in CONTRACTS.values()
    if c.is_streaming
]


class CropLoadConfig(Config):
    commodities: list[str] = ["CORN", "SOYBEANS"]  # noqa: RUF012 - pydantic copies defaults
    since_year: int = 2018


@asset(
    key=bronze_key(CONTRACTS["crop_condition"]),
    group_name="bronze",
    kinds={"python", "iceberg"},
    owners=["team:agronomy"],
    description=CONTRACTS["crop_condition"].description,
)
def crop_condition(context: AssetExecutionContext, config: CropLoadConfig) -> MaterializeResult:
    from grainflow.sources.crops import fetch_condition

    settings = get_settings()
    if not settings.nass_api_key:
        raise RuntimeError("GRAINFLOW_NASS_API_KEY is not set")

    contract = CONTRACTS["crop_condition"]
    catalog = lake.catalog(settings)
    written = rejected = 0
    for commodity in config.commodities:
        records = fetch_condition(settings.nass_api_key, commodity, config.since_year)
        result = lake.load_batch(catalog, contract, records, replace_where=("commodity", commodity))
        context.log.info(f"{commodity}: {result.written} written, {result.rejected} quarantined")
        written += result.written
        rejected += result.rejected

    return MaterializeResult(metadata={"rows_written": written, "rows_quarantined": rejected})


@dbt_assets(manifest=dbt_project.manifest_path, project=dbt_project)
def grainflow_dbt(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


def freshness_check(contract: Contract):
    """Fail when a bronze table hasn't received data within its contract's freshness target."""

    @asset_check(
        asset=bronze_key(contract),
        name="fresh_per_contract",
        description=f"New rows within {contract.freshness_minutes} minutes (from the contract).",
    )
    def _check(trino: TrinoResource) -> AssetCheckResult:
        latest = trino.scalar(f"select max(_ingested_at) from {contract.table}")
        if latest is None:
            return AssetCheckResult(passed=False, metadata={"reason": "table is empty"})

        lag_minutes = (datetime.now(timezone.utc) - latest).total_seconds() / 60
        return AssetCheckResult(
            passed=lag_minutes <= contract.freshness_minutes,
            severity=AssetCheckSeverity.WARN if contract.is_streaming else AssetCheckSeverity.ERROR,
            metadata={
                "lag_minutes": round(lag_minutes, 1),
                "target_minutes": contract.freshness_minutes,
                "latest_ingested_at": latest.isoformat(),
            },
        )

    return _check


freshness_checks = [freshness_check(c) for c in CONTRACTS.values()]
