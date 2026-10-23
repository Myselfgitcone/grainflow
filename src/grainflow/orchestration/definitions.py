"""Dagster entry point: `dagster dev -m grainflow.orchestration.definitions`."""

from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    Definitions,
    OpExecutionContext,
    ScheduleDefinition,
    define_asset_job,
    job,
    op,
)
from dagster_dbt import DbtCliResource

from grainflow.orchestration.assets import (
    CONTRACTS,
    crop_condition,
    dbt_project,
    freshness_checks,
    grainflow_dbt,
    streaming_bronze,
)
from grainflow.orchestration.resources import TrinoResource
from grainflow.settings import get_settings

transform_job = define_asset_job(
    "transform",
    selection=AssetSelection.assets(grainflow_dbt),
    description="dbt build: silver and gold models plus their tests.",
)

crops_job = define_asset_job(
    "load_crop_condition",
    selection=AssetSelection.assets(crop_condition),
)

freshness_job = define_asset_job(
    "check_freshness",
    selection=AssetSelection.checks(*freshness_checks),
)


@op
def compact_streaming_tables(context: OpExecutionContext, trino: TrinoResource) -> None:
    """Streaming writes leave many small files and snapshots behind; fold them up."""
    for contract in CONTRACTS.values():
        if not contract.is_streaming:
            continue
        table = contract.table
        trino.execute(f"ALTER TABLE {table} EXECUTE optimize(file_size_threshold => '64MB')")
        trino.execute(f"ALTER TABLE {table} EXECUTE expire_snapshots(retention_threshold => '7d')")
        trino.execute(
            f"ALTER TABLE {table} EXECUTE remove_orphan_files(retention_threshold => '7d')"
        )
        context.log.info(f"compacted {table}")


@job(description="Nightly Iceberg compaction and snapshot cleanup for streaming tables.")
def lake_maintenance():
    compact_streaming_tables()


settings = get_settings()

defs = Definitions(
    assets=[*streaming_bronze, crop_condition, grainflow_dbt],
    asset_checks=freshness_checks,
    jobs=[transform_job, crops_job, freshness_job, lake_maintenance],
    schedules=[
        ScheduleDefinition(
            job=transform_job,
            cron_schedule="*/15 * * * *",
            default_status=DefaultScheduleStatus.RUNNING,
        ),
        ScheduleDefinition(
            job=freshness_job,
            cron_schedule="*/10 * * * *",
            default_status=DefaultScheduleStatus.RUNNING,
        ),
        # NASS publishes Crop Progress at 4pm ET on Mondays during the season.
        ScheduleDefinition(
            job=crops_job,
            cron_schedule="30 16 * * 1",
            execution_timezone="America/New_York",
            default_status=DefaultScheduleStatus.RUNNING,
        ),
        ScheduleDefinition(
            job=lake_maintenance,
            cron_schedule="15 3 * * *",
            execution_timezone="America/Chicago",
            default_status=DefaultScheduleStatus.RUNNING,
        ),
    ],
    resources={
        "dbt": DbtCliResource(project_dir=dbt_project),
        "trino": TrinoResource(host=settings.trino_host, port=settings.trino_port, user="dagster"),
    },
)
