"""Command line entry point: `grainflow --help`."""

from __future__ import annotations

import json
import logging
import subprocess
import time
from pathlib import Path

import httpx
import typer

from grainflow import contracts as contract_lib
from grainflow.settings import get_settings

app = typer.Typer(no_args_is_help=True, add_completion=False)
contracts_app = typer.Typer(no_args_is_help=True, help="Inspect and publish data contracts.")
produce_app = typer.Typer(no_args_is_help=True, help="Stream records into Kafka.")
load_app = typer.Typer(no_args_is_help=True, help="Batch-load records into the lake.")
lake_app = typer.Typer(no_args_is_help=True, help="Manage Iceberg tables.")
app.add_typer(contracts_app, name="contracts")
app.add_typer(produce_app, name="produce")
app.add_typer(load_app, name="load")
app.add_typer(lake_app, name="lake")

log = logging.getLogger("grainflow")


@app.callback()
def main(verbose: bool = typer.Option(False, "--verbose", "-v")) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s  %(message)s",
    )
    # httpx logs every request URL at INFO, and the NASS API takes its key as a
    # query parameter. Keep request logs off so the key never reaches a log.
    logging.getLogger("httpx").setLevel(logging.WARNING)


# --- contracts ---------------------------------------------------------------


@contracts_app.command("check")
def contracts_check() -> None:
    """Parse every contract and fail on the first problem."""
    for name, contract in contract_lib.load_contracts().items():
        where = contract.topic if contract.is_streaming else "batch"
        typer.echo(f"ok  {name} v{contract.version}  ({where} -> {contract.table})")


@contracts_app.command("schema")
def contracts_schema(name: str) -> None:
    """Print the JSON Schema generated for one contract."""
    contract = contract_lib.load_contracts()[name]
    typer.echo(json.dumps(contract.json_schema, indent=2))


@contracts_app.command("register")
def contracts_register() -> None:
    """Register each streaming contract's JSON Schema in the schema registry."""
    registry = get_settings().schema_registry_url.rstrip("/")
    for contract in contract_lib.load_contracts().values():
        if not contract.is_streaming:
            continue
        subject = f"{contract.topic}-value"
        response = httpx.post(
            f"{registry}/subjects/{subject}/versions",
            json={"schema": json.dumps(contract.json_schema), "schemaType": "JSON"},
            timeout=10,
        )
        response.raise_for_status()
        typer.echo(f"registered {subject} (id {response.json()['id']})")


@contracts_app.command("dbt-sources")
def contracts_dbt_sources(
    check: bool = typer.Option(False, "--check", help="Fail if the file is out of date."),
) -> None:
    """Write transform/models/staging/_sources.yml from the contracts."""
    from grainflow.dbt_sources import SOURCES_FILE, render_sources

    rendered = render_sources(contract_lib.load_contracts().values())
    current = SOURCES_FILE.read_text() if SOURCES_FILE.exists() else ""

    if check:
        if rendered != current:
            typer.secho("dbt sources are stale: run `grainflow contracts dbt-sources`", fg="red")
            raise typer.Exit(code=1)
        typer.echo("dbt sources match the contracts")
        return

    SOURCES_FILE.write_text(rendered)
    typer.echo(f"wrote {SOURCES_FILE.relative_to(Path.cwd())}")


@contracts_app.command("compat")
def contracts_compat(
    against: str = typer.Option("origin/main", help="Git ref to compare with."),
) -> None:
    """Fail if a contract has a breaking change without a version bump."""
    failures = 0
    for path in sorted(contract_lib.default_contracts_dir().glob("*.yaml")):
        new = contract_lib.load_contract(path)
        old_text = _git_show(against, path)
        if old_text is None:
            typer.echo(f"new  {new.name}")
            continue

        old = contract_lib.parse_contract(old_text, source=f"{against}:{path.name}")
        problems = contract_lib.breaking_changes(old, new)
        if problems and new.version <= old.version:
            failures += 1
            typer.secho(f"FAIL {new.name}: breaking change without a version bump", fg="red")
            for problem in problems:
                typer.echo(f"     - {problem}")
        else:
            typer.echo(f"ok   {new.name}")

    raise typer.Exit(code=1 if failures else 0)


def _git_show(ref: str, path: Path) -> str | None:
    relative = path.resolve().relative_to(Path.cwd().resolve())
    result = subprocess.run(
        ["git", "show", f"{ref}:{relative.as_posix()}"], capture_output=True, text=True
    )
    return result.stdout if result.returncode == 0 else None


# --- lake --------------------------------------------------------------------


@lake_app.command("init")
def lake_init() -> None:
    """Create namespaces, bronze tables and Kafka topics. Safe to rerun."""
    from grainflow import lake
    from grainflow.publisher import ensure_topics

    settings = get_settings()
    all_contracts = contract_lib.load_contracts().values()
    lake.ensure_tables(lake.catalog(settings), all_contracts)
    ensure_topics(settings.kafka_bootstrap, all_contracts)
    typer.echo("lake and topics ready")


# --- produce -----------------------------------------------------------------


def _publisher(contract_name: str):
    from grainflow.publisher import Publisher, kafka_producer

    contract = contract_lib.load_contracts()[contract_name]
    return Publisher(contract, kafka_producer(get_settings().kafka_bootstrap))


@produce_app.command("prices")
def produce_prices(
    mode: str = typer.Option(
        "live", help="live: poll Yahoo for new 1m bars. replay: play history back."
    ),
    symbols: str = typer.Option("ZC=F,ZS=F,ZW=F", help="Comma-separated tickers."),
    every: int = typer.Option(60, help="Seconds between live polls."),
    speed: float = typer.Option(60.0, help="Replay speed-up factor."),
) -> None:
    """Stream 1-minute futures bars."""
    from grainflow.sources import prices

    tickers = [s.strip() for s in symbols.split(",") if s.strip()]
    publisher = _publisher("price_bar")

    if mode == "replay":
        history = [bar for t in tickers for bar in prices.fetch_bars(t, interval="1m", period="5d")]
        log.info("replaying %d bars at %sx", len(history), speed)
        counts = publisher.publish_all(prices.replay(history, speed=speed))
        typer.echo(dict(counts))
        return

    poller = prices.LivePoller(tickers)
    while True:
        bars = poller.poll()
        publisher.publish_all(bars)
        log.info("sent %d new bars (%s)", len(bars), dict(publisher.counts))
        time.sleep(every)


@produce_app.command("price-history")
def produce_price_history(
    symbols: str = typer.Option("ZC=F,ZS=F,ZW=F"),
    period: str = typer.Option("2y", help="yfinance period, e.g. 6mo, 2y, max."),
) -> None:
    """Send daily bars once, to backfill the daily price marts."""
    from grainflow.sources import prices

    publisher = _publisher("price_bar")
    for ticker in (s.strip() for s in symbols.split(",") if s.strip()):
        publisher.publish_all(prices.fetch_bars(ticker, interval="1d", period=period))
    typer.echo(dict(publisher.counts))


@produce_app.command("weather")
def produce_weather(
    every: int = typer.Option(600, help="Seconds between polls."),
    once: bool = typer.Option(False, help="Poll once and exit (backfills ~2 days)."),
) -> None:
    """Poll NOAA stations across the corn belt."""
    from grainflow.sources.weather import CORN_BELT_STATIONS, SeenObservations, WeatherClient

    settings = get_settings()
    client = WeatherClient(settings.noaa_user_agent)
    publisher = _publisher("weather_observation")
    seen = SeenObservations()
    limit = 48 if once else 6

    while True:
        for station in CORN_BELT_STATIONS:
            try:
                observations = client.recent_observations(station, limit=limit)
            except httpx.HTTPError as error:
                log.warning("skipping %s this round: %s", station, error)
                continue
            publisher.publish_all(r for r in observations if seen.add(r))
        log.info("weather round done (%s)", dict(publisher.counts))
        if once:
            return
        time.sleep(every)


# --- load --------------------------------------------------------------------


@load_app.command("crops")
def load_crops(
    commodity: str = typer.Option("CORN"),
    since: int = typer.Option(2018, help="First crop year to load."),
) -> None:
    """Load USDA NASS crop condition for one commodity (replaces what's there)."""
    from grainflow import lake
    from grainflow.sources.crops import fetch_condition

    settings = get_settings()
    if not settings.nass_api_key:
        raise typer.BadParameter("set GRAINFLOW_NASS_API_KEY first")

    contract = contract_lib.load_contracts()["crop_condition"]
    records = fetch_condition(settings.nass_api_key, commodity, since)
    result = lake.load_batch(
        lake.catalog(settings), contract, records, replace_where=("commodity", commodity)
    )
    typer.echo(f"{commodity}: wrote {result.written}, quarantined {result.rejected}")


# --- publish -----------------------------------------------------------------

publish_app = typer.Typer(no_args_is_help=True, help="Build the public dashboard data.")
app.add_typer(publish_app, name="publish")

PUBLISH_DB = Path("transform/grainflow_publish.duckdb")


@publish_app.command("load")
def publish_load(
    db: Path = typer.Option(PUBLISH_DB, help="DuckDB file the dbt publish target reads."),
    period: str = typer.Option("10y", help="How much daily price history to load."),
) -> None:
    """Fetch every source into DuckDB tables shaped like bronze."""
    from grainflow.publish import load_sources

    db.unlink(missing_ok=True)
    stats = load_sources(db, get_settings(), price_period=period)
    typer.echo(json.dumps(stats, indent=2))


@publish_app.command("export")
def publish_export(
    out: Path = typer.Option(Path("site/dist"), help="Folder the static site is served from."),
    db: Path = typer.Option(PUBLISH_DB),
    run_results: Path = typer.Option(Path("transform/target-publish/run_results.json")),
    video_url: str = typer.Option("", envvar="GRAINFLOW_VIDEO_URL"),
    portfolio_url: str = typer.Option(
        "", envvar="GRAINFLOW_PORTFOLIO_URL", help="Adds an 'All projects' link when set."
    ),
) -> None:
    """Write the gold results and run stats as JSON for the dashboard."""
    from grainflow.publish import export_site_data

    summary = export_site_data(db, out, run_results, video_url, portfolio_url)
    signal = summary["signal"]
    if not signal["weeks"]:
        # Failing the build keeps the last good deploy live instead of an empty page.
        typer.echo("no weeks with both ratings and returns; refusing to publish", err=True)
        raise typer.Exit(1)
    typer.echo(
        f"exported {signal['weeks']} weeks, r={signal['correlation']:.3f}, "
        f"data through {summary['freshness']['latest_price_date']}"
    )


if __name__ == "__main__":
    app()
