# grainflow

A streaming lakehouse for grain markets. It pulls CBOT futures, corn belt
weather and USDA crop condition into Apache Iceberg. It enforces one set of
data contracts from the producer to the gold tables. The output is a weekly
mart that asks a real question: **does a drop in crop ratings show up in next
week's corn price?**

Everything runs on a laptop with `docker compose`, using public data and no
paid services.

```mermaid
flowchart LR
    subgraph sources[Public sources]
        Y[Yahoo Finance<br/>CBOT 1m bars]
        N[NOAA / NWS<br/>14 ASOS stations]
        U[USDA NASS<br/>crop condition]
    end

    subgraph ingest[Ingestion]
        P[producers<br/>validate vs contract]
        DLQ[(topic.dlq)]
        K[(Redpanda<br/>Kafka API)]
        B[batch loader<br/>pyiceberg]
    end

    subgraph lake[Iceberg on S3 / SeaweedFS, JDBC catalog in Postgres]
        BR[bronze]
        Q[bronze.quarantine]
        SI[silver]
        GO[gold]
    end

    C{{contracts/*.yaml}}

    Y --> P
    N --> P
    P -- valid --> K
    P -- invalid --> DLQ
    K --> S[Spark Structured<br/>Streaming] --> BR
    S -- fails contract --> Q
    U --> B --> BR
    B -- fails contract --> Q
    BR --> D[dbt on Trino] --> SI --> GO
    DG[Dagster] -. schedules, freshness,<br/>compaction .-> D

    C -.-> P
    C -.-> S
    C -.-> B
    C -.-> D
```

## What's in here

| Layer | Tech | What it does |
| --- | --- | --- |
| Contracts | YAML + JSON Schema | One file per dataset: fields, types, ranges, allowed values, topic, table, partitioning, freshness SLA |
| Ingestion | Python, confluent-kafka | Live 1m futures polling (or sped-up replay), NWS polling, NASS batch loads |
| Streaming | Spark Structured Streaming | Kafka → Iceberg bronze, re-validates every row, routes failures to quarantine |
| Storage | Iceberg v2, SeaweedFS (S3 API), Postgres | Day-partitioned bronze tables; one catalog shared by Spark, Trino and pyiceberg |
| Transform | dbt on Trino | Incremental dedupe in silver; gold facts with enforced model contracts |
| Orchestration | Dagster | Asset lineage from Kafka to gold, contract-driven freshness checks, nightly compaction |
| CI | GitHub Actions | Lint, 44 unit tests incl. PySpark, 60 dbt nodes on DuckDB, contract compatibility gate |

## Quickstart

You need Docker with ~6 GB of memory, plus [uv](https://docs.astral.sh/uv/) for local development.

```bash
cp .env.example .env        # add a free NASS key + your email for the NWS user agent
make up                     # builds images, creates tables + topics, starts everything
make backfill               # 10y of daily prices, 2 days of weather, crop condition
make transform              # dbt build now instead of waiting for the 15-min schedule
make query                  # weekly corn belt mart via Trino
```

| UI | URL |
| --- | --- |
| Dagster (lineage, runs, checks) | http://localhost:3000 |
| Redpanda Console (topics, DLQ, schemas) | http://localhost:8088 |
| Trino | http://localhost:8080 |
| Spark UI (streaming queries) | http://localhost:4040 |
| SeaweedFS filer (Iceberg files under /buckets/warehouse) | http://localhost:8888 |

If CBOT is closed (weekends, or the daily break), stream the last few days at 60x speed instead:

```bash
docker compose run --rm cli grainflow produce prices --mode replay --speed 60
```

Without Docker, everything except the live stack runs locally:

```bash
make setup
make check                  # lint, contracts, unit tests, dbt build on DuckDB fixtures
```

## Results from a real run

First full run on a laptop (Docker on WSL2, 7 GB), October 2026, live public data.

| Layer | What landed |
| --- | --- |
| Bronze | Streamed 1-minute CBOT bars, 9,049 daily bars (10 years), 800+ NWS observations from 14 stations, 68,730 USDA crop condition rows (corn + soybeans since 2018) |
| Quarantine | 0 rows: every record passed its contract |
| Silver | `stg_price_bars` keeps one row per bar no matter how often it was replayed. Producer restarts and a re-run backfill sent duplicates, which is the at-least-once case ADR 0003 plans for |
| Gold | dbt on Trino: 56 models and tests, 0 failures. `mart_corn_belt_weekly` holds ~520 weeks |

### Does a drop in crop ratings move next week's corn price?

257 growing-season weeks, 2018–2026, rating change averaged over the 18 Crop
Progress states:

| | Weeks | Avg. next-week corn return |
| --- | --- | --- |
| Good/excellent fell 1.5+ points | 56 | **+0.93%** |
| Good/excellent rose 1.5+ points | 44 | **−0.79%** |
| Correlation (rating change vs next-week return) | 257 | **−0.16** |

The sign matches the economics (worse crop, tighter supply, higher price), and
with nine seasons it is statistically significant: t ≈ −2.6, p ≈ 0.01. On the
first run, with two seasons, the same correlation was −0.20 and not significant
(p ≈ 0.16). More data turned a hint into evidence.

What this does **not** show:

- **It explains very little.** r² ≈ 0.03, so rating changes account for about 3% of
  weekly price variance. Weather forecasts, exports and the dollar drive the rest.
- **It is not a tradable signal as measured.** NASS releases ratings on Monday at
  4pm ET, after CBOT closes, but "next week" here runs from Friday's close to the
  following Friday. The Friday-to-Monday part happens before the report is public.
  A clean test measures from Monday's close after the release. That is the next
  model change.

One modeling fix came out of checking the data. NASS rates corn in 41 states,
including CT, RI and VT. An equal-weight average let those states dilute Iowa
and Illinois, so the mart now uses only the 18 states in USDA's national Crop
Progress report (`ref_crop_progress_states`), and a test caps the count at 18.

The 10-year backfill also found a real streaming bug: unsorted micro-batches
spanning ~2,500 daily partitions made Iceberg's fanout writer hold thousands of
open Parquet files, and Spark ran out of heap in a restart loop. Sorting within
partitions switched Iceberg to its clustered writer, and memory stayed flat.

## Design decisions

**Contracts are the single source of truth.** `contracts/price_bar.yaml` drives
producer validation, the JSON Schema in the registry, Spark's read schema and
checks, the Iceberg table definition, dbt source tests and docs
(`_sources.yml` is generated, and CI fails if it's stale), and the Dagster
freshness checks. A pull request that removes a field, narrows an enum or
makes a field required fails CI unless the contract version is bumped.
→ [ADR 0002](docs/adr/0002-contracts-as-code.md)

**At-least-once into bronze, dedupe in silver.** `foreachBatch` can replay a
micro-batch after a crash. Exactly-once sinks would add a lot of complexity.
Instead, `stg_price_bars` is an incremental model that keeps the newest copy
per natural key, with a one-hour lookback for late duplicates. The fixtures
include a replayed bar so CI proves it.
→ [ADR 0003](docs/adr/0003-at-least-once-delivery.md)

**Bad data is quarantined, never dropped.** There are three gates. Producers
send invalid records to `<topic>.dlq` with the errors in a header. Spark and
the batch loader re-check rows and write failures to `bronze.quarantine` with
the raw payload. `gold.rpt_quarantine_daily` turns that into a trend.

**Iceberg with a JDBC catalog.** Spark, Trino and pyiceberg all point at the
same Postgres tables, with no Hive metastore and no extra service. In AWS this
becomes Glue or a REST catalog (Polaris), and no model code changes.
→ [ADR 0001](docs/adr/0001-iceberg-jdbc-catalog.md)

**Domain logic lives in SQL where analysts can read it.** Corn growing degree
days use the 86/50 method (a dbt macro). Precipitation is deduplicated per
station-hour, because ASOS "special" reports would otherwise double-count rain.
Station-days with fewer than 18 hours of readings are excluded from state
averages.

## Data model

```
bronze  price_bars, weather_observations, crop_condition, quarantine      (Iceberg, contract-shaped)
silver  stg_price_bars (incremental), stg_weather_observations,
        stg_crop_condition, int_weather_station_daily, ref_symbols
gold    fct_price_daily            returns, 20d realized vol
        fct_weather_state_daily    tmin/tmax, GDD, precip per state
        fct_crop_condition_weekly  rating mix, condition index, week-over-week change
        mart_corn_belt_weekly      price + GDD + ratings + next week's return
        rpt_quarantine_daily       rejected records by contract and reason
```

Gold models use dbt's enforced contracts, so a type change fails the build
instead of reaching a dashboard or the agent in project 2.

## Repo layout

```
contracts/            one YAML per dataset
src/grainflow/        contracts, publisher, sources, lake (pyiceberg), CLI, dbt source generator
  orchestration/      Dagster assets, checks, jobs, schedules
spark/                streaming job + image
transform/            dbt project (Trino in the stack, DuckDB in CI)
infra/                Trino catalog, Postgres init, Dagster instance config
scripts/              synthetic fixture generator for CI
tests/                unit tests: contracts, producers, sources, lake, Spark logic, Dagster graph
docs/adr/             decision records
```

## How this maps to production

| Local | AWS equivalent |
| --- | --- |
| SeaweedFS | S3 |
| Iceberg JDBC catalog (Postgres) | Glue Data Catalog or Apache Polaris; Lake Formation for access |
| Redpanda | MSK, or Confluent Cloud with Schema Registry |
| Spark `local[2]` container | EMR Serverless or Databricks job cluster |
| Trino | Athena, or Starburst for the same SQL |
| Dagster OSS | Dagster+ or ECS tasks |
| `docker compose` | Terraform (on the roadmap) |

## Limits

- Yahoo Finance quotes are delayed and unofficial. They're fine for a demo,
  but not for trading.
- NASS crop condition is only published during the growing season
  (roughly April to November).
- The CI fixtures are synthetic: random walks with a fixed seed. They test
  logic, not insight.

## Roadmap

- WASDE monthly PDFs → LLM extraction to contract-validated tables, plus
  embeddings with per-chunk versioning, so only changed text is re-embedded
- Terraform for the AWS layout above
- Project 2: an MCP server so an agent can query the gold layer through
  governed metrics instead of raw SQL
