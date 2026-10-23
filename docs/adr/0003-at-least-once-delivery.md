# 0003: At-least-once into bronze, dedupe in silver

## Context

Spark's `foreachBatch` gives at-least-once delivery. If the query dies after
committing to Iceberg but before committing its offsets, the batch is
processed again on restart. Producers can also resend data: a replay, or a
poller restart that forgets what it already sent.

Getting exactly-once into Iceberg would mean tracking batch IDs in table
properties or using idempotent MERGE writes for every batch. That adds
complexity and write cost on the hot path.

## Decision

Bronze accepts duplicates. Every bronze row carries `_kafka_partition`,
`_kafka_offset` and `_ingested_at`. Silver models keep one row per natural key,
choosing the newest by `_ingested_at` and then by offset:

- `stg_price_bars` uses `(symbol, bar_interval, bar_start)`. It is incremental
  (`delete+insert`) with a one-hour lookback, so late duplicates collapse
  without a full rebuild.
- `stg_weather_observations` uses `(station_id, observed_at)`.
- `stg_crop_condition` uses `(commodity, state, week, class)`. The latest NASS
  revision wins.

## Consequences

- The streaming path stays simple: append and move on.
- Bronze row counts aren't business counts. Anything reporting volumes reads
  silver.
- `unique_combination` tests on every silver model prove the dedupe works. The
  CI fixtures deliberately include a replayed bar.
