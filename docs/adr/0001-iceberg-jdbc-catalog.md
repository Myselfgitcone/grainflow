# 0001: Iceberg with a JDBC catalog in Postgres

## Context

Three engines write to or read from the lake. Spark appends streaming data,
pyiceberg creates tables and loads batch data, and Trino serves dbt and
queries. They need one shared catalog. The options were:

- **Hive metastore:** works everywhere, but it's another JVM service with its
  own database, and it's legacy for Iceberg.
- **REST catalog** (Polaris, Lakekeeper, the Iceberg fixture image): this is
  the modern standard. Locally, though, it's one more service to bootstrap,
  and the fixture image keeps its state in memory unless you configure it
  otherwise.
- **JDBC catalog:** the catalog is two tables in a database we already run
  for Dagster. Spark, Trino and pyiceberg all support it natively.

## Decision

Use the Iceberg JDBC catalog in Postgres, named `lake` everywhere. Table
creation happens only in `grainflow lake init` (pyiceberg), driven by the
contracts. Spark only appends.

## Consequences

- One fewer service, and catalog state survives restarts with the Postgres volume.
- Creating tables in one place means partitioning and schema can't drift
  between engines.
- No catalog-level access control. In AWS this moves to Glue with Lake
  Formation, or to Polaris. Only connection config changes; the models stay
  the same.
