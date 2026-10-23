# 0002: Data contracts as code, one file per dataset

## Context

The same facts about a dataset (its fields, types, allowed values, owner and
freshness target) are usually restated in producer code, Spark schemas, DDL,
dbt YAML and monitoring config. Those copies drift, and drift shows up as a
2am incident.

## Decision

`contracts/<dataset>.yaml` is the only place those facts live. Everything else
is derived from it:

| Consumer | Derived artifact |
| --- | --- |
| Producers | JSON Schema validation; failures go to `<topic>.dlq` |
| Schema registry | JSON Schema registered per topic (`grainflow contracts register`) |
| Spark | Read schema plus per-row checks; failures go to `bronze.quarantine` |
| Iceberg | Table schema and partition spec (`grainflow lake init`) |
| dbt | `_sources.yml`: column docs, not_null / accepted_values tests, freshness |
| Dagster | One freshness asset check per dataset |

Compatibility rules are checked in CI (`grainflow contracts compat`). Adding
an optional field or widening int → long is fine. Removing or retyping a
field, making a field required, or narrowing an enum is breaking, and needs a
version bump (and a new `.vN` topic).

## Consequences

- A contract change is one reviewed diff, and CI shows its blast radius.
- The format is deliberately smaller than ODCS. It can be mapped to ODCS later
  if a catalog tool needs it.
- The Spark job imports the contracts module, so that module must stay free of
  heavy imports. jsonschema is loaded lazily.
