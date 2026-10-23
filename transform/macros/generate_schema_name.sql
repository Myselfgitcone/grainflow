{#
  Use the configured schema as-is (bronze / silver / gold) instead of dbt's
  default "<target_schema>_<custom_schema>". The medallion layers are real
  Iceberg namespaces shared with Spark and Trino, so their names must be stable.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ (custom_schema_name or target.schema) | trim }}
{%- endmacro %}
