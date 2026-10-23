"""Runtime configuration, read from GRAINFLOW_* environment variables."""

from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="GRAINFLOW_", env_file=".env", extra="ignore")

    kafka_bootstrap: str = "localhost:19092"
    schema_registry_url: str = "http://localhost:18081"

    # Iceberg JDBC catalog. The same Postgres tables are read by Spark and Trino.
    catalog_name: str = "lake"
    catalog_uri: str = "postgresql+psycopg2://grainflow:grainflow@localhost:5432/iceberg"
    warehouse: str = "s3://warehouse/"
    s3_endpoint: str | None = "http://localhost:9000"
    s3_access_key: str = "grainflow"
    s3_secret_key: str = "grainflow-secret"
    s3_region: str = "us-east-1"

    trino_host: str = "localhost"
    trino_port: int = 8080
    trino_user: str = "grainflow"

    nass_api_key: str | None = None
    noaa_user_agent: str = "grainflow (github.com/Myselfgitcone/grainflow)"


@lru_cache
def get_settings() -> Settings:
    return Settings()
