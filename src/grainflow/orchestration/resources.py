from typing import Any

from dagster import ConfigurableResource


class TrinoResource(ConfigurableResource):
    """Thin Trino client for checks and table maintenance."""

    host: str = "localhost"
    port: int = 8080
    user: str = "dagster"
    catalog: str = "lake"

    def execute(self, sql: str) -> list[tuple[Any, ...]]:
        import trino

        connection = trino.dbapi.connect(
            host=self.host, port=self.port, user=self.user, catalog=self.catalog
        )
        try:
            cursor = connection.cursor()
            cursor.execute(sql)
            return cursor.fetchall()
        finally:
            connection.close()

    def scalar(self, sql: str) -> Any:
        rows = self.execute(sql)
        return rows[0][0] if rows else None
