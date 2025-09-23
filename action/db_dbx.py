"""Databricks tokenization support (optional)."""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence

try:
    from databricks import sql as dbsql
except ImportError:  # pragma: no cover - the connector is optional
    dbsql = None

from .token_logic import tokenize_row
from .types import TokenizationResult

LOGGER = logging.getLogger(__name__)


def _quote_identifier(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"


@dataclass
class DatabricksConfig:
    jdbc_url: Optional[str] = None
    server_hostname: Optional[str] = None
    http_path: Optional[str] = None
    access_token: Optional[str] = None
    catalog: Optional[str] = None
    token: Optional[str] = None
    timeout: int = 30


class DatabricksTokenizer:
    """Tokenize columns in Databricks tables when credentials are available."""

    def __init__(self, config: DatabricksConfig, *, pk_column: str = "id", limit: int = 1000) -> None:
        self.config = config
        self.pk_column = pk_column
        self.limit = limit
        self.enabled = bool(dbsql and config and (config.server_hostname and config.http_path and config.access_token))
        if not self.enabled:
            LOGGER.info("Databricks tokenizer disabled: missing configuration or connector")

    @classmethod
    def from_env(cls) -> "DatabricksTokenizer":
        jdbc_url = os.getenv("DBX_JDBC_URL")
        server_hostname = os.getenv("DBX_HOST")
        http_path = os.getenv("DBX_HTTP_PATH")
        access_token = os.getenv("DBX_TOKEN")
        catalog = os.getenv("DBX_CATALOG")
        pk_column = os.getenv("DBX_PK_COLUMN", "id")
        limit = int(os.getenv("DBX_TOKENIZE_LIMIT", "1000"))

        if jdbc_url:
            parsed = cls._parse_jdbc_url(jdbc_url)
            server_hostname = server_hostname or parsed.get("server_hostname")
            http_path = http_path or parsed.get("http_path")
            access_token = access_token or parsed.get("access_token")

        config = DatabricksConfig(
            jdbc_url=jdbc_url,
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=access_token,
            catalog=catalog,
        )
        return cls(config, pk_column=pk_column, limit=limit)

    @staticmethod
    def _parse_jdbc_url(jdbc_url: str) -> Dict[str, str]:
        result: Dict[str, str] = {}
        if not jdbc_url.startswith("jdbc:databricks://"):
            return result
        body = jdbc_url[len("jdbc:databricks://") :]
        host_part, _, params_part = body.partition("/")
        host, _, port = host_part.partition(":")
        result["server_hostname"] = host
        if port:
            result["port"] = port
        for segment in params_part.split(";"):
            if not segment or "=" not in segment:
                continue
            key, value = segment.split("=", 1)
            key = key.strip()
            value = value.strip()
            if key.lower() == "httppath":
                result["http_path"] = value
            elif key.lower() == "pwd":
                result["access_token"] = value
        return result

    def tokenize(
        self,
        *,
        catalog: Optional[str],
        schema: str,
        table: str,
        columns: Sequence[str],
    ) -> TokenizationResult:
        dataset_name = ".".join(part for part in [catalog, schema, table] if part)
        if not columns:
            return TokenizationResult(
                dataset=dataset_name,
                platform="databricks",
                columns=list(columns),
                rows_scanned=0,
                rows_updated=0,
                details="No columns requested",
            )
        if not self.enabled:
            return TokenizationResult(
                dataset=dataset_name,
                platform="databricks",
                columns=list(columns),
                rows_scanned=0,
                rows_updated=0,
                details="Databricks connection not configured",
            )

        quoted_table = self._qualified_table(catalog or self.config.catalog, schema, table)
        LOGGER.info("Starting Databricks tokenization for %s", quoted_table)

        with dbsql.connect(
            server_hostname=self.config.server_hostname,
            http_path=self.config.http_path,
            access_token=self.config.access_token,
            timeout=self.config.timeout,
        ) as connection:
            with connection.cursor() as cursor:
                select_sql, params = self._build_select_sql(quoted_table, columns)
                cursor.execute(select_sql, params)
                rows = cursor.fetchall()
                column_names = [desc[0] for desc in cursor.description]
                rows_scanned = len(rows)
                rows_updated = 0

                for row in rows:
                    row_dict = dict(zip(column_names, row))
                    pk_value = row_dict[self.pk_column]
                    updates = tokenize_row(row_dict, columns)
                    if not updates or all(row_dict[col] == updates[col] for col in updates):
                        continue
                    update_sql = self._build_update_sql(quoted_table, updates.keys())
                    cursor.execute(update_sql, list(updates.values()) + [pk_value])
                    rows_updated += 1

            connection.commit()

        return TokenizationResult(
            dataset=dataset_name,
            platform="databricks",
            columns=list(columns),
            rows_scanned=rows_scanned,
            rows_updated=rows_updated,
        )

    def _build_select_sql(self, table: str, columns: Sequence[str]) -> tuple[str, List[object]]:
        select_cols = [_quote_identifier(self.pk_column)] + [
            _quote_identifier(col) for col in columns if col != self.pk_column
        ]
        where_conditions = [
            f"({_quote_identifier(col)} IS NOT NULL AND {_quote_identifier(col)} NOT LIKE ?)"
            for col in columns
        ]
        sql_query = (
            f"SELECT {', '.join(select_cols)} FROM {table} "
            f"WHERE {' OR '.join(where_conditions)} ORDER BY {_quote_identifier(self.pk_column)} LIMIT {self.limit}"
        )
        params: List[object] = ["tok_%_poc" for _ in columns]
        return sql_query, params

    def _build_update_sql(self, table: str, columns: Iterable[str]) -> str:
        assignments = [f"{_quote_identifier(col)} = ?" for col in columns]
        sql_query = (
            f"UPDATE {table} SET {', '.join(assignments)} "
            f"WHERE {_quote_identifier(self.pk_column)} = ?"
        )
        return sql_query

    @staticmethod
    def _qualified_table(catalog: Optional[str], schema: str, table: str) -> str:
        parts = [part for part in [catalog, schema, table] if part]
        return ".".join(_quote_identifier(part) for part in parts)
