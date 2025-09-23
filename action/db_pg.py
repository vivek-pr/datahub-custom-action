"""PostgreSQL tokenization routines."""
from __future__ import annotations

import logging
import os
from typing import Iterable, List, Sequence

import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor

from .types import TokenizationResult
from .token_logic import tokenize_row

LOGGER = logging.getLogger(__name__)


class PostgresTokenizer:
    """Tokenize PII columns inside a PostgreSQL table."""

    def __init__(self, conn_str: str, *, pk_column: str = "id", limit: int = 1000) -> None:
        self.conn_str = conn_str
        self.pk_column = pk_column
        self.limit = limit

    @classmethod
    def from_env(cls) -> "PostgresTokenizer":
        conn_str = os.getenv("PG_CONN_STR")
        if not conn_str:
            raise RuntimeError("PG_CONN_STR environment variable is required for Postgres tokenization")
        pk_column = os.getenv("PG_PK_COLUMN", "id")
        limit = int(os.getenv("PG_TOKENIZE_LIMIT", "1000"))
        return cls(conn_str, pk_column=pk_column, limit=limit)

    def tokenize(
        self,
        *,
        database: str,
        schema: str,
        table: str,
        columns: Sequence[str],
    ) -> TokenizationResult:
        if not columns:
            LOGGER.info("No columns to tokenize for %s.%s.%s", database, schema, table)
            return TokenizationResult(
                dataset=f"{database}.{schema}.{table}",
                platform="postgres",
                columns=list(columns),
                rows_scanned=0,
                rows_updated=0,
            )

        LOGGER.info(
            "Starting tokenization for %s.%s.%s (columns=%s)",
            database,
            schema,
            table,
            ",".join(columns),
        )
        rows_scanned = 0
        rows_updated = 0

        with psycopg2.connect(self.conn_str) as conn:
            conn.autocommit = False
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                select_query, params = self._build_select_query(schema, table, columns)
                LOGGER.debug("Executing select query: %s", select_query.as_string(cur))
                cur.execute(select_query, params)
                rows = cur.fetchall()
                rows_scanned = len(rows)

                for row in rows:
                    pk_value = row[self.pk_column]
                    updates = tokenize_row(row, columns)
                    if not updates or all(row[col] == updates[col] for col in updates):
                        continue
                    update_query = self._build_update_query(schema, table, updates.keys())
                    update_params = list(updates.values()) + [pk_value]
                    LOGGER.debug("Updating row %s: %s", pk_value, updates)
                    cur.execute(update_query, update_params)
                    rows_updated += 1

            conn.commit()

        return TokenizationResult(
            dataset=f"{database}.{schema}.{table}",
            platform="postgres",
            columns=list(columns),
            rows_scanned=rows_scanned,
            rows_updated=rows_updated,
        )

    def _build_select_query(
        self,
        schema: str,
        table: str,
        columns: Sequence[str],
    ) -> tuple[sql.SQL, List[object]]:
        select_columns = [self.pk_column] + [col for col in columns if col != self.pk_column]
        select_list = sql.SQL(", ").join(sql.Identifier(col) for col in select_columns)
        conditions = [
            sql.SQL("({col} IS NOT NULL AND {col} NOT LIKE %s)").format(col=sql.Identifier(col))
            for col in columns
        ]
        where_clause = sql.SQL(" OR ").join(conditions)
        query = sql.SQL(
            "SELECT {columns} FROM {table} WHERE {where_clause} ORDER BY {pk} LIMIT %s FOR UPDATE"
        ).format(
            columns=select_list,
            table=self._qualified_table(schema, table),
            where_clause=where_clause,
            pk=sql.Identifier(self.pk_column),
        )
        params: List[object] = ["tok_%_poc" for _ in columns]
        params.append(self.limit)
        return query, params

    def _build_update_query(self, schema: str, table: str, columns: Iterable[str]):
        assignments = [
            sql.SQL("{col} = %s").format(col=sql.Identifier(col))
            for col in columns
        ]
        query = sql.SQL("UPDATE {table} SET {assignments} WHERE {pk} = %s").format(
            table=self._qualified_table(schema, table),
            assignments=sql.SQL(", ").join(assignments),
            pk=sql.Identifier(self.pk_column),
        )
        return query

    @staticmethod
    def _qualified_table(schema: str, table: str):
        if schema:
            return sql.SQL("{}.{}").format(sql.Identifier(schema), sql.Identifier(table))
        return sql.Identifier(table)
