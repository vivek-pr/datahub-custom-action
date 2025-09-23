"""Coordinate tokenization runs and update DataHub."""
from __future__ import annotations

import json
import logging
import threading
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional, Sequence

from .datahub_client import DataHubClient
from .db_dbx import DatabricksTokenizer
from .db_pg import PostgresTokenizer
from .pii_detector import PIIDetector
from .types import TokenizationResult

LOGGER = logging.getLogger(__name__)

RUN_TAG = "urn:li:tag:tokenize/run"
DONE_TAG = "urn:li:tag:tokenize/done"
STATUS_PREFIX = "urn:li:tag:tokenize/status:"


class RunManager:
    """Entry point shared by MCL consumer and API trigger."""

    def __init__(self) -> None:
        self.client = DataHubClient()
        self.detector = PIIDetector()
        self._lock = threading.Lock()
        try:
            self.pg = PostgresTokenizer.from_env()
        except RuntimeError as exc:  # pragma: no cover - configuration validated in runtime environment
            LOGGER.warning("Postgres tokenizer disabled: %s", exc)
            self.pg = None
        self.dbx = DatabricksTokenizer.from_env()

    def trigger(self, dataset_urn: str, columns: Optional[Sequence[str]] = None) -> Dict[str, object]:
        with self._lock:
            run_id = str(uuid.uuid4())
            started_at = datetime.now(timezone.utc)
            LOGGER.info("Starting tokenization run %s for %s", run_id, dataset_urn)

            dataset = self.client.get_dataset(dataset_urn)
            schema_fields = self.client.extract_schema_fields(dataset)
            selected_columns = self.detector.detect(schema_fields, override_columns=columns)
            platform, dataset_key, _env = _parse_dataset_urn(dataset_urn)

            results: List[TokenizationResult] = []
            status = "SUCCESS"
            error_message: Optional[str] = None

            try:
                if platform == "postgres":
                    if not self.pg:
                        raise RuntimeError("Postgres tokenizer not configured")
                    database, schema, table = _split_dataset_key(dataset_key)
                    result = self.pg.tokenize(
                        database=database,
                        schema=schema,
                        table=table,
                        columns=selected_columns,
                    )
                    results.append(result)
                elif platform == "databricks":
                    database, schema, table = _split_dataset_key(dataset_key)
                    result = self.dbx.tokenize(
                        catalog=database,
                        schema=schema,
                        table=table,
                        columns=selected_columns,
                    )
                    results.append(result)
                else:
                    raise RuntimeError(f"Unsupported platform: {platform}")
            except Exception as exc:  # pragma: no cover - runtime failure surface
                status = "FAILED"
                error_message = str(exc)
                LOGGER.exception("Tokenization run %s failed", run_id)
            finally:
                finished_at = datetime.now(timezone.utc)
                self._finalize(
                    dataset,
                    dataset_urn,
                    run_id,
                    selected_columns,
                    results,
                    status,
                    error_message,
                    started_at,
                    finished_at,
                )

            total_updated = sum(result.rows_updated for result in results)
            total_scanned = sum(result.rows_scanned for result in results)
            duration_s = (finished_at - started_at).total_seconds()

            return {
                "run_id": run_id,
                "dataset": dataset_urn,
                "status": status,
                "error": error_message,
                "columns": selected_columns,
                "results": [result.__dict__ for result in results],
                "started_at": started_at.isoformat(),
                "finished_at": finished_at.isoformat(),
                "duration_seconds": duration_s,
                "rows_scanned": total_scanned,
                "rows_updated": total_updated,
            }

    def _finalize(
        self,
        dataset: dict,
        dataset_urn: str,
        run_id: str,
        columns: Sequence[str],
        results: Sequence[TokenizationResult],
        status: str,
        error_message: Optional[str],
        started_at: datetime,
        finished_at: datetime,
    ) -> None:
        documentation = self._build_documentation(run_id, status, columns, results, started_at, finished_at, error_message)
        custom_properties = self.client.extract_custom_properties(dataset)
        custom_properties["last_tokenization_run"] = json.dumps(
            {
                "run_id": run_id,
                "dataset": dataset_urn,
                "status": status,
                "columns": list(columns),
                "results": [result.__dict__ for result in results],
                "error": error_message,
                "started_at": started_at.isoformat(),
                "finished_at": finished_at.isoformat(),
            },
            indent=2,
        )

        self.client.update_editable_properties(dataset_urn, documentation, custom_properties)
        self._update_tags(dataset, dataset_urn, status)

    def _build_documentation(
        self,
        run_id: str,
        status: str,
        columns: Sequence[str],
        results: Sequence[TokenizationResult],
        started_at: datetime,
        finished_at: datetime,
        error_message: Optional[str],
    ) -> str:
        header = [
            f"## Tokenization run `{run_id}`",
            "",
            f"*Status*: **{status}**",
            f"*Started*: {started_at.isoformat()}",
            f"*Finished*: {finished_at.isoformat()}",
            f"*Columns*: {', '.join(columns) if columns else 'none detected'}",
        ]
        if error_message:
            header.append(f"*Error*: `{error_message}`")
        body: List[str] = header + ["", "### Per-platform summary", ""]
        for result in results:
            body.extend(
                [
                    f"- **{result.platform}** `{result.dataset}`",
                    f"  - Columns: {', '.join(result.columns) if result.columns else 'none'}",
                    f"  - Rows scanned: {result.rows_scanned}",
                    f"  - Rows updated: {result.rows_updated}",
                ]
            )
            if result.details:
                body.append(f"  - Details: {result.details}")
        if not results:
            body.append("_No tokenization executed_")
        return "\n".join(body)

    def _update_tags(self, dataset: dict, dataset_urn: str, status: str) -> None:
        current_tags = self.client._extract_tag_urns(dataset.get("globalTags"))
        add_tags = {DONE_TAG, f"{STATUS_PREFIX}{status}"}
        remove_tags: set[str] = {tag for tag in current_tags if tag.startswith(STATUS_PREFIX)}
        if status == "SUCCESS":
            remove_tags.add(RUN_TAG)
        updated = self.client.update_dataset_tags(
            dataset_urn,
            add=add_tags,
            remove=remove_tags,
            current_tags=current_tags,
        )
        LOGGER.info("Updated tags for %s: %s", dataset_urn, updated)


def _parse_dataset_urn(dataset_urn: str) -> tuple[str, str, str]:
    try:
        inner = dataset_urn.split("(", 1)[1].rstrip(")")
        platform_urn, name, env = inner.split(",")
        platform = platform_urn.split(":")[-1]
        return platform, name, env
    except Exception as exc:  # pragma: no cover - defensive
        raise ValueError(f"Unable to parse dataset URN {dataset_urn}: {exc}")


def _split_dataset_key(key: str) -> tuple[str, str, str]:
    parts = key.split(".")
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    if len(parts) == 2:
        return "", parts[0], parts[1]
    if len(parts) == 1:
        return "", "public", parts[0]
    return parts[0], parts[1], ".".join(parts[2:])
