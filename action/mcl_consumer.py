"""Background consumer that listens for MetadataChangeLog events."""
from __future__ import annotations

import logging
import os
import threading
import time
from typing import List, Optional, Sequence

from confluent_kafka import KafkaException
from confluent_kafka.deserializing_consumer import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.schema_registry.error import SchemaRegistryError
from confluent_kafka.serialization import StringDeserializer

from .run_manager import RunManager

LOGGER = logging.getLogger(__name__)
TARGET_TAG = "urn:li:tag:tokenize/run"
TOPIC = "MetadataChangeLog_Versioned_v1"


def _unwrap_union(value: Optional[dict]) -> dict:
    if isinstance(value, dict) and len(value) == 1:
        return next(iter(value.values()))
    return value or {}


def _extract_tags(tag_container: Optional[dict]) -> List[str]:
    tags: List[str] = []
    container = _unwrap_union(tag_container)
    for association in container.get("tags", []) or []:
        tag = association.get("tag") or {}
        urn = tag.get("urn")
        if urn:
            tags.append(urn)
    return tags


def _extract_field_columns(aspect: dict) -> List[str]:
    columns: List[str] = []
    info_list = aspect.get("editableSchemaFieldInfo", []) or []
    for info in info_list:
        tags = _extract_tags(info.get("globalTags"))
        if TARGET_TAG in tags:
            field_path = info.get("fieldPath")
            if field_path:
                columns.append(field_path.split(".")[-1])
    return columns


class _SchemaUnavailable(RuntimeError):
    """Raised when the MetadataChangeLog schema has not been registered yet."""


class MetadataChangeLogConsumer(threading.Thread):
    """Consume MetadataChangeLog events and hand off to :class:`RunManager`."""

    def __init__(self, run_manager: RunManager, *, poll_interval: float = 1.0) -> None:
        super().__init__(name="mcl-consumer", daemon=True)
        self.run_manager = run_manager
        self.poll_interval = poll_interval
        self._stop_event = threading.Event()
        self._consumer: Optional[DeserializingConsumer] = None

    def stop(self) -> None:
        self._stop_event.set()
        if self._consumer is not None:
            self._consumer.close()

    def run(self) -> None:
        LOGGER.info("Starting MetadataChangeLog consumer thread")
        while not self._stop_event.is_set():
            if not self._ensure_consumer():
                time.sleep(5.0)
                continue
            try:
                message = self._consumer.poll(timeout=self.poll_interval)
            except KafkaException as exc:  # pragma: no cover - runtime errors
                LOGGER.warning("Kafka poll failed: %s", exc)
                time.sleep(5.0)
                continue

            if message is None:
                continue
            if message.error():
                LOGGER.debug("Kafka message error: %s", message.error())
                continue

            value = message.value()
            if not value:
                continue
            self._handle_message(value)

        LOGGER.info("MetadataChangeLog consumer stopped")

    def _ensure_consumer(self) -> bool:
        if self._consumer is not None:
            return True
        try:
            self._consumer = self._build_consumer()
            return True
        except _SchemaUnavailable:
            LOGGER.warning(
                "MetadataChangeLog schema not yet available in Schema Registry; "
                "will retry"
            )
            self._consumer = None
            return False
        except Exception as exc:  # pragma: no cover - initialization failures
            LOGGER.warning("Failed to initialize Kafka consumer: %s", exc)
            self._consumer = None
            return False

    def _build_consumer(self) -> DeserializingConsumer:
        bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVER", "broker:29092")
        schema_registry_url = os.getenv("KAFKA_SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
        group_id = os.getenv("KAFKA_GROUP_ID", "tokenization-action")
        offset_reset = os.getenv("KAFKA_AUTO_OFFSET_RESET", "latest")

        schema_registry = SchemaRegistryClient({"url": schema_registry_url})
        try:
            latest_schema = schema_registry.get_latest_version(f"{TOPIC}-value")
        except SchemaRegistryError as error:
            if error.error_code == 40401:
                raise _SchemaUnavailable() from error
            raise
        value_deserializer = AvroDeserializer(
            schema_registry_client=schema_registry,
            schema_str=latest_schema.schema.schema_str,
        )
        key_deserializer = StringDeserializer("utf_8")

        consumer = DeserializingConsumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": group_id,
                "auto.offset.reset": offset_reset,
                "key.deserializer": key_deserializer,
                "value.deserializer": value_deserializer,
            }
        )
        consumer.subscribe([TOPIC])
        return consumer

    def _handle_message(self, message: dict) -> None:
        entity_type = message.get("entityType")
        if entity_type != "dataset":
            return
        entity_urn = message.get("entityUrn")
        aspect_name = message.get("aspectName")
        aspect = _unwrap_union(message.get("aspect"))
        change_type = message.get("changeType")

        if not entity_urn or change_type not in {"UPSERT", "PATCH"}:
            return

        if aspect_name == "globalTags":
            tags = _extract_tags(aspect)
            if TARGET_TAG in tags:
                LOGGER.info("Detected dataset tag trigger for %s", entity_urn)
                self._trigger(entity_urn, None)
        elif aspect_name == "editableSchemaMetadata":
            columns = _extract_field_columns(aspect)
            if columns:
                LOGGER.info("Detected field tag trigger for %s columns=%s", entity_urn, columns)
                self._trigger(entity_urn, columns)

    def _trigger(self, dataset_urn: str, columns: Optional[Sequence[str]]) -> None:
        try:
            response = self.run_manager.trigger(dataset_urn, columns=columns)
            LOGGER.info("Triggered run %s for %s", response.get("run_id"), dataset_urn)
        except Exception as exc:  # pragma: no cover - runtime failure
            LOGGER.exception("Failed to execute tokenization run for %s: %s", dataset_urn, exc)
