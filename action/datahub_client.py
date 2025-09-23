"""Helpers for interacting with DataHub's Graph service."""
from __future__ import annotations

import logging
import os
from typing import Dict, Iterable, List, Optional, Sequence, Set

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.config import DatahubClientConfig
from datahub.metadata.schema_classes import (
    EditableDatasetPropertiesClass,
    GlobalTagsClass,
    TagAssociationClass,
)

LOGGER = logging.getLogger(__name__)


DATASET_QUERY = """
query dataset($urn: String!) {
  dataset(urn: $urn) {
    urn
    name
    platform {
      urn
      name
    }
    properties {
      name
      description
    }
    editableProperties {
      description
      customProperties {
        key
        value
      }
    }
    schemaMetadata {
      fields {
        fieldPath
        nativeDataType
        description
        globalTags {
          tags {
            tag {
              urn
              name
            }
          }
        }
      }
    }
    editableSchemaMetadata {
      editableSchemaFieldInfo {
        fieldPath
        globalTags {
          tags {
            tag {
              urn
              name
            }
          }
        }
      }
    }
    globalTags {
      tags {
        tag {
          urn
          name
        }
      }
    }
  }
}
"""


class DataHubClient:
    """Thin wrapper around :class:`~datahub.ingestion.graph.client.DataHubGraph`."""

    def __init__(self) -> None:
        server = os.getenv("DATAHUB_GMS", "http://datahub-gms:8080").rstrip("/")
        token = os.getenv("DATAHUB_TOKEN")
        config = DatahubClientConfig(server=server, token=token)
        self.graph = DataHubGraph(config)

    def get_dataset(self, urn: str) -> dict:
        response = self.graph.execute_graphql(DATASET_QUERY, variables={"urn": urn})
        dataset = response.get("dataset") if response else None
        if not dataset:
            raise ValueError(f"Dataset {urn} not found")
        return dataset

    def update_editable_properties(
        self,
        urn: str,
        description: Optional[str],
        custom_properties: Optional[Dict[str, str]] = None,
    ) -> None:
        aspect = EditableDatasetPropertiesClass(
            description=description,
            customProperties=custom_properties or {},
        )
        proposal = MetadataChangeProposalWrapper(entityUrn=urn, aspect=aspect)
        LOGGER.info("Updating editable dataset properties for %s", urn)
        self.graph.emit_mcp(proposal)

    def update_dataset_tags(
        self,
        urn: str,
        *,
        add: Iterable[str] = (),
        remove: Iterable[str] = (),
        current_tags: Optional[Sequence[str]] = None,
    ) -> List[str]:
        existing: Set[str]
        if current_tags is None:
            dataset = self.get_dataset(urn)
            existing = self._extract_tag_urns(dataset.get("globalTags"))
        else:
            existing = set(current_tags)

        updated = (existing - set(remove)) | set(add)
        aspect = GlobalTagsClass(
            tags=[TagAssociationClass(tag=tag_urn) for tag_urn in sorted(updated)]
        )
        proposal = MetadataChangeProposalWrapper(entityUrn=urn, aspect=aspect)
        LOGGER.info("Updating tags for %s: %s", urn, sorted(updated))
        self.graph.emit_mcp(proposal)
        return sorted(updated)

    @staticmethod
    def extract_schema_fields(dataset: dict) -> List[dict]:
        fields = dataset.get("schemaMetadata", {}).get("fields", []) or []
        editable_info = dataset.get("editableSchemaMetadata", {}).get(
            "editableSchemaFieldInfo", []
        ) or []
        editable_map = {info.get("fieldPath"): info for info in editable_info}
        merged: List[dict] = []
        for field in fields:
            field_path = field.get("fieldPath")
            merged_tags = DataHubClient._extract_tag_urns(field.get("globalTags"))
            if field_path in editable_map:
                merged_tags |= DataHubClient._extract_tag_urns(
                    editable_map[field_path].get("globalTags")
                )
            merged.append({
                "fieldPath": field_path,
                "globalTags": {"tags": [{"tag": {"urn": tag}} for tag in sorted(merged_tags)]},
                "nativeDataType": field.get("nativeDataType"),
                "description": field.get("description"),
            })
        return merged

    @staticmethod
    def extract_custom_properties(dataset: dict) -> Dict[str, str]:
        props_list = dataset.get("editableProperties", {}).get("customProperties") or []
        properties: Dict[str, str] = {}
        for item in props_list:
            key = item.get("key")
            value = item.get("value")
            if key is not None and value is not None:
                properties[str(key)] = str(value)
        return properties

    @staticmethod
    def _extract_tag_urns(global_tags: Optional[dict]) -> Set[str]:
        tag_urns: Set[str] = set()
        if not global_tags:
            return tag_urns
        for assoc in global_tags.get("tags", []) or []:
            tag = assoc.get("tag") or {}
            urn = tag.get("urn")
            if urn:
                tag_urns.add(urn)
        return tag_urns
