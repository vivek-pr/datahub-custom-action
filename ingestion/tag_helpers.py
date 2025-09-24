"""Helper functions used by ingestion recipes."""

from __future__ import annotations

from typing import List

from datahub.metadata.schema_classes import TagAssociationClass


def tokenize_run_tags(_: str) -> List[TagAssociationClass]:
    """Return the tokenize/run tag association for every dataset."""

    return [TagAssociationClass(tag="urn:li:tag:tokenize/run")]

