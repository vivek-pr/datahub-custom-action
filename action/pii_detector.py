"""Utilities to identify PII columns for tokenization."""
from __future__ import annotations

import re
from typing import Iterable, List, Optional, Sequence, Set


class PIIDetector:
    """Detect PII columns using DataHub tags and naming heuristics."""

    DEFAULT_PII_TAGS: Set[str] = {
        "urn:li:tag:pii.email",
        "urn:li:tag:pii.phone",
        "urn:li:tag:sensitive",
        "urn:li:tag:pii",
    }

    DEFAULT_NAME_PATTERNS: Sequence[str] = (
        r"email",
        r"e_mail",
        r"phone",
        r"mobile",
        r"contact",
        r"ssn",
        r"aadhaar",
    )

    def __init__(
        self,
        pii_tags: Optional[Iterable[str]] = None,
        name_patterns: Optional[Iterable[str]] = None,
    ) -> None:
        self.pii_tags = set(pii_tags or self.DEFAULT_PII_TAGS)
        self.patterns = [
            re.compile(pattern, re.IGNORECASE) for pattern in (name_patterns or self.DEFAULT_NAME_PATTERNS)
        ]

    def detect(self, schema_fields: Sequence[dict], override_columns: Optional[Iterable[str]] = None) -> List[str]:
        """Return an ordered list of columns that should be tokenized."""

        if override_columns:
            return list(dict.fromkeys([col for col in override_columns if isinstance(col, str)]))

        tagged_columns: List[str] = []
        fallback_columns: List[str] = []

        for field in schema_fields:
            field_path = field.get("fieldPath")
            if not field_path:
                continue
            normalized_path = field_path.split(".")[-1]

            tags = self._extract_tags(field)
            if self.pii_tags.intersection(tags):
                tagged_columns.append(normalized_path)
                continue

            if any(pattern.search(normalized_path) for pattern in self.patterns):
                fallback_columns.append(normalized_path)

        ordered = tagged_columns + [col for col in fallback_columns if col not in tagged_columns]
        return ordered

    @staticmethod
    def _extract_tags(field: dict) -> Set[str]:
        tags: Set[str] = set()
        global_tags = field.get("globalTags") or {}
        for tag_entry in global_tags.get("tags", []):
            tag = tag_entry.get("tag") or {}
            urn = tag.get("urn")
            if urn:
                tags.add(urn)
        return tags
