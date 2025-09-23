"""Shared dataclasses for the tokenization action."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional


@dataclass
class TokenizationResult:
    dataset: str
    platform: str
    columns: List[str]
    rows_scanned: int
    rows_updated: int
    details: Optional[str] = None
