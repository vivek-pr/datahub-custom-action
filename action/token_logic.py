"""Tokenization helpers for the proof-of-concept workflow."""
from __future__ import annotations

import base64
import re
from typing import Any, Dict, Iterable

TOKEN_PREFIX = "tok_"
TOKEN_SUFFIX = "_poc"
TOKEN_PATTERN = re.compile(r"^tok_[A-Za-z0-9+/=]+_poc$")


def is_tokenized(value: Any) -> bool:
    """Return True if the value already looks tokenized."""
    if value is None:
        return False
    return bool(TOKEN_PATTERN.match(str(value)))


def tokenize_value(value: Any) -> str:
    """Tokenize a single value deterministically.

    Non-string values are converted to strings before tokenization so that we
    produce a consistent token regardless of the original type.
    """

    if value is None:
        return value

    value_str = str(value)
    if is_tokenized(value_str):
        return value_str

    encoded = base64.b64encode(value_str.encode("utf-8")).decode("ascii")
    return f"{TOKEN_PREFIX}{encoded}{TOKEN_SUFFIX}"


def tokenize_row(row: Dict[str, Any], columns: Iterable[str]) -> Dict[str, Any]:
    """Return a copy of ``row`` with ``columns`` tokenized.

    Columns not present in ``row`` are ignored. The returned dictionary only
    contains the updated columns and can be fed directly into a SQL ``UPDATE``
    statement.
    """

    updates: Dict[str, Any] = {}
    for column in columns:
        if column in row:
            updates[column] = tokenize_value(row[column])
    return updates
