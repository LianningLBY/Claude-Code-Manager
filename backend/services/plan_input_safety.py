"""Narrow high-confidence credential guard for durable Plan user input."""

from __future__ import annotations

import re
from collections.abc import Iterable

_HIGH_CONFIDENCE_SECRET = re.compile(
    r"(?:"
    r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----|"
    r"\bAKIA[0-9A-Z]{16}\b|"
    r"\bgithub_pat_[A-Za-z0-9_]{40,}\b|"
    r"\bgh[pousr]_[A-Za-z0-9]{30,}\b|"
    r"\bxox[baprs]-[A-Za-z0-9-]{20,}\b|"
    r"\bsk-[A-Za-z0-9_-]{24,}\b|"
    r"\bBearer\s+[A-Za-z0-9._~+/-]{32,}={0,2}\b"
    r")",
    re.IGNORECASE,
)


def contains_high_confidence_secret(values: Iterable[object]) -> bool:
    for value in values:
        if isinstance(value, str) and _HIGH_CONFIDENCE_SECRET.search(value):
            return True
        if isinstance(value, list) and contains_high_confidence_secret(value):
            return True
    return False
