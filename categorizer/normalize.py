
from __future__ import annotations

import re
from typing import Optional

_WHITESPACE_RE = re.compile(r"\s+")
_BOUNDARY_RE = re.compile(r"^\W+|\W+$", re.UNICODE)


def normalize_token(value: Optional[str]) -> str:
    """Normalize alias tokens for comparison (preserve unicode letters)."""
    if value is None:
        return ''
    text = value.strip().lower()
    if not text:
        return ''
    text = text.replace('-', ' ').replace('_', ' ')
    text = _WHITESPACE_RE.sub(' ', text)
    text = _BOUNDARY_RE.sub('', text).strip()
    return text
