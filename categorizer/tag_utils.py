from __future__ import annotations

from typing import Optional

from categorizer.normalize import normalize_token
from categorizer.tag_typing import enrich_tag_type

_ALLOWED_TYPES = {'sport', 'tournament', 'team', 'player'}


def normalize_tag_name(value: Optional[str]) -> str:
    return normalize_token(value)


def normalize_tag_url(url: Optional[str]) -> str:
    if not url:
        return ''
    text = url.strip()
    if not text:
        return ''
    if text.startswith('//'):
        text = 'https:' + text
    if text.startswith('http://'):
        text = 'https://' + text[len('http://'):]
    for sep in ('?', '#'):
        if sep in text:
            text = text.split(sep, 1)[0]
    text = text.rstrip('/')
    return text


def normalize_tag_type(
    raw: Optional[str],
    name: Optional[str] = None,
    url: Optional[str] = None,
    context: Optional[str] = None,
) -> str:
    normalized = normalize_tag_name(raw)
    if normalized in _ALLOWED_TYPES:
        return normalized
    if normalized in {'league', 'competition'}:
        return 'tournament'
    if normalized in {'club', 'teamclub'}:
        return 'team'
    if normalized in {'athlete', 'sportsman'}:
        return 'player'
    return enrich_tag_type(normalized or raw, name, url, context)
