
from __future__ import annotations

from datetime import datetime, UTC
from typing import Optional, Tuple

import logging
import sqlite3

from categorizer.normalize import normalize_token
from categorizer.tag_typing import enrich_tag_type
from categorizer.tag_utils import normalize_tag_type

LOGGER = logging.getLogger(__name__)
_ALLOWED_ENTITY_TYPES = {'sport', 'tournament', 'team', 'player'}
_UPGRADED_TAGS_LOGGED: set[int] = set()


def upsert_tag(
    conn: sqlite3.Connection,
    *,
    name: str,
    url: Optional[str],
    tag_type: str,
    context: Optional[str] = None,
) -> Tuple[int, bool]:
    name = (name or '').strip()
    url = (url or '').strip()
    raw_type = (tag_type or '').strip() or 'unknown'
    tag_type = enrich_tag_type(raw_type, name, url, context)

    cur = conn.cursor()

    cur.execute('SELECT id, name, url, type FROM tags WHERE url = ? LIMIT 1', (url,))
    row = cur.fetchone()
    if row:
        tag_id = row['id'] if isinstance(row, sqlite3.Row) else row[0]
        updates = []
        params = []
        existing_name = row['name'] if isinstance(row, sqlite3.Row) else row[1]
        existing_type = row['type'] if isinstance(row, sqlite3.Row) else row[3]
        existing_type_clean = (existing_type or '').strip().lower() or 'unknown'
        if name and not (existing_name or '').strip():
            updates.append('name = ?')
            params.append(name)
        upgraded_type = None
        if tag_type != 'unknown' and existing_type_clean in ('', 'unknown'):
            updates.append('type = ?')
            params.append(tag_type)
            if tag_type in {'team', 'player'}:
                upgraded_type = tag_type
        if updates:
            params.append(tag_id)
            cur.execute(f"UPDATE tags SET {', '.join(updates)} WHERE id = ?", params)
            if upgraded_type and tag_id not in _UPGRADED_TAGS_LOGGED:
                LOGGER.info(
                    'tag type upgraded: tag_id=%s old_type=%s new_type=%s name=%s url=%s',
                    tag_id,
                    existing_type_clean,
                    upgraded_type,
                    name,
                    url,
                )
                _UPGRADED_TAGS_LOGGED.add(tag_id)
        return tag_id, False

    cur.execute('SELECT id, name, url, type FROM tags WHERE name = ? COLLATE NOCASE LIMIT 1', (name,))
    row = cur.fetchone()
    if row:
        tag_id = row['id'] if isinstance(row, sqlite3.Row) else row[0]
        existing_url = row['url'] if isinstance(row, sqlite3.Row) else row[2]
        existing_type = row['type'] if isinstance(row, sqlite3.Row) else row[3]
        existing_type_clean = (existing_type or '').strip().lower() or 'unknown'
        updates = []
        params = []
        if url and not (existing_url or '').strip():
            updates.append('url = ?')
            params.append(url)
        upgraded_type = None
        if tag_type != 'unknown' and existing_type_clean in ('', 'unknown'):
            updates.append('type = ?')
            params.append(tag_type)
            if tag_type in {'team', 'player'}:
                upgraded_type = tag_type
        if updates:
            params.append(tag_id)
            cur.execute(f"UPDATE tags SET {', '.join(updates)} WHERE id = ?", params)
            if upgraded_type and tag_id not in _UPGRADED_TAGS_LOGGED:
                LOGGER.info(
                    'tag type upgraded: tag_id=%s old_type=%s new_type=%s name=%s url=%s',
                    tag_id,
                    existing_type_clean,
                    upgraded_type,
                    name,
                    url,
                )
                _UPGRADED_TAGS_LOGGED.add(tag_id)
        return tag_id, False

    cur.execute(
        'INSERT INTO tags (name, url, type) VALUES (?, ?, ?)',
        (name or None, url or None, None if tag_type == 'unknown' else tag_type),
    )
    return cur.lastrowid, True


def link_article_tag(conn: sqlite3.Connection, *, news_id: int, tag_id: int) -> bool:
    cur = conn.cursor()
    cur.execute(
        'INSERT OR IGNORE INTO news_article_tags (news_id, tag_id) VALUES (?, ?)',
        (news_id, tag_id),
    )
    return cur.rowcount > 0


def _resolve_entity_type(conn: sqlite3.Connection, tag_id: int, tag_type: str) -> Optional[str]:
    normalized = normalize_tag_type(tag_type)
    if normalized in _ALLOWED_ENTITY_TYPES:
        return normalized
    cur = conn.cursor()
    cur.execute('SELECT type FROM tags WHERE id = ?', (tag_id,))
    row = cur.fetchone()
    if row and row[0]:
        fallback = normalize_tag_type(row[0])
        if fallback in _ALLOWED_ENTITY_TYPES:
            return fallback
    return None


def _ensure_entity(conn: sqlite3.Connection, *, alias_normalized: str, entity_type: str, lang: str) -> int:
    cur = conn.cursor()
    cur.execute(
        'SELECT id FROM entities WHERE name = ? AND type = ? LIMIT 1',
        (alias_normalized, entity_type),
    )
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute(
        'INSERT INTO entities (name, type, lang) VALUES (?, ?, ?)',
        (alias_normalized, entity_type, lang),
    )
    return cur.lastrowid


def upsert_alias_from_tag(
    conn: sqlite3.Connection,
    *,
    tag_id: int,
    name: str,
    tag_type: str,
    source: str = 'championat',
    lang: str = 'ru',
) -> bool:
    alias_original = (name or '').strip()
    alias_normalized = normalize_token(alias_original)
    if not alias_normalized:
        LOGGER.info('alias ignored: empty normalized value for %s', alias_original)
        return False

    entity_type = _resolve_entity_type(conn, tag_id, tag_type)
    if not entity_type:
        LOGGER.info('alias ignored: unsupported type %s for %s', tag_type, alias_original)
        return False

    entity_id = _ensure_entity(conn, alias_normalized=alias_normalized, entity_type=entity_type, lang=lang)

    cur = conn.cursor()
    now_iso = datetime.now(UTC).isoformat(timespec='seconds')
    LOGGER.info(
        'alias insert attempt: alias=%s normalized=%s entity_type=%s entity_id=%s source=%s',
        alias_original,
        alias_normalized,
        entity_type,
        entity_id,
        source,
    )
    cur.execute(
        'INSERT OR IGNORE INTO entity_aliases (alias, alias_normalized, entity_type, entity_id, source, lang, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)',
        (alias_original or alias_normalized, alias_normalized, entity_type, entity_id, source, lang, now_iso),
    )
    LOGGER.info('alias insert rowcount=%s lastrowid=%s', cur.rowcount, cur.lastrowid)
    if cur.rowcount:
        return True

    cur.execute(
        "UPDATE entity_aliases SET source = COALESCE(source, ?), lang = COALESCE(lang, ?), entity_id = COALESCE(entity_id, ?) WHERE alias_normalized = ? AND entity_type = ?",
        (source, lang, entity_id, alias_normalized, entity_type),
    )
    LOGGER.info('alias update rowcount=%s', cur.rowcount)
    return False
