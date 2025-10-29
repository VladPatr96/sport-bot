
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

import sqlite3

from categorizer.normalize import normalize_token
from categorizer.tag_utils import normalize_tag_name, normalize_tag_type

LOGGER = logging.getLogger(__name__)
_ALLOWED_TYPES = ("sport", "tournament", "team", "player")


def _ensure_assignments_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS news_entity_assignments (
            news_id INTEGER PRIMARY KEY,
            sport_id INTEGER,
            tournament_id INTEGER,
            team_id INTEGER,
            player_id INTEGER,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(news_id) REFERENCES news(id)
        )
        """
    )
    conn.execute(
        """
        CREATE VIEW IF NOT EXISTS news_articles AS
        SELECT
            n.id AS news_id,
            nea.sport_id,
            nea.tournament_id,
            nea.team_id,
            nea.player_id
        FROM news AS n
        LEFT JOIN news_entity_assignments AS nea ON nea.news_id = n.id
        """
    )


def resolve_entity(conn: sqlite3.Connection, *, alias: str, entity_type: str) -> Optional[Tuple[str, int]]:
    alias_normalized = normalize_token(alias)
    if not alias_normalized or not entity_type:
        return None

    cur = conn.cursor()
    cur.execute(
        """
        SELECT COALESCE(entity_type, type) AS etype, entity_id
          FROM entity_aliases
         WHERE alias_normalized = ?
           AND COALESCE(entity_type, type) = ?
           AND entity_id IS NOT NULL
         ORDER BY id ASC
         LIMIT 1
        """,
        (alias_normalized, entity_type),
    )
    row = cur.fetchone()
    if not row:
        return None
    return row[0], row[1]


def assign_entities_for_article(
    conn: sqlite3.Connection,
    *,
    news_id: int,
    prefer_existing: bool = True,
) -> Dict[str, Any]:
    _ensure_assignments_table(conn)

    result: Dict[str, Any] = {
        "assigned": {t: 0 for t in _ALLOWED_TYPES},
        "unknown": [],
        "conflicts": [],
    }

    if not news_id:
        return result

    cur = conn.cursor()
    tag_rows = cur.execute(
        """
        SELECT t.id AS tag_id, t.name, t.type
          FROM news_article_tags nat
          JOIN tags t ON t.id = nat.tag_id
         WHERE nat.news_id = ?
        """,
        (news_id,),
    ).fetchall()

    if not tag_rows:
        return result

    existing_row = cur.execute(
        "SELECT sport_id, tournament_id, team_id, player_id FROM news_entity_assignments WHERE news_id = ?",
        (news_id,),
    ).fetchone()
    existing_values: Dict[str, Optional[int]] = {
        "sport": existing_row[0] if existing_row else None,
        "tournament": existing_row[1] if existing_row else None,
        "team": existing_row[2] if existing_row else None,
        "player": existing_row[3] if existing_row else None,
    }

    resolved: Dict[str, List[Tuple[int, str, int]]] = {t: [] for t in _ALLOWED_TYPES}

    for row in tag_rows:
        tag_id = row["tag_id"] if isinstance(row, sqlite3.Row) else row[0]
        raw_name = row["name"] if isinstance(row, sqlite3.Row) else row[1]
        raw_type = row["type"] if isinstance(row, sqlite3.Row) else row[2]

        normalized_type = normalize_tag_type(raw_type)
        if normalized_type not in _ALLOWED_TYPES:
            continue

        alias_candidate = normalize_tag_name(raw_name) or (raw_name or "")
        resolved_entry = resolve_entity(conn, alias=alias_candidate, entity_type=normalized_type)
        if resolved_entry:
            _, entity_id = resolved_entry
            resolved[normalized_type].append((entity_id, raw_name or alias_candidate, tag_id))
        else:
            result["unknown"].append(
                {
                    "alias": raw_name or alias_candidate,
                    "type": normalized_type,
                    "tag_id": tag_id,
                }
            )

    final_ids: Dict[str, Optional[int]] = {t: existing_values[t] for t in _ALLOWED_TYPES}

    for etype in _ALLOWED_TYPES:
        candidates = resolved[etype]
        if not candidates:
            if final_ids[etype] is not None:
                result["assigned"][etype] = 1
            continue

        unique_ids: List[int] = []
        alias_names: List[str] = []
        for entity_id, alias_value, _ in candidates:
            if entity_id not in unique_ids:
                unique_ids.append(entity_id)
            alias_names.append(alias_value)

        if prefer_existing and final_ids[etype] is not None:
            chosen_id = final_ids[etype]
            if chosen_id not in unique_ids:
                LOGGER.warning(
                    "Conflict for news_id=%s type=%s: existing_id=%s not among candidates=%s",
                    news_id,
                    etype,
                    chosen_id,
                    unique_ids,
                )
                result["conflicts"].append(
                    {
                        "type": etype,
                        "aliases": alias_names,
                        "entity_ids": unique_ids,
                    }
                )
        else:
            chosen_id = unique_ids[0]
            if len(unique_ids) > 1:
                LOGGER.warning(
                    "Multiple entities for news_id=%s type=%s: picked=%s from=%s",
                    news_id,
                    etype,
                    chosen_id,
                    unique_ids,
                )
                result["conflicts"].append(
                    {
                        "type": etype,
                        "aliases": alias_names,
                        "entity_ids": unique_ids,
                    }
                )
            final_ids[etype] = chosen_id

        if final_ids[etype] is not None:
            result["assigned"][etype] = 1

    if any(final_ids.values()):
        if existing_row is None:
            cur.execute(
                """
                INSERT OR REPLACE INTO news_entity_assignments (
                    news_id, sport_id, tournament_id, team_id, player_id, updated_at
                ) VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                (
                    news_id,
                    final_ids['sport'],
                    final_ids['tournament'],
                    final_ids['team'],
                    final_ids['player'],
                ),
            )
        else:
            updates: List[str] = []
            params: List[Any] = []
            for col, etype in (
                ("sport_id", "sport"),
                ("tournament_id", "tournament"),
                ("team_id", "team"),
                ("player_id", "player"),
            ):
                current_value = existing_values[etype]
                target_value = final_ids[etype]
                if prefer_existing and current_value is not None:
                    target_value = current_value
                if target_value != current_value:
                    updates.append(f"{col} = ?")
                    params.append(target_value)
            if updates:
                params.append(news_id)
                cur.execute(
                    f"UPDATE news_entity_assignments SET {', '.join(updates)}, updated_at = CURRENT_TIMESTAMP WHERE news_id = ?",
                    params,
                )
    conn.commit()
    return result
