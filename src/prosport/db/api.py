
from __future__ import annotations
import sqlite3
from typing import Optional, Iterable

def connect_ro(db_path: str) -> sqlite3.Connection:
    """Подключение в режим read-only."""
    return sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)

def connect_rw(db_path: str) -> sqlite3.Connection:
    """Обычное подключение (read-write)."""
    return sqlite3.connect(db_path)

def get_article_by_id(conn: sqlite3.Connection, article_id: int) -> Optional[dict]:
    row = conn.execute("""
        SELECT id, title, published_at
        FROM news_articles
        WHERE id = ?
    """, (article_id,)).fetchone()
    if not row:
        return None
    return {"id": row[0], "title": row[1], "published_at": row[2]}

def find_tags_by_article(conn: sqlite3.Connection, article_id: int) -> list[dict]:
    cur = conn.execute("""
      SELECT t.id, t.name, t.url, t.type
      FROM tags t
      JOIN news_article_tags nat ON nat.tag_id = t.id
      WHERE nat.news_id = ?
    """, (article_id,))
    return [{"id": r[0], "name": r[1], "url": r[2], "type": r[3]} for r in cur.fetchall()]
