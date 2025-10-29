"""Утилиты для работы с SQLite: единое подключение и индексы."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Optional, Union

# Корень проекта: папка db лежит в корне репозитория → parents[1]
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = PROJECT_ROOT / "database" / "prosport.db"


def resolve_db_path(db_path: Optional[Union[str, Path]] = None) -> Path:
    """Превращает относительные/отсутствующие пути в абсолютный путь до файла БД."""
    if db_path is None:
        path = DEFAULT_DB
    else:
        path = Path(db_path).expanduser()
        if not path.is_absolute():
            path = (PROJECT_ROOT / path).resolve()
    if path.parent and not path.parent.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
    return path


def ensure_indexes(conn: sqlite3.Connection) -> None:
    """Ensure auxiliary indexes exist, skipping missing tables gracefully."""
    for sql in (
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_tags_url ON tags(url);",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_news_article_tags_unique ON news_article_tags(news_id, tag_id);",
        "CREATE INDEX IF NOT EXISTS idx_story_articles_news_id ON story_articles(news_id);",
        "CREATE INDEX IF NOT EXISTS idx_story_articles_story_id ON story_articles(story_id);",
        "CREATE INDEX IF NOT EXISTS idx_publish_queue_status_sched ON publish_queue(status, scheduled_at DESC);",
        "CREATE INDEX IF NOT EXISTS idx_publish_queue_sent_at ON publish_queue(sent_at DESC);",
        "CREATE INDEX IF NOT EXISTS idx_stories_updated_at ON stories(updated_at DESC);",
        "CREATE INDEX IF NOT EXISTS idx_publish_edits_item ON publish_edits(item_type, item_id, created_at DESC);",
        "CREATE INDEX IF NOT EXISTS idx_digests_period_since ON digests(period, since_utc);",
        "CREATE INDEX IF NOT EXISTS idx_digest_items_story ON digest_items(story_id);",
        "CREATE INDEX IF NOT EXISTS idx_monitor_ts ON monitor_logs(ts_utc);",
        "CREATE INDEX IF NOT EXISTS idx_monitor_metric_ts ON monitor_logs(metric, ts_utc);",
    ):
        try:
            conn.execute(sql)
        except sqlite3.OperationalError as exc:
            if "no such table" in str(exc).lower():
                continue
            raise


def get_conn(db_path: Optional[Union[str, Path]] = None) -> sqlite3.Connection:
    """Возвращает подключение к SQLite с включёнными внешними ключами и гарантированными индексами."""
    path = resolve_db_path(db_path)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    ensure_indexes(conn)
    return conn


__all__ = [
    "get_conn",
    "resolve_db_path",
    "ensure_indexes",
    "PROJECT_ROOT",
    "DEFAULT_DB",
]
