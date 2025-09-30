#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Мини-скрипт категоризации новостей для PROSPORT.
— Мягкая миграция таблицы news_entities (добавляет недостающие поля и индексы).
— Прямая категоризация по тегам (sport / tournament / team / athlete).
— Достроение иерархии вверх (athlete→team, team|athlete→tournament, tournament|team→sport).
— Идемпотентные INSERT OR IGNORE (можно гонять хоть каждый час).

Ожидаемая схема таблиц/полей (минимально):
  - news_articles(id INTEGER PRIMARY KEY, published_at TEXT, ...)
  - tags(id INTEGER PRIMARY KEY, sport_id,tournament_id,team_id,athlete_id, type, entity_id, ...)
  - news_article_tags(news_id INTEGER, tag_id INTEGER, ...)
  - sports(id), tournaments(id, sport_id), teams(id), athletes(id, team_id)
  - team_tournaments(team_id, tournament_id, is_primary INTEGER DEFAULT 0)
  - athlete_tournaments(athlete_id, tournament_id, is_primary INTEGER DEFAULT 0)
  - news_entities(news_id INTEGER, entity_id INTEGER, confidence REAL)  -- будет расширена

Запуск:
  python categorize_articles.py --db-path ./db/prosport.db

Автор: ПРОSPORT.
"""

import argparse
import sqlite3
from contextlib import closing
from typing import Dict, List, Tuple


DDL_INDEXES = [
    # Уникальность одной связи (новость + тип сущности + id)
    """
    CREATE UNIQUE INDEX IF NOT EXISTS idx_news_entities_uniq
    ON news_entities(news_id, entity_type, entity_id);
    """,
    # Быстрые выборки
    """
    CREATE INDEX IF NOT EXISTS idx_news_entities_news
    ON news_entities(news_id);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_news_entities_entity
    ON news_entities(entity_type, entity_id);
    """,
]

# Блоки INSERT-ов. Каждый блок выполнится в транзакции и даст счётчик вставок.
INSERT_BLOCKS: List[Tuple[str, str]] = [
    (
        "SPORT из тегов",
        """
        INSERT OR IGNORE INTO news_entities (news_id, entity_id, confidence, entity_type, method)
        SELECT nat.news_id, t.sport_id, 1.0, 'sport', 'direct:tag'
        FROM news_article_tags nat
        JOIN tags t ON t.id = nat.tag_id
        WHERE t.sport_id IS NOT NULL;
        """,
    ),
    (
        "TOURNAMENT из тегов",
        """
        INSERT OR IGNORE INTO news_entities (news_id, entity_id, confidence, entity_type, method)
        SELECT nat.news_id, t.tournament_id, 1.0, 'tournament', 'direct:tag'
        FROM news_article_tags nat
        JOIN tags t ON t.id = nat.tag_id
        WHERE t.tournament_id IS NOT NULL;
        """,
    ),
    (
        "TEAM из тегов",
        """
        INSERT OR IGNORE INTO news_entities (news_id, entity_id, confidence, entity_type, method)
        SELECT nat.news_id, t.team_id, 1.0, 'team', 'direct:tag'
        FROM news_article_tags nat
        JOIN tags t ON t.id = nat.tag_id
        WHERE t.team_id IS NOT NULL;
        """,
    ),
    (
        "ATHLETE из тегов",
        """
        INSERT OR IGNORE INTO news_entities (news_id, entity_id, confidence, entity_type, method)
        SELECT nat.news_id, t.athlete_id, 1.0, 'athlete', 'direct:tag'
        FROM news_article_tags nat
        JOIN tags t ON t.id = nat.tag_id
        WHERE t.athlete_id IS NOT NULL;
        """,
    ),
    # Иерархия вверх: athlete → team (если задан текущий team_id)
    (
        "ATHLETE → TEAM",
        """
        INSERT OR IGNORE INTO news_entities (news_id, entity_id, confidence, entity_type, method)
        SELECT ne.news_id, a.team_id, 0.95, 'team', 'infer:hierarchy'
        FROM news_entities ne
        JOIN athletes a ON a.id = ne.entity_id
        WHERE ne.entity_type='athlete' AND a.team_id IS NOT NULL;
        """,
    ),
    # TEAM → primary tournament
    (
        "TEAM → TOURNAMENT (primary)",
        """
        INSERT OR IGNORE INTO news_entities (news_id, entity_id, confidence, entity_type, method)
        SELECT DISTINCT ne.news_id, tt.tournament_id, 0.95, 'tournament', 'infer:hierarchy'
        FROM news_entities ne
        JOIN team_tournaments tt ON tt.team_id = ne.entity_id AND tt.is_primary=1
        WHERE ne.entity_type='team';
        """,
    ),
    # ATHLETE → primary tournament
    (
        "ATHLETE → TOURNAMENT (primary)",
        """
        INSERT OR IGNORE INTO news_entities (news_id, entity_id, confidence, entity_type, method)
        SELECT DISTINCT ne.news_id, at.tournament_id, 0.95, 'tournament', 'infer:hierarchy'
        FROM news_entities ne
        JOIN athlete_tournaments at ON at.athlete_id = ne.entity_id AND at.is_primary=1
        WHERE ne.entity_type='athlete';
        """,
    ),
    # TOURNAMENT → SPORT
    (
        "TOURNAMENT → SPORT",
        """
        INSERT OR IGNORE INTO news_entities (news_id, entity_id, confidence, entity_type, method)
        SELECT DISTINCT ne.news_id, tr.sport_id, 0.95, 'sport', 'infer:hierarchy'
        FROM news_entities ne
        JOIN tournaments tr ON tr.id = ne.entity_id
        WHERE ne.entity_type='tournament' AND tr.sport_id IS NOT NULL;
        """,
    ),
    # TEAM → SPORT (через primary tournament)
    (
        "TEAM → SPORT (через primary tournament)",
        """
        INSERT OR IGNORE INTO news_entities (news_id, entity_id, confidence, entity_type, method)
        SELECT DISTINCT ne.news_id, tr.sport_id, 0.95, 'sport', 'infer:hierarchy'
        FROM news_entities ne
        JOIN team_tournaments tt ON tt.team_id = ne.entity_id AND tt.is_primary=1
        JOIN tournaments tr ON tr.id = tt.tournament_id
        WHERE ne.entity_type='team' AND tr.sport_id IS NOT NULL;
        """,
    ),
]


def column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    cur = conn.execute(f"PRAGMA table_info('{table.replace("'", "''")}')")
    return any(row[1] == column for row in cur.fetchall())


def ensure_migration(conn: sqlite3.Connection) -> None:
    """Мягко расширяем news_entities нужными полями и индексами."""
    # Добавляем колонки, если их нет
    with conn:
        if not column_exists(conn, 'news_entities', 'entity_type'):
            conn.execute("ALTER TABLE news_entities ADD COLUMN entity_type TEXT")
        if not column_exists(conn, 'news_entities', 'method'):
            conn.execute("ALTER TABLE news_entities ADD COLUMN method TEXT")
        if not column_exists(conn, 'news_entities', 'ambiguous'):
            conn.execute("ALTER TABLE news_entities ADD COLUMN ambiguous INTEGER DEFAULT 0")
        if not column_exists(conn, 'news_entities', 'created_at'):
            # SQLite не поддерживает DEFAULT CURRENT_TIMESTAMP с ALTER? Поддерживает — ок.
            conn.execute("ALTER TABLE news_entities ADD COLUMN created_at TEXT DEFAULT CURRENT_TIMESTAMP")

        for ddl in DDL_INDEXES:
            conn.execute(ddl)


def run_inserts(conn: sqlite3.Connection) -> Dict[str, int]:
    """Выполняем блоки INSERT OR IGNORE, считаем добавленные строки по дельте total_changes."""
    stats: Dict[str, int] = {}
    for title, sql in INSERT_BLOCKS:
        before = conn.total_changes
        conn.execute("BEGIN")
        try:
            conn.executescript(sql)
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        after = conn.total_changes
        stats[title] = max(0, after - before)
    return stats


def main():
    ap = argparse.ArgumentParser(description="Категоризация новостей (канонические связи)")
    ap.add_argument("--db-path", default="./db/prosport.db", help="Путь до SQLite базы (по умолчанию ./db/prosport.db)")
    args = ap.parse_args()

    with closing(sqlite3.connect(args.db_path)) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")

        print("🗄️  Открыта БД:", args.db_path)
        ensure_migration(conn)
        print("🧩 Миграция news_entities — OK (колонки и индексы на месте)")

        stats = run_inserts(conn)

        # Итоговые количества по типам
        def count_by(etype: str) -> int:
            cur = conn.execute(
                "SELECT COUNT(*) FROM news_entities WHERE entity_type=?",
                (etype,),
            )
            return int(cur.fetchone()[0])

        totals = {
            'sport': count_by('sport'),
            'tournament': count_by('tournament'),
            'team': count_by('team'),
            'athlete': count_by('athlete'),
        }

        cur_news = conn.execute("SELECT COUNT(DISTINCT id) FROM news_articles")
        total_news = int(cur_news.fetchone()[0]) if cur_news.fetchone() else 0

        cur_linked = conn.execute("SELECT COUNT(DISTINCT news_id) FROM news_entities")
        news_with_links = int(cur_linked.fetchone()[0])

        print("\n✅ Категоризация завершена.")
        print("— Добавлено за прогон:")
        for k, v in stats.items():
            print(f"   • {k}: +{v}")
        print("— Итого связей в news_entities:")
        for k, v in totals.items():
            print(f"   • {k}: {v}")
        print(f"— Новостей всего: {total_news}")
        print(f"— Новостей с хотя бы одной связью: {news_with_links}")
        coverage = (news_with_links / total_news * 100.0) if total_news else 0.0
        print(f"— Покрытие: {coverage:.1f}%")

        print("\nПодсказка: ставь этот скрипт после парсинга и backfill_m2m_and_tags в кроне/Actions.")


if __name__ == "__main__":
    main()
