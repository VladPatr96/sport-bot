
from __future__ import annotations

import argparse
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, List

from categorizer.alias_mapper import assign_entities_for_article
from categorizer.normalize import normalize_token
from db.utils import get_conn

from scripts.sync_champ_news import sync_news_since_anchor_url

LOGGER = logging.getLogger(__name__)
UTC = timezone.utc


def parse_datetime(value: str) -> datetime:
    try:
        dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f'Invalid datetime: {value}') from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def resolve_entity_id(conn, *, entity_type: str, entity_id: int | None, alias: str | None) -> int:
    if entity_id is not None:
        return entity_id
    if not alias:
        raise ValueError('Either --entity-id or --alias must be provided')
    alias_norm = normalize_token(alias)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT entity_id
          FROM entity_aliases
         WHERE alias_normalized = ?
           AND entity_type = ?
           AND entity_id IS NOT NULL
         ORDER BY id DESC
         LIMIT 1
        """,
        (alias_norm, entity_type),
    )
    row = cur.fetchone()
    if not row:
        raise ValueError(f"Alias '{alias}' for type '{entity_type}' not found (assign seed first)")
    return row[0]


def window_from_args(args) -> tuple[datetime, datetime]:
    now = datetime.now(UTC)
    since = None
    until = None
    if args.days is not None:
        until = now
        since = now - timedelta(days=args.days)
    if args.since:
        since = parse_datetime(args.since)
    if args.until:
        until = parse_datetime(args.until)
    if since is None or until is None:
        raise ValueError('Specify --days or both --since/--until')
    if since >= until:
        raise ValueError('since must be earlier than until')
    return since, until


def coverage_for_entity(conn, *, entity_type: str, entity_id: int, since: datetime, until: datetime) -> int:
    col = f"{entity_type}_id"
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT COUNT(*)
          FROM news n
          JOIN news_entity_assignments nea ON nea.news_id = n.id
         WHERE nea.{col} = ?
           AND n.created_at >= ?
           AND n.created_at < ?
        """,
        (entity_id, since.isoformat(timespec='seconds'), until.isoformat(timespec='seconds')),
    )
    return cur.fetchone()[0]


def news_ids_in_window(conn, *, since: datetime, until: datetime) -> List[int]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id
          FROM news
         WHERE created_at >= ?
           AND created_at < ?
        """,
        (since.isoformat(timespec='seconds'), until.isoformat(timespec='seconds')),
    )
    return [row[0] for row in cur.fetchall()]


def fetch_results(conn, *, entity_type: str, entity_id: int, since: datetime, until: datetime, limit: int):
    col = f"{entity_type}_id"
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT n.id, n.title, n.url, n.created_at, n.published_at, n.source
          FROM news n
          JOIN news_entity_assignments nea ON nea.news_id = n.id
         WHERE nea.{col} = ?
           AND n.created_at >= ?
           AND n.created_at < ?
         ORDER BY n.created_at DESC
         LIMIT ?
        """,
        (entity_id, since.isoformat(timespec='seconds'), until.isoformat(timespec='seconds'), limit),
    )
    return cur.fetchall()


def run_sync(args) -> None:
    asyncio.run(
        sync_news_since_anchor_url(
            max_pages=args.max_pages,
            manual_anchor=None,
            dry_run=False,
            smoke=False,
        )
    )


def assign_entities(conn, news_ids: Iterable[int]) -> int:
    assigned = 0
    for news_id in news_ids:
        result = assign_entities_for_article(conn, news_id=news_id, prefer_existing=True)
        assigned += sum(result['assigned'].values())
    return assigned


def main() -> None:
    parser = argparse.ArgumentParser(description='Fetch news for an entity on demand')
    parser.add_argument('--entity-type', required=True, choices=['sport', 'tournament', 'team', 'player'])
    parser.add_argument('--entity-id', type=int)
    parser.add_argument('--alias')
    parser.add_argument('--days', type=int)
    parser.add_argument('--since')
    parser.add_argument('--until')
    parser.add_argument('--limit', type=int, default=50)
    parser.add_argument('--max-pages', type=int, default=5)
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--verbose', action='store_true')
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

    since, until = window_from_args(args)
    LOGGER.info('Window UTC: since=%s until=%s', since.isoformat(), until.isoformat())

    conn = get_conn()
    try:
        entity_id = resolve_entity_id(conn, entity_type=args.entity_type, entity_id=args.entity_id, alias=args.alias)
        LOGGER.info('Entity resolved: type=%s id=%s alias=%s', args.entity_type, entity_id, args.alias)

        before_total_news = conn.execute('SELECT COUNT(*) FROM news').fetchone()[0]
        coverage_before = coverage_for_entity(conn, entity_type=args.entity_type, entity_id=entity_id, since=since, until=until)
        LOGGER.info('coverage_before=%s (limit=%s)', coverage_before, args.limit)

        parsed_new = 0
        assigned_count = 0

        if not args.dry_run and coverage_before < args.limit:
            LOGGER.info('Coverage insufficient; running sync (max_pages=%s)', args.max_pages)
            run_sync(args)
            after_total_news = conn.execute('SELECT COUNT(*) FROM news').fetchone()[0]
            parsed_new = after_total_news - before_total_news
            LOGGER.info('Parsed new articles: %s', parsed_new)

            news_ids = news_ids_in_window(conn, since=since, until=until)
            assigned_count = assign_entities(conn, news_ids)
            LOGGER.info('assign_entities applied: %s assignments', assigned_count)
        elif args.dry_run:
            LOGGER.info('Dry-run mode: skipping sync/assignment')

        coverage_after = coverage_for_entity(conn, entity_type=args.entity_type, entity_id=entity_id, since=since, until=until)
        LOGGER.info('coverage_after=%s', coverage_after)

        if args.dry_run:
            return

        rows = fetch_results(conn, entity_type=args.entity_type, entity_id=entity_id, since=since, until=until, limit=args.limit)
        LOGGER.info(
            'Final summary: coverage_before=%s parsed_new=%s assigned_after=%s final_returned=%s',
            coverage_before,
            parsed_new,
            assigned_count,
            len(rows),
        )
        for row in rows:
            LOGGER.info('Article %s | %s | created=%s', row[0], row[1], row[3])
    finally:
        conn.close()


if __name__ == '__main__':
    main()
