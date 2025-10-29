from __future__ import annotations

import argparse
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List

from cluster.fingerprints import compute_signatures
from db.utils import get_conn

LOGGER = logging.getLogger(__name__)


def _fetch_news(conn, since_days: int, limit: int) -> List[dict]:
    threshold = datetime.now(timezone.utc) - timedelta(days=since_days)
    threshold_iso = threshold.replace(microsecond=0).isoformat(timespec="seconds")
    rows = conn.execute(
        """
        SELECT id, title, COALESCE(published, published_at, created_at) AS published_at
        FROM news
        WHERE COALESCE(published, published_at, created_at) >= ?
        ORDER BY COALESCE(published, published_at, created_at) DESC
        LIMIT ?
        """,
        (threshold_iso, limit),
    ).fetchall()
    return [dict(row) for row in rows]


def _fetch_entities(conn, news_ids: List[int]) -> Dict[int, Dict[str, str]]:
    if not news_ids:
        return {}
    placeholders = ",".join("?" for _ in news_ids)
    tag_rows = conn.execute(
        f"""
        SELECT nat.news_id, t.type, t.name
        FROM news_article_tags nat
        JOIN tags t ON t.id = nat.tag_id
        WHERE nat.news_id IN ({placeholders})
          AND t.type IN ('sport','tournament','team','player')
        """,
        tuple(news_ids),
    ).fetchall()
    result: Dict[int, Dict[str, str]] = {news_id: {} for news_id in news_ids}
    for row in tag_rows:
        bucket = result.setdefault(row["news_id"], {})
        bucket.setdefault(row["type"], row["name"])
    return result


def backfill_fingerprints(since_days: int, limit: int) -> None:
    conn = get_conn()
    processed = 0
    upserted = 0
    try:
        news_items = _fetch_news(conn, since_days, limit)
        entities_map = _fetch_entities(conn, [item["id"] for item in news_items])

        for item in news_items:
            processed += 1
            title = item["title"] or ""
            if not title.strip():
                LOGGER.debug("Skipping news_id=%s due to empty title", item["id"])
                continue

            entities = entities_map.get(item["id"], {})
            sport = entities.get("sport")
            tournament = entities.get("tournament")
            team = entities.get("team")
            player = entities.get("player")
            title_sig, entity_sig = compute_signatures(
                title,
                {
                    "sport": sport,
                    "tournament": tournament,
                    "team": team,
                    "player": player,
                },
            )
            conn.execute(
                """
                INSERT INTO content_fingerprints (news_id, title_sig, entity_sig, created_at)
                VALUES (?, ?, ?, STRFTIME('%Y-%m-%dT%H:%M:%SZ','now'))
                ON CONFLICT(news_id) DO UPDATE SET
                    title_sig = excluded.title_sig,
                    entity_sig = excluded.entity_sig
                """,
                (item["id"], title_sig, entity_sig),
            )
            upserted += 1
            LOGGER.debug(
                "Upserted fingerprint news_id=%s title_sig=%s entity_sig=%s",
                item["id"],
                title_sig,
                entity_sig,
            )

        conn.commit()
        LOGGER.info("processed=%s upserted=%s", processed, upserted)
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill content fingerprints")
    parser.add_argument("--since-days", type=int, default=7)
    parser.add_argument("--limit", type=int, default=1000)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    backfill_fingerprints(args.since_days, args.limit)


if __name__ == "__main__":
    main()
