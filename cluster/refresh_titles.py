from __future__ import annotations

import argparse
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, Sequence

from cluster.title_refiner import build_article_payload, compute_story_title
from db.utils import get_conn

LOGGER = logging.getLogger(__name__)


def _load_story_article_ids(conn, story_id: int) -> list[int]:
    rows = conn.execute(
        """
        SELECT n.id
        FROM story_articles sa
        JOIN news n ON n.id = sa.news_id
        WHERE sa.story_id = ?
        ORDER BY COALESCE(n.published, n.published_at, n.created_at) DESC
        """,
        (story_id,),
    ).fetchall()
    return [row["id"] for row in rows]


def refresh_titles(
    *,
    since_days: int,
    limit: int,
    dry_run: bool,
) -> None:
    conn = get_conn()
    try:
        threshold = datetime.now(timezone.utc) - timedelta(days=since_days)
        threshold_iso = threshold.replace(microsecond=0).isoformat(timespec="seconds")
        rows = conn.execute(
            """
            SELECT id, title, updated_at
            FROM stories
            WHERE updated_at >= ?
            ORDER BY updated_at DESC
            LIMIT ?
            """,
            (threshold_iso, limit),
        ).fetchall()

        processed = 0
        updated = 0
        unchanged = 0

        for row in rows:
            story_id = row["id"]
            old_title = row["title"] or ""
            article_ids = _load_story_article_ids(conn, story_id)
            if not article_ids:
                LOGGER.warning("Story %s has no linked articles", story_id)
                continue
            articles = build_article_payload(conn, article_ids)
            if not articles:
                LOGGER.warning("Failed to build article payload for story %s", story_id)
                continue

            new_title = compute_story_title(articles) or old_title
            processed += 1

            old_norm = " ".join(old_title.split()).lower()
            new_norm = " ".join(new_title.split()).lower()

            if old_norm == new_norm:
                unchanged += 1
                continue

            if dry_run:
                LOGGER.info('ID=%s old→new: "%s" → "%s"', story_id, old_title, new_title)
                updated += 1
                continue

            conn.execute(
                "UPDATE stories SET title = ?, updated_at = ? WHERE id = ?",
                (
                    new_title,
                    datetime.now(timezone.utc).replace(microsecond=0).isoformat(timespec="seconds"),
                    story_id,
                ),
            )
            updated += 1
            LOGGER.info('ID=%s updated: "%s" → "%s"', story_id, old_title, new_title)

        if not dry_run and conn.in_transaction:
            conn.commit()

        LOGGER.info(
            "processed=%s updated=%s unchanged=%s",
            processed,
            updated,
            unchanged,
        )
    finally:
        conn.close()


def run(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Refresh story titles")
    parser.add_argument("--since-days", type=int, default=7, help="Window in days for updated stories")
    parser.add_argument("--limit", type=int, default=200, help="Limit number of stories to check")
    parser.add_argument("--dry-run", action="store_true", help="Preview title changes without updating DB")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    refresh_titles(
        since_days=args.since_days,
        limit=args.limit,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    run()
