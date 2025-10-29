
from __future__ import annotations

import argparse
import logging
from datetime import datetime, timedelta, UTC
from pathlib import Path
from typing import Dict

from categorizer.alias_mapper import assign_entities_for_article
from db.utils import get_conn

LOGGER = logging.getLogger(__name__)
LOG_DIR = Path(__file__).resolve().parents[1] / "database" / "logs"


def _write_log(path: Path, lines) -> None:
    if not lines:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        for line in lines:
            fh.write(f"{line}\n")
    LOGGER.info("Appended %s records to %s", len(lines), path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Assign canonical entities to news based on alias mappings")
    parser.add_argument("--since-days", type=int, default=2)
    parser.add_argument("--limit", type=int, default=200)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    cutoff = datetime.now(UTC) - timedelta(days=max(args.since_days, 0))
    cutoff_iso = cutoff.astimezone(UTC).replace(tzinfo=None).isoformat(timespec="seconds")

    conn = get_conn()
    try:
        cur = conn.cursor()
        rows = cur.execute(
            """
            SELECT id, created_at
              FROM news
             WHERE created_at >= ?
             ORDER BY created_at DESC
             LIMIT ?
            """,
            (cutoff_iso, args.limit),
        ).fetchall()

        if not rows:
            LOGGER.info("No news rows found for since-days=%s", args.since_days)
            return

        stats: Dict[str, int] = {
            "processed": 0,
            "unknown": 0,
            "conflicts": 0,
        }
        assigned_totals: Dict[str, int] = {t: 0 for t in ("sport", "tournament", "team", "player")}
        unknown_lines = []
        conflict_lines = []

        for row in rows:
            news_id = row["id"] if hasattr(row, "keys") else row[0]
            result = assign_entities_for_article(conn, news_id=news_id, prefer_existing=True)
            stats["processed"] += 1
            for etype, flag in result["assigned"].items():
                assigned_totals[etype] += flag
            if result["unknown"]:
                stats["unknown"] += len(result["unknown"])
                for item in result["unknown"]:
                    unknown_lines.append(
                        f"{news_id}	{item.get('alias','')}	{item.get('type','')}	{item.get('tag_id','')}"
                    )
            if result["conflicts"]:
                stats["conflicts"] += len(result["conflicts"])
                for item in result["conflicts"]:
                    conflict_lines.append(
                        f"news_id={news_id}	type={item.get('type')}	entity_ids={item.get('entity_ids')}	aliases={item.get('aliases')}"
                    )
            if args.verbose:
                LOGGER.info(
                    "news_id=%s assigned=%s unknown=%s conflicts=%s",
                    news_id,
                    result["assigned"],
                    len(result["unknown"]),
                    len(result["conflicts"]),
                )

        LOGGER.info(
            "Assign summary: processed=%s sport=%s tournament=%s team=%s player=%s unknown=%s conflicts=%s",
            stats["processed"],
            assigned_totals["sport"],
            assigned_totals["tournament"],
            assigned_totals["team"],
            assigned_totals["player"],
            stats["unknown"],
            stats["conflicts"],
        )

        timestamp = datetime.now(UTC).strftime("%Y%m%d")
        if unknown_lines:
            _write_log(LOG_DIR / f"unknown_aliases_{timestamp}.log", unknown_lines)
        if conflict_lines:
            _write_log(LOG_DIR / f"conflicts_{timestamp}.log", conflict_lines)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
