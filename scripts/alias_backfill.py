from __future__ import annotations

import logging

from db.utils import get_conn
from categorizer.normalize import normalize_token

logger = logging.getLogger(__name__)


def backfill_aliases() -> None:
    conn = get_conn()
    try:
        cur = conn.cursor()
        rows = cur.execute(
            "SELECT id, alias FROM entity_aliases WHERE alias IS NOT NULL AND alias_normalized IS NULL"
        ).fetchall()

        updated = 0
        skipped = 0
        for row in rows:
            alias = row[1]
            normalized = normalize_token(alias)
            if not normalized:
                skipped += 1
                continue
            cur.execute(
                "UPDATE entity_aliases SET alias_normalized = ? WHERE id = ?",
                (normalized, row[0]),
            )
            updated += 1

        conn.commit()
        logger.info('Alias backfill completed: updated=%s skipped=%s', updated, skipped)
    finally:
        conn.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    backfill_aliases()


if __name__ == '__main__':
    main()
