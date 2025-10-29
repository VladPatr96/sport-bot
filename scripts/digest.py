from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path
from typing import List, Optional, Sequence, Tuple

from db.utils import get_conn
from webapp.digest_service import (
    build_dataset,
    build_telegram_messages,
    default_window,
    parse_date,
    send_digest_messages,
    store_digest,
    update_digest_status,
    write_exports,
)


def _parse_formats(value: str) -> List[str]:
    if value == "both":
        return ["md", "html"]
    return [value]


def parse_delta(period: str):
    from datetime import timedelta

    return timedelta(days=7 if period == "weekly" else 1)


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Generate and send digests.")
    parser.add_argument("--period", choices=("daily", "weekly"), default="daily")
    parser.add_argument("--since", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--until", help="End date (YYYY-MM-DD)")
    parser.add_argument("--limit", type=int, default=int(os.getenv("DIGEST_DEFAULT_LIMIT", "25")))
    parser.add_argument("--format", choices=("html", "md", "both"), default="both")
    parser.add_argument("--out-dir", default="exports")
    parser.add_argument("--send", action="store_true", help="Send digest to Telegram")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    if args.since:
        since = parse_date(args.since)
        until = parse_date(args.until) if args.until else since + parse_delta(args.period)
    else:
        since, until = default_window(args.period)

    dataset = build_dataset(args.period, since, until, limit=args.limit)
    logging.info(
        "Digest dataset built period=%s window=%s..%s stories=%s",
        dataset["period"],
        dataset["since"],
        dataset["until"],
        dataset.get("count"),
    )

    if dataset.get("count", 0) == 0:
        logging.info("No stories in window; nothing to export.")
        return

    formats = _parse_formats(args.format)
    written = write_exports(dataset, formats, Path(args.out_dir))
    for path in written:
        logging.info("Exported %s", path)

    if args.dry_run:
        logging.info("Dry-run mode: skipping database writes and sending.")
        return

    conn = get_conn()
    try:
        digest_id = store_digest(conn, dataset, status="ready")
        logging.info("Digest stored id=%s", digest_id)

        if args.send:
            chunk_size = int(os.getenv("DIGEST_THREAD_CHUNK", "5"))
            messages = build_telegram_messages(dataset, args.period, chunk_size)
            root_id, message_ids = send_digest_messages(messages)
            update_digest_status(conn, digest_id, "sent", str(root_id))
            logging.info("Digest sent root_message_id=%s replies=%s", root_id, len(message_ids) - 1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
