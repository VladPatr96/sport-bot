from __future__ import annotations

import argparse
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional, Sequence, Tuple

from zoneinfo import ZoneInfo

from bot.publish import publish_article, publish_story
from db.utils import get_conn

LOGGER = logging.getLogger(__name__)


@dataclass
class SchedulerConfig:
    interval_sec: int
    max_per_hour: int
    max_per_day: int
    quiet_hours: Optional[Tuple[int, int]]
    tzinfo: ZoneInfo
    dedup_window_days: int


def _load_config() -> SchedulerConfig:
    interval = int(os.getenv("PUBLISH_INTERVAL_SEC", "300"))
    max_hour = int(os.getenv("PUBLISH_MAX_PER_HOUR", "8"))
    max_day = int(os.getenv("PUBLISH_MAX_PER_DAY", "40"))
    dedup_days = int(os.getenv("DEDUP_WINDOW_DAYS", "3"))
    tz_name = os.getenv("TZ", "UTC")
    try:
        tzinfo = ZoneInfo(tz_name)
    except Exception:  # pragma: no cover - fallback for invalid TZ
        LOGGER.warning("Unknown TZ=%s, falling back to UTC", tz_name)
        tzinfo = ZoneInfo("UTC")

    quiet_raw = os.getenv("PUBLISH_QUIET_HOURS", "")
    quiet_hours: Optional[Tuple[int, int]] = None
    if quiet_raw:
        try:
            start_s, end_s = quiet_raw.split("-", 1)
            quiet_hours = (int(start_s), int(end_s))
        except Exception:
            LOGGER.warning("Invalid PUBLISH_QUIET_HOURS=%s, ignoring", quiet_raw)
            quiet_hours = None

    return SchedulerConfig(
    interval_sec=interval,
    max_per_hour=max_hour,
    max_per_day=max_day,
    quiet_hours=quiet_hours,
    tzinfo=tzinfo,
    dedup_window_days=dedup_days,
)


def _is_quiet(now_local: datetime, quiet_hours: Optional[Tuple[int, int]]) -> bool:
    if not quiet_hours:
        return False
    start, end = quiet_hours
    hour = now_local.hour
    if start == end:
        return False
    if start < end:
        return start <= hour < end
    return hour >= start or hour < end


def _iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _parse_iso(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def enqueue_recent_stories(
    *,
    limit: int,
    since_days: int,
    priority: int,
    scheduled_at: Optional[str],
    dry_run: bool,
    config: SchedulerConfig,
) -> Tuple[int, int]:
    conn = get_conn()
    inserted = 0
    skipped = 0
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

        window_start = datetime.now(timezone.utc) - timedelta(days=config.dedup_window_days)
        window_iso = window_start.replace(microsecond=0).isoformat(timespec="seconds")

        for row in rows:
            story_id = row["id"]
            dedup_key = f"story:{story_id}"
            existing = conn.execute(
                """
                SELECT id, status, enqueued_at, sent_at
                FROM publish_queue
                WHERE dedup_key = ?
                  AND COALESCE(sent_at, enqueued_at) >= ?
                """,
                (dedup_key, window_iso),
            ).fetchone()
            if existing:
                skipped += 1
                LOGGER.warning(
                    "Skipped enqueue story_id=%s dedup_key=%s reason=dedup",
                    story_id,
                    dedup_key,
                )
                continue

            if dry_run:
                LOGGER.info(
                    "DRY-RUN: would enqueue story_id=%s priority=%s scheduled_at=%s",
                    story_id,
                    priority,
                    scheduled_at,
                )
                inserted += 1
                continue

            conn.execute(
                """
                INSERT INTO publish_queue (item_type, item_id, priority, scheduled_at, dedup_key)
                VALUES ('story', ?, ?, ?, ?)
                """,
                (story_id, priority, scheduled_at, dedup_key),
            )
            inserted += 1
            LOGGER.info(
                "Enqueued story_id=%s priority=%s scheduled_at=%s dedup_key=%s",
                story_id,
                priority,
                scheduled_at,
                dedup_key,
            )

        if not dry_run and inserted:
            conn.commit()
    finally:
        conn.close()

    return inserted, skipped


def _dequeue_next(conn, now_iso: str):
    row = conn.execute(
        """
        SELECT *
        FROM publish_queue
        WHERE status = 'queued'
          AND (scheduled_at IS NULL OR scheduled_at <= ?)
        ORDER BY priority DESC, enqueued_at ASC
        LIMIT 1
        """,
        (now_iso,),
    ).fetchone()
    return row


def _check_rate_limits(conn, now_utc: datetime, config: SchedulerConfig) -> Tuple[bool, str]:
    last_sent = conn.execute(
        """
        SELECT sent_at
        FROM publish_queue
        WHERE status = 'sent' AND sent_at IS NOT NULL
        ORDER BY sent_at DESC
        LIMIT 1
        """
    ).fetchone()
    if last_sent and last_sent["sent_at"]:
        last_dt = _parse_iso(last_sent["sent_at"])
        if last_dt and (now_utc - last_dt).total_seconds() < config.interval_sec:
            return False, "interval"

    hour_threshold = now_utc - timedelta(hours=1)
    hour_count = conn.execute(
        """
        SELECT COUNT(*) FROM publish_queue
        WHERE status = 'sent' AND sent_at >= ?
        """,
        (hour_threshold.replace(microsecond=0).isoformat(timespec="seconds"),),
    ).fetchone()[0]
    if hour_count >= config.max_per_hour:
        return False, "hour"

    day_threshold = now_utc - timedelta(days=1)
    day_count = conn.execute(
        """
        SELECT COUNT(*) FROM publish_queue
        WHERE status = 'sent' AND sent_at >= ?
        """,
        (day_threshold.replace(microsecond=0).isoformat(timespec="seconds"),),
    ).fetchone()[0]
    if day_count >= config.max_per_day:
        return False, "day"

    return True, ""


def mark_status(
    conn,
    queue_id: int,
    status: str,
    *,
    error: Optional[str] = None,
    message_id: Optional[str] = None,
) -> None:
    params = {
        "status": status,
        "error": error,
        "message_id": message_id,
        "sent_at": _iso_now() if status == "sent" else None,
        "id": queue_id,
    }
    conn.execute(
        """
        UPDATE publish_queue
        SET status = :status,
            error = :error,
            message_id = :message_id,
            sent_at = CASE WHEN :sent_at IS NOT NULL THEN :sent_at ELSE sent_at END
        WHERE id = :id
        """,
        params,
    )
    conn.commit()


def _send_queue_item(row, *, dry_run: bool, mode: str) -> Tuple[bool, Optional[str]]:
    item_type = row["item_type"]
    item_id = row["item_id"]
    token = os.getenv("TG_BOT_TOKEN", "")
    channel_id_env = os.getenv("TG_CHANNEL_ID")
    chat_id = int(channel_id_env) if channel_id_env else None

    if not dry_run and (not token or chat_id is None):
        LOGGER.error("Missing TG_BOT_TOKEN or TG_CHANNEL_ID in environment")
        return False, None

    try:
        if item_type == "story":
            message_ids = publish_story(
                item_id,
                dry_run=dry_run,
                mode=mode,
                token=token,
                chat_id=chat_id,
            )
        elif item_type == "article":
            message_ids = publish_article(
                item_id,
                dry_run=dry_run,
                mode=mode,
                token=token,
                chat_id=chat_id,
            )
        else:
            LOGGER.error("Unknown item_type=%s for queue_id=%s", item_type, row["id"])
            return False, None
    except Exception as exc:  # pragma: no cover - network errors
        LOGGER.error("Failed to publish %s #%s: %s", item_type, item_id, exc)
        if not dry_run:
            raise
        return False, None

    msg_id = str(message_ids[0]) if message_ids else None
    return True, msg_id


def process_once(
    *,
    dry_run: bool,
    mode: str,
    config: SchedulerConfig,
) -> bool:
    conn = get_conn()
    try:
        now_utc = datetime.now(timezone.utc)
        now_local = now_utc.astimezone(config.tzinfo)
        if not dry_run and _is_quiet(now_local, config.quiet_hours):
            LOGGER.info("Skipped sending due to quiet hours (local time %s)", now_local.strftime("%H:%M"))
            return False

        now_iso = now_utc.replace(microsecond=0).isoformat(timespec="seconds")
        row = _dequeue_next(conn, now_iso)
        if not row:
            LOGGER.info("No queued items ready for publishing")
            return False

        if not dry_run:
            ok, reason = _check_rate_limits(conn, now_utc, config)
            if not ok:
                LOGGER.info("Rate limit (%s) hit, keeping queue item id=%s", reason, row["id"])
                return False

        LOGGER.info(
            "%s queue_id=%s item=%s#%s",
            "DRY-RUN: would send" if dry_run else "Dequeued",
            row["id"],
            row["item_type"],
            row["item_id"],
        )

        if dry_run:
            success, _ = _send_queue_item(row, dry_run=True, mode=mode)
            return success

        try:
            success, msg_id = _send_queue_item(row, dry_run=False, mode=mode)
        except Exception as exc:  # pragma: no cover - network errors
            mark_status(conn, row["id"], "error", error=str(exc))
            return False

        if success:
            mark_status(conn, row["id"], "sent", message_id=msg_id)
            LOGGER.info("Sent queue_id=%s msg_id=%s", row["id"], msg_id)
        else:
            mark_status(conn, row["id"], "error", error="dispatch failed")
        return success
    finally:
        conn.close()


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Telegram publish scheduler")
    parser.add_argument("--enqueue-recent", action="store_true", help="Enqueue recent stories")
    parser.add_argument("--limit", type=int, default=50, help="Limit for enqueue or fetch")
    parser.add_argument("--since-days", type=int, default=2, help="Window in days for enqueue")
    parser.add_argument("--priority", type=int, default=0, help="Priority for enqueued items")
    parser.add_argument("--run-once", action="store_true", help="Process queue once")
    parser.add_argument("--loop", action="store_true", help="Run scheduler loop")
    parser.add_argument("--dry-run", action="store_true", help="Preview actions without DB/send changes")
    parser.add_argument("--mode", choices=("html", "markdown"), default="html", help="Parse mode for Telegram messages")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    config = _load_config()

    if args.enqueue_recent:
        inserted, skipped = enqueue_recent_stories(
            limit=args.limit,
            since_days=args.since_days,
            priority=args.priority,
            scheduled_at=None,
            dry_run=args.dry_run,
            config=config,
        )
        LOGGER.info("enqueue_recent: enqueued=%s skipped=%s", inserted, skipped)

    if args.run_once:
        process_once(dry_run=args.dry_run, mode=args.mode, config=config)

    if args.loop:
        LOGGER.info("Starting scheduler loop (interval=%ss)", config.interval_sec)
        try:
            while True:
                processed = process_once(dry_run=args.dry_run, mode=args.mode, config=config)
                LOGGER.info("Sleeping for %s seconds", config.interval_sec)
                time.sleep(config.interval_sec)
        except KeyboardInterrupt:
            LOGGER.info("Scheduler loop interrupted by user")


if __name__ == "__main__":
    main()
