from __future__ import annotations

import argparse
import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from db.utils import get_conn
from bot.sender import init_bot, send_text

LOGGER = logging.getLogger(__name__)

METRIC_DEFINITIONS = [
    ("news.ingested_1h", "1h"),
    ("news.ingested_24h", "24h"),
    ("stories.built_24h", "24h"),
    ("queue.size_queued", None),
    ("queue.sent_1h", "1h"),
    ("queue.sent_24h", "24h"),
    ("tg.rate_limit_hits_1h", "1h"),
    ("digest.sent_24h", "24h"),
]


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _parse_env_float(name: str, default: Optional[float]) -> Optional[float]:
    value = os.getenv(name)
    if not value:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        LOGGER.warning("Invalid %s=%s, using default %s", name, value, default)
        return default


def _parse_env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _select_scalar(conn, query: str, params: Sequence[object]) -> float:
    row = conn.execute(query, params).fetchone()
    if not row:
        return 0.0
    value = row[0]
    return float(value or 0)


def collect_metrics(conn) -> List[Dict[str, object]]:
    now = utcnow()
    iso_now = now.replace(microsecond=0).isoformat(timespec="seconds")
    since_1h = (now - timedelta(hours=1)).replace(microsecond=0).isoformat(timespec="seconds")
    since_24h = (now - timedelta(hours=24)).replace(microsecond=0).isoformat(timespec="seconds")

    metrics: List[Dict[str, object]] = []

    metrics.append(
        {
            "metric": "news.ingested_1h",
            "value": _select_scalar(
                conn,
                "SELECT COUNT(*) FROM news WHERE created_at >= ?",
                (since_1h,),
            ),
            "meta": {"window": "1h", "generated_at": iso_now},
        }
    )
    metrics.append(
        {
            "metric": "news.ingested_24h",
            "value": _select_scalar(
                conn,
                "SELECT COUNT(*) FROM news WHERE created_at >= ?",
                (since_24h,),
            ),
            "meta": {"window": "24h", "generated_at": iso_now},
        }
    )
    metrics.append(
        {
            "metric": "stories.built_24h",
            "value": _select_scalar(
                conn,
                "SELECT COUNT(*) FROM stories WHERE updated_at >= ?",
                (since_24h,),
            ),
            "meta": {"window": "24h", "generated_at": iso_now},
        }
    )
    metrics.append(
        {
            "metric": "queue.size_queued",
            "value": _select_scalar(
                conn,
                "SELECT COUNT(*) FROM publish_queue WHERE status = 'queued'",
                (),
            ),
            "meta": {"window": "snapshot", "generated_at": iso_now},
        }
    )
    metrics.append(
        {
            "metric": "queue.sent_1h",
            "value": _select_scalar(
                conn,
                "SELECT COUNT(*) FROM publish_queue WHERE status = 'sent' AND sent_at >= ?",
                (since_1h,),
            ),
            "meta": {"window": "1h", "generated_at": iso_now},
        }
    )
    metrics.append(
        {
            "metric": "queue.sent_24h",
            "value": _select_scalar(
                conn,
                "SELECT COUNT(*) FROM publish_queue WHERE status = 'sent' AND sent_at >= ?",
                (since_24h,),
            ),
            "meta": {"window": "24h", "generated_at": iso_now},
        }
    )
    metrics.append(
        {
            "metric": "tg.rate_limit_hits_1h",
            "value": 0.0,
            "meta": {"window": "1h", "generated_at": iso_now, "note": "stub"},
        }
    )
    metrics.append(
        {
            "metric": "digest.sent_24h",
            "value": _select_scalar(
                conn,
                "SELECT COUNT(*) FROM digests WHERE status = 'sent' AND created_at >= ?",
                (since_24h,),
            ),
            "meta": {"window": "24h", "generated_at": iso_now},
        }
    )
    return metrics


def store_metrics(conn, metrics: Iterable[Dict[str, object]]) -> None:
    conn.executemany(
        """
        INSERT INTO monitor_logs (metric, value, meta)
        VALUES (?, ?, ?)
        """,
        (
            (
                metric["metric"],
                float(metric["value"]),
                json.dumps(metric.get("meta", {}), ensure_ascii=False),
            )
            for metric in metrics
        ),
    )
    conn.commit()


def log_metrics(metrics: List[Dict[str, object]]) -> None:
    summary = ", ".join(f"{m['metric']}={int(m['value']) if float(m['value']).is_integer() else m['value']}" for m in metrics)
    LOGGER.info("metrics: %s", summary)


def evaluate_alerts(metrics: List[Dict[str, object]]) -> List[str]:
    if not _parse_env_bool("ALERT_ENABLED", True):
        return []
    thresholds = {
        "news.ingested_1h": ("min", _parse_env_float("ALERT_NEWS_MIN_1H", None)),
        "queue.size_queued": ("max", _parse_env_float("ALERT_QUEUE_MAX", None)),
        "queue.sent_24h": ("min", _parse_env_float("ALERT_SENT_MIN_24H", None)),
    }
    metric_map = {m["metric"]: float(m["value"]) for m in metrics}
    triggered: List[str] = []
    for metric, (kind, threshold) in thresholds.items():
        if threshold is None:
            continue
        value = metric_map.get(metric)
        if value is None:
            continue
        if kind == "min" and value < threshold:
            triggered.append(f"{metric}={value:g} < {threshold:g}")
        elif kind == "max" and value > threshold:
            triggered.append(f"{metric}={value:g} > {threshold:g}")
    return triggered


def send_alerts(messages: List[str]) -> None:
    if not messages:
        return
    target_chat = os.getenv("ALERT_CHAT_ID") or os.getenv("TG_CHANNEL_ID")
    token = os.getenv("TG_BOT_TOKEN")
    if not token or not target_chat:
        LOGGER.warning("Alert triggered but TG_BOT_TOKEN or chat id is missing; skipping send.")
        return
    chat_id = int(target_chat)
    bot = init_bot(token)
    text = "⚠️ Monitor alerts:\n" + "\n".join(messages)
    try:
        asyncio.run(send_text(bot, chat_id, text, parse_mode="HTML", disable_web_page_preview=True))
        LOGGER.warning("Alert sent: %s", "; ".join(messages))
    finally:
        try:
            asyncio.run(bot.session.close())
        except RuntimeError:
            pass


def run_once(dry_run: bool = False) -> None:
    conn = get_conn()
    try:
        metrics = collect_metrics(conn)
        log_metrics(metrics)
        if not dry_run:
            store_metrics(conn, metrics)
            alerts = evaluate_alerts(metrics)
            if alerts:
                send_alerts(alerts)
    finally:
        conn.close()


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Monitor metrics collector.")
    parser.add_argument("--once", action="store_true", help="Run once and exit (default).")
    parser.add_argument("--loop", action="store_true", help="Run continuously.")
    parser.add_argument("--interval", type=int, default=300, help="Loop interval in seconds (default: 300).")
    parser.add_argument("--dry-run", action="store_true", help="Do not write to DB or send alerts.")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    if args.loop:
        LOGGER.info("Monitor loop started (interval=%ss)", args.interval)
        try:
            while True:
                try:
                    run_once(dry_run=args.dry_run)
                except Exception as exc:
                    LOGGER.exception("Monitor run failed: %s", exc)
                time.sleep(max(1, args.interval))
        except KeyboardInterrupt:
            LOGGER.info("Monitor loop interrupted by user.")
    else:
        run_once(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
