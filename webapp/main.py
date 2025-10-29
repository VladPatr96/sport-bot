from __future__ import annotations

import asyncio
import html
import json
import logging
import os
import re
import secrets
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Sequence, Tuple
from urllib.parse import urlencode

from fastapi import Body, Depends, FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from zoneinfo import ZoneInfo

from bot.publish import publish_story, render_story_message, store_publish_map, _compose_story_chunks
from bot.publisher import render_story_update
from bot.scheduler import mark_status
from bot.sender import edit_text, init_bot, reply_text
from cluster.antidup import filter_near_duplicates
from cluster.fingerprints import compute_signatures
from db.utils import get_conn
from webapp.digest_render import render_html as render_digest_html_doc, render_markdown as render_digest_markdown
from webapp.digest_service import (
    build_dataset,
    build_filename,
    build_telegram_messages,
    default_window,
    load_digest_dataset,
    parse_date,
    send_digest_messages,
    store_digest,
    update_digest_status,
)

LOGGER = logging.getLogger(__name__)
app = FastAPI(title="Story Publisher")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))
static_path = os.path.join(BASE_DIR, "static")
app.mount("/static", StaticFiles(directory=static_path), name="static")

security = HTTPBasic()
AUTH_ENV = os.getenv("WEBAPP_BASIC_AUTH", "")


class SchedulerConfig:
    def __init__(
        self,
        interval_sec: int,
        max_per_hour: int,
        max_per_day: int,
        quiet_hours: Optional[Tuple[int, int]],
        tzinfo: ZoneInfo,
        dedup_window_days: int,
    ):
        self.interval_sec = interval_sec
        self.max_per_hour = max_per_hour
        self.max_per_day = max_per_day
        self.quiet_hours = quiet_hours
        self.tzinfo = tzinfo
        self.dedup_window_days = dedup_window_days


def load_scheduler_config() -> SchedulerConfig:
    interval = int(os.getenv("PUBLISH_INTERVAL_SEC", "300"))
    max_hour = int(os.getenv("PUBLISH_MAX_PER_HOUR", "8"))
    max_day = int(os.getenv("PUBLISH_MAX_PER_DAY", "40"))
    dedup_days = int(os.getenv("DEDUP_WINDOW_DAYS", "3"))
    tz_name = os.getenv("TZ", "UTC")
    try:
        tzinfo = ZoneInfo(tz_name)
    except Exception:
        LOGGER.warning("Unknown TZ=%s, defaulting to UTC", tz_name)
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


CONFIG = load_scheduler_config()


if AUTH_ENV:
    def require_auth(credentials: HTTPBasicCredentials = Depends(security)) -> None:
        try:
            username, password = AUTH_ENV.split(":", 1)
        except ValueError:
            LOGGER.error("Invalid WEBAPP_BASIC_AUTH format, expected user:pass")
            raise HTTPException(status_code=500, detail="Auth misconfigured")
        correct_username = secrets.compare_digest(credentials.username, username)
        correct_password = secrets.compare_digest(credentials.password, password)
        if not (correct_username and correct_password):
            raise HTTPException(status_code=401, detail="Unauthorized")
else:
    def require_auth() -> None:
        return None


def _format_local(dt_str: Optional[str]) -> str:
    if not dt_str:
        return ""
    try:
        dt = datetime.fromisoformat(dt_str)
    except ValueError:
        return dt_str
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt_local = dt.astimezone(CONFIG.tzinfo)
    return dt_local.strftime("%Y-%m-%d %H:%M")


DEFAULT_SORT = "updated_desc"
MAX_PER_PAGE = 100
LIKE_ESCAPE = "^"
TELEGRAM_LIMIT = 4096
MONITOR_METRICS = [
    ("news.ingested_1h", "1h"),
    ("news.ingested_24h", "24h"),
    ("stories.built_24h", "24h"),
    ("queue.size_queued", None),
    ("queue.sent_1h", "1h"),
    ("queue.sent_24h", "24h"),
    ("tg.rate_limit_hits_1h", "1h"),
    ("digest.sent_24h", "24h"),
]
ENTITY_CONFIG = {
    "sport": {"table": "sports", "column": "sport_id", "icon": "🏅"},
    "tournament": {"table": "tournaments", "column": "tournament_id", "icon": "🏆"},
    "team": {"table": "teams", "column": "team_id", "icon": "🏟️"},
    "player": {"table": "players", "column": "player_id", "icon": "👤"},
}
SORT_SQL = {
    "updated_desc": "s.updated_at DESC, s.id DESC",
    "size_desc": "article_count DESC, s.updated_at DESC, s.id DESC",
    "title_asc": "LOWER(s.title) ASC, s.id ASC",
}
SORT_LABELS = {
    "updated_desc": "По обновлению (новые сверху)",
    "size_desc": "По размеру истории (по убыванию)",
    "title_asc": "По названию (A→Я)",
}


def _sanitize_like_term(term: str) -> str:
    escaped = term.replace(LIKE_ESCAPE, LIKE_ESCAPE * 2)
    escaped = escaped.replace("%", f"{LIKE_ESCAPE}%").replace("_", f"{LIKE_ESCAPE}_")
    return escaped


def _normalize_iso_param(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt_utc = dt.astimezone(timezone.utc)
    return dt_utc.replace(microsecond=0).isoformat(timespec="seconds")


def highlight(text: str, query: Optional[str]) -> str:
    if not text:
        return ""
    if not query:
        return html.escape(text)
    pattern = re.compile(re.escape(query), re.IGNORECASE)
    last_idx = 0
    parts: List[str] = []
    for match in pattern.finditer(text):
        start, end = match.span()
        if start > last_idx:
            parts.append(html.escape(text[last_idx:start]))
        parts.append(f"<mark>{html.escape(match.group(0))}</mark>")
        last_idx = end
    if last_idx < len(text):
        parts.append(html.escape(text[last_idx:]))
    return "".join(parts)


def _coerce_query_value(value: object) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _build_query_string(base: Dict[str, str], **updates: object) -> str:
    merged = {key: value for key, value in base.items() if value not in ("", None)}
    for key, value in updates.items():
        if value in (None, ""):
            merged.pop(key, None)
            continue
        merged[key] = _coerce_query_value(value)
    if not merged:
        return "?"
    return f"?{urlencode(merged, doseq=True)}"


def _resolve_publish_target(conn, item_type: str, item_id: int) -> Tuple[Optional[int], Optional[str], Optional[str]]:
    row = conn.execute(
        """
        SELECT message_id, text, mode
        FROM publish_map
        WHERE item_type = ? AND item_id = ?
        """,
        (item_type, item_id),
    ).fetchone()
    if row and row["message_id"]:
        try:
            message_id = int(row["message_id"])
        except (TypeError, ValueError):
            message_id = None
        return message_id, row["text"], row["mode"]

    row = conn.execute(
        """
        SELECT message_id
        FROM publish_queue
        WHERE item_type = ?
          AND item_id = ?
          AND status = 'sent'
          AND message_id IS NOT NULL
        ORDER BY COALESCE(sent_at, enqueued_at) DESC, id DESC
        LIMIT 1
        """,
        (item_type, item_id),
    ).fetchone()
    if row and row["message_id"]:
        try:
            message_id = int(row["message_id"])
        except (TypeError, ValueError):
            message_id = None
        return message_id, None, None
    return None, None, None


def _ensure_telegram_env() -> Tuple[str, int]:
    token = os.getenv("TG_BOT_TOKEN")
    channel = os.getenv("TG_CHANNEL_ID")
    if not token or not channel:
        raise HTTPException(status_code=500, detail="Missing TG_BOT_TOKEN or TG_CHANNEL_ID")
    try:
        chat_id = int(channel)
    except ValueError as exc:
        raise HTTPException(status_code=500, detail="Invalid TG_CHANNEL_ID") from exc
    return token, chat_id


def _validate_telegram_text(text: str) -> None:
    if len(text) > TELEGRAM_LIMIT:
        raise HTTPException(
            status_code=400,
            detail=f"Text is too long ({len(text)} chars, limit {TELEGRAM_LIMIT})",
        )


def _record_publish_edit(
    conn,
    *,
    item_type: str,
    item_id: int,
    action: str,
    message_id: int,
    reply_msg_id: Optional[int],
    old_text: Optional[str],
    new_text: str,
    mode: str,
    error: Optional[str] = None,
) -> None:
    conn.execute(
        """
        INSERT INTO publish_edits (
            item_type,
            item_id,
            action,
            message_id,
            reply_msg_id,
            old_text,
            new_text,
            mode,
            error
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            item_type,
            item_id,
            action,
            str(message_id),
            str(reply_msg_id) if reply_msg_id is not None else None,
            old_text,
            new_text,
            mode,
            error,
        ),
    )
    conn.commit()


def _fetch_last_append_text(conn, item_type: str, item_id: int) -> Optional[str]:
    row = conn.execute(
        """
        SELECT new_text
        FROM publish_edits
        WHERE item_type = ?
          AND item_id = ?
          AND action = 'append'
        ORDER BY created_at DESC, id DESC
        LIMIT 1
        """,
        (item_type, item_id),
    ).fetchone()
    return row["new_text"] if row else None


async def _edit_message_async(
    *,
    token: str,
    chat_id: int,
    message_id: int,
    text: str,
    parse_mode: str,
):
    bot = init_bot(token)
    try:
        return await edit_text(
            bot,
            chat_id,
            message_id,
            text,
            parse_mode=parse_mode,
        )
    finally:
        await bot.session.close()


async def _append_message_async(
    *,
    token: str,
    chat_id: int,
    reply_to: int,
    text: str,
    parse_mode: str,
):
    bot = init_bot(token)
    try:
        return await reply_text(
            bot,
            chat_id,
            reply_to,
            text,
            parse_mode=parse_mode,
        )
    finally:
        await bot.session.close()


def _query_stories(
    conn,
    *,
    q: Optional[str],
    sport: Optional[int],
    tournament: Optional[int],
    team: Optional[int],
    player: Optional[int],
    since: Optional[str],
    until: Optional[str],
    has_neardups: Optional[bool],
    in_queue: Optional[bool],
    sort: str,
    page: int,
    per_page: int,
) -> Tuple[List[dict], int]:
    where_clauses: List[str] = []
    params: List[object] = []

    if q:
        sanitized = _sanitize_like_term(q.strip())
        pattern = f"%{sanitized.lower()}%"
        where_clauses.append(
            "("
            "LOWER(s.title) LIKE ? ESCAPE '^'"
            " OR EXISTS ("
            "SELECT 1 FROM story_articles sa_q "
            "JOIN news n_q ON n_q.id = sa_q.news_id "
            "WHERE sa_q.story_id = s.id AND LOWER(n_q.title) LIKE ? ESCAPE '^'"
            ")"
            ")"
        )
        params.extend([pattern, pattern])

    if since:
        where_clauses.append("s.updated_at >= ?")
        params.append(since)
    if until:
        where_clauses.append("s.updated_at <= ?")
        params.append(until)

    entity_filters = {
        "sport": sport,
        "tournament": tournament,
        "team": team,
        "player": player,
    }
    for name, value in entity_filters.items():
        if value is None:
            continue
        column = ENTITY_CONFIG[name]["column"]
        where_clauses.append(
            "("
            "EXISTS ("
            f"SELECT 1 FROM story_articles sa_{name} "
            f"JOIN news_articles na_{name} ON na_{name}.news_id = sa_{name}.news_id "
            f"WHERE sa_{name}.story_id = s.id AND na_{name}.{column} = ?"
            ")"
            ")"
        )
        params.append(value)

    if has_neardups is True:
        where_clauses.append("df.story_id IS NOT NULL")
    elif has_neardups is False:
        where_clauses.append("df.story_id IS NULL")

    queue_exists_sql = (
        "EXISTS ("
        "SELECT 1 FROM publish_queue pq "
        "WHERE pq.item_type = 'story' AND pq.item_id = s.id AND pq.status = 'queued'"
        ")"
    )
    if in_queue is True:
        where_clauses.append(queue_exists_sql)
    elif in_queue is False:
        where_clauses.append(f"NOT ({queue_exists_sql})")

    where_sql = ""
    if where_clauses:
        where_sql = "WHERE " + " AND ".join(where_clauses)

    sort_sql = SORT_SQL.get(sort, SORT_SQL[DEFAULT_SORT])
    offset = (page - 1) * per_page

    dup_cte = """
    WITH dup_flags AS (
        SELECT DISTINCT story_id
        FROM (
            SELECT sa.story_id AS story_id
            FROM story_articles sa
            JOIN content_fingerprints cf ON cf.news_id = sa.news_id
            WHERE cf.title_sig IS NOT NULL
            GROUP BY sa.story_id, cf.title_sig
            HAVING COUNT(*) > 1

            UNION ALL

            SELECT sa.story_id AS story_id
            FROM story_articles sa
            JOIN content_fingerprints cf ON cf.news_id = sa.news_id
            WHERE cf.entity_sig IS NOT NULL
            GROUP BY sa.story_id, cf.entity_sig
            HAVING COUNT(*) > 1
        )
    )
    """

    count_sql = f"""
    {dup_cte}
    SELECT COUNT(*)
    FROM stories s
    LEFT JOIN dup_flags df ON df.story_id = s.id
    {where_sql}
    """
    total = conn.execute(count_sql, tuple(params)).fetchone()[0]

    data_sql = f"""
    {dup_cte}
    SELECT
        s.id,
        s.title,
        s.updated_at,
        COUNT(DISTINCT sa.news_id) AS article_count,
        COUNT(DISTINCT na.sport_id) AS sport_count,
        COUNT(DISTINCT na.tournament_id) AS tournament_count,
        COUNT(DISTINCT na.team_id) AS team_count,
        COUNT(DISTINCT na.player_id) AS player_count,
        CASE WHEN df.story_id IS NOT NULL THEN 1 ELSE 0 END AS has_neardups_flag,
        {queue_exists_sql} AS queued_flag
    FROM stories s
    LEFT JOIN story_articles sa ON sa.story_id = s.id
    LEFT JOIN news_articles na ON na.news_id = sa.news_id
    LEFT JOIN dup_flags df ON df.story_id = s.id
    {where_sql}
    GROUP BY s.id
    ORDER BY {sort_sql}
    LIMIT ? OFFSET ?
    """
    rows = conn.execute(data_sql, tuple(params + [per_page, offset])).fetchall()

    stories: List[dict] = []
    for row in rows:
        badges = []
        for entity_name, config in ENTITY_CONFIG.items():
            count_value = row[f"{entity_name}_count"]
            if count_value:
                badges.append(
                    {
                        "type": entity_name,
                        "icon": config["icon"],
                        "count": count_value,
                    }
                )
        stories.append(
            {
                "id": row["id"],
                "title": row["title"],
                "title_highlighted": highlight(row["title"], q),
                "updated_at": _format_local(row["updated_at"]),
                "article_count": row["article_count"],
                "badges": badges,
                "has_neardups": bool(row["has_neardups_flag"]),
                "in_queue": bool(row["queued_flag"]),
            }
        )
    return stories, total


def _fetch_entity_options(conn, limit: int = 30) -> Dict[str, List[Tuple[int, str, int]]]:
    options: Dict[str, List[Tuple[int, str, int]]] = {key: [] for key in ENTITY_CONFIG}
    for name, config in ENTITY_CONFIG.items():
        table = config["table"]
        column = config["column"]
        try:
            rows = conn.execute(
                f"""
                SELECT e.id, e.name, COUNT(*) AS usage_count
                FROM story_articles sa
                JOIN news_articles na ON na.news_id = sa.news_id
                JOIN {table} e ON e.id = na.{column}
                WHERE na.{column} IS NOT NULL
                GROUP BY e.id, e.name
                ORDER BY usage_count DESC, e.name
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        except (sqlite3.OperationalError, sqlite3.DatabaseError):
            rows = []
        options[name] = [(row["id"], row["name"], row["usage_count"]) for row in rows]
    return options


def _fetch_publish_history(conn, item_type: str, item_id: int) -> List[dict]:
    rows = conn.execute(
        """
        SELECT id, action, message_id, reply_msg_id, old_text, new_text, mode, created_at, error
        FROM publish_edits
        WHERE item_type = ?
          AND item_id = ?
        ORDER BY created_at DESC, id DESC
        """,
        (item_type, item_id),
    ).fetchall()
    history: List[dict] = []
    for row in rows:
        history.append(
            {
                "id": row["id"],
                "action": row["action"],
                "message_id": row["message_id"],
                "reply_msg_id": row["reply_msg_id"],
                "old_text": row["old_text"],
                "new_text": row["new_text"],
                "mode": row["mode"],
                "created_at": row["created_at"],
                "error": row["error"],
            }
        )
    return history


def _fetch_publish_map_entry(conn, item_type: str, item_id: int) -> Optional[dict]:
    row = conn.execute(
        """
        SELECT message_id, text, mode, sent_at
        FROM publish_map
        WHERE item_type = ? AND item_id = ?
        """,
        (item_type, item_id),
    ).fetchone()
    if not row:
        return None
    return {
        "message_id": row["message_id"],
        "text": row["text"],
        "mode": row["mode"],
        "sent_at": row["sent_at"],
    }


def _fetch_digest_rows(
    conn,
    *,
    period: Optional[str] = None,
    since: Optional[str] = None,
    until: Optional[str] = None,
) -> List[dict]:
    clauses = []
    params: List[object] = []
    if period in {"daily", "weekly"}:
        clauses.append("d.period = ?")
        params.append(period)
    if since:
        clauses.append("d.since_utc >= ?")
        params.append(since)
    if until:
        clauses.append("d.until_utc <= ?")
        params.append(until)
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    rows = conn.execute(
        f"""
        SELECT
            d.id,
            d.period,
            d.since_utc,
            d.until_utc,
            d.title,
            d.status,
            d.message_id,
            d.created_at,
            COUNT(di.story_id) AS story_count
        FROM digests d
        LEFT JOIN digest_items di ON di.digest_id = d.id
        {where_sql}
        GROUP BY d.id
        ORDER BY d.created_at DESC
        """,
        tuple(params),
    ).fetchall()
    result = []
    for row in rows:
        result.append(
            {
                "id": row["id"],
                "period": row["period"],
                "since": row["since_utc"],
                "until": row["until_utc"],
                "title": row["title"],
                "status": row["status"],
                "message_id": row["message_id"],
                "created_at": row["created_at"],
                "story_count": row["story_count"],
            }
        )
    return result


def _get_digest_meta(conn, digest_id: int) -> Optional[dict]:
    row = conn.execute(
        """
        SELECT id, period, since_utc, until_utc, title, status, message_id, created_at
        FROM digests
        WHERE id = ?
        """,
        (digest_id,),
    ).fetchone()
    if not row:
        return None
    return dict(row)


def _extract_html_body(document: str) -> str:
    lower = document.lower()
    start = lower.find("<body>")
    end = lower.rfind("</body>")
    if start != -1 and end != -1:
        return document[start + 6 : end].strip()
    return document


def _monitor_thresholds() -> Dict[str, Tuple[str, Optional[float]]]:
    def _env_float(name: str) -> Optional[float]:
        value = os.getenv(name)
        if not value:
            return None
        try:
            return float(value)
        except ValueError:
            LOGGER.warning("Invalid %s=%s, ignoring threshold", name, value)
            return None

    return {
        "news.ingested_1h": ("min", _env_float("ALERT_NEWS_MIN_1H")),
        "queue.size_queued": ("max", _env_float("ALERT_QUEUE_MAX")),
        "queue.sent_24h": ("min", _env_float("ALERT_SENT_MIN_24H")),
    }


def _metric_status(metric: str, value: Optional[float]) -> Tuple[str, Optional[float], Optional[str]]:
    thresholds = _monitor_thresholds()
    rule = thresholds.get(metric)
    if value is None or rule is None or rule[1] is None:
        return "unknown", None, None
    kind, threshold = rule
    if kind == "min":
        return ("ok" if value >= threshold else "alert", threshold, kind)
    if kind == "max":
        return ("ok" if value <= threshold else "alert", threshold, kind)
    return "unknown", threshold, kind


def _load_latest_metric(conn, metric: str) -> Optional[dict]:
    row = conn.execute(
        """
        SELECT ts_utc, value, meta
        FROM monitor_logs
        WHERE metric = ?
        ORDER BY ts_utc DESC
        LIMIT 1
        """,
        (metric,),
    ).fetchone()
    if not row:
        return None
    meta = {}
    if row["meta"]:
        try:
            meta = json.loads(row["meta"])
        except json.JSONDecodeError:
            meta = {"raw": row["meta"]}
    return {"metric": metric, "ts_utc": row["ts_utc"], "value": float(row["value"]), "meta": meta}


def _load_metric_series(conn, metric: str, since_iso: str) -> List[dict]:
    rows = conn.execute(
        """
        SELECT ts_utc, value, meta
        FROM monitor_logs
        WHERE metric = ? AND ts_utc >= ?
        ORDER BY ts_utc ASC
        """,
        (metric, since_iso),
    ).fetchall()
    series: List[dict] = []
    for row in rows:
        meta = {}
        if row["meta"]:
            try:
                meta = json.loads(row["meta"])
            except json.JSONDecodeError:
                meta = {"raw": row["meta"]}
        series.append(
            {
                "ts_utc": row["ts_utc"],
                "value": float(row["value"]),
                "meta": meta,
            }
        )
    return series


def _fetch_story_articles(story_id: int, limit: Optional[int] = 5) -> List[dict]:
    conn = get_conn()
    try:
        sql = """
            SELECT
                n.id,
                n.title,
                n.url,
                COALESCE(n.published, n.published_at, n.created_at) AS published_at,
                cf.title_sig,
                cf.entity_sig
            FROM story_articles sa
            JOIN news n ON n.id = sa.news_id
            LEFT JOIN content_fingerprints cf ON cf.news_id = n.id
            WHERE sa.story_id = ?
            ORDER BY COALESCE(n.published, n.published_at, n.created_at) DESC
        """
        params: Tuple[object, ...]
        if limit is not None:
            sql += " LIMIT ?"
            params = (story_id, limit)
        else:
            params = (story_id,)
        rows = conn.execute(sql, params).fetchall()
        articles = []
        for row in rows:
            published_local = _format_local(row["published_at"])
            articles.append(
                {
                    "id": row["id"],
                    "title": row["title"],
                    "url": row["url"],
                    "published": published_local,
                    "title_sig": row["title_sig"],
                    "entity_sig": row["entity_sig"],
                }
            )
        return articles
    finally:
        conn.close()




def _split_visible_hidden(articles: List[dict]) -> Tuple[List[dict], List[dict]]:
    prepared = []
    for art in articles:
        title = art.get("title", "")
        title_sig = art.get("title_sig")
        entity_sig = art.get("entity_sig")
        if not title_sig:
            title_sig, computed_entity = compute_signatures(
                title,
                {"sport": None, "tournament": None, "team": None, "player": None},
            )
            if not entity_sig:
                entity_sig = computed_entity
        prepared.append((
            art["id"],
            title_sig or "",
            entity_sig,
            art,
        ))
    visible, hidden = filter_near_duplicates(prepared)
    return visible, hidden
def _fetch_recent_queue_entry(conn, story_id: int) -> Optional[sqlite3.Row]:
    dedup_key = f"story:{story_id}"
    window_start = datetime.now(timezone.utc) - timedelta(days=CONFIG.dedup_window_days)
    window_iso = window_start.replace(microsecond=0).isoformat(timespec="seconds")
    return conn.execute(
        """
        SELECT id, status, enqueued_at, scheduled_at, sent_at, message_id
        FROM publish_queue
        WHERE dedup_key = ?
          AND status IN ('queued', 'sent')
          AND COALESCE(sent_at, enqueued_at) >= ?
        ORDER BY COALESCE(sent_at, enqueued_at) DESC, id DESC
        LIMIT 1
        """,
        (dedup_key, window_iso),
    ).fetchone()


def _create_queue_entry(conn, story_id: int) -> int:
    dedup_key = f"story:{story_id}"
    cursor = conn.execute(
        """
        INSERT INTO publish_queue (item_type, item_id, priority, dedup_key)
        VALUES ('story', ?, 0, ?)
        """,
        (story_id, dedup_key),
    )
    conn.commit()
    return cursor.lastrowid


@app.get("/", response_class=HTMLResponse)
def list_stories(
    request: Request,
    q: Optional[str] = None,
    sport: Optional[int] = None,
    tournament: Optional[int] = None,
    team: Optional[int] = None,
    player: Optional[int] = None,
    since: Optional[str] = None,
    until: Optional[str] = None,
    has_neardups: Optional[bool] = None,
    in_queue: Optional[bool] = None,
    sort: str = DEFAULT_SORT,
    page: int = 1,
    per_page: int = 20,
    _: None = Depends(require_auth),
):
    q_clean = q.strip() if q and q.strip() else None
    sort_key = sort if sort in SORT_SQL else DEFAULT_SORT
    page = max(page, 1)
    per_page = max(1, min(per_page, MAX_PER_PAGE))
    since_iso = _normalize_iso_param(since)
    until_iso = _normalize_iso_param(until)

    conn = get_conn()
    try:
        stories, total = _query_stories(
            conn,
            q=q_clean,
            sport=sport,
            tournament=tournament,
            team=team,
            player=player,
            since=since_iso,
            until=until_iso,
            has_neardups=has_neardups,
            in_queue=in_queue,
            sort=sort_key,
            page=page,
            per_page=per_page,
        )
        entity_options = _fetch_entity_options(conn)
    finally:
        conn.close()

    total_pages = max(1, (total + per_page - 1) // per_page)
    base_params: Dict[str, str] = {}
    if q_clean:
        base_params["q"] = q_clean
    for name, value in (
        ("sport", sport),
        ("tournament", tournament),
        ("team", team),
        ("player", player),
    ):
        if value is not None:
            base_params[name] = _coerce_query_value(value)
    if since_iso and since:
        base_params["since"] = since
    if until_iso and until:
        base_params["until"] = until
    if has_neardups is not None:
        base_params["has_neardups"] = _coerce_query_value(has_neardups)
    if in_queue is not None:
        base_params["in_queue"] = _coerce_query_value(in_queue)
    base_params["per_page"] = _coerce_query_value(per_page)
    base_params["sort"] = sort_key

    page_links = [
        {
            "page": idx,
            "url": _build_query_string(base_params, page=idx),
            "current": idx == page,
        }
        for idx in range(1, total_pages + 1)
    ]
    prev_url = _build_query_string(base_params, page=page - 1) if page > 1 else None
    next_url = _build_query_string(base_params, page=page + 1) if page < total_pages else None
    has_neardups_value = "true" if has_neardups is True else "false" if has_neardups is False else ""
    in_queue_value = "true" if in_queue is True else "false" if in_queue is False else ""

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "stories": stories,
            "page": page,
            "pagination": {
                "total": total,
                "per_page": per_page,
                "total_pages": total_pages,
                "page_links": page_links,
                "prev_url": prev_url,
                "next_url": next_url,
            },
            "filters": {
                "q": q_clean or "",
                "sport": sport,
                "tournament": tournament,
                "team": team,
                "player": player,
                "since": since if since_iso else "",
                "until": until if until_iso else "",
                "has_neardups_value": has_neardups_value,
                "in_queue_value": in_queue_value,
                "sort": sort_key,
                "per_page": per_page,
            },
            "entity_options": entity_options,
            "sort_options": SORT_LABELS,
            "mode": "html",
        },
    )


@app.get("/stories/{story_id}", response_class=HTMLResponse)
def story_detail(story_id: int, request: Request, _: None = Depends(require_auth)):
    conn_meta = get_conn()
    try:
        publish_history = _fetch_publish_history(conn_meta, "story", story_id)
        publish_map_entry = _fetch_publish_map_entry(conn_meta, "story", story_id)
    finally:
        conn_meta.close()

    articles = _fetch_story_articles(story_id, limit=None)
    visible_articles, hidden_articles = _split_visible_hidden(articles)
    for hidden in hidden_articles:
        reason_parts = []
        duplicate_of = hidden.get("duplicate_of")
        if duplicate_of:
            reason_parts.append(f"дубликат новости #{duplicate_of}")
        jaccard = hidden.get("jaccard")
        if jaccard is not None:
            reason_parts.append(f"Jaccard {jaccard:.2f}")
        if hidden.get("entity_match"):
            reason_parts.append("совпадение сущностей")
        if not reason_parts:
            reason_parts.append("похожий заголовок")
        hidden["reason"] = "; ".join(reason_parts)
    message = render_story_message(story_id)
    publish_mode = (publish_map_entry or {}).get("mode") or "html"
    return templates.TemplateResponse(
        "story_detail.html",
        {
            "request": request,
            "story_id": story_id,
            "title": message.meta.get("title") if message.meta else message.text.splitlines()[0],
            "articles": visible_articles,
            "hidden_articles": hidden_articles,
            "hidden_count": len(hidden_articles),
            "publish_map": publish_map_entry,
            "publish_mode": publish_mode,
            "publish_history": publish_history,
            "publish_history_count": len(publish_history),
        },
    )


@app.get("/queue", response_class=HTMLResponse)
def queue_view(
    request: Request,
    status: str = "queued",
    since: Optional[str] = None,
    until: Optional[str] = None,
    page: int = 1,
    per_page: int = 50,
    _: None = Depends(require_auth),
):
    allowed_status = {"queued", "sent", "error", "any"}
    status_key = status if status in allowed_status else "queued"
    page = max(page, 1)
    per_page = max(1, min(per_page, MAX_PER_PAGE))
    since_iso = _normalize_iso_param(since)
    until_iso = _normalize_iso_param(until)

    where_clauses: List[str] = []
    params: List[object] = []
    if status_key != "any":
        where_clauses.append("status = ?")
        params.append(status_key)

    if status_key == "sent":
        since_field = "sent_at"
    elif status_key == "queued":
        since_field = "scheduled_at"
    elif status_key == "any":
        since_field = "COALESCE(sent_at, scheduled_at, enqueued_at)"
    else:
        since_field = "COALESCE(scheduled_at, enqueued_at)"

    if since_iso:
        where_clauses.append(f"{since_field} >= ?")
        params.append(since_iso)
    if until_iso:
        where_clauses.append(f"{since_field} <= ?")
        params.append(until_iso)

    where_sql = ""
    if where_clauses:
        where_sql = "WHERE " + " AND ".join(where_clauses)

    if status_key == "sent":
        order_sql = "sent_at DESC, id DESC"
    elif status_key == "queued":
        order_sql = "COALESCE(scheduled_at, enqueued_at) DESC, id DESC"
    else:
        order_sql = "COALESCE(sent_at, scheduled_at, enqueued_at) DESC, id DESC"

    offset = (page - 1) * per_page

    conn = get_conn()
    try:
        count_sql = f"SELECT COUNT(*) FROM publish_queue {where_sql}"
        total = conn.execute(count_sql, tuple(params)).fetchone()[0]
        data_sql = f"""
            SELECT id, item_type, item_id, status, enqueued_at, scheduled_at, sent_at, message_id, error
            FROM publish_queue
            {where_sql}
            ORDER BY {order_sql}
            LIMIT ? OFFSET ?
        """
        rows = conn.execute(data_sql, tuple(params + [per_page, offset])).fetchall()
        items = []
        for row in rows:
            has_hidden = False
            if row["item_type"] == "story":
                story_articles = _fetch_story_articles(row["item_id"], limit=10)
                _, hidden_articles = _split_visible_hidden(story_articles)
                has_hidden = len(hidden_articles) > 0
            items.append(
                {
                    "id": row["id"],
                    "item_type": row["item_type"],
                    "item_id": row["item_id"],
                    "status": row["status"],
                    "enqueued_at": _format_local(row["enqueued_at"]),
                    "scheduled_at": _format_local(row["scheduled_at"]),
                    "sent_at": _format_local(row["sent_at"]),
                    "message_id": row["message_id"],
                    "error": row["error"],
                    "has_hidden": has_hidden,
                    "link": f"/stories/{row['item_id']}" if row["item_type"] == "story" else None,
                }
            )

        total_pages = max(1, (total + per_page - 1) // per_page)
        base_params: Dict[str, str] = {"status": status_key}
        if since_iso and since:
            base_params["since"] = since
        if until_iso and until:
            base_params["until"] = until
        base_params["per_page"] = _coerce_query_value(per_page)
        page_links = [
            {
                "page": idx,
                "url": _build_query_string(base_params, page=idx),
                "current": idx == page,
            }
            for idx in range(1, total_pages + 1)
        ]
        prev_url = _build_query_string(base_params, page=page - 1) if page > 1 else None
        next_url = _build_query_string(base_params, page=page + 1) if page < total_pages else None
        status_options = [
            ("queued", "Запланировано"),
            ("sent", "Отправлено"),
            ("error", "Ошибка"),
            ("any", "Все статусы"),
        ]
        return templates.TemplateResponse(
            "queue.html",
            {
                "request": request,
                "items": items,
                "filters": {
                    "status": status_key,
                    "since": since if since_iso else "",
                    "until": until if until_iso else "",
                    "per_page": per_page,
                },
                "status_options": status_options,
                "pagination": {
                    "total": total,
                    "page": page,
                    "per_page": per_page,
                    "total_pages": total_pages,
                    "page_links": page_links,
                    "prev_url": prev_url,
                    "next_url": next_url,
                },
            },
        )
    finally:
        conn.close()


@app.get("/api/stories/{story_id}/preview")
def story_preview(story_id: int, mode: str = "html", _: None = Depends(require_auth)):
    message = render_story_message(story_id)
    chunks = _compose_story_chunks(message, mode)
    total_chars = sum(len(chunk) for chunk in chunks)
    return JSONResponse({"chunks": chunks, "total_chars": total_chars})


@app.post("/api/stories/{story_id}/append")
def story_append_update(
    story_id: int,
    request: Request,
    dry_run: bool = Query(False),
    mode: str = Query("html"),
    text: Optional[str] = Query(None),
    from_render: Optional[str] = Query(None),
    _: None = Depends(require_auth),
):
    mode = mode.lower()
    if mode not in {"html", "markdown"}:
        raise HTTPException(status_code=400, detail="mode must be html or markdown")
    if from_render and from_render not in {"short", "full"}:
        raise HTTPException(status_code=400, detail="from_render must be short or full")

    conn = get_conn()
    try:
        message_id, _, stored_mode = _resolve_publish_target(conn, "story", story_id)
        if message_id is None:
            raise HTTPException(status_code=404, detail="No published message found for this story")

        if stored_mode in {"html", "markdown"} and "mode" not in request.query_params:
            if stored_mode != mode:
                LOGGER.info(
                    "Using stored mode '%s' for story_id=%s instead of requested '%s'",
                    stored_mode,
                    story_id,
                    mode,
                )
            mode = stored_mode

        payload_text = text.strip() if text else ""
        if not payload_text and from_render:
            payload_text = render_story_update(story_id, kind=from_render, mode=mode)
        if not payload_text:
            raise HTTPException(status_code=400, detail="text or from_render is required")

        _validate_telegram_text(payload_text)

        last_append = _fetch_last_append_text(conn, "story", story_id)
        if last_append is not None and last_append == payload_text:
            LOGGER.warning("Append text matches previous append for story_id=%s", story_id)

        if dry_run:
            LOGGER.info(
                "DRY-RUN append story_id=%s message_id=%s len=%s",
                story_id,
                message_id,
                len(payload_text),
            )
            return JSONResponse(
                {
                    "status": "preview",
                    "text": payload_text,
                    "message_id": message_id,
                    "mode": mode,
                }
            )

        token, chat_id = _ensure_telegram_env()
        parse_mode = "HTML" if mode == "html" else "MarkdownV2"
        try:
            message = asyncio.run(
                _append_message_async(
                    token=token,
                    chat_id=chat_id,
                    reply_to=message_id,
                    text=payload_text,
                    parse_mode=parse_mode,
                )
            )
        except Exception as exc:  # pragma: no cover - network errors
            LOGGER.error("Failed to append update for story_id=%s: %s", story_id, exc)
            _record_publish_edit(
                conn,
                item_type="story",
                item_id=story_id,
                action="append",
                message_id=message_id,
                reply_msg_id=None,
                old_text=None,
                new_text=payload_text,
                mode=mode,
                error=str(exc),
            )
            raise HTTPException(status_code=500, detail="Failed to append update") from exc

        reply_msg_id = getattr(message, "message_id", None)
        _record_publish_edit(
            conn,
            item_type="story",
            item_id=story_id,
            action="append",
            message_id=message_id,
            reply_msg_id=reply_msg_id,
            old_text=None,
            new_text=payload_text,
            mode=mode,
        )
        LOGGER.info(
            "Append update sent story_id=%s root_msg=%s reply_msg=%s len=%s",
            story_id,
            message_id,
            reply_msg_id,
            len(payload_text),
        )
        return JSONResponse(
            {
                "status": "sent",
                "message_id": message_id,
                "reply_msg_id": reply_msg_id,
                "mode": mode,
            }
        )
    finally:
        conn.close()


@app.get("/digests", response_class=HTMLResponse)
def digest_list(
    request: Request,
    period: Optional[str] = None,
    since: Optional[str] = None,
    until: Optional[str] = None,
    _: None = Depends(require_auth),
):
    conn = get_conn()
    try:
        rows = _fetch_digest_rows(conn, period=period, since=since, until=until)
    finally:
        conn.close()

    daily_since, daily_until = default_window("daily")
    weekly_since, weekly_until = default_window("weekly")
    return templates.TemplateResponse(
        "digests_list.html",
        {
            "request": request,
            "digests": rows,
            "filters": {
                "period": period or "",
                "since": since or "",
                "until": until or "",
            },
            "defaults": {
                "daily_since": daily_since.strftime("%Y-%m-%d"),
                "daily_until": daily_until.strftime("%Y-%m-%d"),
                "weekly_since": weekly_since.strftime("%Y-%m-%d"),
                "weekly_until": weekly_until.strftime("%Y-%m-%d"),
            },
        },
    )


@app.get("/digests/{digest_id}", response_class=HTMLResponse)
def digest_detail_view(digest_id: int, request: Request, _: None = Depends(require_auth)):
    conn = get_conn()
    try:
        meta = _get_digest_meta(conn, digest_id)
        if not meta:
            raise HTTPException(status_code=404, detail="Digest not found")
        dataset = load_digest_dataset(conn, digest_id)
    finally:
        conn.close()
    if not dataset:
        raise HTTPException(status_code=404, detail="Digest dataset unavailable")

    html_doc = render_digest_html_doc(dataset)
    body_html = _extract_html_body(html_doc)
    markdown_text = render_digest_markdown(dataset)
    return templates.TemplateResponse(
        "digest_detail.html",
        {
            "request": request,
            "digest": meta,
            "dataset": dataset,
            "digest_html": body_html,
            "digest_markdown": markdown_text,
            "story_count": len(dataset.get("items", [])),
        },
    )


@app.get("/digests/{digest_id}/download.{fmt}")
def digest_download(digest_id: int, fmt: str, _: None = Depends(require_auth)):
    fmt = fmt.lower()
    if fmt not in {"html", "md"}:
        raise HTTPException(status_code=400, detail="Unsupported format")
    conn = get_conn()
    try:
        dataset = load_digest_dataset(conn, digest_id)
        if not dataset:
            raise HTTPException(status_code=404, detail="Digest not found")
    finally:
        conn.close()

    since = datetime.fromisoformat(dataset["since"])
    until = datetime.fromisoformat(dataset["until"])
    filename = build_filename(dataset["period"], since, until, "html" if fmt == "html" else "md")
    if fmt == "html":
        content = render_digest_html_doc(dataset)
        media_type = "text/html; charset=utf-8"
    else:
        content = render_digest_markdown(dataset)
        media_type = "text/markdown; charset=utf-8"
    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return Response(content, media_type=media_type, headers=headers)


@app.post("/api/digests")
def digest_create_api(
    payload: Dict[str, object],
    dry_run: bool = Query(False),
    _: None = Depends(require_auth),
):
    period = str(payload.get("period") or "daily")
    since_value = payload.get("since")
    until_value = payload.get("until")
    limit = int(payload.get("limit") or int(os.getenv("DIGEST_DEFAULT_LIMIT", "25")))
    if since_value:
        since = parse_date(str(since_value))
        if until_value:
            until = parse_date(str(until_value))
        else:
            until = since + (timedelta(days=7) if period == "weekly" else timedelta(days=1))
    else:
        since, until = default_window(period)

    dataset = build_dataset(period, since, until, limit=limit)
    if dataset.get("count", 0) == 0:
        return JSONResponse({"status": "empty", "dataset": dataset})

    if dry_run:
        html_doc = render_digest_html_doc(dataset)
        markdown_text = render_digest_markdown(dataset)
        return JSONResponse(
            {
                "status": "preview",
                "dataset": dataset,
                "html": html_doc,
                "markdown": markdown_text,
            }
        )

    conn = get_conn()
    try:
        digest_id = store_digest(conn, dataset, status="ready")
    finally:
        conn.close()
    return JSONResponse({"status": "stored", "digest_id": digest_id, "count": dataset["count"]})


@app.post("/api/digests/{digest_id}/send")
def digest_send_api(digest_id: int, _: None = Depends(require_auth)):
    conn = get_conn()
    try:
        dataset = load_digest_dataset(conn, digest_id)
        if not dataset:
            raise HTTPException(status_code=404, detail="Digest not found")
    finally:
        conn.close()

    chunk_size = int(os.getenv("DIGEST_THREAD_CHUNK", "5"))
    messages = build_telegram_messages(dataset, dataset["period"], chunk_size)
    try:
        root_id, message_ids = send_digest_messages(messages)
    except Exception as exc:  # pragma: no cover - network errors
        raise HTTPException(status_code=500, detail=f"Failed to send digest: {exc}") from exc

    conn = get_conn()
    try:
        update_digest_status(conn, digest_id, "sent", str(root_id))
    finally:
        conn.close()

    return JSONResponse(
        {
            "status": "sent",
            "root_message_id": root_id,
            "messages": message_ids,
            "count": len(dataset.get("items", [])),
        }
    )


@app.get("/monitor", response_class=HTMLResponse)
def monitor_view(request: Request, _: None = Depends(require_auth)):
    conn = get_conn()
    try:
        latest = []
        for metric, window in MONITOR_METRICS:
            entry = _load_latest_metric(conn, metric)
            if entry:
                status, threshold, mode = _metric_status(metric, entry["value"])
            else:
                status, threshold, mode = "unknown", None, None
                entry = {"metric": metric, "ts_utc": "-", "value": None, "meta": {"window": window}}
            latest.append(
                {
                    "metric": metric,
                    "value": entry["value"],
                    "ts_utc": entry["ts_utc"],
                    "meta": entry.get("meta", {}),
                    "status": status,
                    "threshold": threshold,
                    "mode": mode,
                }
            )
    finally:
        conn.close()

    return templates.TemplateResponse(
        "monitor.html",
        {
            "request": request,
            "metrics": latest,
        },
    )


@app.get("/api/monitor/metrics")
def monitor_metrics_api(
    metric: str,
    since: Optional[str] = None,
    _: None = Depends(require_auth),
):
    if not metric:
        raise HTTPException(status_code=400, detail="metric is required")
    if since:
        since_iso = _normalize_iso_param(since)
        if not since_iso:
            raise HTTPException(status_code=400, detail="Invalid since parameter")
    else:
        since_iso = (datetime.now(timezone.utc) - timedelta(hours=24)).replace(microsecond=0).isoformat(timespec="seconds")
    conn = get_conn()
    try:
        series = _load_metric_series(conn, metric, since_iso)
    finally:
        conn.close()
    return JSONResponse({"metric": metric, "since": since_iso, "points": series})


@app.post("/api/stories/{story_id}/edit")
def story_edit_update(
    story_id: int,
    request: Request,
    payload: Dict[str, str] = Body(...),
    dry_run: bool = Query(False),
    mode: str = Query("html"),
    _: None = Depends(require_auth),
):
    mode = mode.lower()
    if mode not in {"html", "markdown"}:
        raise HTTPException(status_code=400, detail="mode must be html or markdown")

    new_text = (payload.get("text") or "").strip()
    if not new_text:
        raise HTTPException(status_code=400, detail="text is required")

    conn = get_conn()
    try:
        message_id, stored_text, stored_mode = _resolve_publish_target(conn, "story", story_id)
        if message_id is None:
            raise HTTPException(status_code=404, detail="No published message found for this story")

        if stored_mode in {"html", "markdown"} and "mode" not in request.query_params:
            if stored_mode != mode:
                LOGGER.info(
                    "Using stored mode '%s' for story_id=%s instead of requested '%s'",
                    stored_mode,
                    story_id,
                    mode,
                )
            mode = stored_mode

        _validate_telegram_text(new_text)

        if stored_text is not None and stored_text == new_text:
            LOGGER.warning("Edit text matches stored text for story_id=%s", story_id)

        if dry_run:
            LOGGER.info(
                "DRY-RUN edit story_id=%s message_id=%s len=%s",
                story_id,
                message_id,
                len(new_text),
            )
            return JSONResponse(
                {
                    "status": "preview",
                    "message_id": message_id,
                    "old_text": stored_text,
                    "new_text": new_text,
                    "mode": mode,
                }
            )

        token, chat_id = _ensure_telegram_env()
        parse_mode = "HTML" if mode == "html" else "MarkdownV2"

        try:
            message = asyncio.run(
                _edit_message_async(
                    token=token,
                    chat_id=chat_id,
                    message_id=message_id,
                    text=new_text,
                    parse_mode=parse_mode,
                )
            )
        except Exception as exc:  # pragma: no cover - network errors
            LOGGER.error("Failed to edit story_id=%s message_id=%s: %s", story_id, message_id, exc)
            _record_publish_edit(
                conn,
                item_type="story",
                item_id=story_id,
                action="edit",
                message_id=message_id,
                reply_msg_id=None,
                old_text=stored_text,
                new_text=new_text,
                mode=mode,
                error=str(exc),
            )
            raise HTTPException(status_code=500, detail="Failed to edit message") from exc

        store_publish_map("story", story_id, message_id, new_text, mode)
        _record_publish_edit(
            conn,
            item_type="story",
            item_id=story_id,
            action="edit",
            message_id=message_id,
            reply_msg_id=None,
            old_text=stored_text,
            new_text=new_text,
            mode=mode,
        )
        LOGGER.info(
            "Edited story message story_id=%s message_id=%s len=%s",
            story_id,
            getattr(message, "message_id", message_id),
            len(new_text),
        )
        return JSONResponse(
            {
                "status": "sent",
                "message_id": message_id,
                "mode": mode,
            }
        )
    finally:
        conn.close()


@app.post("/publish/story/{story_id}")
def publish_story_now(
    story_id: int,
    request: Request,
    dry_run: bool = False,
    mode: str = "html",
    _: None = Depends(require_auth),
):
    client_host = request.client.host if request.client else "unknown"
    conn = get_conn()
    try:
        existing_entry = _fetch_recent_queue_entry(conn, story_id)

        if dry_run:
            message = render_story_message(story_id)
            chunks = _compose_story_chunks(message, mode)
            total_chars = sum(len(chunk) for chunk in chunks)
            response_payload = {
                "status": "preview",
                "chunks": chunks,
                "total_chars": total_chars,
            }
            if existing_entry:
                response_payload["dedup"] = {
                    "queue_id": existing_entry["id"],
                    "status": existing_entry["status"],
                    "sent_at": existing_entry["sent_at"],
                }
            LOGGER.info(
                "Preview request from %s story_id=%s chunks=%s dedup=%s",
                client_host,
                story_id,
                len(chunks),
                bool(existing_entry),
            )
            return JSONResponse(response_payload)

        if existing_entry and existing_entry["status"] == "sent":
            LOGGER.info(
                "Publish dedup hit (already sent) host=%s story_id=%s queue_id=%s",
                client_host,
                story_id,
                existing_entry["id"],
            )
            return JSONResponse(
                {
                    "status": "already",
                    "queue_id": existing_entry["id"],
                    "sent_at": existing_entry["sent_at"],
                    "message_id": existing_entry["message_id"],
                }
            )

        token = os.getenv("TG_BOT_TOKEN")
        channel_id_env = os.getenv("TG_CHANNEL_ID")
        if not token or not channel_id_env:
            raise HTTPException(status_code=500, detail="Missing TG_BOT_TOKEN or TG_CHANNEL_ID")
        try:
            chat_id = int(channel_id_env)
        except ValueError as exc:
            raise HTTPException(status_code=500, detail="Invalid TG_CHANNEL_ID") from exc

        reuse_existing = existing_entry is not None and existing_entry["status"] == "queued"
        if reuse_existing:
            queue_id = existing_entry["id"]
        else:
            try:
                queue_id = _create_queue_entry(conn, story_id)
            except Exception as exc:
                LOGGER.exception("Failed to enqueue story %s: %s", story_id, exc)
                raise HTTPException(status_code=500, detail="Failed to enqueue story") from exc
        LOGGER.info(
            "Publish request from %s story_id=%s queue_id=%s reuse=%s",
            client_host,
            story_id,
            queue_id,
            reuse_existing,
        )

        try:
            message_ids = publish_story(
                story_id,
                dry_run=False,
                mode=mode,
                token=token,
                chat_id=chat_id,
            )
        except Exception as exc:
            mark_status(conn, queue_id, "error", error=str(exc))
            LOGGER.exception("Failed to publish story %s queue_id=%s: %s", story_id, queue_id, exc)
            raise HTTPException(status_code=500, detail="Failed to send story") from exc

        primary_msg = str(message_ids[0]) if message_ids else None
        mark_status(conn, queue_id, "sent", message_id=primary_msg)
        LOGGER.info(
            "Story %s sent from %s queue_id=%s msg_id=%s",
            story_id,
            client_host,
            queue_id,
            primary_msg,
        )
        return JSONResponse(
            {
                "status": "sent",
                "queue_id": queue_id,
                "message_ids": message_ids,
                "primary_msg_id": primary_msg,
            }
        )
    finally:
        conn.close()
