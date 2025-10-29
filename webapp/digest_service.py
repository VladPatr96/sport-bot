from __future__ import annotations

import asyncio
import html
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional, Sequence, Tuple

from bot.sender import init_bot, reply_text, send_text
from db.utils import get_conn
from webapp.digest_render import build_digest_dataset, render_html, render_markdown

TELEGRAM_LIMIT = 4000


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def default_window(period: str) -> Tuple[datetime, datetime]:
    now = now_utc()
    if period == "weekly":
        weekday = now.weekday()
        end = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
        since = end - timedelta(days=weekday + 7)
        until = since + timedelta(days=7)
    else:
        end = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
        since = end - timedelta(days=1)
        until = since + timedelta(days=1)
    return since, until


def parse_date(value: str) -> datetime:
    dt = datetime.strptime(value, "%Y-%m-%d")
    return dt.replace(tzinfo=timezone.utc)


def build_dataset(
    period: str,
    since: datetime,
    until: datetime,
    limit: int,
    story_ids: Optional[Sequence[int]] = None,
) -> dict:
    return build_digest_dataset(period, since, until, limit=limit, story_ids=story_ids)


def write_exports(dataset: dict, formats: Sequence[str], out_dir: Path) -> List[Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    since = datetime.fromisoformat(dataset["since"])
    until = datetime.fromisoformat(dataset["until"])
    period = dataset["period"]
    written: List[Path] = []
    for fmt in formats:
        if fmt == "md":
            content = render_markdown(dataset)
            filename = build_filename(period, since, until, "md")
        elif fmt == "html":
            content = render_html(dataset)
            filename = build_filename(period, since, until, "html")
        else:
            continue
        path = out_dir / filename
        path.write_text(content, encoding="utf-8")
        written.append(path)
    return written


def build_filename(period: str, since: datetime, until: datetime, suffix: str) -> str:
    since_part = since.strftime("%Y%m%d")
    until_part = until.strftime("%Y%m%d")
    if period == "daily" or since_part == until_part:
        name = f"digest_{period}_{since_part}.{suffix}"
    else:
        name = f"digest_{period}_{since_part}_{until_part}.{suffix}"
    return name


def _split_text(text: str, limit: int = TELEGRAM_LIMIT) -> List[str]:
    parts: List[str] = []
    current: List[str] = []
    current_len = 0
    for line in text.splitlines():
        additional = len(line) + (1 if current else 0)
        if current and current_len + additional > limit:
            parts.append("\n".join(current))
            current = [line]
            current_len = len(line)
        else:
            if current:
                current_len += 1 + len(line)
                current.append(line)
            else:
                current = [line]
                current_len = len(line)
    if current:
        parts.append("\n".join(current))
    return parts or [""]


def _format_tags(article: dict) -> str:
    tags = article.get("tags", [])[:3]
    if not tags:
        return ""
    return " · ".join(f"{tag['icon']} {tag['name']}" for tag in tags)


def _dataset_badges(item: dict) -> str:
    summary = []
    if item.get("tournament_count"):
        summary.append(f"🏆 {item['tournament_count']}")
    if item.get("team_count"):
        summary.append(f"🏟️ {item['team_count']}")
    if item.get("player_count"):
        summary.append(f"👤 {item['player_count']}")
    if item.get("article_count"):
        summary.append(f"📰 {item['article_count']}")
    return " · ".join(summary)


def _render_block(dataset: dict, items: Sequence[dict], start_index: int) -> str:
    lines: List[str] = []
    for idx, item in enumerate(items, start=start_index):
        title = item["title"]
        lines.append(f"<b>{idx}. {html.escape(title)}</b>")
        badge_summary = _dataset_badges(item)
        if badge_summary:
            lines.append(f"<i>{html.escape(badge_summary)}</i>")
        for article in item.get("articles", []):
            title_html = html.escape(article["title"])
            url = article.get("url")
            tags = _format_tags(article)
            if url:
                entry = f"• <a href=\"{html.escape(url)}\">{title_html}</a>"
            else:
                entry = f"• {title_html}"
            if tags:
                entry += f" — {html.escape(tags)}"
            lines.append(entry)
        lines.append("")
    return "\n".join(lines).strip()


def build_telegram_messages(dataset: dict, period: str, chunk_size: int) -> List[str]:
    title = dataset.get("title") or "Дайджест"
    header = (
        f"<b>{html.escape(title)}</b>\n"
        f"<i>Период: {html.escape(dataset.get('since'))} — {html.escape(dataset.get('until'))}</i>\n"
    )
    items = dataset.get("items", [])
    if not items:
        return [header + "\n(пусто)"]

    if period == "weekly":
        overview_lines = [header, "<b>Содержание:</b>"]
        for idx, item in enumerate(items[:10], start=1):
            badge_summary = _dataset_badges(item)
            line = f"{idx}. {html.escape(item['title'])}"
            if badge_summary:
                line += f" ({html.escape(badge_summary)})"
            overview_lines.append(line)
        messages = ["\n".join(overview_lines)]
        start_index = 1
        for i in range(0, len(items), chunk_size):
            chunk = items[i : i + chunk_size]
            block = _render_block(dataset, chunk, start_index=start_index)
            start_index += len(chunk)
            messages.extend(_split_text(block))
        return messages

    block = _render_block(dataset, items, start_index=1)
    combined = header + "\n\n" + block
    return _split_text(combined)


async def _send_async(messages: List[str]) -> Tuple[int, List[int]]:
    token = os.getenv("TG_BOT_TOKEN")
    channel = os.getenv("TG_CHANNEL_ID")
    if not token or not channel:
        raise RuntimeError("TG_BOT_TOKEN and TG_CHANNEL_ID must be set")
    chat_id = int(channel)
    bot = init_bot(token)
    try:
        parse_mode = "HTML"
        root_id: Optional[int] = None
        sent_ids: List[int] = []
        logger = logging.getLogger(__name__)
        for idx, text in enumerate(messages):
            if idx == 0:
                msg = await send_text(bot, chat_id, text, parse_mode=parse_mode, disable_web_page_preview=False)
                root_id = msg.message_id
                sent_ids.append(root_id)
                logger.info("Digest root message_id=%s len=%s", root_id, len(text))
            else:
                msg = await reply_text(
                    bot,
                    chat_id,
                    reply_to_message_id=root_id or sent_ids[-1],
                    text=text,
                    parse_mode=parse_mode,
                    disable_web_page_preview=False,
                )
                sent_ids.append(msg.message_id)
                logger.info("Digest reply message_id=%s len=%s", msg.message_id, len(text))
        return sent_ids[0], sent_ids
    finally:
        await bot.session.close()


def send_digest_messages(messages: List[str]) -> Tuple[int, List[int]]:
    return asyncio.run(_send_async(messages))


def store_digest(
    conn,
    dataset: dict,
    *,
    status: str = "ready",
    message_id: Optional[str] = None,
) -> int:
    existing = conn.execute(
        """
        SELECT id FROM digests
        WHERE period = ? AND since_utc = ? AND until_utc = ?
        """,
        (dataset["period"], dataset["since"], dataset["until"]),
    ).fetchone()
    if existing:
        digest_id = existing["id"]
        conn.execute(
            """
            UPDATE digests
            SET title = ?, status = ?, message_id = ?
            WHERE id = ?
            """,
            (dataset["title"], status, message_id, digest_id),
        )
        conn.execute("DELETE FROM digest_items WHERE digest_id = ?", (digest_id,))
    else:
        cursor = conn.execute(
            """
            INSERT INTO digests (period, since_utc, until_utc, title, status, message_id)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (dataset["period"], dataset["since"], dataset["until"], dataset["title"], status, message_id),
        )
        digest_id = cursor.lastrowid
    for rank, item in enumerate(dataset.get("items", []), start=1):
        conn.execute(
            """
            INSERT INTO digest_items (digest_id, rank, story_id, total_articles)
            VALUES (?, ?, ?, ?)
            """,
            (digest_id, rank, item["story_id"], item.get("article_count")),
        )
    conn.commit()
    return digest_id


def load_digest_dataset(conn, digest_id: int) -> Optional[dict]:
    row = conn.execute(
        """
        SELECT id, period, since_utc, until_utc, title
        FROM digests
        WHERE id = ?
        """,
        (digest_id,),
    ).fetchone()
    if not row:
        return None
    items = conn.execute(
        """
        SELECT story_id
        FROM digest_items
        WHERE digest_id = ?
        ORDER BY rank ASC
        """,
        (digest_id,),
    ).fetchall()
    story_ids = [item["story_id"] for item in items]
    dataset = build_digest_dataset(
        row["period"],
        datetime.fromisoformat(row["since_utc"]),
        datetime.fromisoformat(row["until_utc"]),
        limit=len(story_ids) or 25,
        story_ids=story_ids,
    )
    dataset["title"] = row["title"]
    return dataset


def update_digest_status(conn, digest_id: int, status: str, message_id: Optional[str]) -> None:
    conn.execute(
        """
        UPDATE digests
        SET status = ?, message_id = ?
        WHERE id = ?
        """,
        (status, message_id, digest_id),
    )
    conn.commit()
