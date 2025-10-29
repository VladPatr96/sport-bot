from __future__ import annotations

import argparse
import asyncio
import html
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Iterable, List, Optional, Sequence

from bot.publisher import (
    ICON_MAP,
    RenderedMessage,
    render_article_message,
    render_story_message,
)
from bot.sender import init_bot, send_text
from db.utils import get_conn

LOGGER = logging.getLogger(__name__)
SOURCE_URL = "https://www.championat.com"
TROPHY = "\U0001F3C6"
INDEX_EMOJI = [
    "1\uFE0F\u20E3",
    "2\uFE0F\u20E3",
    "3\uFE0F\u20E3",
    "4\uFE0F\u20E3",
    "5\uFE0F\u20E3",
    "6\uFE0F\u20E3",
    "7\uFE0F\u20E3",
    "8\uFE0F\u20E3",
    "9\uFE0F\u20E3",
]

MD_V2_SPECIAL = set(r"_*[]()~`>#+-=|{}.!")  # Markdown V2 escaping


def _safe_print(text: str) -> None:
    try:
        print(text)
    except UnicodeEncodeError:
        sys.stdout.buffer.write(text.encode("utf-8", "replace") + b"\n")


def _escape_markdown(text: str) -> str:
    return "".join(f"\\{c}" if c in MD_V2_SPECIAL else c for c in text)


def _truncate_plain(text: str, limit: int) -> str:
    if limit <= 0:
        return ""
    if len(text) <= limit:
        return text
    return text[: max(limit - 1, 0)] + "…"


def _split_messages(lines: Iterable[str], limit: int = 4096) -> List[str]:
    chunks: List[str] = []
    current_lines: List[str] = []
    current_length = 0

    for raw_line in lines:
        line = raw_line.rstrip()
        if not line:
            candidate_length = current_length + (1 if current_lines else 0)
        else:
            candidate_length = current_length + (1 if current_lines else 0) + len(line)

        if candidate_length > limit and current_lines:
            chunks.append("\n".join(current_lines))
            current_lines = [line] if line else []
            current_length = len(line)
        else:
            if current_lines:
                current_lines.append(line)
                current_length += 1 + len(line)
            else:
                if len(line) > limit:
                    chunks.append(line[:limit])
                    current_lines = []
                    current_length = 0
                else:
                    current_lines = [line]
                    current_length = len(line)
    if current_lines:
        chunks.append("\n".join(current_lines))

    return chunks or [""]


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat(timespec="seconds")


def store_publish_map(
    item_type: str,
    item_id: int,
    message_id: int,
    text: str,
    mode: str,
) -> None:
    conn = get_conn()
    try:
        conn.execute(
            """
            INSERT INTO publish_map (item_type, item_id, message_id, sent_at, text, mode)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(item_type, item_id) DO UPDATE SET
                message_id = excluded.message_id,
                sent_at = excluded.sent_at,
                text = excluded.text,
                mode = excluded.mode
            """,
            (
                item_type,
                item_id,
                str(message_id),
                _utc_now_iso(),
                text,
                mode,
            ),
        )
        conn.commit()
        LOGGER.info(
            "publish_map updated item=%s#%s message_id=%s",
            item_type,
            item_id,
            message_id,
        )
    except Exception as exc:
        LOGGER.error(
            "Failed to update publish_map for %s#%s: %s",
            item_type,
            item_id,
            exc,
        )
    finally:
        conn.close()


def _compose_story_chunks(message: RenderedMessage, mode: str) -> List[str]:
    meta = message.meta or {}
    if meta.get("type") != "story":
        return [message.text]

    title = meta.get("title", "История")
    articles = meta.get("articles", [])

    if mode == "html":
        header = f"{TROPHY} <b>{html.escape(title)}</b>"
        source_line = f'Источник: <a href="{SOURCE_URL}">Championat</a>'
    else:
        header = f"{TROPHY} *{_escape_markdown(title)}*"
        source_line = f"Источник: [Championat]({SOURCE_URL})"

    lines: List[str] = [header, ""]
    for index, article in enumerate(articles, start=1):
        icon = INDEX_EMOJI[index - 1] if index - 1 < len(INDEX_EMOJI) else f"{index}."
        title_text = _truncate_plain(article.get("title", "Без названия"), 256)
        url = article.get("url", "")
        tags = article.get("tags", [])

        tag_names_plain = " · ".join(
            f"{tag.get('icon', ICON_MAP.get(tag.get('type'), '🏷️'))} {_truncate_plain(tag['name'], 48)}"
            for tag in tags[:4]
        )
        if mode == "html":
            link = f'<a href="{html.escape(url)}">{html.escape(title_text)}</a>' if url else html.escape(title_text)
            tags_line = html.escape(tag_names_plain)
        else:
            link = f"[{_escape_markdown(title_text)}]({url})" if url else _escape_markdown(title_text)
            tags_line = _escape_markdown(tag_names_plain)

        base_line = f"{icon} {link}"
        if tags_line:
            combined = f"{base_line} — {tags_line}"
            line = combined if len(combined) <= 1024 else base_line
        else:
            line = base_line

        lines.append(line)

    lines.append("")
    lines.append(source_line)

    return _split_messages(lines)


def _compose_article_chunks(message: RenderedMessage, mode: str) -> List[str]:
    meta = message.meta or {}
    if meta.get("type") != "article":
        return [message.text]

    title = meta.get("title", "Без названия")
    url = meta.get("url", "")
    tags = meta.get("tags", [])

    tag_names_plain = " · ".join(
        f"{tag.get('icon', ICON_MAP.get(tag.get('type'), '🏷️'))} {_truncate_plain(tag['name'], 64)}"
        for tag in tags[:4]
    )

    if mode == "html":
        header = f"<b>{html.escape(title)}</b>"
        tags_line = html.escape(tag_names_plain) if tag_names_plain else ""
        link_line = f'<a href="{html.escape(url)}">{html.escape(url)}</a>' if url else ""
        source_line = f'Источник: <a href="{SOURCE_URL}">Championat</a>'
    else:
        header = f"*{_escape_markdown(title)}*"
        tags_line = _escape_markdown(tag_names_plain) if tag_names_plain else ""
        link_line = url
        source_line = f"Источник: [Championat]({SOURCE_URL})"

    lines = [header]
    if tags_line:
        lines.append(tags_line)
    if link_line:
        lines.append(link_line)
    lines.append("")
    lines.append(source_line)

    return _split_messages(lines)


def _print_chunks(title: str, chunks: Sequence[str], entities: Sequence[str], links: Sequence[str]) -> None:
    for idx, chunk in enumerate(chunks, start=1):
        _safe_print("=" * 80)
        _safe_print(f"{title} (chunk {idx})")
        _safe_print("-" * 80)
        _safe_print(chunk)
    _safe_print("-" * 80)
    _safe_print(f"Entities: {list(entities)}")
    _safe_print(f"Links: {list(links)}")
    _safe_print("[DRY-RUN] Не отправлено")
    _safe_print("=" * 80)


async def _send_chunks(chunks: Sequence[str], mode: str, token: str, chat_id: int) -> List[int]:
    bot = init_bot(token)
    LOGGER.info("Sending to chat_id=%s (mode=%s)", chat_id, mode)
    reply_to: Optional[int] = None
    message_ids: List[int] = []
    parse_mode = "HTML" if mode == "html" else "MarkdownV2"
    try:
        for chunk in chunks:
            message = await send_text(
                bot,
                chat_id,
                chunk,
                parse_mode=parse_mode,
                disable_web_page_preview=True,
                reply_to_message_id=reply_to,
            )
            reply_to = message.message_id
            message_ids.append(message.message_id)
    finally:
        await bot.session.close()
    return message_ids


def publish_story(
    story_id: int,
    *,
    dry_run: bool,
    mode: str,
    token: Optional[str],
    chat_id: Optional[int],
) -> List[int]:
    try:
        message = render_story_message(story_id)
    except Exception as exc:
        LOGGER.error("Failed to render story %s: %s", story_id, exc)
        return []

    hidden_count = len((message.meta or {}).get("hidden_articles", []))
    if hidden_count:
        LOGGER.info("filtered_near_dups=%s for story_id=%s", hidden_count, story_id)

    chunks = _compose_story_chunks(message, mode)

    if dry_run:
        _print_chunks(f"Story #{story_id}", chunks, message.entities, message.links)
        return []

    if not token or chat_id is None:
        LOGGER.error("Missing TG_BOT_TOKEN or TG_CHANNEL_ID in environment")
        sys.exit(1)
    message_ids = asyncio.run(_send_chunks(chunks, mode, token, chat_id))
    if message_ids:
        first_chunk = chunks[0] if chunks else ""
        store_publish_map("story", story_id, message_ids[0], first_chunk, mode)
    return message_ids


def publish_article(
    news_id: int,
    *,
    dry_run: bool,
    mode: str,
    token: Optional[str],
    chat_id: Optional[int],
) -> List[int]:
    try:
        message = render_article_message(news_id)
    except Exception as exc:
        LOGGER.error("Failed to render article %s: %s", news_id, exc)
        return []

    chunks = _compose_article_chunks(message, mode)

    if dry_run:
        _print_chunks(f"Article #{news_id}", chunks, message.entities, message.links)
        return []

    if not token or chat_id is None:
        LOGGER.error("Missing TG_BOT_TOKEN or TG_CHANNEL_ID in environment")
        sys.exit(1)
    message_ids = asyncio.run(_send_chunks(chunks, mode, token, chat_id))
    if message_ids:
        first_chunk = chunks[0] if chunks else ""
        store_publish_map("article", news_id, message_ids[0], first_chunk, mode)
    return message_ids


def publish_latest(
    limit: int,
    *,
    dry_run: bool,
    mode: str,
    token: Optional[str],
    chat_id: Optional[int],
) -> None:
    conn = get_conn()
    try:
        rows = conn.execute(
            """
            SELECT id
            FROM stories
            ORDER BY updated_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        if not rows:
            LOGGER.info("No stories available to publish")
            return
        for row in rows:
            publish_story(row["id"], dry_run=dry_run, mode=mode, token=token, chat_id=chat_id)
    finally:
        conn.close()


def run(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Telegram publisher")
    parser.add_argument("--story-id", type=int, help="Publish specific story")
    parser.add_argument("--article-id", type=int, help="Publish specific article")
    parser.add_argument("--latest", action="store_true", help="Publish latest stories")
    parser.add_argument("--limit", type=int, default=5, help="Number of latest items to publish")
    parser.add_argument("--send", action="store_true", help="Send to Telegram (default: dry-run)")
    parser.add_argument("--dry-run", action="store_true", help="Force dry-run even with --send")
    parser.add_argument("--mode", choices=("html", "markdown"), default="html", help="Parse mode")
    parser.add_argument("--verbose", action="store_true", help="Verbose logging")

    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    selected = sum(
        [
            args.story_id is not None,
            args.article_id is not None,
            args.latest,
        ]
    )
    if selected != 1:
        parser.error("Specify exactly one of --story-id, --article-id, or --latest")

    dry_run = not args.send or args.dry_run
    token = os.getenv("TG_BOT_TOKEN")
    channel_id_env = os.getenv("TG_CHANNEL_ID")
    chat_id = int(channel_id_env) if channel_id_env else None

    if args.send and (not token or chat_id is None):
        LOGGER.error("Missing TG_BOT_TOKEN or TG_CHANNEL_ID in environment")
        sys.exit(1)

    kwargs = {
        "dry_run": dry_run,
        "mode": args.mode,
        "token": token,
        "chat_id": chat_id,
    }

    if args.story_id is not None:
        publish_story(args.story_id, **kwargs)
    elif args.article_id is not None:
        publish_article(args.article_id, **kwargs)
    elif args.latest:
        publish_latest(limit=args.limit, **kwargs)


if __name__ == "__main__":
    run()
