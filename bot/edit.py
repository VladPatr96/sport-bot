from __future__ import annotations

import argparse
import asyncio
import logging
import os
from typing import Optional, Tuple

from aiogram.types import Message

from bot.publish import store_publish_map
from bot.publisher import render_story_update
from bot.sender import edit_text, init_bot, reply_text
from db.utils import get_conn

LOGGER = logging.getLogger(__name__)
TELEGRAM_LIMIT = 4096


def _parse_args(argv: Optional[list[str]]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Edit or append updates to published items.")
    target_group = parser.add_mutually_exclusive_group(required=True)
    target_group.add_argument("--story-id", type=int, help="Target story id")
    target_group.add_argument("--article-id", type=int, help="Target article id")

    action_group = parser.add_mutually_exclusive_group(required=True)
    action_group.add_argument("--edit", action="store_true", help="Edit main message")
    action_group.add_argument("--append", action="store_true", help="Append reply update")

    parser.add_argument("--mode", choices=("html", "markdown"), default="html", help="Parse mode")
    parser.add_argument("--text", help="Text for edit/append. Required for --edit.")
    parser.add_argument(
        "--from-render",
        choices=("short", "full"),
        help="Auto-generate update text (story only) when appending.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Preview without sending or writing to DB")
    parser.add_argument("--verbose", action="store_true", help="Verbose logging")
    args = parser.parse_args(argv)
    argv_list = argv or []
    args.mode_explicit = any(opt.startswith("--mode") for opt in argv_list)
    return args


def _resolve_target(
    conn,
    item_type: str,
    item_id: int,
) -> Tuple[Optional[int], Optional[str], Optional[str]]:
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


def _ensure_env() -> Tuple[str, int]:
    token = os.getenv("TG_BOT_TOKEN")
    channel = os.getenv("TG_CHANNEL_ID")
    if not token or not channel:
        raise RuntimeError("TG_BOT_TOKEN and TG_CHANNEL_ID must be set")
    try:
        chat_id = int(channel)
    except ValueError as exc:
        raise RuntimeError("TG_CHANNEL_ID must be an integer") from exc
    return token, chat_id


def _validate_length(text: str) -> None:
    if len(text) > TELEGRAM_LIMIT:
        raise ValueError(f"Text exceeds Telegram limit ({TELEGRAM_LIMIT} characters)")


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


async def _perform_edit(
    *,
    token: str,
    chat_id: int,
    message_id: int,
    text: str,
    parse_mode: str,
) -> Message:
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


async def _perform_append(
    *,
    token: str,
    chat_id: int,
    reply_to: int,
    text: str,
    parse_mode: str,
) -> Message:
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


def main(argv: Optional[list[str]] = None) -> None:
    args = _parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    item_type = "story" if args.story_id is not None else "article"
    item_id = args.story_id if args.story_id is not None else args.article_id
    action = "edit" if args.edit else "append"
    mode = args.mode

    conn = get_conn()
    try:
        message_id, stored_text, stored_mode = _resolve_target(conn, item_type, item_id)
        if message_id is None:
            raise RuntimeError(f"No sent message found for {item_type}#{item_id}")

        if stored_mode in {"html", "markdown"} and not getattr(args, "mode_explicit", False):
            if stored_mode != mode:
                LOGGER.info(
                    "Using stored mode '%s' from publish_map (requested '%s')",
                    stored_mode,
                    mode,
                )
            mode = stored_mode

        parse_mode = "HTML" if mode == "html" else "MarkdownV2"

        text = (args.text or "").strip() if args.text else None
        if action == "append" and not text:
            if not args.from_render:
                raise RuntimeError("--text or --from-render is required for append")
            if item_type != "story":
                raise RuntimeError("--from-render is only available for stories")
            text = render_story_update(item_id, kind=args.from_render, mode=mode)
        if action == "edit" and not text:
            raise RuntimeError("--text is required for edit")
        if not text:
            raise RuntimeError("Text payload is empty")

        _validate_length(text)

        if action == "edit" and stored_text is not None and stored_text == text:
            LOGGER.warning("New text matches stored original text")
        if action == "append":
            last_append = _fetch_last_append_text(conn, item_type, item_id)
            if last_append is not None and last_append == text:
                LOGGER.warning("Append text matches the most recent append entry")

        if args.dry_run:
            LOGGER.info(
                "DRY-RUN %s %s#%s message_id=%s len=%s",
                action,
                item_type,
                item_id,
                message_id,
                len(text),
            )
            if action == "edit":
                LOGGER.info("Old text:\n%s", stored_text or "(unknown)")
                LOGGER.info("New text:\n%s", text)
            else:
                LOGGER.info("Append text:\n%s", text)
            return

        token, chat_id = _ensure_env()

        if action == "edit":
            try:
                message = asyncio.run(
                    _perform_edit(
                        token=token,
                        chat_id=chat_id,
                        message_id=message_id,
                        text=text,
                        parse_mode=parse_mode,
                    )
                )
            except Exception as exc:
                LOGGER.error(
                    "Failed to edit %s#%s message_id=%s: %s",
                    item_type,
                    item_id,
                    message_id,
                    exc,
                )
                _record_publish_edit(
                    conn,
                    item_type=item_type,
                    item_id=item_id,
                    action="edit",
                    message_id=message_id,
                    reply_msg_id=None,
                    old_text=stored_text,
                    new_text=text,
                    mode=mode,
                    error=str(exc),
                )
                raise

            store_publish_map(item_type, item_id, message_id, text, mode)
            _record_publish_edit(
                conn,
                item_type=item_type,
                item_id=item_id,
                action="edit",
                message_id=message_id,
                reply_msg_id=None,
                old_text=stored_text,
                new_text=text,
                mode=mode,
            )
            LOGGER.info(
                "Edited %s#%s message_id=%s len=%s",
                item_type,
                item_id,
                message.message_id if isinstance(message, Message) else message_id,
                len(text),
            )
        else:
            try:
                message = asyncio.run(
                    _perform_append(
                        token=token,
                        chat_id=chat_id,
                        reply_to=message_id,
                        text=text,
                        parse_mode=parse_mode,
                    )
                )
            except Exception as exc:
                LOGGER.error(
                    "Failed to append update for %s#%s parent=%s: %s",
                    item_type,
                    item_id,
                    message_id,
                    exc,
                )
                _record_publish_edit(
                    conn,
                    item_type=item_type,
                    item_id=item_id,
                    action="append",
                    message_id=message_id,
                    reply_msg_id=None,
                    old_text=None,
                    new_text=text,
                    mode=mode,
                    error=str(exc),
                )
                raise

            reply_msg_id = message.message_id if isinstance(message, Message) else None
            _record_publish_edit(
                conn,
                item_type=item_type,
                item_id=item_id,
                action="append",
                message_id=message_id,
                reply_msg_id=reply_msg_id,
                old_text=None,
                new_text=text,
                mode=mode,
            )
            LOGGER.info(
                "Appended update for %s#%s parent=%s reply_msg_id=%s len=%s",
                item_type,
                item_id,
                message_id,
                reply_msg_id,
                len(text),
            )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
