from __future__ import annotations

import asyncio
import logging
import random
from typing import Optional, Union

from aiogram import Bot
from aiogram.exceptions import TelegramAPIError, TelegramRetryAfter
from aiogram.types import (
    ForceReply,
    InlineKeyboardMarkup,
    Message,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
)

LOGGER = logging.getLogger(__name__)
MAX_ATTEMPTS = 3
ReplyMarkup = Optional[
    Union[InlineKeyboardMarkup, ReplyKeyboardMarkup, ReplyKeyboardRemove, ForceReply]
]


def init_bot(token: str) -> Bot:
    """Initialise aiogram.Bot instance."""
    if not token:
        raise ValueError("Telegram bot token is required")
    return Bot(token=token)


async def _with_retry(action: str, coroutine_factory):
    attempt = 0
    last_exception: Optional[Exception] = None
    while attempt < MAX_ATTEMPTS:
        attempt += 1
        try:
            return await coroutine_factory()
        except TelegramRetryAfter as exc:
            retry_after = exc.retry_after or (attempt * 2)
            jitter = random.uniform(0, retry_after * 0.3)
            wait_for = retry_after + jitter
            LOGGER.warning(
                "%s rate limited (retry-after=%s). Retrying in %.2fs (attempt %s/%s)",
                action,
                exc.retry_after,
                wait_for,
                attempt,
                MAX_ATTEMPTS,
            )
            await asyncio.sleep(wait_for)
            last_exception = exc
        except TelegramAPIError as exc:
            LOGGER.error("Telegram API error during %s: %s", action, exc)
            raise
        except Exception as exc:  # pragma: no cover - unexpected errors
            LOGGER.error("Unexpected error during %s: %s", action, exc)
            raise
    raise RuntimeError(f"Failed to {action}") from last_exception


async def send_text(
    bot: Bot,
    chat_id: int,
    text: str,
    *,
    parse_mode: str = "HTML",
    disable_web_page_preview: bool = True,
    reply_to_message_id: Optional[int] = None,
    reply_markup: ReplyMarkup = None,
) -> Message:
    """Send a text message with retry/backoff on rate limits."""

    async def _do_send():
        return await bot.send_message(
            chat_id=chat_id,
            text=text,
            parse_mode=parse_mode,
            disable_web_page_preview=disable_web_page_preview,
            reply_to_message_id=reply_to_message_id,
            reply_markup=reply_markup,
        )

    message = await _with_retry(f"send message to chat_id={chat_id}", _do_send)
    LOGGER.info(
        "Message sent chat_id=%s length=%s message_id=%s reply_to=%s",
        chat_id,
        len(text),
        message.message_id,
        reply_to_message_id,
    )
    return message


async def edit_text(
    bot: Bot,
    chat_id: int,
    message_id: int,
    text: str,
    *,
    parse_mode: str = "HTML",
    disable_web_page_preview: bool = True,
) -> Message:
    """Edit an existing message with retry/backoff."""

    async def _do_edit():
        return await bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=text,
            parse_mode=parse_mode,
            disable_web_page_preview=disable_web_page_preview,
        )

    message = await _with_retry(
        f"edit message chat_id={chat_id} message_id={message_id}",
        _do_edit,
    )
    LOGGER.info(
        "Message edited chat_id=%s message_id=%s length=%s",
        chat_id,
        message_id,
        len(text),
    )
    return message


async def reply_text(
    bot: Bot,
    chat_id: int,
    reply_to_message_id: int,
    text: str,
    *,
    parse_mode: str = "HTML",
    disable_web_page_preview: bool = True,
    reply_markup: ReplyMarkup = None,
) -> Message:
    """Send a reply message with retry/backoff."""
    message = await send_text(
        bot,
        chat_id,
        text,
        parse_mode=parse_mode,
        disable_web_page_preview=disable_web_page_preview,
        reply_to_message_id=reply_to_message_id,
        reply_markup=reply_markup,
    )
    LOGGER.info(
        "Reply sent chat_id=%s reply_to=%s message_id=%s",
        chat_id,
        reply_to_message_id,
        message.message_id,
    )
    return message
