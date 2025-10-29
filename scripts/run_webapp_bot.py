from __future__ import annotations

import argparse
import asyncio
import logging
import os
from pathlib import Path
from typing import Optional

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
from dotenv import load_dotenv
import uvicorn

from bot.sender import init_bot, send_text
from scripts.db_migrate import apply_migrations


LOGGER = logging.getLogger(__name__)
DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 8000


def _load_env(dotenv_path: Optional[str]) -> None:
    env_file = None
    if dotenv_path:
        candidate = Path(dotenv_path)
        if not candidate.exists():
            raise FileNotFoundError(f".env file not found: {candidate}")
        env_file = candidate
    else:
        default_path = Path(".env")
        if default_path.exists():
            env_file = default_path
    if env_file:
        load_dotenv(env_file)
        LOGGER.info("Loaded environment variables from %s", env_file)


async def _notify_webapp_button(
    *,
    chat_id: int,
    token: str,
    webapp_url: str,
    message_text: str,
    button_text: str,
) -> None:
    bot = init_bot(token)
    try:
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text=button_text,
                        web_app=WebAppInfo(url=webapp_url),
                    )
                ]
            ]
        )
        await send_text(
            bot,
            chat_id,
            message_text,
            reply_markup=keyboard,
        )
    finally:
        await bot.session.close()


def _ensure_required_vars(*names: str) -> None:
    missing = [name for name in names if not os.getenv(name)]
    if missing:
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")


def main(argv: Optional[list[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Run webapp and optionally notify Telegram chat.")
    parser.add_argument("--host", default=None, help="Host for uvicorn (default from WEBAPP_HOST or 0.0.0.0)")
    parser.add_argument("--port", type=int, default=None, help="Port for uvicorn (default from WEBAPP_PORT or 8000)")
    parser.add_argument("--reload", action="store_true", help="Enable auto-reload for development")
    parser.add_argument("--env-file", default=None, help="Path to .env file (defaults to ./.env if present)")
    parser.add_argument(
        "--webapp-url",
        default=None,
        help="Public HTTPS URL for the webapp (overrides WEBAPP_PUBLIC_URL)",
    )
    parser.add_argument(
        "--notify-chat",
        default=None,
        help="Telegram chat id to notify (overrides WEBAPP_NOTIFY_CHAT)",
    )
    parser.add_argument(
        "--button-text",
        default=None,
        help="Text for the WebApp button (overrides WEBAPP_BUTTON_TEXT)",
    )
    parser.add_argument(
        "--message-text",
        default=None,
        help="Message shown before the button (overrides WEBAPP_MESSAGE_TEXT)",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    _load_env(args.env_file)

    host = args.host or os.getenv("WEBAPP_HOST", DEFAULT_HOST)
    port = args.port or int(os.getenv("WEBAPP_PORT", str(DEFAULT_PORT)))

    webapp_url = args.webapp_url or os.getenv("WEBAPP_PUBLIC_URL")
    notify_chat = args.notify_chat or os.getenv("WEBAPP_NOTIFY_CHAT")
    button_text = args.button_text or os.getenv("WEBAPP_BUTTON_TEXT", "Открыть веб‑приложение")
    message_text = args.message_text or os.getenv("WEBAPP_MESSAGE_TEXT", "WebApp запущен, откройте интерфейс:")

    LOGGER.info("Applying database migrations...")
    apply_migrations()

    if notify_chat:
        if not webapp_url:
            raise RuntimeError("WEBAPP_PUBLIC_URL (or --webapp-url) is required when notifying chat.")
        if not webapp_url.startswith("https://"):
            raise RuntimeError("Telegram WebApps require HTTPS endpoint; please provide https:// URL.")
        _ensure_required_vars("TG_BOT_TOKEN")
        token = os.getenv("TG_BOT_TOKEN")
        LOGGER.info("Sending WebApp button to chat %s", notify_chat)
        try:
            chat_id_int = int(notify_chat)
        except ValueError as exc:
            raise RuntimeError("notify chat id must be an integer") from exc
        asyncio.run(
            _notify_webapp_button(
                chat_id=chat_id_int,
                token=token,
                webapp_url=webapp_url,
                message_text=message_text,
                button_text=button_text,
            )
        )

    LOGGER.info("Starting uvicorn on %s:%s", host, port)
    try:
        uvicorn.run(
            "webapp.main:app",
            host=host,
            port=port,
            reload=args.reload,
            log_level="info",
        )
    except KeyboardInterrupt:
        LOGGER.info("Shutting down on user request")


if __name__ == "__main__":
    main()
