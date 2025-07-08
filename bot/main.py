# bot/main.py

import os
import asyncio
import yaml
from urllib.parse import quote_plus
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
from utils.render import render_preview
from parsers.champ_parser import ChampParser

# Загружаем токен, чат и базовый URL Web App из переменных окружения
BOT_TOKEN   = os.getenv("BOT_TOKEN")
CHAT_ID     = int(os.getenv("CHAT_ID"))
WEBAPP_BASE = os.getenv("WEBAPP_BASE")  # например https://<ваш-login>.github.io/<repo-name>

def load_config():
    with open("sources_config.yml", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    return cfg["championat"]

async def send_article(bot: Bot, article: dict):
    preview = render_preview(article)
    # формируем slug для GitHub Pages: последний сегмент URL + .html
    raw = article["url"].rstrip("/").split("/")[-1]
    slug = raw if raw.endswith(".html") else raw + ".html"
    wa_url = f"{WEBAPP_BASE}/{slug}"
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton(
            text="📖 Полная статья",
            web_app=WebAppInfo(url=wa_url)
        )
    ]])
    await bot.send_message(
        chat_id=CHAT_ID,
        text=preview,
        parse_mode="HTML",
        disable_web_page_preview=False,
        reply_markup=keyboard
    )

async def main():
    cfg    = load_config()
    parser = ChampParser(cfg)
    bot    = Bot(BOT_TOKEN)

    metas = parser.fetch_list()
    for meta in metas:
        article = parser.fetch_article(meta)
        await send_article(bot, article)
        await asyncio.sleep(0.5)

if __name__ == "__main__":
    asyncio.run(main())
