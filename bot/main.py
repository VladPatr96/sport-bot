import os
import asyncio
import logging
import requests
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
from utils.render import render_preview  # если используешь кастомный рендер
from parsers.champ_parser import ChampParser
import yaml

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Переменные окружения
BOT_TOKEN = os.environ["BOT_TOKEN"]
CHAT_ID = os.environ["CHAT_ID"]
WEBAPP_BASE = os.environ["WEBAPP_BASE"]

# Telegram Bot
bot = Bot(token=BOT_TOKEN)

# Загрузка конфигурации
cfg = yaml.safe_load(open("sources_config.yml"))["championat"]
parser = ChampParser(cfg)

# Фолбэк-изображение, если нет картинки
fallback_image = "https://www.championat.com/static/i/svg/logo.svg"

async def send_article(bot, article):
    slug = article["url"].rstrip("/").split("/")[-1]
    if not slug.endswith(".html"):
        slug += ".html"
    wa_url = f"{WEBAPP_BASE}/{slug}"

    # Главное изображение
    main_image = article["images"][0] if article["images"] else fallback_image

    # Превью
    caption = f"🏆 <b>{article['title']}</b>\n\n{article['summary']}"

    # Кнопка
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📖 Читать полностью", web_app=WebAppInfo(url=wa_url))]
    ])

    # Отправка
    await bot.send_photo(
        chat_id=CHAT_ID,
        photo=main_image,
        caption=caption,
        parse_mode="HTML",
        reply_markup=keyboard
    )
    logger.info(f"Отправлено: {article['title']}")

async def main():
    articles = parser.fetch_list()
    logger.info(f"Найдено статей: {len(articles)}")

    for meta in articles[:1]:  # можно убрать [:1], чтобы отправлять все
        article = parser.fetch_article(meta)
        await send_article(bot, article)

if __name__ == "__main__":
    asyncio.run(main())
