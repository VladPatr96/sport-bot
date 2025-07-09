import os
import asyncio
import logging
import requests
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
from telegram.error import TelegramError
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

# Конфигурация парсера
cfg = yaml.safe_load(open("sources_config.yml"))["championat"]
parser = ChampParser(cfg)

# Фолбэк-изображение
fallback_image = "https://www.championat.com/static/i/svg/logo.svg"

def is_image_accessible(url):
    try:
        resp = requests.head(url, timeout=5)
        return resp.status_code == 200
    except Exception as e:
        logger.warning(f"[WARN] Недоступно изображение: {url} — {e}")
        return False

async def send_article(bot, article):
    slug = article["url"].rstrip("/").split("/")[-1]
    if not slug.endswith(".html"):
        slug += ".html"
    wa_url = f"{WEBAPP_BASE}/{slug}"

    # Проверка изображения
    image_path = article.get("image") or (article["images"][0] if article["images"] else None)
    photo_url = f"{WEBAPP_BASE}/{image_path}" if image_path else fallback_image
    if not is_image_accessible(photo_url):
        photo_url = fallback_image

    # Формирование текста
    summary = article["summary"]
    if len(summary) > 300:
        summary = summary[:297] + "..."

    caption = f"🏆 <b>{article['title']}</b>\n\n{summary}"

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📖 Читать полностью", web_app=WebAppInfo(url=wa_url))]
    ])

    try:
        await bot.send_photo(
            chat_id=CHAT_ID,
            photo=photo_url,
            caption=caption,
            parse_mode="HTML",
            reply_markup=keyboard
        )
        logger.info(f"✅ Отправлено: {article['title']}")
    except TelegramError as e:
        logger.error(f"❌ Ошибка при отправке статьи: {e}")

async def main():
    articles = parser.fetch_list()
    logger.info(f"Найдено статей: {len(articles)}")

    for meta in articles[:1]:  # можно убрать [:1], чтобы отправлять все
        article = parser.fetch_article(meta)
        logger.debug(f"[DEBUG] Заголовок: {article['title']}")
        await send_article(bot, article)

if __name__ == "__main__":
    asyncio.run(main())
