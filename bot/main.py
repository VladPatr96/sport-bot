import os
import asyncio
import logging
import requests
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
from telegram.error import TelegramError
from parsers.champ_parser import ChampParser
import yaml

# Логирование
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Переменные окружения
BOT_TOKEN    = os.environ["BOT_TOKEN"]
CHAT_ID      = os.environ["CHAT_ID"]
WEBAPP_BASE  = os.environ["WEBAPP_BASE"].rstrip("/")

# Инициализация бота и парсера
bot    = Bot(token=BOT_TOKEN)
cfg    = yaml.safe_load(open("sources_config.yml"))["championat"]
parser = ChampParser(cfg)

# Фолбэк-картинка — локальный PNG, задеплоенный вместе с сайтом
fallback_image = f"{WEBAPP_BASE}/logo.png"

def proxify_image(url: str) -> str:
    """Проксировать внешний URL через images.weserv.nl или вернуть fallback."""
    if not url:
        return fallback_image
    clean = url.replace("https://", "").replace("http://", "")
    return f"https://images.weserv.nl/?url={clean}"

async def send_article(article: dict):
    # Нормализованный slug для ссылки
    raw  = article["url"].rstrip("/").split("/")[-1]
    slug = raw.removesuffix(".html") + ".html"
    wa_url = f"{WEBAPP_BASE}/{slug}"
    logger.info(f"[DEBUG] wa_url = {wa_url}")

    # Ссылка на изображение (проксируется или fallback)
    img_src = article["images"][0] if article["images"] else None
    photo_url = proxify_image(img_src) if img_src else fallback_image
    logger.info(f"[DEBUG] photo_url = {photo_url}")

    # Обрезка summary
    summary = article["summary"]
    if len(summary) > 300:
        summary = summary[:297] + "..."

    caption = f"🏆 <b>{article['title']}</b>\n\n{summary}"
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("📖 Читать полностью", web_app=WebAppInfo(url=wa_url))
    ]])

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
        logger.error(f"❌ Ошибка при отправке: {e}")

async def main():
    metas = parser.fetch_list()
    logger.info(f"Найдено статей: {len(metas)}")
    for meta in metas:
        article = parser.fetch_article(meta)
        logger.debug(f"[DEBUG] Заголовок: {article['title']}")
        await send_article(article)

if __name__ == "__main__":
    asyncio.run(main())
