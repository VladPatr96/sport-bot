# parsers/sources/championat/parsers/champ_parser.py

import asyncio
import os
import re
from datetime import datetime
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    WebDriverException, TimeoutException, NoSuchElementException, SessionNotCreatedException
)

# Месяцы по-русски (родительный падеж -> номер)
MONTH_MAPPING_RU = {
    "января": 1, "февраля": 2, "марта": 3, "апреля": 4,
    "мая": 5, "июня": 6, "июля": 7, "августа": 8,
    "сентября": 9, "октября": 10, "ноября": 11, "декабря": 12,
}

def _strip(s: str | None) -> str:
    return re.sub(r"\s+", " ", (s or "").replace("\xa0", " ")).strip()

def _to_iso(date_str: str, time_str: str | None) -> str | None:
    """
    date_str: '1 сентября 2025'
    time_str: '21:50' (может быть None)
    -> '2025-09-01T21:50:00'
    """
    if not date_str:
        return None
    m = re.search(r"(\d{1,2})\s+([А-Яа-яёЁ]+)\s+(\d{4})", date_str)
    if not m:
        return None
    day = int(m.group(1))
    mon_name = m.group(2).lower()
    year = int(m.group(3))
    month = MONTH_MAPPING_RU.get(mon_name)
    if not month:
        return None
    hh, mm = 0, 0
    if time_str:
        tm = re.search(r"(\d{1,2}):(\d{2})", time_str)
        if tm:
            hh, mm = int(tm.group(1)), int(tm.group(2))
    try:
        return datetime(year, month, day, hh, mm, 0).isoformat()
    except Exception:
        return None

class ChampParserSelenium:
    """
    Selenium-парсер championat.com.
    - Список: только внутри div.page-content
    - Дата публикации = дата из <div class="news-items__head"> + время из <div class="news-item__time">
    - Статья: текст до «Материалы по теме», теги после.
    """
    def __init__(self, config):
        self.base_url = config.get("base_url")
        self.cfg = config.get("selectors", {}) or {}
        self.timeout = config.get("timeout", 30)
        self.delay = config.get("delay", 1)
        self.driver = None
        self.is_initialized = False

        # Контейнер контента – по умолчанию div.page-content
        self.page_container_sel = self.cfg.get("page_container") or "div.page-content"

        required = ["list_item", "list_item_url", "list_item_title", "article_title", "article_body"]
        missing = [k for k in required if not self.cfg.get(k)]
        if missing:
            raise ValueError(f"Invalid championat selectors (missing: {', '.join(missing)})")

    async def __aenter__(self):
        await self.init_driver()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.driver:
            self.driver.quit()
        if exc_val:
            print(f"❌ Ошибка в асинхронном блоке: {exc_val}")
        await asyncio.sleep(self.delay)

    async def init_driver(self):
        try:
            chrome_options = Options()
            chrome_options.add_argument("--headless=new")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--disable-software-rasterizer")
            chrome_options.add_argument("window-size=1920,1080")

            if os.environ.get("CHROME_DRIVER_PATH"):
                service = Service(executable_path=os.environ.get("CHROME_DRIVER_PATH"))
            else:
                service = Service()

            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            self.is_initialized = True
            print("✅ WebDriver успешно инициализирован.")
        except SessionNotCreatedException as e:
            print(f"❌ Не удалось создать сессию WebDriver: {e}")
            self.driver = None
        except WebDriverException as e:
            print(f"❌ Ошибка WebDriver: {e}")
            self.driver = None

    def _wait_and_get_soup(self, url: str, wait_selector: str) -> BeautifulSoup | None:
        if not self.driver:
            return None
        try:
            self.driver.get(url)
            WebDriverWait(self.driver, self.timeout).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, wait_selector))
            )
            return BeautifulSoup(self.driver.page_source, "html.parser")
        except (WebDriverException, TimeoutException) as e:
            print(f"❌ Ошибка при загрузке {url}: {e}")
            return None

    async def fetch_list(self) -> list[dict]:
        """
        Возвращает список мета-объектов новостей ТОЛЬКО из блока page-content.
        Дату собираем из news-items__head + news-item__time.
        """
        if not self.is_initialized:
            print("❌ Парсер не инициализирован.")
            return []

        soup = self._wait_and_get_soup(self.base_url, self.page_container_sel)
        if not soup:
            return []

        container = soup.select_one(self.page_container_sel)
        if not container:
            print(f"⚠️ Контейнер контента не найден по селектору: {self.page_container_sel}")
            return []

        news: list[dict] = []

        # Каждая группа выглядит как <div class="news-items">...</div>
        groups = container.select("div.news-items")
        for grp in groups:
            # Дата группы
            head = grp.select_one(self.cfg.get("date_group", "div.news-items__head"))
            group_date = _strip(head.text) if head else None

            # Карточки внутри группы
            items = grp.select(self.cfg["list_item"])
            for it in items:
                try:
                    a = it.select_one(self.cfg["list_item_url"])
                    if not a or not a.get("href"):
                        continue
                    url = urljoin(self.base_url, a["href"])

                    title_el = it.select_one(self.cfg["list_item_title"])
                    title = _strip(title_el.text) if title_el else None

                    # Время карточки
                    time_text = None
                    if self.cfg.get("list_item_published"):
                        t = it.select_one(self.cfg["list_item_published"])
                        if t:
                            time_text = _strip(t.text)

                    published_iso = _to_iso(group_date, time_text)

                    if url and title:
                        news.append({
                            "url": url,
                            "title": title,
                            "published": published_iso
                        })
                except Exception as e:
                    print(f"⚠️ Ошибка при парсинге элемента списка: {e}")

        await asyncio.sleep(self.delay)
        return news

    async def fetch_article(self, news_meta: dict, max_retries: int = 3):
        """
        Парсит статью. Текст/медиа — только до блока «Материалы по теме».
        Теги — после этого блока.
        """
        if not self.is_initialized:
            print("❌ Парсер не инициализирован.")
            return None
        article_url = news_meta.get("url")
        if not article_url:
            return None

        attempt = 0
        while attempt < max_retries:
            try:
                soup = self._wait_and_get_soup(article_url, self.cfg["article_body"])
                if not soup:
                    raise RuntimeError("Не удалось загрузить страницу статьи.")

                title_el = soup.select_one(self.cfg["article_title"])
                title = _strip(title_el.text) if title_el else None

                body_root = soup.select_one(self.cfg["article_body"])
                if not body_root:
                    raise RuntimeError("Не найден основной контейнер статьи.")

                # Найти блок «Материалы по теме»
                cutoff = body_root.find(lambda tag: (
                    getattr(tag, "get", lambda *_: None)("class") and
                    any("external-article" in c for c in tag.get("class", []))
                ))

                # Параграфы до cutoff
                body_chunks: list[str] = []
                for node in body_root.children:
                    if cutoff and node == cutoff:
                        break
                    if getattr(node, "name", None) == "p":
                        txt = _strip(node.get_text(" "))
                        if txt:
                            body_chunks.append(txt)
                body_text = "\n".join(body_chunks).strip()

                # Изображения (шапка + контент до cutoff)
                image_urls: list[str] = []
                if self.cfg.get("article_images"):
                    for img in soup.select(self.cfg["article_images"]):
                        src = img.get("data-src") or img.get("src")
                        if not src:
                            continue
                        if not src.startswith(("http://", "https://")):
                            continue
                        image_urls.append(src)

                # Видео – только из тела (до cutoff)
                video_urls: list[str] = []
                if self.cfg.get("article_videos"):
                    scope = body_root if not cutoff else cutoff.find_previous_sibling() or body_root
                    for v in scope.select(self.cfg["article_videos"]):
                        src = v.get("src") or v.get("data-src")
                        if src and src.startswith(("http://", "https://")):
                            video_urls.append(src)

                # Теги – после «Материалы по теме» (на странице они ниже основного контента)
                tags_data = []
                if self.cfg.get("article_tags"):
                    for t in soup.select(self.cfg["article_tags"]):
                        name = _strip(t.text)
                        href = t.get("href")
                        if name and href:
                            tags_data.append({"name": name, "url": urljoin(article_url, href)})

                return {
                    "title": title,
                    "url": article_url,
                    "published": news_meta.get("published"),
                    "summary": None,
                    "body": body_text,
                    "tags": list({td["name"]: td for td in tags_data}.values()),
                    "images": list(dict.fromkeys(image_urls)),
                    "videos": list(dict.fromkeys(video_urls)),
                }

            except Exception as e:
                attempt += 1
                print(f"❌ Ошибка при парсинге статьи {article_url} (попытка {attempt}): {e}")
                if attempt < max_retries:
                    await asyncio.sleep(self.delay * 2)
                else:
                    return None
