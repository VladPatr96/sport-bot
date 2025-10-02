# sync_champ_news.py — Championat incremental sync (anchor by URL, page-content, published_at)

import os
import re
import json
import argparse
import asyncio
import sqlite3
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple

import yaml
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import WebDriverException, TimeoutException
from urllib3.exceptions import ReadTimeoutError
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

from database.prosport_db import init_db
from parsers.sources.championat.parsers.champ_parser import ChampParserSelenium

from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parents[1]



# ===================== Текст/дата =====================
MONTH_RU = {
    "января": 1, "февраля": 2, "марта": 3, "апреля": 4,
    "мая": 5, "июня": 6, "июля": 7, "августа": 8,
    "сентября": 9, "октября": 10, "ноября": 11, "декабря": 12,
}

def _strip(s: Optional[str]) -> str:
    return re.sub(r"\s+", " ", (s or "").replace("\xa0", " ")).strip()

def sanitize_text(text: Optional[str]) -> Optional[str]:
    t = _strip(text)
    return t if t else None

def dedupe(items: List[str]) -> List[str]:
    seen, out = set(), []
    for x in items or []:
        if x and x not in seen:
            seen.add(x)
            out.append(x)
    return out

def to_iso(group_date: Optional[str], time_text: Optional[str]) -> Optional[str]:
    """ group_date: '1 сентября 2025'; time_text: '21:50' -> ISO8601 """
    if not group_date:
        return None
    m = re.search(r"(\d{1,2})\s+([А-Яа-яёЁ]+)\s+(\d{4})", group_date)
    if not m:
        return None
    day = int(m.group(1))
    mon = MONTH_RU.get(m.group(2).lower())
    year = int(m.group(3))
    hh, mm = 0, 0
    if time_text:
        tm = re.search(r"(\d{1,2}):(\d{2})", time_text)
        if tm:
            hh, mm = int(tm.group(1)), int(tm.group(2))
    try:
        return datetime(year, mon, day, hh, mm, 0).isoformat()
    except Exception:
        return None

# ===================== URL =====================
def _strip_utm(q: List[Tuple[str, str]]) -> List[Tuple[str, str]]:
    return [(k, v) for k, v in q if not k.lower().startswith("utm_")]

def normalize_url(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    try:
        url = url.strip()
        if url.startswith("//"):
            url = "https:" + url
        u = urlparse(url)
        scheme = "https"
        netloc = u.netloc.lower().replace("www.", "")
        if netloc.endswith("championat.com") and netloc != "championat.com":
            netloc = "championat.com"
        path = u.path.rstrip("/")
        q = urlencode(_strip_utm(parse_qsl(u.query, keep_blank_values=True)))
        return urlunparse((scheme, netloc, path, "", q, ""))
    except Exception:
        return url

def page_url_from_base(base_url: str, page: int) -> str:
    u = urlparse(base_url)
    q = urlencode(_strip_utm(parse_qsl(u.query, keep_blank_values=True)))
    clean = urlunparse((u.scheme or "https", u.netloc, u.path, "", q, ""))
    m = re.search(r"/news/(\d+)\.html$", clean)
    if m:
        return re.sub(r"/news/\d+\.html$", f"/news/{page}.html", clean)
    if clean.rstrip("/").endswith("/news"):
        return clean.rstrip("/") + f"/{page}.html"
    if clean.rstrip("/").endswith("/news/"):
        return clean.rstrip("/") + f"{page}.html"
    return f"https://www.championat.com/news/{page}.html"

# ===================== БД =====================
def _list_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table});")
    return [r[1] for r in cur.fetchall()]

def choose_field_names(conn: sqlite3.Connection) -> Dict[str, str]:
    cols = set(_list_columns(conn, "news"))
    def pick(*names):
        for n in names:
            if n in cols:
                return n
        return ""
    return {
        "id":           "id" if "id" in cols else "rowid",
        "title":        pick("title"),
        "url":          pick("url"),
        "content":      pick("content", "body"),
        "published":    pick("published_at", "published"),
        "source":       pick("source"),
        "lang":         pick("lang"),
        "is_published": pick("is_published"),
        "image_url":    pick("image_url"),
        "image_urls":   pick("image_urls", "images"),
        "video_urls":   pick("video_urls", "videos"),
        "images":       pick("images"),
        "videos":       pick("videos"),
    }

def ensure_useful_indexes(conn: sqlite3.Connection, f: Dict[str, str]):
    cur = conn.cursor()
    if f["url"]:
        cur.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS idx_news_url ON news({f['url']});")
    if f["published"]:
        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_news_published ON news({f['published']});")
    conn.commit()

def get_anchor_from_db(conn: sqlite3.Connection, f: Dict[str, str]) -> Optional[str]:
    cur = conn.cursor()
    anchor_url = None
    if f["published"]:
        try:
            cur.execute(
                f"SELECT {f['url']} FROM news "
                f"WHERE {f['published']} IS NOT NULL "
                f"ORDER BY {f['published']} DESC LIMIT 1"
            )
            row = cur.fetchone()
            if row and row[0]:
                anchor_url = row[0]
        except Exception:
            anchor_url = None
    if not anchor_url:
        cur.execute(f"SELECT {f['url']} FROM news ORDER BY {f['id']} DESC LIMIT 1")
        row = cur.fetchone()
        if row and row[0]:
            anchor_url = row[0]
    return normalize_url(anchor_url)

# ===================== Конфиг =====================
def load_champ_config() -> dict | None:
    config_path = PROJECT_ROOT / "parsers" / "sources" / "championat" / "config" / "sources_config.yml"
    if not os.path.exists(config_path):
        print(f"❌ Конфиг не найден: {config_path}")
        return None
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
    except Exception as e:
        print(f"❌ Ошибка чтения YAML: {e}")
        return None

    src = (raw.get("championat") or {}).copy()
    if not src:
        print("❌ В YAML нет секции 'championat'")
        return None

    url = src.get("url")
    driver_path = src.get("driver_path")
    parser_section = src.get("selectors") or {}

    selectors = {
        "page_container":        parser_section.get("page_container") or "div.page-content",
        "date_group":            parser_section.get("date_group") or "div.news-items__head",
        "list_item":             parser_section.get("news_item_container") or "div.news-item",
        "list_item_url":         parser_section.get("article_link") or "div.news-item__content a.news-item__title",
        "list_item_title":       parser_section.get("article_link") or "div.news-item__content a.news-item__title",
        "list_item_published":   parser_section.get("time") or "div.news-item__time",
        "article_title":         parser_section.get("article_title") or "div.article-head__title",
        "article_body":          parser_section.get("article_body_container") or "div#articleBody",
        "article_tags":          parser_section.get("article_tags") or "a.tags__item",
        "article_images":        parser_section.get("article_images"),
        "article_videos":        parser_section.get("article_videos"),
    }

    cfg = {
        "base_url": url,
        "url": url,
        "timeout": 25,
        "delay": 1,
        "selectors": selectors,
    }

    if driver_path:
        os.environ["CHROME_DRIVER_PATH"] = driver_path

    required = ("list_item", "list_item_url", "list_item_title", "article_title", "article_body")
    missing = [k for k in required if not selectors.get(k)]
    if missing:
        print("❌ Некорректная конфигурация Championat. Отсутствуют селекторы:", ", ".join(missing))
        return None

    return cfg

# ===================== Сохранение новости/тегов =====================
def upsert_news(conn: sqlite3.Connection, f: Dict[str, str], article: Dict[str, Any]) -> Optional[int]:
    cur = conn.cursor()

    url = article.get("url")
    nurl = normalize_url(url)
    if not nurl or not f["url"]:
        print("⚠️ Пропуск записи без URL или без столбца URL в БД")
        return None

    title = sanitize_text(article.get("title"))
    content = sanitize_text(article.get("body"))
    published = article.get("published")
    source = "Championat.com"
    lang = "ru"
    imgs = dedupe(article.get("images") or [])
    vids = dedupe(article.get("videos") or [])
    main_image = imgs[0] if imgs else None

    cur.execute(f"SELECT {f.get('id','rowid')}, {f['is_published']} FROM news WHERE {f['url']} = ?", (nurl,))
    row = cur.fetchone()
    if row:
        news_id, _ = row
        sets, vals = [], []
        if f["title"] and title:         sets.append(f"{f['title']} = ?");         vals.append(title)
        if f["content"] and content:     sets.append(f"{f['content']} = ?");       vals.append(content)
        if f["published"] and published: sets.append(f"{f['published']} = ?");     vals.append(published)
        if f["source"]:                  sets.append(f"{f['source']} = ?");        vals.append(source)
        if f["lang"]:                    sets.append(f"{f['lang']} = ?");          vals.append(lang)
        if f["image_url"] and main_image:
            sets.append(f"{f['image_url']} = ?"); vals.append(main_image)
        img_json = json.dumps(imgs, ensure_ascii=False)
        vid_json = json.dumps(vids, ensure_ascii=False)
        if f["image_urls"]:  sets.append(f"{f['image_urls']} = ?"); vals.append(img_json)
        if f["images"] and f["images"] != f["image_urls"]:
            sets.append(f"{f['images']} = ?"); vals.append(img_json)
        if f["video_urls"]:  sets.append(f"{f['video_urls']} = ?"); vals.append(vid_json)
        if f["videos"] and f["videos"] != f["video_urls"]:
            sets.append(f"{f['videos']} = ?"); vals.append(vid_json)

        if sets:
            sql = f"UPDATE news SET {', '.join(sets)} WHERE {f['url']} = ?"
            vals.append(nurl)
            cur.execute(sql, tuple(vals))
        conn.commit()
        print(f"    ✅ Новость обновлена: {title or nurl}")
        return news_id
    else:
        cols = [f["url"]]; vals = [nurl]
        if f["title"] and title:         cols.append(f["title"]);         vals.append(title)
        if f["content"] and content:     cols.append(f["content"]);       vals.append(content)
        if f["published"] and published: cols.append(f["published"]);     vals.append(published)
        if f["source"]:                  cols.append(f["source"]);        vals.append(source)
        if f["lang"]:                    cols.append(f["lang"]);          vals.append(lang)
        if f["image_url"] and main_image:
            cols.append(f["image_url"]); vals.append(main_image)
        img_json = json.dumps(imgs, ensure_ascii=False)
        vid_json = json.dumps(vids, ensure_ascii=False)
        if f["image_urls"]:  cols.append(f["image_urls"]);  vals.append(img_json)
        if f["images"] and f["images"] != f["image_urls"]:
            cols.append(f["images"]);    vals.append(img_json)
        if f["video_urls"]:  cols.append(f["video_urls"]);  vals.append(vid_json)
        if f["videos"] and f["videos"] != f["video_urls"]:
            cols.append(f["videos"]);    vals.append(vid_json)
        if f["is_published"]:
            cols.append(f["is_published"]); vals.append(0)

        placeholders = ", ".join(["?"] * len(vals))
        sql = f"INSERT INTO news ({', '.join(cols)}) VALUES ({placeholders})"
        cur.execute(sql, tuple(vals))
        conn.commit()

        cur.execute(f"SELECT {f.get('id','rowid')} FROM news WHERE {f['url']} = ?", (nurl,))
        row = cur.fetchone()
        news_id = row[0] if row else None

        print(f"    ✅ Новость добавлена: {title or nurl}")
        return news_id


def _norm_tag_url(u: str | None) -> str | None:
    if not u:
        return None
    u = u.strip()
    if not u:
        return None
    if not u.startswith("http"):
        if not u.startswith("/"):
            u = "/" + u
        u = "https://www.championat.com" + u
    u = u.split("?", 1)[0].rstrip("/")
    return u
def upsert_article_tags(conn: sqlite3.Connection, news_id: int, tags: List[Dict[str, Any]]):
    """
    Надёжный апсерт тегов:
    - ключ: URL (нормализуем), если URL нет — пробуем по name (без уникального индекса по имени)
    - если тег уже есть -> ДОзаполняем type/url/entity_id (только если пусты)
    - создаём связь в news_article_tags
    """
    if not news_id or not tags:
        return

    from urllib.parse import urlparse, urlunparse  # локально, чтобы не трогать верх файла
    cur = conn.cursor()

    # Индексы (URL уникален; по паре news_id/tag_id уникальность связи)
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_tags_url_unique ON tags(url);")
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_news_article_tags_uniq ON news_article_tags(news_id, tag_id);")

    for t in tags:
        raw_name = (t.get("name") or "").strip()
        raw_url  = t.get("url")
        ttype    = (t.get("type") or None)
        entity_id = t.get("entity_id")

        # Нормализованный URL (используем для вставки/апдейта)
        nurl = _norm_tag_url(raw_url)

        # Кандидаты для поиска существующего тега строим ИЗ raw_url:
        # абсолютный/относительный, со/без хвостового '/', c www/без www
        candidates: List[str] = []
        if raw_url:
            u = str(raw_url).strip()
            if not u.startswith("http"):
                if not u.startswith("/"):
                    u = "/" + u
                abs_u = "https://www.championat.com" + u
                rel_u = u
            else:
                abs_u = u.split("?", 1)[0]
                _p = urlparse(abs_u)
                rel_u = _p.path

            def _alts(x: str) -> List[str]:
                x = x.split("?", 1)[0]
                base = x.rstrip("/")
                return [base, base + "/"]

            # базовые варианты
            candidates.extend(_alts(abs_u))
            candidates.extend(_alts(rel_u))

            # вариант без www (если вдруг так хранится)
            if abs_u.startswith("https://www.championat.com"):
                no_www = abs_u.replace("https://www.championat.com", "https://championat.com", 1)
                candidates.extend(_alts(no_www))

            # убрать дубликаты, сохранив порядок
            seen = set()
            candidates = [x for x in candidates if not (x in seen or seen.add(x))]

        tag_id = None

        try:
            if candidates:
                # 1) пытаемся найти по любому из эквивалентных URL
                placeholders = ",".join("?" for _ in candidates)
                cur.execute(
                    f"SELECT id, name, type, entity_id FROM tags WHERE url IN ({placeholders}) LIMIT 1",
                    candidates
                )
                row = cur.fetchone()
            else:
                row = None

            if row:
                tag_id = row[0]
                # Мягкое обновление: дополняем пустые поля
                sets, vals = [], []
                if raw_name and (row[1] or "").strip() == "":
                    sets.append("name = ?"); vals.append(raw_name)
                if ttype and (row[2] is None or str(row[2]).strip() == ""):
                    sets.append("type = ?"); vals.append(ttype)
                if entity_id is not None and row[3] is None:
                    sets.append("entity_id = ?"); vals.append(entity_id)
                if sets:
                    cur.execute(f"UPDATE tags SET {', '.join(sets)} WHERE id = ?", (*vals, tag_id))

            else:
                if nurl:
                    # Вставляем по нормализованному URL
                    cur.execute(
                        "INSERT OR IGNORE INTO tags (name, url, type, entity_id) VALUES (?, ?, ?, ?)",
                        (raw_name or None, nurl, ttype, entity_id)
                    )
                    # Берём id уже существующей/вставленной записи — снова через IN-кандидаты
                    if candidates:
                        placeholders = ",".join("?" for _ in candidates)
                        cur.execute(
                            f"SELECT id FROM tags WHERE url IN ({placeholders}) ORDER BY id DESC LIMIT 1",
                            candidates
                        )
                        r = cur.fetchone()
                        tag_id = r[0] if r else None
                    else:
                        tag_id = None
                else:
                    # URL нет — fallback по имени (НЕ делаем UNIQUE по name!)
                    if not raw_name:
                        continue
                    cur.execute("SELECT id, url, type, entity_id FROM tags WHERE name = ? COLLATE NOCASE ORDER BY id DESC LIMIT 1", (raw_name,))
                    row = cur.fetchone()
                    if row:
                        tag_id = row[0]
                        sets, vals = [], []
                        if nurl and (row[1] is None or str(row[1]).strip() == ""):
                            sets.append("url = ?"); vals.append(nurl)
                        if ttype and (row[2] is None or str(row[2]).strip() == ""):
                            sets.append("type = ?"); vals.append(ttype)
                        if entity_id is not None and row[3] is None:
                            sets.append("entity_id = ?"); vals.append(entity_id)
                        if sets:
                            cur.execute(f"UPDATE tags SET {', '.join(sets)} WHERE id = ?", (*vals, tag_id))
                    else:
                        cur.execute(
                            "INSERT INTO tags (name, url, type, entity_id) VALUES (?, NULL, ?, ?)",
                            (raw_name, ttype, entity_id)
                        )
                        tag_id = cur.lastrowid

            if tag_id:
                cur.execute(
                    "INSERT OR IGNORE INTO news_article_tags (news_id, tag_id) VALUES (?, ?)",
                    (news_id, tag_id)
                )

        except sqlite3.IntegrityError as e:
            # На случай гонки/дубля: добираем существующий id и ставим связь
            r = None
            if candidates:
                placeholders = ",".join("?" for _ in candidates)
                cur.execute(
                    f"SELECT id FROM tags WHERE url IN ({placeholders}) ORDER BY id DESC LIMIT 1",
                    candidates
                )
                r = cur.fetchone()
            if not r and raw_name:
                cur.execute("SELECT id FROM tags WHERE name = ? COLLATE NOCASE ORDER BY id DESC LIMIT 1", (raw_name,))
                r = cur.fetchone()
            if r:
                tag_id = r[0]
                cur.execute("INSERT OR IGNORE INTO news_article_tags (news_id, tag_id) VALUES (?, ?)", (news_id, tag_id))
            print(f"    ⚠️ Tag upsert warning for {raw_name or nurl}: {e}")

    conn.commit()



# ===================== Загрузка HTML (без parser._wait_for_page_load) =====================
def get_soup(driver, url: str, css_to_wait: str, timeout: int = 25) -> Optional[BeautifulSoup]:
    try:
        driver.get(url)
        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, css_to_wait))
        )
        return BeautifulSoup(driver.page_source, "html.parser")
    except Exception as e:
        print(f"❌ Ошибка загрузки {url}: {e}")
        return None

# ===================== Сбор карточек до якоря (page-content + дата группы) =====================
async def collect_until_anchor_by_url(
    parser: ChampParserSelenium,
    cfg: Dict[str, Any],
    anchor_url: Optional[str],
    max_pages: int = 50
) -> List[Dict[str, Any]]:
    base_url = cfg.get("base_url") or cfg.get("url")
    anchor_norm = normalize_url(anchor_url) if anchor_url else None
    all_metas: List[Dict[str, Any]] = []
    found_anchor = False

    for page in range(1, max_pages + 1):
        page_url = page_url_from_base(base_url, page)
        print(f"ℹ️ Загружаем список новостей с {page_url}...")
        soup = get_soup(parser.driver, page_url, parser.cfg["list_item"], timeout=cfg.get("timeout", 25))
        if not soup:
            continue
        container = soup.select_one(cfg["selectors"]["page_container"]) if cfg.get("selectors") else soup.select_one("div.page-content")
        if not container:
            continue

        groups = container.select("div.news-items")
        for grp in groups:
            head = grp.select_one(parser.cfg.get("date_group", "div.news-items__head"))
            group_date = _strip(head.text) if head else None
            items = grp.select(parser.cfg["list_item"])  # div.news-item

            for item in items:
                a = item.select_one(parser.cfg["list_item_url"])  # ссылка на новость
                if not a or not a.get("href"):
                    continue
                href = a.get("href").strip()
                full_url = normalize_url(href if href.startswith("http") else "https://championat.com" + href)

                t_el = item.select_one(parser.cfg.get("list_item_published", "div.news-item__time"))
                time_text = _strip(t_el.text) if t_el else None
                published_iso = to_iso(group_date, time_text)

                title_el = item.select_one(parser.cfg["list_item_title"])  # заголовок
                title = _strip(title_el.text) if title_el else None

                if anchor_norm and full_url == anchor_norm:
                    print(f"🔖 Найден якорь: {full_url} на странице {page}")
                    found_anchor = True
                    break

                all_metas.append({
                    "url": full_url,
                    "title": title,
                    "published": published_iso,
                })
            if found_anchor:
                break
        if found_anchor:
            break

    if not found_anchor:
        print("⚠️ Якорная новость не найдена на первых страницах.")
    return all_metas

# ===================== Основной цикл =====================
async def sync_news_since_anchor_url(max_pages: int = 50, manual_anchor: Optional[str] = None, dry_run: bool = False):
    db_path = PROJECT_ROOT / "database" / "prosport.db"

    try:
        init_db(db_path)
    except Exception as e:
        print(f"❌ Ошибка инициализации БД: {e}")
        return

    try:
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA foreign_keys = ON;")
    except Exception as e:
        print(f"❌ Не удалось открыть БД: {e}")
        return

    try:
        f = choose_field_names(conn)
        if not f["url"]:
            print("❌ В таблице news отсутствует столбец URL — работа невозможна.")
            return
        ensure_useful_indexes(conn, f)

        cfg = load_champ_config()
        if not cfg:
            print("❌ Синхронизация отменена из-за конфигурации.")
            return

        anchor_url = normalize_url(manual_anchor) if manual_anchor else get_anchor_from_db(conn, f)
        if anchor_url:
            print(f"🔖 Якорная новость: {anchor_url}")

        processed = 0
        existing_urls = set()

        async with ChampParserSelenium(cfg) as parser:
            if not parser.is_initialized:
                print("❌ WebDriver не инициализирован. Выход.")
                return

            metas = await collect_until_anchor_by_url(parser, cfg, anchor_url, max_pages)
            if not metas:
                print("ℹ️ Нет новых карточек для обработки.")
                return

            for i, meta in enumerate(metas, 1):
                nurl = normalize_url(meta.get("url"))
                if not nurl or nurl in existing_urls:
                    continue
                print(f"Обработка {i}/{len(metas)}: {nurl}")
                if dry_run:
                    continue
                try:
                    article = await parser.fetch_article(meta)
                    if article and not article.get("published") and meta.get("published"):
                        article["published"] = meta["published"]
                    if not article:
                        print("    ⚠️ Пропуск: не удалось распарсить статью.")
                        continue
                    news_id = upsert_news(conn, f, article)
                    if news_id:
                        upsert_article_tags(conn, news_id, article.get("tags") or [])
                    existing_urls.add(nurl)
                    processed += 1
                except (WebDriverException, TimeoutException, ReadTimeoutError) as e:
                    print(f"❌ Ошибка на статье {nurl}: {e} — пропускаю.")

        if not dry_run:
            print(f"🔄 Синхронизация завершена. Успешно обработано: {processed}")
        else:
            print(f"🔄 Dry-run завершён. Новых статей: {len(metas)} (не записаны в БД).")

    finally:
        try:
            conn.close()
            print("✅ Соединение с базой данных закрыто.")
        except Exception:
            pass

# ===================== CLI =====================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync Championat news (URL anchor, page-content, published_at)")
    parser.add_argument("--max-pages", type=int, default=50)
    parser.add_argument("--anchor-url", type=str, default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    try:
        asyncio.run(sync_news_since_anchor_url(
            max_pages=args.max_pages,
            manual_anchor=args.anchor_url,
            dry_run=args.dry_run,
        ))
    except Exception as e:
        print(f"Произошла непредвиденная ошибка вне основной функции: {e}")
