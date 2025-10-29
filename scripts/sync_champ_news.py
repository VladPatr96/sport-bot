# sync_champ_news.py — Championat incremental sync (anchor by URL, page-content, published_at)

import os
import re
import json
import argparse
import asyncio
import sqlite3
import logging

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

from categorizer.db_tags import link_article_tag, upsert_alias_from_tag, upsert_tag
from categorizer.tag_utils import normalize_tag_name, normalize_tag_type, normalize_tag_url
from cluster.fingerprints import compute_signatures
from db.utils import get_conn
from database.prosport_db import init_db
from parsers.sources.championat.parsers.champ_parser import ChampParserSelenium

from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parents[1]

logger = logging.getLogger(__name__)

# ===================== Текст/дата =====================



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


def upsert_fingerprint(conn, news_id: int, title: str, tags: List[Dict[str, Any]]) -> None:
    entities = {"sport": None, "tournament": None, "team": None, "player": None}
    for tag in tags or []:
        tag_type = (tag.get("type") or "").strip().lower()
        name = tag.get("name")
        if not name:
            continue
        if tag_type == "tournament" and not entities["tournament"]:
            entities["tournament"] = name
        elif tag_type == "team" and not entities["team"]:
            entities["team"] = name
        elif tag_type == "player" and not entities["player"]:
            entities["player"] = name
        elif tag_type == "sport" and not entities["sport"]:
            entities["sport"] = name
    title_sig, entity_sig = compute_signatures(
        title or "",
        {
            "sport": entities["sport"],
            "tournament": entities["tournament"],
            "team": entities["team"],
            "player": entities["player"],
        },
    )
    conn.execute(
        """
        INSERT INTO content_fingerprints (news_id, title_sig, entity_sig, created_at)
        VALUES (?, ?, ?, STRFTIME('%Y-%m-%dT%H:%M:%SZ','now'))
        ON CONFLICT(news_id) DO UPDATE SET
            title_sig = excluded.title_sig,
            entity_sig = excluded.entity_sig
        """,
        (news_id, title_sig, entity_sig),
    )
    logger.info(
        'fingerprint upserted news_id=%s title_sig="%s" entity_sig=%s',
        news_id,
        title_sig,
        entity_sig,
    )

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
        logger.error("Championat config file is missing: %s", config_path)
        return None
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
    except Exception as e:
        logger.exception("Failed to parse Championat config")
        return None

    src = (raw.get("championat") or {}).copy()
    if not src:
        logger.error("Section 'championat' not found in config")
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
        logger.error("Championat config is missing required selectors: %s", ", ".join(missing))
        return None

    return cfg

# ===================== Сохранение новости/тегов =====================


def upsert_news(
    conn: sqlite3.Connection,
    f: Dict[str, str],
    article: Dict[str, Any],
) -> Tuple[Optional[int], bool]:
    cur = conn.cursor()

    url = article.get("url")
    nurl = normalize_url(url)
    if not nurl or not f["url"]:
        logger.warning("Skipping article without URL or URL column")
        return None, False

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
        sets: List[str] = []
        vals: List[Any] = []
        if f["title"] and title:
            sets.append(f"{f['title']} = ?"); vals.append(title)
        if f["content"] and content:
            sets.append(f"{f['content']} = ?"); vals.append(content)
        if f["published"] and published:
            sets.append(f"{f['published']} = ?"); vals.append(published)
        if f["source"]:
            sets.append(f"{f['source']} = ?"); vals.append(source)
        if f["lang"]:
            sets.append(f"{f['lang']} = ?"); vals.append(lang)
        if f["image_url"] and main_image:
            sets.append(f"{f['image_url']} = ?"); vals.append(main_image)
        img_json = json.dumps(imgs, ensure_ascii=False)
        vid_json = json.dumps(vids, ensure_ascii=False)
        if f["image_urls"]:
            sets.append(f"{f['image_urls']} = ?"); vals.append(img_json)
        if f["images"] and f["images"] != f["image_urls"]:
            sets.append(f"{f['images']} = ?"); vals.append(img_json)
        if f["video_urls"]:
            sets.append(f"{f['video_urls']} = ?"); vals.append(vid_json)
        if f["videos"] and f["videos"] != f["video_urls"]:
            sets.append(f"{f['videos']} = ?"); vals.append(vid_json)

        if sets:
            vals.append(nurl)
            sql = f"UPDATE news SET {', '.join(sets)} WHERE {f['url']} = ?"
            cur.execute(sql, tuple(vals))
        return news_id, False

    cols = [f["url"]]
    vals = [nurl]
    if f["title"] and title:
        cols.append(f["title"]); vals.append(title)
    if f["content"] and content:
        cols.append(f["content"]); vals.append(content)
    if f["published"] and published:
        cols.append(f["published"]); vals.append(published)
    if f["source"]:
        cols.append(f["source"]); vals.append(source)
    if f["lang"]:
        cols.append(f["lang"]); vals.append(lang)
    if f["image_url"] and main_image:
        cols.append(f["image_url"]); vals.append(main_image)
    img_json = json.dumps(imgs, ensure_ascii=False)
    vid_json = json.dumps(vids, ensure_ascii=False)
    if f["image_urls"]:
        cols.append(f["image_urls"]); vals.append(img_json)
    if f["images"] and f["images"] != f["image_urls"]:
        cols.append(f["images"]); vals.append(img_json)
    if f["video_urls"]:
        cols.append(f["video_urls"]); vals.append(vid_json)
    if f["videos"] and f["videos"] != f["video_urls"]:
        cols.append(f["videos"]); vals.append(vid_json)
    if f["is_published"]:
        cols.append(f["is_published"]); vals.append(0)

    placeholders = ", ".join(["?"] * len(vals))
    sql = f"INSERT INTO news ({', '.join(cols)}) VALUES ({placeholders})"
    cur.execute(sql, tuple(vals))

    cur.execute(f"SELECT {f.get('id','rowid')} FROM news WHERE {f['url']} = ?", (nurl,))
    row = cur.fetchone()
    news_id = row[0] if row else None
    return news_id, True
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


def upsert_article_tags(
    conn: sqlite3.Connection,
    news_id: int,
    tags: List[Dict[str, Any]],
    *,
    source: str = 'championat',
    lang: str = 'ru',
    context: Optional[str] = None,
) -> Dict[str, int]:
    """Normalize tag payload and ensure tags and links exist."""
    counters = {
        'processed': 0,
        'created': 0,
        'linked': 0,
        'duplicates': 0,
        'invalid': 0,
        'aliases': 0,
    }

    if not news_id or not tags:
        return counters

    for raw in tags:
        raw_name = raw.get('name')
        raw_url = raw.get('url')
        raw_type = raw.get('type')

        name_normalized = normalize_tag_name(raw_name)
        url_normalized = normalize_tag_url(raw_url)
        type_normalized = normalize_tag_type(
            raw_type,
            name=raw_name,
            url=raw_url,
            context=context,
        )

        if not name_normalized and not url_normalized:
            counters['invalid'] += 1
            continue

        tag_id, created = upsert_tag(
            conn,
            name=name_normalized or (raw_name or '').strip(),
            url=url_normalized or None,
            tag_type=type_normalized,
            context=context,
        )
        if created:
            counters['created'] += 1

        if link_article_tag(conn, news_id=news_id, tag_id=tag_id):
            counters['linked'] += 1
        else:
            counters['duplicates'] += 1

        if upsert_alias_from_tag(
            conn,
            tag_id=tag_id,
            name=name_normalized or (raw_name or '').strip(),
            tag_type=type_normalized,
            source=source,
            lang=lang,
        ):
            counters['aliases'] += 1

        counters['processed'] += 1

    return counters


# ===================== Загрузка HTML (без parser._wait_for_page_load) =====================
def get_soup(driver, url: str, css_to_wait: str, timeout: int = 25) -> Optional[BeautifulSoup]:
    try:
        driver.get(url)
        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, css_to_wait))
        )
        return BeautifulSoup(driver.page_source, "html.parser")
    except Exception as e:
        logger.warning("Failed to load %s: %s", url, e)
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
        logger.info("Fetching listing page: %s", page_url)
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
                    logger.info("Anchor found: %s (page %s)", full_url, page)
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
        logger.warning("Anchor article not found within page limit")
    return all_metas

# ===================== Основной цикл =====================



async def sync_news_since_anchor_url(
    max_pages: int = 50,
    manual_anchor: Optional[str] = None,
    dry_run: bool = False,
    smoke: bool = False,
) -> None:
    db_path = PROJECT_ROOT / "database" / "prosport.db"

    logger.info("Starting Championat sync (smoke=%s, dry_run=%s)", smoke, dry_run)
    logger.info("Database path: %s", db_path)

    try:
        init_db(db_path)
    except Exception:
        logger.exception("Failed to initialize database at %s", db_path)
        return

    try:
        conn = get_conn(db_path)
    except Exception:
        logger.exception("Failed to connect to database at %s", db_path)
        return

    try:
        f = choose_field_names(conn)
        if not f["url"]:
            logger.error("Table news does not expose a URL column; aborting sync")
            return
        ensure_useful_indexes(conn, f)

        cfg = load_champ_config()
        if not cfg:
            logger.error("Championat config is missing or invalid; aborting sync")
            return

        anchor_url = normalize_url(manual_anchor) if manual_anchor else get_anchor_from_db(conn, f)
        if anchor_url:
            logger.info("Anchor URL: %s", anchor_url)

        processed_total = 0
        inserted_total = 0
        skipped_total = 0
        existing_urls = set()
        tags_created_total = 0
        tag_links_created_total = 0
        tag_links_skipped_total = 0
        aliases_upserted_total = 0

        async with ChampParserSelenium(cfg) as parser:
            if not parser.is_initialized:
                logger.error("WebDriver failed to initialize; stopping sync")
                return

            pages_limit = 1 if smoke else max_pages
            metas = await collect_until_anchor_by_url(parser, cfg, anchor_url, pages_limit)
            if not metas:
                logger.info("No articles collected from Championat")
                return

            if smoke:
                metas = metas[:3]
                logger.info("Smoke mode enabled: limiting inserts to %s article(s)", len(metas))

            total_candidates = len(metas)
            logger.info("Collected %s article candidates", total_candidates)

            for index, meta in enumerate(metas, 1):
                nurl = normalize_url(meta.get("url"))
                if not nurl:
                    logger.warning("Skipping item %s/%s: empty URL", index, total_candidates)
                    skipped_total += 1
                    continue
                if nurl in existing_urls:
                    logger.info("Skipping duplicate from current batch: %s", nurl)
                    skipped_total += 1
                    continue

                logger.info("Processing item %s/%s: %s", index, total_candidates, nurl)
                existing_urls.add(nurl)
                processed_total += 1

                if dry_run:
                    skipped_total += 1
                    continue

                try:
                    article = await parser.fetch_article(meta)
                    if article and not article.get("published") and meta.get("published"):
                        article["published"] = meta["published"]
                    if not article:
                        logger.warning("Skipping %s: parser returned empty payload", nurl)
                        skipped_total += 1
                        continue

                    news_id, inserted = upsert_news(conn, f, article)
                    if not news_id:
                        skipped_total += 1
                        continue

                    if inserted:
                        inserted_total += 1
                        logger.info("Inserted article id=%s for %s", news_id, nurl)
                        upsert_fingerprint(conn, news_id, article.get("title", ""), article.get("tags") or [])
                    else:
                        skipped_total += 1
                        logger.info("Skipped duplicate article: %s", nurl)

                    tag_stats = upsert_article_tags(
                        conn,
                        news_id,
                        article.get("tags") or [],
                        source='championat',
                        lang='ru',
                        context=sanitize_text(article.get("title")),
                    )
                    tags_created_total += tag_stats['created']
                    tag_links_created_total += tag_stats['linked']
                    tag_links_skipped_total += tag_stats['duplicates']
                    aliases_upserted_total += tag_stats['aliases']
                    logger.info(
                        "Tags for %s: processed=%s linked=%s duplicates=%s invalid=%s",
                        nurl,
                        tag_stats['processed'],
                        tag_stats['linked'],
                        tag_stats['duplicates'],
                        tag_stats['invalid'],
                    )
                    conn.commit()
                except (WebDriverException, TimeoutException, ReadTimeoutError):
                    logger.exception("Failed to fetch article %s; skipping", nurl)
                    skipped_total += 1

        logger.info("Sync stats: processed=%s inserted=%s skipped=%s", processed_total, inserted_total, skipped_total)
        logger.info(
            "Tag stats: tags_created=%s tag_links_created=%s tag_links_skipped=%s aliases_upserted=%s",
            tags_created_total,
            tag_links_created_total,
            tag_links_skipped_total,
            aliases_upserted_total,
        )
        if dry_run:
            logger.info("Dry-run mode: no changes were written to the database")
        else:
            logger.info("Championat sync finished successfully")
    finally:
        try:
            conn.close()
            logger.info("Database connection closed")
        except Exception:
            pass





def report_tag_stats(sample_size: int = 3) -> None:
    conn = get_conn()
    try:
        cur = conn.cursor()
        news_ids = [row[0] for row in cur.execute(
            "SELECT id FROM news ORDER BY id DESC LIMIT ?",
            (sample_size,),
        ).fetchall()]
        if not news_ids:
            logger.info("Report-only: no news rows found; nothing to report")
            return

        placeholders = ','.join('?' for _ in news_ids)
        links_count = cur.execute(
            f"SELECT COUNT(*) FROM news_article_tags WHERE news_id IN ({placeholders})",
            news_ids,
        ).fetchone()[0]

        tags_total = cur.execute("SELECT COUNT(*) FROM tags").fetchone()[0]
        aliases_total = cur.execute(
            "SELECT COUNT(*) FROM entity_aliases WHERE alias_normalized IS NOT NULL",
        ).fetchone()[0]

        logger.info("Report-only sample news_ids=%s", news_ids)
        logger.info(
            "Report-only counts: tag_links_recent=%s tags_total=%s aliases_total=%s",
            links_count,
            tags_total,
            aliases_total,
        )
        logger.info(
            "Report-only note: tag_links_skipped estimated as 0 due to missing candidate stats",
        )
    finally:
        conn.close()
# ===================== CLI =====================
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Sync Championat news (URL anchor, page-content, published_at)")
    parser.add_argument("--max-pages", type=int, default=50)
    parser.add_argument("--anchor-url", type=str, default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--smoke", action="store_true")
    parser.add_argument("--report-only", action="store_true")
    args = parser.parse_args()

    try:
        if args.report_only:
            report_tag_stats()
        else:
            asyncio.run(sync_news_since_anchor_url(
                max_pages=args.max_pages,
                manual_anchor=args.anchor_url,
                dry_run=args.dry_run,
                smoke=args.smoke,
            ))
    except Exception:
        logger.exception("Unexpected error while running sync command")

