# -*- coding: utf-8 -*-
"""
Championat → JSON + (опционально) синхронизация в SQLite (news, tags, news_article_tags).

Примеры:
  # 1) Только лента → JSON
  python -m scripts.test_champ_to_json_sync --list 20 --out ./out/champ_list.json

  # 2) Лента + статьи (разобрать 10) → JSON (без БД)
  python -m scripts.test_champ_to_json_sync --limit 10 --out ./out/champ_articles.json

  # 3) Разобрать конкретные URL → JSON
  python -m scripts.test_champ_to_json_sync --url https://www.championat.com/... --out ./out/custom.json

  # 4) Перекрыть base_url поверх YAML
  python -m scripts.test_champ_to_json_sync --base-url https://www.championat.com/news/football/2.html --limit 5

  # 5) Моделировать sync_champ_news.py: парсинг → запись в БД
  python -m scripts.test_champ_to_json_sync --limit 5 \
      --db F:/projects/Projects/sport-news-bot/database/prosport.db --write --include-body

Если указан --db без --write — будет DRY‑RUN: операции только печатаются, БД не меняется.
"""
import os
import sys
import yaml
import json
import asyncio
import argparse
import sqlite3
from typing import List, Dict, Any, Optional, Tuple

PROJ_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
CONFIG_PATH = os.path.join(
    PROJ_ROOT, "parsers", "sources", "championat", "config", "sources_config.yml"
)

sys.path.insert(0, PROJ_ROOT)
from parsers.sources.championat.parsers.champ_parser import ChampParserSelenium  # noqa: E402

# -------------------- конфиг --------------------

def load_config() -> dict:
    path = os.path.abspath(CONFIG_PATH)
    with open(path, encoding="utf-8") as f:
        all_cfg = yaml.safe_load(f) or {}
    cfg = all_cfg.get("championat") or {}
    if "base_url" not in cfg and "url" in cfg:
        cfg["base_url"] = cfg["url"]
    return cfg

# -------------------- JSON I/O --------------------

def save_json(data: Any, out_path: str) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(out_path)) or ".", exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"💾 JSON сохранён: {os.path.abspath(out_path)}")


def read_urls_file(path: str) -> List[str]:
    urls: List[str] = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            urls.append(line)
    return urls


def compact_article(art: Dict[str, Any], include_body: bool) -> Dict[str, Any]:
    body = (art.get("body") or "")
    return {
        "url": art.get("url"),
        "title": art.get("title"),
        "published": art.get("published"),
        "summary": art.get("summary"),
        "tags": art.get("tags") or [],
        "images": art.get("images") or [],
        "videos": art.get("videos") or [],
        **({"body": body} if include_body else {}),
    }

# -------------------- DB helpers --------------------

def table_exists(con: sqlite3.Connection, name: str) -> bool:
    return con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone() is not None


def columns(con: sqlite3.Connection, table: str):
    return [r[1] for r in con.execute(f"PRAGMA table_info({table})").fetchall()]


def pick(cols, candidates):
    low = {c.lower(): c for c in cols}
    for c in candidates:
        if c in low:
            return low[c]
    return None


def ensure_indexes(cur: sqlite3.Cursor) -> None:
    cur.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_news_article_tags_unique
        ON news_article_tags(news_id, tag_id)
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_news_article_tags_tag ON news_article_tags(tag_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_tags_url ON tags(url)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_tags_name ON tags(name)")

# ----- tag resolve -----
import re as _re
from urllib.parse import urlparse as _urlparse

def _norm_tag_url(u: Optional[str]) -> str:
    if not u:
        return ""
    u = u.strip()
    if u.startswith('/'):
        path = u
    else:
        p = _urlparse(u)
        path = p.path or u
    path = _re.sub(r"/+\Z", "", path)
    if not path.startswith('/'):
        path = '/' + path
    return path


def resolve_tag_id(cur: sqlite3.Cursor, tag: dict) -> int:
    name = (tag.get('name') or '').strip()
    typ  = (tag.get('type') or '').strip() or None
    url_raw  = (tag.get('url') or '').strip()
    url_norm = _norm_tag_url(url_raw)
    url_abs  = url_raw if url_raw.startswith('http') else (('https://www.championat.com' + url_norm) if url_norm else '')

    row = cur.execute("SELECT id, name, url, type FROM tags WHERE url IN (?, ?) LIMIT 1", (url_norm, url_abs)).fetchone()
    if row:
        tag_id, cur_name, cur_url, cur_type = row
        updates, params = [], []
        if name and cur_name != name:
            updates.append('name = ?'); params.append(name)
        if url_norm and (cur_url or '') != url_norm:
            updates.append('url = ?'); params.append(url_norm)
        if typ and (cur_type or '') != typ:
            updates.append('type = ?'); params.append(typ)
        if updates:
            params.append(tag_id)
            cur.execute(f"UPDATE tags SET {', '.join(updates)} WHERE id = ?", params)
        return int(tag_id)

    if name:
        row = cur.execute("SELECT id, name, url, type FROM tags WHERE name = ? COLLATE NOCASE LIMIT 1", (name,)).fetchone()
        if row:
            tag_id, cur_name, cur_url, cur_type = row
            updates, params = [], []
            if url_norm and (cur_url or '') != url_norm:
                updates.append('url = ?'); params.append(url_norm)
            if typ and (cur_type or '') != typ:
                updates.append('type = ?'); params.append(typ)
            if updates:
                params.append(tag_id)
                cur.execute(f"UPDATE tags SET {', '.join(updates)} WHERE id = ?", params)
            return int(tag_id)

    cur.execute("INSERT INTO tags (name, url, type) VALUES (?, ?, ?)", (name or (url_norm or url_abs) or None, url_norm or (url_abs or None), typ))
    return int(cur.lastrowid)

# ----- news upsert & link -----

def detect_news_columns(con: sqlite3.Connection) -> Tuple[str, str, str, Optional[str]]:
    if not table_exists(con, 'news'):
        raise SystemExit("❌ Нет таблицы 'news' — создайте схему заранее.")
    cols = columns(con, 'news')
    id_col = pick(cols, ['id','news_id']) or 'id'
    title_col = pick(cols, ['title','headline','name']) or 'title'
    url_col = pick(cols, ['url','link']) or 'url'
    pub_col = pick(cols, ['published_at','published','pub_date','date','datetime'])
    return id_col, title_col, url_col, pub_col


def upsert_news(cur: sqlite3.Cursor, article: dict, pub_col: Optional[str]) -> int:
    url = (article.get('url') or '').strip()
    row = cur.execute("SELECT id FROM news WHERE url = ?", (url,)).fetchone()
    if row:
        news_id = int(row[0])
        cur.execute(
            f"UPDATE news SET title = COALESCE(?, title){', ' + pub_col + ' = ?' if pub_col else ''} WHERE id = ?",
            (article.get('title'), article.get('published') if pub_col else None, news_id) if pub_col else (article.get('title'), news_id)
        )
        return news_id
    if pub_col:
        cur.execute(
            f"INSERT INTO news (title, url, {pub_col}) VALUES (?, ?, ?)",
            (article.get('title'), url, article.get('published'))
        )
    else:
        cur.execute("INSERT INTO news (title, url) VALUES (?, ?)", (article.get('title'), url))
    return int(cur.lastrowid)


def link_news_tags(cur: sqlite3.Cursor, news_id: int, tags: list) -> int:
    linked = 0
    for tag in tags or []:
        try:
            tag_id = resolve_tag_id(cur, tag)
            cur.execute("INSERT OR IGNORE INTO news_article_tags (news_id, tag_id) VALUES (?, ?)", (news_id, tag_id))
            linked += 1
        except Exception as e:
            print(f"  ⚠️ Ошибка связи тега: {e} — {tag}")
    return linked

# -------------------- CLI --------------------

def print_article(art: dict, include_body: bool) -> None:
    tags = [t.get("name") or t.get("url") for t in (art.get("tags") or [])]
    body = (art.get("body") or "").strip()
    print("\n— — — СТАТЬЯ — — —")
    print("URL:      ", art.get("url"))
    print("Title:    ", art.get("title"))
    print("Published:", art.get("published"))
    print("Tags:     ", tags)
    if include_body:
        print("Body:     ", (body[:800] + "…") if len(body) > 800 else body)


def print_list(items: list, k: int) -> None:
    k = min(k, len(items))
    print(f"\n— — — ПЕРВЫЕ {k} ЭЛЕМЕНТОВ ЛЕНТЫ — — —")
    for i in range(k):
        it = items[i]
        print(f"[{i}] {it.get('published')} — {it.get('title')}\n    {it.get('url')}")


async def main():
    ap = argparse.ArgumentParser(description="Championat → JSON (+опц. SQLite sync)")
    ap.add_argument("--list", type=int, default=0, help="Сколько элементов ленты просто вывести/сохранить (без статей)")
    ap.add_argument("--limit", type=int, default=10, help="Сколько статей разобрать из ленты (0 = не разбирать)")
    ap.add_argument("--url", action="append", help="Добавить конкретный URL статьи (можно несколько раз)")
    ap.add_argument("--urls-file", help="Путь к файлу со списком URL (по одному в строке)")
    ap.add_argument("--base-url", help="Перекрыть base_url из YAML для ленты")
    ap.add_argument("--out", default="./out/champ_dump.json", help="Куда сохранить JSON")
    ap.add_argument("--include-body", action="store_true", help="Включать полный текст body в JSON/печать")
    # DB options
    ap.add_argument("--db", help="Путь к SQLite (если указан — будет dry‑run, пока не задан --write)")
    ap.add_argument("--write", action="store_true", help="Разрешить запись в БД (по умолчанию dry‑run)")
    args = ap.parse_args()

    cfg = load_config()
    if args.base_url:
        cfg["base_url"] = args.base_url

    result: Dict[str, Any] = {
        "config": {k: v for k, v in cfg.items() if k in ("base_url", "timeout", "delay")},
        "list": [],
        "articles": [],
    }

    # собрать списки целей
    target_urls: List[str] = []
    if args.url:
        target_urls.extend(args.url)
    if args.urls_file:
        target_urls.extend(read_urls_file(args.urls_file))

    async with ChampParserSelenium(cfg) as parser:
        if parser.driver is None:
            print("❌ WebDriver не инициализирован. Проверь соответствие Chrome ↔ ChromeDriver.")
            return

        items = await parser.fetch_list()
        print(f"📰 Лента: {len(items)} элементов")
        if args.list > 0:
            result["list"] = items[: args.list]

        # Если есть прямые URL — разбираем их
        articles: List[Dict[str, Any]] = []
        if target_urls:
            for u in target_urls:
                meta = {"url": u, "title": None, "published": None, "summary": None}
                art = await parser.fetch_article(meta)
                if art:
                    articles.append(art)
        else:
            # иначе — разбираем первые limit из ленты
            lim = max(0, args.limit)
            for meta in items[:lim]:
                art = await parser.fetch_article(meta)
                if art:
                    articles.append(art)

        # вывод/JSON
        result["articles"] = [compact_article(a, args.include_body) for a in articles]
        if args.out:
            save_json(result, args.out)
        else:
            for a in articles:
                print_article(a, include_body=args.include_body)

        # --------- optional DB sync ---------
        if args.db:
            db_path = os.path.abspath(args.db)
            print(f"\n🗄️  DB path: {db_path} ({'WRITE' if args.write else 'DRY‑RUN'})")
            con = sqlite3.connect(db_path)
            con.row_factory = sqlite3.Row
            cur = con.cursor()

            # sanity checks
            if not table_exists(con, 'tags') or not table_exists(con, 'news_article_tags') or not table_exists(con, 'news'):
                print("❌ В БД отсутствуют обязательные таблицы: news, tags, news_article_tags.")
                con.close(); return

            ensure_indexes(cur)
            id_col, title_col, url_col, pub_col = detect_news_columns(con)
            print(f"   ↳ news columns: id={id_col}, title={title_col}, url={url_col}, published={pub_col}")

            total_new = total_upd = total_links = 0
            for a in articles:
                # news upsert
                if args.write:
                    nid = upsert_news(cur, a, pub_col)
                else:
                    exists = cur.execute("SELECT id FROM news WHERE url=?", (a.get('url'),)).fetchone()
                    if exists:
                        nid = int(exists[0])
                        print(f"DRY‑RUN: UPDATE news id={nid} title='{a.get('title')}' pub='{a.get('published')}'")
                    else:
                        print(f"DRY‑RUN: INSERT news url={a.get('url')} title='{a.get('title')}' pub='{a.get('published')}'")
                        nid = -1
                # tags link
                if args.write and nid != -1:
                    linked = link_news_tags(cur, nid, a.get('tags') or [])
                    total_links += linked
                else:
                    # simulate
                    print(f"DRY‑RUN: link {len(a.get('tags') or [])} tags for news url={a.get('url')}")

            if args.write:
                con.commit()
                print(f"✅ Запись завершена. Обработано статей: {len(articles)}. Связей добавлено: {total_links}.")
            else:
                print("ℹ️ DRY‑RUN завершён — БД не изменялась.")
            con.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print("❌ Ошибка выполнения:", e)
