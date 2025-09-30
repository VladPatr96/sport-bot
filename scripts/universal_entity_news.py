#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Universal Entity News — интерактивная выборка новостей по сущности
(спорт / турнир / команда / спортсмен) через tag_url → tag_id → news_ids → news.

Возможности:
  • Интерактивный выбор типа сущности и конкретной записи (поиск по подстроке или ID)
  • Умный поиск тега в таблице `tags` (url, нормализованный путь, LIKE по хвосту, slug/name)
  • Подсказка минимальной/максимальной даты публикаций в БД для выбранного тега
  • Выбор периода: пресеты (6ч/12ч/24ч/3д/7д/сегодня/вчера) или ручной FROM/TO
  • Выборка статей из `news` по списку `news_id` из `news_article_tags`
  • Опциональный fallback-парсинг HTML страницы тега (с заголовками, чтобы не ловить 403)
  • Экспорт результатов в CSV/JSON
  • Windows-safe обработка путей к БД, печать резолвленного пути при -v

Ожидаемая схема (имена колонок autodetect):
  - Таблица сущностей для каждого типа (ищется по кандидатам):
      team → teams/team; player → players/player; tournament → tournaments/tournament/leagues/league; sport → sports/sport
      поля: id | name/title | tag_url/tag/url
  - tags(id|tag_id, url|href [, slug] [, name/title/label])
  - news_article_tags(news_id|article_id, tag_id|label_id)
  - news(id|news_id, title|headline|name, url|link, published_at|published|pub_date|date|datetime [, source|origin|site])

Пример запуска:
  python -m scripts.universal_entity_news --db F:/projects/Projects/sport-news-bot/database/prosport.db -v
  python -m scripts.universal_entity_news --db ./database/prosport.db --export csv --export-path ./out/cska.csv
"""
from __future__ import annotations

import os
import re
import sys
import csv
import json
import sqlite3
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any, TYPE_CHECKING

# ---- typing-only ZoneInfo type to satisfy Pylance ----
if TYPE_CHECKING:
    from zoneinfo import ZoneInfo as ZoneInfoType  # for annotations only
else:  # runtime placeholder
    class ZoneInfoType:  # type: ignore
        pass

# ---- runtime import (may be None) ----
try:
    from zoneinfo import ZoneInfo as _ZoneInfo  # py>=3.9
except Exception:  # pragma: no cover
    try:
        from backports.zoneinfo import ZoneInfo as _ZoneInfo  # type: ignore
    except Exception:  # pragma: no cover
        _ZoneInfo = None  # type: ignore

try:
    import requests  # optional for fallback parse
except Exception:  # pragma: no cover
    requests = None  # type: ignore

# -----------------------------
# Models
# -----------------------------
@dataclass
class Entity:
    id: int
    name: str
    tag_url: str

@dataclass
class Article:
    id: int
    title: str
    url: str
    published_at: str  # ISO with 'T'
    source: Optional[str]

# -----------------------------
# TZ helpers
# -----------------------------

def get_tz() -> Optional[ZoneInfoType]:
    tz_name = os.environ.get("APP_TZ") or os.environ.get("TZ") or "UTC"
    try:
        return _ZoneInfo(tz_name) if _ZoneInfo else None
    except Exception:
        return None


def parse_dt_local(s: str, tz: Optional[ZoneInfoType]) -> datetime:
    s = s.strip()
    if not s:
        raise ValueError("empty datetime string")
    fmt = "%Y-%m-%d %H:%M" if " " in s else "%Y-%m-%d"
    dt = datetime.strptime(s, fmt)
    return dt.replace(tzinfo=tz) if tz else dt


def to_iso_T(dt: datetime) -> str:
    """Return 'YYYY-MM-DDTHH:MM:SS' in UTC-naive for lexicographic compare in SQLite."""
    if dt.tzinfo is not None and _ZoneInfo:
        dt = dt.astimezone(_ZoneInfo("UTC")).replace(tzinfo=None)
    return dt.isoformat(timespec="seconds")

# -----------------------------
# DB helpers
# -----------------------------

def resolve_db_path(db_arg: str) -> str:
    raw = db_arg.strip()
    if raw.lower().startswith("sqlite:///"):
        raw = raw[10:]
    raw = os.path.expandvars(os.path.expanduser(raw))
    raw = os.path.normpath(raw)
    abs_path = os.path.abspath(raw)
    parent = os.path.dirname(abs_path)
    if parent and not os.path.exists(parent):
        os.makedirs(parent, exist_ok=True)
    return abs_path


def connect_db(db_path: str, verbose: bool = False) -> sqlite3.Connection:
    resolved = resolve_db_path(db_path)
    if verbose:
        print(f"🗄️  DB path resolved to: {resolved}")
    con = sqlite3.connect(resolved)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys = ON;")
    return con


def table_exists(con: sqlite3.Connection, name: str) -> bool:
    return con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone() is not None


def columns(con: sqlite3.Connection, table: str) -> List[str]:
    return [r[1] for r in con.execute(f"PRAGMA table_info({table})").fetchall()]


def pick(cols: List[str], cands: List[str]) -> Optional[str]:
    low = {c.lower(): c for c in cols}
    for c in cands:
        if c in low:
            return low[c]
    return None

# -----------------------------
# Entity registry & loaders
# -----------------------------
ENTITY_TABLES: Dict[str, List[str]] = {
    "team":        ["teams", "team"],
    "player":      ["players", "player"],
    "tournament":  ["tournaments", "tournament", "leagues", "league"],
    "sport":       ["sports", "sport"],
}

NAME_FIELDS = ["name", "title"]
TAGURL_FIELDS = ["tag_url", "tag", "url"]


def load_entities(con: sqlite3.Connection, etype: str) -> Tuple[str, List[Entity]]:
    table = None
    for cand in ENTITY_TABLES.get(etype, []):
        if table_exists(con, cand):
            table = cand
            break
    if not table:
        raise SystemExit(f"❌ Не найдена таблица для типа '{etype}'. Ожидались: {ENTITY_TABLES.get(etype, [])}")

    cols = columns(con, table)
    id_col = pick(cols, ["id", f"{etype}_id"]) or cols[0]
    name_col = pick(cols, NAME_FIELDS) or id_col
    tag_col = pick(cols, TAGURL_FIELDS)
    if not tag_col:
        raise SystemExit(f"❌ В таблице '{table}' нет tag_url (ожид.: {TAGURL_FIELDS})")

    rows = con.execute(
        f"SELECT {id_col} AS id, {name_col} AS name, {tag_col} AS tag_url FROM {table} ORDER BY name"
    ).fetchall()
    return table, [Entity(int(r["id"]), str(r["name"]), str(r["tag_url"])) for r in rows]


def select_entity(entities: List[Entity], label: str) -> Entity:
    if not entities:
        raise SystemExit("❌ Сущностей нет в таблице.")
    print(f"\nНайдено {len(entities)} {label}. Введите ID или строку поиска, 'list' — показать первые 20.")
    while True:
        q = input("ID / поиск / list: ").strip()
        if not q:
            continue
        if q.lower() == 'list':
            for e in entities[:20]:
                print(f"  {e.id:>6}  {e.name}")
            continue
        if q.isdigit():
            eid = int(q)
            for e in entities:
                if e.id == eid:
                    return e
            print("⚠️ Нет такой записи.")
            continue
        hits = [e for e in entities if q.lower() in e.name.lower()]
        if not hits:
            print("⚠️ Не найдено, попробуйте иначе.")
            continue
        print(f"Найдено совпадений: {len(hits)}. Выберите ID (показаны первые 20):")
        for e in hits[:20]:
            print(f"  {e.id:>6}  {e.name}")
        while True:
            s = input("Введите ID из списка: ").strip()
            if s.isdigit():
                eid = int(s)
                for e in hits:
                    if e.id == eid:
                        return e
                print("⚠️ ID не из списка.")
            else:
                print("⚠️ Введите цифровой ID.")

# -----------------------------
# Period selection (with presets and robust input)
# -----------------------------

def select_period(tz: Optional[ZoneInfoType]) -> Tuple[datetime, datetime]:
    now = datetime.now(tz) if tz else datetime.now()
    while True:
        print("\nВыберите период:")
        print("  1) Последние 6 часов")
        print("  2) Последние 12 часов")
        print("  3) Последние 24 часа")
        print("  4) Последние 3 дня")
        print("  5) Последние 7 дней")
        print("  6) Сегодня")
        print("  7) Вчера")
        print("  8) Задать явно: FROM/TO")
        s = input("Ваш выбор [1-8]: ").strip()
        if s == '1':
            return now - timedelta(hours=6), now
        if s == '2':
            return now - timedelta(hours=12), now
        if s == '3':
            return now - timedelta(days=1), now
        if s == '4':
            return now - timedelta(days=3), now
        if s == '5':
            return now - timedelta(days=7), now
        if s == '6':
            start = now.replace(hour=0, minute=0, second=0, microsecond=0)
            return start, now
        if s == '7':
            y = (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            return y, y + timedelta(days=1)
        if s == '8':
            f = input("FROM (YYYY-MM-DD[ HH:MM], пусто — назад): ").strip()
            if not f:
                continue
            t = input("TO   (YYYY-MM-DD[ HH:MM], пусто — сейчас): ").strip()
            try:
                start = parse_dt_local(f, tz)
                end = parse_dt_local(t, tz) if t else (datetime.now(tz) if tz else datetime.now())
                if end <= start:
                    print("⚠️ TO должен быть позже FROM."); continue
                return start, end
            except Exception as e:
                print("⚠️ Некорректные даты:", e); continue
        print("⚠️ Введите число 1-8.")

# -----------------------------
# Tag helpers (smart lookup in tags)
# -----------------------------

def _norm_path(u: str) -> str:
    u = u.strip()
    m = re.sub(r"^https?://[^/]+", "", u, flags=re.I)
    m = m.rstrip('/')
    return m if m.startswith('/') else '/' + m


def find_tag_id_smart(con: sqlite3.Connection, tag_url: str, verbose: bool = False) -> int:
    if not table_exists(con, 'tags'):
        raise SystemExit("❌ Нет таблицы 'tags'.")
    cols = columns(con, 'tags')
    url_col  = pick(cols, ['url','href'])
    id_col   = pick(cols, ['id','tag_id']) or cols[0]
    slug_col = pick(cols, ['slug'])
    name_col = pick(cols, ['name','title','label'])

    if verbose:
        print(f"🔖 tag_url: {tag_url}")

    # 1) точный url
    if url_col:
        row = con.execute(f"SELECT {id_col} FROM tags WHERE {url_col}=?", (tag_url,)).fetchone()
        if row:
            return int(row[0])

    # 2) нормализованный путь / хвост
    path = _norm_path(tag_url)
    tail = path.split('/')[-1]
    if url_col:
        row = con.execute(f"SELECT {id_col} FROM tags WHERE {url_col} IN (?, ?) LIMIT 1", (path, path.rstrip('/'))).fetchone()
        if row:
            return int(row[0])
        row = con.execute(f"SELECT {id_col} FROM tags WHERE {url_col} LIKE ? ORDER BY LENGTH({url_col}) DESC LIMIT 1", (f"%{tail}%",)).fetchone()
        if row:
            return int(row[0])

    # 3) slug / name
    if slug_col:
        row = con.execute(f"SELECT {id_col} FROM tags WHERE {slug_col}=? COLLATE NOCASE", (tail,)).fetchone()
        if row:
            return int(row[0])
    if name_col:
        row = con.execute(f"SELECT {id_col} FROM tags WHERE {name_col} LIKE ? COLLATE NOCASE ORDER BY LENGTH({name_col}) ASC LIMIT 1", (f"%{tail}%",)).fetchone()
        if row:
            return int(row[0])

    # 4) показать кандидатов
    candidates: List[Dict[str, Any]] = []
    if url_col:
        candidates += [dict(r) for r in con.execute(f"SELECT {id_col} AS id, {url_col} AS url FROM tags WHERE {url_col} LIKE ? LIMIT 20", (f"%{tail}%",))]
    if slug_col:
        candidates += [dict(r) for r in con.execute(f"SELECT {id_col} AS id, {slug_col} AS slug FROM tags WHERE {slug_col} LIKE ? LIMIT 20", (f"%{tail}%",))]
    if name_col and not candidates:
        candidates += [dict(r) for r in con.execute(f"SELECT {id_col} AS id, {name_col} AS name FROM tags WHERE {name_col} LIKE ? LIMIT 20", (f"%{tail}%",))]

    if candidates:
        print("Не удалось выбрать однозначно. Кандидаты:")
        for r in candidates:
            print(" ", r)
        s = input("Введите нужный tag_id из списка: ").strip()
        if s.isdigit():
            return int(s)

    raise SystemExit(f"❌ Не найден tag по url: {tag_url} (пробовал url/slug/name/LIKE)")

# -----------------------------
# Linkage → news
# -----------------------------

def news_ids_by_tag(con: sqlite3.Connection, tag_id: int) -> List[int]:
    if not table_exists(con, 'news_article_tags'):
        raise SystemExit("❌ Нет таблицы 'news_article_tags' (связи новость↔тег).")
    nat_cols = columns(con, 'news_article_tags')
    news_col = pick(nat_cols, ['news_id','article_id']) or nat_cols[0]
    tag_col  = pick(nat_cols, ['tag_id','label_id']) or nat_cols[-1]
    rows = con.execute(f"SELECT {news_col} AS nid FROM news_article_tags WHERE {tag_col}=?", (tag_id,)).fetchall()
    return [int(r['nid']) for r in rows]


def news_date_bounds(con: sqlite3.Connection, news_ids: List[int]) -> Optional[Tuple[str, str]]:
    if not news_ids:
        return None
    if not table_exists(con, 'news'):
        return None
    ncols = columns(con, 'news')
    id_col = pick(ncols, ['id','news_id']) or ncols[0]
    pub_col = pick(ncols, ['published_at','published','pub_date','date','datetime'])
    if not pub_col:
        return None
    placeholders = ",".join(["?"]*len(news_ids))
    row = con.execute(
        f"SELECT MIN({pub_col}) AS mn, MAX({pub_col}) AS mx FROM news WHERE {id_col} IN ({placeholders})",
        news_ids,
    ).fetchone()
    return (row["mn"], row["mx"]) if row and row["mn"] and row["mx"] else None


def fetch_news(con: sqlite3.Connection, news_ids: List[int], start: datetime, end: datetime, limit: Optional[int]) -> List[Article]:
    if not news_ids:
        return []
    if not table_exists(con, 'news'):
        raise SystemExit("❌ Нет таблицы 'news'.")
    ncols = columns(con, 'news')
    id_col = pick(ncols, ['id','news_id']) or ncols[0]
    title_col = pick(ncols, ['title','headline','name']) or id_col
    url_col = pick(ncols, ['url','link']) or id_col
    pub_col = pick(ncols, ['published_at','published','pub_date','date','datetime'])
    source_col = pick(ncols, ['source','origin','site'])
    if not pub_col:
        raise SystemExit("❌ В 'news' нет колонки даты публикации (ожид.: published_at/published/pub_date/date/datetime)")

    sT, eT = to_iso_T(start), to_iso_T(end)
    placeholders = ",".join(["?"]*len(news_ids))
    sql = (
        f"SELECT {id_col} AS id, {title_col} AS title, {url_col} AS url, {pub_col} AS published, "
        + (f"{source_col} AS source" if source_col else "NULL AS source") +
        f" FROM news WHERE {id_col} IN ({placeholders}) AND {pub_col}>=? AND {pub_col}<? ORDER BY {pub_col} DESC"
    )
    params: Tuple[Any, ...] = (*news_ids, sT, eT)
    if limit:
        sql += " LIMIT ?"
        params += (limit,)

    out: List[Article] = []
    for r in con.execute(sql, params):
        out.append(Article(int(r['id']), str(r['title']), str(r['url']), str(r['published']), r['source']))
    return out

# -----------------------------
# Fallback parse (optional, with headers to avoid 403)
# -----------------------------
NEWS_ITEM_RE = re.compile(r'<div class="news-item__time">(?P<time>\d{2}:\d{2})</div>.*?<a href="(?P<href>/[^"]+)"[^>]*class="news-item__title[^"]*">(?P<title>[^<]+)</a>', re.S|re.I)
DATE_HEAD_RE = re.compile(r'<div class="news-items__head">(?P<date>\d{1,2} [а-яА-Я]+ \d{4})</div>')
MONTHS_RU = {'января':1,'февраля':2,'марта':3,'апреля':4,'мая':5,'июня':6,'июля':7,'августа':8,'сентября':9,'октября':10,'ноября':11,'декабря':12}


def parse_tag_page(tag_url: str, base: str = "https://www.championat.com") -> List[Article]:
    if not requests:
        print("⚠️ requests не установлен — fallback-парсер отключён")
        return []
    sess = requests.Session()
    sess.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "ru-RU,ru;q=0.9",
        "Referer": "https://www.championat.com/",
        "Connection": "keep-alive",
    })
    try:
        r = sess.get(tag_url, timeout=20)
        if r.status_code == 403:
            import time
            from random import randint
            time.sleep(1 + randint(0, 2))
            r = sess.get(tag_url + f"?_ts={int(time.time())}", timeout=20)
        r.raise_for_status()
    except Exception as e:
        print("⚠️ Не удалось скачать страницу тега:", e)
        return []

    html = r.text
    mdate = DATE_HEAD_RE.search(html)
    date_obj = None
    if mdate:
        try:
            dd, mm_rus, yyyy = mdate.group('date').split()
            date_obj = datetime(int(yyyy), MONTHS_RU[mm_rus.lower()], int(dd))
        except Exception:
            date_obj = None

    arts: List[Article] = []
    for m in NEWS_ITEM_RE.finditer(html):
        hh, mm = m.group('time').split(':')
        dt = datetime(date_obj.year, date_obj.month, date_obj.day, int(hh), int(mm)) if date_obj else None
        title = m.group('title').strip()
        href = m.group('href')
        full_url = href if href.startswith('http') else base.rstrip('/') + href
        iso = dt.isoformat(timespec='seconds') if dt else ''
        arts.append(Article(-1, title, full_url, iso, 'championat'))
    return arts

# -----------------------------
# Export helpers
# -----------------------------

def export_results(arts: List[Article], kind: Optional[str], path: Optional[str]) -> None:
    if not arts or not kind or not path:
        return
    os.makedirs(os.path.dirname(path) or '.', exist_ok=True)
    if kind.lower() == 'csv':
        with open(path, 'w', newline='', encoding='utf-8') as f:
            w = csv.DictWriter(f, fieldnames=['id','title','url','published_at','source'])
            w.writeheader()
            for a in arts:
                w.writerow(asdict(a))
        print(f"💾 Экспортировано в CSV: {path}")
    elif kind.lower() == 'json':
        with open(path, 'w', encoding='utf-8') as f:
            json.dump([asdict(a) for a in arts], f, ensure_ascii=False, indent=2)
        print(f"💾 Экспортировано в JSON: {path}")

# -----------------------------
# CLI
# -----------------------------

def main() -> None:
    import argparse
    ap = argparse.ArgumentParser(description="Универсальный интерактив: sport/tournament/team/player → период → новости")
    ap.add_argument("--db", default=os.environ.get("DB_PATH", "./database/prosport.db"))
    ap.add_argument("-v","--verbose", action="store_true")
    ap.add_argument("--limit", type=int, default=100)
    ap.add_argument("--export", choices=['csv','json'])
    ap.add_argument("--export-path")
    args = ap.parse_args()

    tz = get_tz()

    con = connect_db(args.db, verbose=args.verbose)
    try:
        # 1) Выбор типа сущности
        print("Выберите тип сущности:")
        print("  1) sport")
        print("  2) tournament")
        print("  3) team")
        print("  4) player")
        map_idx = {'1':'sport','2':'tournament','3':'team','4':'player'}
        etype = None
        while etype is None:
            s = input("[1-4]: ").strip()
            etype = map_idx.get(s)
        table, entities = load_entities(con, etype)
        ent = select_entity(entities, label=f"записей в '{table}'")
        print(f"\n✅ Выбрано: [{ent.id}] {ent.name}")

        # 2) Связи: tag → news_ids
        tag_id = find_tag_id_smart(con, ent.tag_url, verbose=args.verbose)
        news_ids = news_ids_by_tag(con, tag_id)
        if args.verbose:
            print("🏷️ tag_id:", tag_id)
            print("🧩 news_ids count:", len(news_ids))
        bounds = news_date_bounds(con, news_ids)
        if bounds:
            print(f"🗓️  В базе для этого тега: от {bounds[0]} до {bounds[1]}")

        # 3) Период
        start, end = select_period(tz)
        print(f"⏱️  Диапазон: [{start:%Y-%m-%d %H:%M}, {end:%Y-%m-%d %H:%M})")

        # 4) Выборка новостей
        arts = fetch_news(con, news_ids, start, end, args.limit)
    finally:
        con.close()

    if arts:
        print(f"\n🔎 Найдено статей: {len(arts)}\n")
        for a in arts:
            print(f"[{a.published_at}] — {a.title}\n{a.url}\n")
        export_results(arts, args.export, args.export_path)
        return

    print("\nНет статей в заданном диапазоне.")
    ans = input("Попробовать fallback‑парсинг страницы тега? [y/N]: ").strip().lower()
    if ans == 'y':
        if requests is None:
            print("⚠️ Установите: pip install requests")
            return
        try:
            tmp = parse_tag_page(ent.tag_url)
            if not tmp:
                print("(ничего не найдено на странице)")
                return
            sT, eT = to_iso_T(start), to_iso_T(end)
            out = [a for a in tmp if a.published_at and (sT <= a.published_at < eT)]
            print(f"📝 найдено на странице: {len(tmp)}, в окне: {len(out)}\n")
            for a in out:
                print(f"[{a.published_at}] — {a.title}\n{a.url}\n")
            export_results(out, args.export, args.export_path)
        except Exception as e:
            print("❌ fallback-парсинг не удался:", e)


if __name__ == "__main__":
    main()
