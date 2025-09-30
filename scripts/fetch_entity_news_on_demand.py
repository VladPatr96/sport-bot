#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fetch_entity_news_on_demand.py — показать новости по сущности за скользящее окно
(адаптируется под схему БД: статьи в таблице `news`, связи — `news_articles_tags`).

⚙️ Что умеет
- Окно: строго [start, end) — при --days 1 это ровно 24 часа до текущего момента
- Поддерживает 2 варианта связей:
  A) `news_articles_tags` содержит (news_id, entity_type, entity_id)
  B) `news_articles_tags` содержит (news_id, tag_id), тогда ищем таблицу тегов
     (например `tags`) и таблицу алиасов (`entity_aliases`) для сопоставления
     tag.name → (entity_type, entity_id)
- Автобэкофилл по желанию (`--backfill-run`) и диагностика (`--debug-scan`)
- Автодетект полей в `news`: id, title, url, published_at (гибкие названия)

Запуск (пример):
  python -m scripts.fetch_entity_news_on_demand --entity team --id 912 --days 1 -v \
      --db F:/projects/Projects/sport-news-bot/database/prosport.db --debug-scan
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

# -----------------------------
# TZ shim
# -----------------------------
try:
    from zoneinfo import ZoneInfo  # py>=3.9
except Exception:  # pragma: no cover
    try:
        from backports.zoneinfo import ZoneInfo  # type: ignore
    except Exception:  # pragma: no cover
        ZoneInfo = None  # type: ignore

SUPPORTED_ENTITY_TYPES = {"sport", "tournament", "team", "player"}


@dataclass
class Article:
    id: int
    title: str
    url: str
    published_at: datetime
    source: Optional[str]


# -----------------------------
# Helpers: TZ & datetime
# -----------------------------

def get_tz() -> Optional["ZoneInfo"]:
    tz_name = os.environ.get("APP_TZ") or os.environ.get("TZ") or "UTC"
    try:
        return ZoneInfo(tz_name) if ZoneInfo else None
    except Exception:
        return None


def parse_dt_local(s: str, tz: Optional["ZoneInfo"]) -> datetime:
    s = s.strip()
    fmt = "%Y-%m-%d %H:%M" if " " in s else "%Y-%m-%d"
    dt = datetime.strptime(s, fmt)
    return dt.replace(tzinfo=tz) if tz else dt


def _to_sqlite_iso(dt: datetime) -> str:
    """Convert dt to naive UTC ISO for SQLite (space separator)."""
    if dt.tzinfo is not None and ZoneInfo:
        dt = dt.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)
    return dt.isoformat(timespec="seconds").replace("T", " ")


def parse_sqlite_dt(s: str, tz: Optional["ZoneInfo"]) -> datetime:
    s = s.replace("T", " ")
    # поддержим "YYYY-MM-DD HH:MM" и "YYYY-MM-DD HH:MM:SS"
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.replace(tzinfo=tz) if tz else dt
        except ValueError:
            continue
    # как fallback — только дата
    dt = datetime.strptime(s.split(" ")[0], "%Y-%m-%d")
    return dt.replace(tzinfo=tz) if tz else dt


# -----------------------------
# Helpers: DB path (Windows-safe)
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
    try:
        conn = sqlite3.connect(resolved)
    except sqlite3.OperationalError as e:
        raise SystemExit(
            f"❌ Не удалось открыть SQLite файл: {e}\nЗапрошено: {db_path}\nРезолвнуто: {resolved}\n"
        )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


# -----------------------------
# Schema helpers (auto-detect)
# -----------------------------

def table_exists(conn: sqlite3.Connection, name: str) -> bool:
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,))
    return cur.fetchone() is not None


def get_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    cur = conn.execute(f"PRAGMA table_info({table})")
    return [r[1] for r in cur.fetchall()]


def pick_first(cols: List[str], candidates: List[str]) -> Optional[str]:
    low = {c.lower(): c for c in cols}
    for name in candidates:
        if name in low:
            return low[name]
    return None


def detect_news_columns(conn: sqlite3.Connection) -> Dict[str, Optional[str]]:
    cols = get_columns(conn, "news")
    # id/title/url/published/source — гибкая детекция
    id_col = pick_first(cols, ["id", "news_id"]) or cols[0]
    title_col = pick_first(cols, ["title", "headline", "name"]) or None
    url_col = pick_first(cols, ["url", "link", "permalink"]) or None
    published_col = pick_first(cols, [
        "published_at", "published", "pub_date", "date", "created_at", "datetime", "time"
    ]) or None
    source_col = pick_first(cols, ["source", "origin", "site"]) or None
    return {
        "id": id_col,
        "title": title_col,
        "url": url_col,
        "published": published_col,
        "source": source_col,
    }


# -----------------------------
# Core query (news + news_articles_tags)
# -----------------------------

def fetch_articles(
    conn: sqlite3.Connection,
    entity_type: str,
    entity_id: int,
    start_dt: datetime,
    end_dt: datetime,
    limit: Optional[int] = None,
) -> List[Article]:
    if not table_exists(conn, "news"):
        raise SystemExit("❌ В БД отсутствует таблица 'news'.")
    if not table_exists(conn, "news_articles_tags"):
        # нет связей — вернуть пусто (и подсказать бэкофилл/миграцию)
        return []

    N = detect_news_columns(conn)
    id_col = N["id"]
    title_col = N["title"] or N["id"]
    url_col = N["url"] or N["id"]
    pub_col = N["published"]
    src_col = N["source"]

    if not pub_col:
        raise SystemExit("❌ Не удалось определить колонку даты публикации в таблице 'news'. Добавьте одну из: published_at/published/pub_date/date/created_at/datetime/time")

    start_iso = _to_sqlite_iso(start_dt)
    end_iso = _to_sqlite_iso(end_dt)

    # Поймём структуру news_articles_tags
    nat_cols = get_columns(conn, "news_articles_tags")
    has_entity_cols = set(c.lower() for c in nat_cols) >= {"news_id", "entity_type", "entity_id"}

    results: List[Article] = []

    if has_entity_cols:
        # Вариант A: прямые entity_type/entity_id
        sql = (
            f"SELECT a.{id_col} AS id, a.{title_col} AS title, a.{url_col} AS url, a.{pub_col} AS published, "
            + (f"a.{src_col} AS source " if src_col else "NULL AS source ") +
            "FROM news a "
            "JOIN news_articles_tags nat ON nat.news_id = a." + id_col + " "
            "WHERE nat.entity_type = ? AND nat.entity_id = ? "
            f"AND a.{pub_col} >= ? AND a.{pub_col} < ? "
            f"ORDER BY a.{pub_col} DESC"
        )
        params: Tuple = (entity_type, entity_id, start_iso, end_iso)
        if limit:
            sql += " LIMIT ?"
            params += (limit,)
        cur = conn.execute(sql, params)
        for r in cur.fetchall():
            results.append(
                Article(
                    id=r["id"],
                    title=str(r["title"]),
                    url=str(r["url"]),
                    published_at=parse_sqlite_dt(str(r["published"]), tz=None),
                    source=r["source"],
                )
            )
        return results

    # Вариант B: через tags → entity_aliases
    # Попробуем найти таблицу с тегами
    tag_table = None
    for cand in ("tags", "news_tags", "article_tags", "labels"):
        if table_exists(conn, cand):
            tag_table = cand
            break
    if not tag_table:
        # нет способа связать теги → сущности
        return []

    tag_cols = get_columns(conn, tag_table)
    tag_id = pick_first(tag_cols, ["id", "tag_id"]) or tag_cols[0]
    tag_name = pick_first(tag_cols, ["name", "title", "label"]) or tag_cols[0]

    if not table_exists(conn, "entity_aliases"):
        # без алиасов нельзя маппить теги на сущности
        return []

    # entity_aliases: alias (lowercase), entity_type, entity_id
    # допустимы варианты названий
    ea_cols = get_columns(conn, "entity_aliases")
    ea_alias = pick_first(ea_cols, ["alias", "name"]) or ea_cols[0]
    ea_type = pick_first(ea_cols, ["entity_type", "type"]) or ea_cols[0]
    ea_id = pick_first(ea_cols, ["entity_id", "id"]) or ea_cols[0]

    sql = (
        f"SELECT a.{id_col} AS id, a.{title_col} AS title, a.{url_col} AS url, a.{pub_col} AS published, "
        + (f"a.{src_col} AS source " if src_col else "NULL AS source ") +
        "FROM news a "
        "JOIN news_articles_tags nat ON nat.news_id = a." + id_col + " "
        f"JOIN {tag_table} t ON t.{tag_id} = nat.tag_id "
        f"JOIN entity_aliases ea ON ea.{ea_alias} = LOWER(t.{tag_name}) "
        f"WHERE ea.{ea_type} = ? AND ea.{ea_id} = ? "
        f"AND a.{pub_col} >= ? AND a.{pub_col} < ? "
        f"ORDER BY a.{pub_col} DESC"
    )
    params = (entity_type, entity_id, start_iso, end_iso)
    if limit:
        sql += " LIMIT ?"
        params += (limit,)

    cur = conn.execute(sql, params)
    for r in cur.fetchall():
        results.append(
            Article(
                id=r["id"],
                title=str(r["title"]),
                url=str(r["url"]),
                published_at=parse_sqlite_dt(str(r["published"]), tz=None),
                source=r["source"],
            )
        )

    return results


# -----------------------------
# Diagnostics & Backfill
# -----------------------------

def debug_counts(conn: sqlite3.Connection, entity: str, eid: int, start_dt: datetime, end_dt: datetime) -> None:
    s_iso = _to_sqlite_iso(start_dt)
    e_iso = _to_sqlite_iso(end_dt)

    have_news = table_exists(conn, "news")
    have_nat = table_exists(conn, "news_articles_tags")
    print(f"🧪 debug: tables news={have_news}, news_articles_tags={have_nat}, window=[{s_iso}, {e_iso})")

    if not (have_news and have_nat):
        return

    nat_cols = [c.lower() for c in get_columns(conn, "news_articles_tags")]
    direct = linked = None

    # обнаружим имя колонки published
    N = detect_news_columns(conn)
    pub_col = N["published"]
    if not pub_col:
        print("⚠️ Не найдена колонка даты публикации в news")
        return

    if {"news_id", "entity_type", "entity_id"}.issubset(set(nat_cols)):
        cur = conn.execute(
            f"SELECT COUNT(*) FROM news a JOIN news_articles_tags nat ON nat.news_id=a.{N['id']} "
            f"WHERE nat.entity_type=? AND nat.entity_id=? AND a.{pub_col}>=? AND a.{pub_col}<?",
            (entity, eid, s_iso, e_iso),
        )
        direct = cur.fetchone()[0]
    if "tag_id" in nat_cols and table_exists(conn, "entity_aliases"):
        # найдём таблицу тегов
        tname = None
        for cand in ("tags", "news_tags", "article_tags", "labels"):
            if table_exists(conn, cand):
                tname = cand
                break
        if tname:
            tcols = get_columns(conn, tname)
            t_id = pick_first(tcols, ["id", "tag_id"]) or tcols[0]
            t_name = pick_first(tcols, ["name", "title", "label"]) or tcols[0]
            ea_cols = get_columns(conn, "entity_aliases")
            ea_alias = pick_first(ea_cols, ["alias", "name"]) or ea_cols[0]
            ea_type = pick_first(ea_cols, ["entity_type", "type"]) or ea_cols[0]
            ea_id = pick_first(ea_cols, ["entity_id", "id"]) or ea_cols[0]
            cur = conn.execute(
                f"SELECT COUNT(*) FROM news a "
                f"JOIN news_articles_tags nat ON nat.news_id=a.{N['id']} "
                f"JOIN {tname} t ON t.{t_id} = nat.tag_id "
                f"JOIN entity_aliases ea ON ea.{ea_alias} = LOWER(t.{t_name}) "
                f"WHERE ea.{ea_type}=? AND ea.{ea_id}=? AND a.{pub_col}>=? AND a.{pub_col}<?",
                (entity, eid, s_iso, e_iso),
            )
            linked = cur.fetchone()[0]

    print(f"   • direct={direct if direct is not None else '(нет прямых entity_* в news_articles_tags)'}")
    print(f"   • via_alias={linked if linked is not None else '(нет tag_id/alias-связки)'}")


def suggest_backfill(entity_type: str, entity_id: int, start_dt: datetime, end_dt: datetime) -> None:
    print("\n⚠️ В указанном окне новостей не найдено.")
    print("Вы можете запустить бэкофилл парсера, например:")
    print(
        f"  python -m parsers.championat.fetch --entity {entity_type} --id {entity_id} "
        f"--from \"{start_dt:%Y-%m-%d %H:%M}\" --to \"{end_dt:%Y-%m-%d %H:%M}\" -v"
    )


def run_backfill(entity: str, eid: int, start_dt: datetime, end_dt: datetime, verbose: bool = False) -> int:
    cmd = [
        sys.executable,
        "-m",
        "parsers.championat.fetch",
        "--entity",
        entity,
        "--id",
        str(eid),
        "--from",
        start_dt.strftime("%Y-%m-%d %H:%M"),
        "--to",
        end_dt.strftime("%Y-%m-%d %H:%M"),
        "-v",
    ]
    if verbose:
        print("🧩 backfill cmd:", " ".join(cmd))
    try:
        return subprocess.call(cmd)
    except FileNotFoundError:
        print("❌ Не найден модуль parsers.championat.fetch — проверьте структуру проекта.")
        return 127


# -----------------------------
# CLI
# -----------------------------

def main():
    tz = get_tz()

    parser = argparse.ArgumentParser(
        description="Показать новости по сущности за заданный диапазон (скользящее окно).",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--entity", required=True, choices=sorted(SUPPORTED_ENTITY_TYPES), help="Тип сущности")
    parser.add_argument("--id", required=True, type=int, help="ID сущности")
    parser.add_argument("--days", type=int, default=1, help="Размер окна в сутках, если не заданы --from/--to")
    parser.add_argument("--from", dest="from_dt", type=str, help="Начало диапазона (локальное время) YYYY-MM-DD[ HH:MM]")
    parser.add_argument("--to", dest="to_dt", type=str, help="Конец диапазона (локальное время) YYYY-MM-DD[ HH:MM]")
    parser.add_argument("--limit", type=int, default=100, help="Ограничить кол-во выводимых статей")
    parser.add_argument("-v", "--verbose", action="store_true", help="Подробный вывод")
    parser.add_argument("--db", default=os.environ.get("DB_PATH", "./database/prosport.db"), help="Путь к SQLite базе")
    parser.add_argument("--backfill", action="store_true", help="Подсказать команду бэкофилла, если пусто")
    parser.add_argument("--backfill-run", action="store_true", help="Автоматически запустить бэкофилл, если пусто")
    parser.add_argument("--debug-scan", action="store_true", help="Показать диагностику по связям и окну")

    args = parser.parse_args()

    now = datetime.now(tz) if tz else datetime.now()

    if args.from_dt:
        start_dt = parse_dt_local(args.from_dt, tz)
        end_dt = parse_dt_local(args.to_dt, tz) if args.to_dt else now
    else:
        # Скользящее окно [now - days, now)
        start_dt = now - timedelta(days=args.days)
        end_dt = parse_dt_local(args.to_dt, tz) if args.to_dt else now

    if end_dt <= start_dt:
        parser.error("Конец диапазона должен быть больше начала.")

    if args.verbose:
        tz_str = getattr(tz, "key", "naive") if tz else "naive"
        print("⚙️  Параметры запроса:")
        print(f"   • entity: {args.entity}")
        print(f"   • id:     {args.id}")
        print(f"   • window: [{start_dt:%Y-%m-%d %H:%M}, {end_dt:%Y-%m-%d %H:%M})")
        print(f"   • db:     {args.db}")
        print(f"   • tz:     {tz_str}\n")

    conn = connect_db(args.db, verbose=args.verbose)
    try:
        if args.debug_scan:
            debug_counts(conn, args.entity, args.id, start_dt, end_dt)
        articles = fetch_articles(
            conn=conn,
            entity_type=args.entity,
            entity_id=args.id,
            start_dt=start_dt,
            end_dt=end_dt,
            limit=args.limit,
        )
    finally:
        conn.close()

    if args.verbose:
        print(f"🔎 Найдено статей: {len(articles)}\n")

    if not articles:
        print("Нет статей в заданном диапазоне.")
        if args.backfill_run:
            rc = run_backfill(args.entity, args.id, start_dt, end_dt, verbose=args.verbose)
            print(f"🔁 backfill exit code: {rc}")
            return
        if args.backfill:
            suggest_backfill(args.entity, args.id, start_dt, end_dt)
        return

    for a in articles:
        when = a.published_at.strftime("%Y-%m-%d %H:%M")
        src = f" · {a.source}" if a.source else ""
        print(f"[{when}]{src} — {a.title}\n{a.url}\n")


if __name__ == "__main__":
    main()
