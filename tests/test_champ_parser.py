# scripts/test_champ_parser.py
# -*- coding: utf-8 -*-
import os
import sys
import yaml
import asyncio
import argparse

PROJ_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
CONFIG_PATH = os.path.join(
    PROJ_ROOT, "parsers", "sources", "championat", "config", "sources_config.yml"
)

sys.path.insert(0, PROJ_ROOT)
from parsers.sources.championat.parsers.champ_parser import ChampParserSelenium  # noqa: E402


def load_config() -> dict:
    path = os.path.abspath(CONFIG_PATH)
    with open(path, encoding="utf-8") as f:
        all_cfg = yaml.safe_load(f) or {}
    cfg = all_cfg.get("championat") or {}

    # Унификация ключей: поддерживаем и base_url, и url
    if "base_url" not in cfg and "url" in cfg:
        cfg["base_url"] = cfg["url"]

    return cfg


def print_article(art: dict) -> None:
    tags = [t.get("name") or t.get("url") for t in (art.get("tags") or [])]
    body = (art.get("body") or "").strip()
    print("\n— — — СТАТЬЯ — — —")
    print("URL:      ", art.get("url"))
    print("Title:    ", art.get("title"))
    print("Published:", art.get("published"))
    print("Tags:     ", tags)
    print("Body:     ", (body[:800] + "…") if len(body) > 800 else body)


def print_list(items: list, k: int) -> None:
    k = min(k, len(items))
    print(f"\n— — — ПЕРВЫЕ {k} ЭЛЕМЕНТОВ ЛЕНТЫ — — —")
    for i in range(k):
        it = items[i]
        print(f"[{i}] {it.get('published')} — {it.get('title')}\n    {it.get('url')}")


async def main():
    ap = argparse.ArgumentParser(description="Тест Championat-парсера")
    ap.add_argument("--url", help="Спарсить конкретную статью по URL")
    ap.add_argument("--n", type=int, default=0, help="Индекс новости из ленты (0 = первая)")
    ap.add_argument("--list", type=int, default=0, help="Вывести K элементов ленты и выйти")
    ap.add_argument("--base-url", dest="base_url", help="Переопределить base_url из YAML")
    args = ap.parse_args()

    cfg = load_config()
    if args.base_url:
        cfg["base_url"] = args.base_url

    resolved_url = cfg.get("base_url")
    print("📄 CONFIG:", os.path.abspath(CONFIG_PATH))
    print("🔑 KEYS:", ", ".join(sorted(cfg.keys())))
    print("🌐 RESOLVED BASE URL:", resolved_url)

    async with ChampParserSelenium(cfg) as parser:
        if parser.driver is None:
            print("❌ WebDriver не инициализирован. Проверь соответствие Chrome ↔ ChromeDriver.")
            return

        if args.url:
            meta = {"url": args.url, "title": None, "published": None, "summary": None}
            art = await parser.fetch_article(meta)
            if not art:
                print("⚠️ Не удалось распарсить статью по URL.")
                return
            print_article(art)
            return

        items = await parser.fetch_list()
        print(f"📰 В списке элементов: {len(items)}")
        if not items:
            print("ℹ️ Похоже, список пуст. Проверь корректность base_url (или попробуй --base-url).")
            return

        if args.list > 0:
            print_list(items, args.list)
            return

        idx = max(0, min(args.n, len(items) - 1))
        chosen = items[idx]
        print("➡️  Выбрано из ленты:")
        print("    URL:   ", chosen.get("url"))
        print("    Title: ", chosen.get("title"))
        print("    Date:  ", chosen.get("published"))

        art = await parser.fetch_article(chosen)
        if not art:
            print("⚠️ Не удалось распарсить выбранную статью.")
            return
        print_article(art)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print("❌ Ошибка теста:", e)
