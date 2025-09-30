# -*- coding: utf-8 -*-
"""
Собрать ленту/статьи Championat в JSON (без записи в БД).

Примеры запуска:
  # 1) Снять 20 элементов ленты (только список), сохранить в out/champ_list.json
  python -m scripts.test_champ_to_json --list 20 --out ./out/champ_list.json

  # 2) Снять 10 элементов ленты и распарсить статьи, сохранить в out/champ_articles.json
  python -m scripts.test_champ_to_json --limit 10 --out ./out/champ_articles.json

  # 3) Разобрать конкретные URL (по одному/из файла), сохранить JSON
  python -m scripts.test_champ_to_json --url https://www.championat.com/football/news-6165562-...
  python -m scripts.test_champ_to_json --urls-file ./urls.txt --out ./out/custom.json

  # 4) Поменять раздел/страницу ленты поверх YAML
  python -m scripts.test_champ_to_json --base-url https://www.championat.com/news/football/2.html --limit 5

По умолчанию читает конфиг parsers/sources/championat/config/sources_config.yml → секция "championat".
"""
import os
import sys
import yaml
import json
import asyncio
import argparse
from typing import List, Dict, Any

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
    # поддержка старого ключа url
    if "base_url" not in cfg and "url" in cfg:
        cfg["base_url"] = cfg["url"]
    return cfg


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


async def main():
    ap = argparse.ArgumentParser(description="Championat → JSON (без БД)")
    ap.add_argument("--list", type=int, default=0, help="Сколько элементов ленты просто вывести и сохранить (без статей)")
    ap.add_argument("--limit", type=int, default=10, help="Сколько статей разобрать из ленты (0 = не разбирать)")
    ap.add_argument("--url", action="append", help="Добавить конкретный URL статьи (можно несколько раз)")
    ap.add_argument("--urls-file", help="Путь к файлу со списком URL (по одному в строке)")
    ap.add_argument("--base-url", help="Перекрыть base_url из YAML для ленты")
    ap.add_argument("--out", default="./out/champ_dump.json", help="Куда сохранить JSON")
    ap.add_argument("--include-body", action="store_true", help="Включать полный текст body в JSON")
    args = ap.parse_args()

    cfg = load_config()
    if args.base_url:
        cfg["base_url"] = args.base_url

    result: Dict[str, Any] = {
        "config": {
            k: v for k, v in cfg.items() if k in ("base_url", "timeout", "delay")
        },
        "list": [],
        "articles": [],
    }

    # Соберём список целевых URL
    target_urls: List[str] = []
    if args.url:
        target_urls.extend(args.url)
    if args.urls_file:
        target_urls.extend(read_urls_file(args.urls_file))

    async with ChampParserSelenium(cfg) as parser:
        if parser.driver is None:
            print("❌ WebDriver не инициализирован. Проверь соответствие Chrome ↔ ChromeDriver.")
            return

        # 1) Если запрошен список
        items = await parser.fetch_list()
        print(f"📰 Лента: {len(items)} элементов")
        if args.list > 0:
            result["list"] = items[: args.list]
            # если статей не нужно — сразу сохраняем
            if not target_urls and args.limit <= 0:
                save_json(result, args.out)
                return

        # 2) Если есть target_urls — разбираем их напрямую
        if target_urls:
            for u in target_urls:
                meta = {"url": u, "title": None, "published": None, "summary": None}
                art = await parser.fetch_article(meta)
                if art:
                    result["articles"].append(compact_article(art, args.include_body))
            save_json(result, args.out)
            return

        # 3) Иначе — берём первые limit из ленты и парсим
        lim = max(0, args.limit)
        for meta in items[:lim]:
            art = await parser.fetch_article(meta)
            if art:
                result["articles"].append(compact_article(art, args.include_body))
        save_json(result, args.out)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print("❌ Ошибка выполнения:", e)
