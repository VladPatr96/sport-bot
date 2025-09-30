# parsers/sources/championat/parsers/sports_parser.py

import aiohttp
import sqlite3
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import asyncio # Для asyncio.sleep
import os # Для os.path.dirname
import yaml # Для загрузки конфига (временно, потом будет передаваться)

# Импортируем общую вспомогательную функцию
from parsers.sources.championat.utils import fetch_page

# === Вспомогательные функции для вставки данных ===
def insert_sport(cursor, name, slug, url):
    """
    Вставляет новый вид спорта в БД или возвращает ID существующего.
    """
    try:
        cursor.execute("INSERT OR IGNORE INTO sports (name, slug, url) VALUES (?, ?, ?)", (name, slug, url))
        if cursor.lastrowid:
            print(f"  ✅ Добавлен вид спорта: {name} (ID: {cursor.lastrowid})")
            return cursor.lastrowid
        else:
            cursor.execute("SELECT id FROM sports WHERE url = ?", (url,))
            existing_id = cursor.fetchone()
            if existing_id:
                # print(f"  ℹ️ Вид спорта '{name}' уже существует (ID: {existing_id[0]}).")
                return existing_id[0]
            else:
                print(f"  ⚠️ Не удалось добавить вид спорта '{name}'.")
                return None
    except sqlite3.Error as e:
        print(f"❌ Ошибка при вставке вида спорта '{name}': {e}")
        return None

# === Основная функция парсинга видов спорта ===
async def parse_sports(session, base_url, parser_cfg):
    """
    Парсит список видов спорта с главной страницы Championat.com.
    Возвращает список словарей с информацией о видах спорта.
    Использует селекторы из parser_cfg.
    """
    print(f"--- Начинаем парсинг видов спорта с {base_url} ---")
    html = await fetch_page(session, base_url)
    if not html:
        print("🤷 Не удалось получить HTML для парсинга видов спорта.")
        return []

    soup = BeautifulSoup(html, 'lxml')
    sports = []
    
    # Используем селекторы из parser_cfg
    sport_elements = soup.select(parser_cfg["sport_item_selector"])

    for el in sport_elements:
        name_el = el.select_one(parser_cfg["sport_link_selector"])
        if name_el:
            name = name_el.get_text(strip=True)
            url = urljoin(base_url, name_el.get("href"))
            slug = el.get("data-label") # data-label пока остается здесь, так как это атрибут элемента

            # Исключаем общие категории, которые не являются конкретными видами спорта
            if name and url and slug and name.lower() not in ["другие", "чемп.play", "ставки", "lifestyle", "олимпиада 2026", "водный чм 2025"]:
                sports.append({"name": name, "slug": slug, "url": url})

    print(f"  ✅ Всего найдено {len(sports)} видов спорта.")
    return sports

# === Новая функция: Загрузка и сохранение видов спорта ===
async def load_and_save_sports(session, cursor, config):
    """
    Загружает виды спорта с Championat.com и сохраняет их в БД.
    """
    print("\n--- Запуск модуля: Загрузка и сохранение видов спорта ---")
    sports_list = await parse_sports(session, config["url"], config["selectors"])
    if sports_list:
        print(f"  Найдено {len(sports_list)} видов спорта для вставки.")
        for sport in sports_list:
            insert_sport(cursor, sport["name"], sport["slug"], sport["url"])
        print(f"  ✅ Виды спорта подготовлены к сохранению.")
    else:
        print("  🤷 Виды спорта не найдены или ошибка парсинга.")

# Пример использования (для тестирования модуля отдельно)
async def main():
    # Временная загрузка конфига для тестирования
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'sources_config.yml')
    if not os.path.exists(config_path):
        print(f"Ошибка: sources_config.yml не найден по пути {config_path}")
        return
    try:
        with open(config_path, encoding="utf-8") as f:
            all_config = yaml.safe_load(f)
        config = all_config["championat"]
        print("Конфигурация championat.com загружена успешно.")
    except Exception as e:
        print(f"Ошибка загрузки конфигурации: {e}")
        return

    db_path = "database/prosport.db" # Используем указанный db_path
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    }

    async with aiohttp.ClientSession(headers=headers) as session:
        await load_and_save_sports(session, cursor, config)
        conn.commit() # Коммит при локальном тестировании
        print("Локальное тестирование sports_parser завершено.")
    conn.close()

if __name__ == "__main__":
    asyncio.run(main())
