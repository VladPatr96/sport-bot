# parsers/sources/championat/parsers/championat_data_loader.py

import asyncio
import aiohttp
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
import sqlite3
import os
import yaml
from datetime import datetime
from database.prosport_db import init_db # Предполагая, что prosport_db.py находится в database/

# === Загрузка конфигурации ===
config_path = os.path.join(os.path.dirname(__file__), "sources", "championat", "config", "sources_config.yml")
if not os.path.exists(config_path):
    print(f"Ошибка: sources_config.yml не найден по пути {config_path}")
    exit(1)

try:
    with open(config_path, encoding="utf-8") as f:
        all_config = yaml.safe_load(f)
    config = all_config["championat"]
    print("Конфигурация championat.com загружена успешно.")
except Exception as e:
    print(f"Ошибка загрузки конфигурации: {e}")
    exit(1)

# === Вспомогательные функции для вставки данных ===
def insert_sport(cursor, name, slug, url):
    try:
        cursor.execute("INSERT OR IGNORE INTO sports (name, slug, url) VALUES (?, ?, ?)", (name, slug, url))
        if cursor.lastrowid:
            print(f"  ✅ Добавлен вид спорта: {name} (ID: {cursor.lastrowid})")
            return cursor.lastrowid
        else:
            cursor.execute("SELECT id FROM sports WHERE url = ?", (url,))
            existing_id = cursor.fetchone()
            if existing_id:
                print(f"  ℹ️ Вид спорта '{name}' уже существует (ID: {existing_id[0]}).")
                return existing_id[0]
            else:
                print(f"  ⚠️ Не удалось добавить вид спорта '{name}'.")
                return None
    except sqlite3.Error as e:
        print(f"❌ Ошибка при вставке вида спорта '{name}': {e}")
        return None

def insert_tournament(cursor, name, url, sport_id):
    try:
        cursor.execute("INSERT OR IGNORE INTO tournaments (name, url, sport_id) VALUES (?, ?, ?)", (name, url, sport_id))
        if cursor.lastrowid:
            print(f"    ✅ Добавлен турнир: {name} (ID: {cursor.lastrowid}) для спорта ID: {sport_id}")
            return cursor.lastrowid
        else:
            cursor.execute("SELECT id FROM tournaments WHERE url = ?", (url,))
            existing_id = cursor.fetchone()
            if existing_id:
                print(f"    ℹ️ Турнир '{name}' уже существует (ID: {existing_id[0]}).")
                return existing_id[0]
            else:
                print(f"    ⚠️ Не удалось добавить турнир '{name}'.")
                return None
    except sqlite3.Error as e:
        print(f"❌ Ошибка при вставке турнира '{name}': {e}")
        return None

def insert_team(cursor, name, alias, url, external_id, tournament_id, tag_url):
    try:
        cursor.execute("INSERT OR IGNORE INTO teams (name, alias, url, external_id, tournament_id, tag_url) VALUES (?, ?, ?, ?, ?, ?)",
                       (name, alias, url, external_id, tournament_id, tag_url))
        if cursor.lastrowid:
            print(f"      ✅ Добавлена команда: {name} (ID: {cursor.lastrowid}) для турнира ID: {tournament_id}")
            return cursor.lastrowid
        else:
            cursor.execute("SELECT id FROM teams WHERE url = ?", (url,))
            existing_id = cursor.fetchone()
            if existing_id:
                print(f"      ℹ️ Команда '{name}' уже существует (ID: {existing_id[0]}).")
                return existing_id[0]
            else:
                print(f"      ⚠️ Не удалось добавить команду '{name}'.")
                return None
    except sqlite3.Error as e:
        print(f"❌ Ошибка при вставке команды '{name}': {e}")
        return None

def insert_athlete(cursor, name, url, team_id, external_id, tag_url):
    try:
        cursor.execute("INSERT OR IGNORE INTO athletes (name, url, team_id, external_id, tag_url) VALUES (?, ?, ?, ?, ?)",
                       (name, url, team_id, external_id, tag_url))
        if cursor.lastrowid:
            print(f"        ✅ Добавлен атлет: {name} (ID: {cursor.lastrowid}) для команды ID: {team_id}")
            return cursor.lastrowid
        else:
            cursor.execute("SELECT id FROM athletes WHERE url = ?", (url,))
            existing_id = cursor.fetchone()
            if existing_id:
                print(f"        ℹ️ Атлет '{name}' уже существует (ID: {existing_id[0]}).")
                return existing_id[0]
            else:
                print(f"        ⚠️ Не удалось добавить атлета '{name}'.")
                return None
    except sqlite3.Error as e:
        print(f"❌ Ошибка при вставке атлета '{name}': {e}")
        return None

# === Вспомогательные функции для парсинга ===
async def fetch_page(session, url):
    """Извлекает HTML-содержимое страницы по заданному URL."""
    try:
        async with session.get(url) as resp:
            resp.raise_for_status()  # Вызовет исключение для статусов 4xx/5xx
            return await resp.text()
    except aiohttp.ClientError as e:
        print(f"❌ Ошибка HTTP при запросе {url}: {e}")
        return None
    except Exception as e:
        print(f"❌ Непредвиденная ошибка при запросе {url}: {e}")
        return None

async def get_sports_list(session, base_url, parser_cfg):
    """
    Парсит список видов спорта с главной страницы.
    """
    print(f"Начинаем парсинг видов спорта с {base_url}...")
    html = await fetch_page(session, base_url)
    if not html:
        return []

    soup = BeautifulSoup(html, 'lxml')
    sports = []
    
    # Селектор для элементов меню видов спорта в шапке
    # Исходя из предоставленного HTML: header-menu-item с data-label
    sport_elements = soup.select("li.header-menu-item[data-label]")

    print(f"  [DEBUG] Найдено {len(sport_elements)} потенциальных элементов видов спорта.")

    for el in sport_elements:
        name_el = el.select_one("a.js-header-menu-item-link")
        if name_el:
            name = name_el.get_text(strip=True)
            url = urljoin(base_url, name_el.get("href"))
            slug = el.get("data-label") # Используем data-label как slug

            # Исключаем общие категории, которые не являются конкретными видами спорта
            if name and url and slug and name.lower() not in ["другие", "чемп.play", "ставки", "lifestyle", "олимпиада 2026", "водный чм 2025"]:
                sports.append({"name": name, "slug": slug, "url": url})
                print(f"    [DEBUG] Найден вид спорта: {name} (Slug: {slug}, URL: {url})")
            else:
                print(f"    [DEBUG] Пропущен элемент меню (общая категория или без имени): {name} (URL: {url})")

    print(f"  [DEBUG] Всего найдено {len(sports)} видов спорта для обработки.")
    return sports

async def get_tournaments_for_sport(session, sport_url, parser_cfg):
    """
    Парсит список турниров для конкретного вида спорта.
    Использует ссылку на страницу вида спорта.
    """
    print(f"  Парсим турниры для вида спорта: {sport_url}")
    html = await fetch_page(session, sport_url)
    if not html:
        return []

    soup = BeautifulSoup(html, 'lxml')
    tournaments = []

    # Селектор для турниров в выпадающем меню шапки
    # Ищем внутри header-menu-item__drop-wrap, который связан с текущим sport_url
    # Это сложнее, так как drop-wrap может быть неактивным.
    # Более надежный способ - искать на странице самого вида спорта, если там есть список турниров.
    # Если на странице вида спорта нет явного списка турниров,
    # то придется парсить их из общего меню или из других источников.

    # Предположим, что турниры находятся в блоке с классом 'livetable-tournament'
    # или в выпадающих меню хедера.
    # Из предоставленного HTML, турниры находятся в <div class="livetable-tournament">
    tournament_elements = soup.select("div.livetable-tournament")
    
    # Также проверяем header-menu-item__drop-link внутри соответствующего sport-item
    # Это более надежный способ, так как эти ссылки всегда присутствуют в HTML
    # (хоть и скрыты JS)
    
    # Чтобы найти правильный drop-wrap, нужно сопоставить sport_url
    # Пройдемся по всем header-menu-item и найдем тот, чей href соответствует sport_url
    # или чей data-label соответствует slug
    
    # Для простоты, давайте попробуем извлечь из всех drop-wrap, а затем отфильтровать
    # по принадлежности к текущему виду спорта (по URL или по slug)
    
    # Найдем соответствующий header-menu-item для текущего sport_url
    sport_menu_item = soup.select_one(f"li.header-menu-item a[href*='{urlparse(sport_url).path}']")
    
    if sport_menu_item:
        # Найдем выпадающее меню внутри этого элемента
        drop_wrap = sport_menu_item.find_next_sibling("div", class_="js-header-submenu")
        if drop_wrap:
            drop_links = drop_wrap.select("a.header-menu-item__drop-link")
            print(f"    [DEBUG] Найдено {len(drop_links)} ссылок в выпадающем меню для {sport_url}.")
            for link in drop_links:
                name = link.get_text(strip=True)
                url = urljoin(sport_url, link.get("href"))
                # Исключаем ссылки, которые ведут на статьи или другие общие страницы
                if name and url and "article" not in url and "page" not in url and "tags" not in url:
                    tournaments.append({"name": name, "url": url})
                    print(f"      [DEBUG] Найден турнир (из меню): {name} (URL: {url})")
        else:
            print(f"    [DEBUG] Выпадающее меню для {sport_url} не найдено.")
    else:
        print(f"    [DEBUG] Элемент меню для {sport_url} не найден.")

    # Дополнительно, парсим livetable-tournament, если они есть на странице
    for el in tournament_elements:
        name_el = el.select_one(".livetable-tournament__title")
        if name_el:
            name = name_el.get_text(strip=True)
            url = urljoin(sport_url, name_el.get("href"))
            if name and url and {"name": name, "url": url} not in tournaments: # Избегаем дубликатов
                tournaments.append({"name": name, "url": url})
                print(f"      [DEBUG] Найден турнир (из livetable): {name} (URL: {url})")

    print(f"  [DEBUG] Всего найдено {len(tournaments)} турниров для {sport_url}.")
    return tournaments

async def get_teams_for_tournament(session, tournament_url, parser_cfg):
    """
    Парсит список команд для конкретного турнира.
    """
    print(f"    Парсим команды для турнира: {tournament_url}")
    html = await fetch_page(session, tournament_url)
    if not html:
        return []

    soup = BeautifulSoup(html, 'lxml')
    teams = []

    # Селекторы для команд могут быть разными в зависимости от страницы турнира
    # Часто команды находятся в таблицах или списках с классами типа 'team-name', 'team-row', 'team-item'
    # Исходя из предоставленного HTML (livetable-event__name):
    team_elements = soup.select(".livetable-event__name .team-name") # Это может быть только название, без ссылки на команду

    # Более надежно: искать ссылки на команды, если они есть
    # Например, на странице турнира может быть список команд с ссылками на их профили.
    # Если нет явных ссылок на команды, то придется использовать только их имена.
    # В текущем HTML, команды появляются как <span class="team-name"> внутри <a class="livetable-event__link">
    # Но ссылка ведет на событие, а не на страницу команды.
    # Если на сайте есть отдельные страницы команд, нужно найти селектор для них.
    
    # Для примера, давайте попробуем найти команды из таблицы или списка, если они есть на странице турнира.
    # На Championat.com команды обычно имеют свои теги.
    # Попробуем найти ссылки на теги команд, если они есть на странице турнира.
    
    # Временно, будем извлекать только имена команд из livetable-event__name
    for el in team_elements:
        name = el.get_text(strip=True)
        # На Championat.com команды могут быть тегами, например:
        # <a href="/tags/885-krasnodar/" class="news-item__tag sport-tag _football">Краснодар</a>
        # Если есть такая структура на странице турнира, можно использовать ее.
        # Для простоты, пока будем использовать только имя.
        if name and {"name": name} not in teams: # Избегаем дубликатов по имени
             teams.append({"name": name, "url": None, "alias": None, "external_id": None, "tag_url": None})
             print(f"        [DEBUG] Найдена команда: {name}")

    print(f"    [DEBUG] Всего найдено {len(teams)} команд для {tournament_url}.")
    return teams


async def get_athletes_for_team(session, team_url, parser_cfg):
    """
    Парсит список атлетов для конкретной команды.
    Это может быть очень специфично для каждого вида спорта и сайта.
    На Championat.com атлеты, скорее всего, будут на страницах тегов (tags) или в составах команд.
    """
    print(f"      Парсим атлетов для команды: {team_url}")
    # Если team_url отсутствует, нет смысла парсить атлетов
    if not team_url:
        print("      [DEBUG] URL команды отсутствует, пропускаем парсинг атлетов.")
        return []

    html = await fetch_page(session, team_url)
    if not html:
        return []

    soup = BeautifulSoup(html, 'lxml')
    athletes = []

    # Селекторы для атлетов очень сильно зависят от структуры страницы команды.
    # На Championat.com, если перейти на страницу команды (например, через тег),
    # могут быть списки игроков.
    # Пример: div.team-squad__item или similar
    
    # В данном случае, без конкретного примера HTML страницы команды,
    # этот парсинг будет очень общим или может не найти ничего.
    # Предполагаем, что атлеты могут быть в списке или таблице.
    
    # Попробуем найти ссылки на профили атлетов, если они есть.
    # Например, если атлеты представлены как ссылки на их теги:
    # <a href="/tags/8927-nikolja-batjum/" class="tags__item">Николя Батюм</a>
    athlete_elements = soup.select(parser_cfg.get("article_tags", "a.tags__item")) # Используем article_tags как общий селектор для тегов
    
    for el in athlete_elements:
        name = el.get_text(strip=True)
        url = urljoin(team_url, el.get("href"))
        
        # Фильтруем, чтобы убедиться, что это действительно атлет, а не общий тег
        # Это очень сложно без конкретных примеров.
        # Для простоты, если тег содержит имя и ведет на страницу тега, считаем его атлетом.
        if name and url and "/tags/" in url:
            # Можно добавить более сложную логику для определения, является ли тег атлетом
            # Например, по наличию определенных классов или структуре страницы тега.
            athletes.append({"name": name, "url": url, "external_id": None, "tag_url": url})
            print(f"          [DEBUG] Найден атлет: {name} (URL: {url})")

    print(f"      [DEBUG] Всего найдено {len(athletes)} атлетов для {team_url}.")
    return athletes

# === Основная функция загрузки данных ===
async def main_data_loader(db_path="prosport.db"):

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    }
    base_url = config["url"]

    async with aiohttp.ClientSession(headers=headers) as session:
        # 2. Парсинг и сохранение видов спорта
        print("\n--- Начинаем парсинг видов спорта ---")
        sport_id_map = {} # slug -> id
        sport_url_to_slug_map = {} # url -> slug (для обратного поиска)

        sports_list = await get_sports_list(session, base_url, config["parser"])
        if sports_list:
            print(f"  Найдено {len(sports_list)} видов спорта для вставки.")
            for sport in sports_list:
                sport_id = insert_sport(cursor, sport["name"], sport["slug"], sport["url"])
                if sport_id:
                    sport_id_map[sport["slug"]] = sport_id
                    sport_url_to_slug_map[sport["url"]] = sport["slug"] # Сохраняем URL -> Slug
            conn.commit()
            print(f"  ✅ Виды спорта сохранены. Всего: {len(sport_id_map)}.")
        else:
            print("  🤷 Виды спорта не найдены или ошибка парсинга.")

        # 3. Парсинг и сохранение турниров
        print("\n--- Начинаем парсинг турниров ---")
        tournament_id_map = {} # url -> id
        if sports_list: # Продолжаем, только если есть виды спорта
            for sport in sports_list: # Перебираем исходный список спорт для получения URL
                sport_id = sport_id_map.get(sport["slug"])
                if not sport_id:
                    print(f"  ⚠️ Пропущен турнир для спорта '{sport['name']}' (ID не найден).")
                    continue

                tournaments_list = await get_tournaments_for_sport(session, sport["url"], config["parser"])
                if tournaments_list:
                    print(f"    Найдено {len(tournaments_list)} турниров для спорта '{sport['name']}'.")
                    for tournament in tournaments_list:
                        tournament_id = insert_tournament(cursor, tournament["name"], tournament["url"], sport_id)
                        if tournament_id:
                            tournament_id_map[tournament["url"]] = tournament_id
                else:
                    print(f"    🤷 Турниры для вида спорта '{sport['name']}' не найдены или ошибка парсинга.")
            conn.commit()
            print(f"  ✅ Турниры сохранены. Всего: {len(tournament_id_map)}.")
        else:
            print("  🤷 Нет видов спорта для парсинга турниров.")

        # 4. Парсинг и сохранение команд и атлетов (для определенных турниров)
        print("\n--- Начинаем парсинг команд и атлетов ---")
        if tournament_id_map: # Продолжаем, только если есть турниры
            for tournament_url, tournament_id in tournament_id_map.items():
                # Получаем sport_id для текущего турнира
                cursor.execute("SELECT sport_id FROM tournaments WHERE id = ?", (tournament_id,))
                result = cursor.fetchone()
                sport_id_for_tournament = result['sport_id'] if result else None

                sport_slug = None
                if sport_id_for_tournament:
                    cursor.execute("SELECT slug FROM sports WHERE id = ?", (sport_id_for_tournament,))
                    sport_slug_result = cursor.fetchone()
                    if sport_slug_result:
                        sport_slug = sport_slug_result['slug']

                # Пропускаем парсинг команд/атлетов для общих категорий, если это не конкретный спорт
                # Например, если sport_slug - это "other" или "lifestyle", часто там нет команд/атлетов
                if sport_slug in ["other", "lifestyle", "cybersport", "bets", "olympicwinter"]: # Добавьте другие общие категории, если нужно
                    print(f"    ℹ️ Пропускаем парсинг команд/атлетов для общей категории: '{sport_slug}' (Турнир: {tournament_url}).")
                    continue

                teams_list = await get_teams_for_tournament(session, tournament_url, config["parser"])
                if teams_list:
                    print(f"      Найдено {len(teams_list)} команд для турнира '{tournament_url}'.")
                    for team in teams_list:
                        team_id = insert_team(cursor, team["name"], team.get("alias"), team.get("url"), team.get("external_id"), tournament_id, team.get("tag_url"))
                        if team_id:
                            # Парсим атлетов, только если у команды есть URL
                            if team.get("url"):
                                athletes_list = await get_athletes_for_team(session, team["url"], config["parser"])
                                if athletes_list:
                                    print(f"        Найдено {len(athletes_list)} атлетов для команды '{team['name']}'.")
                                    for athlete in athletes_list:
                                        insert_athlete(cursor, athlete["name"], athlete.get("url"), team_id, athlete.get("external_id"), athlete.get("tag_url"))
                                else:
                                    print(f"        🤷 Атлеты для команды '{team['name']}' не найдены или ошибка парсинга.")
                            else:
                                print(f"        ℹ️ URL команды '{team['name']}' отсутствует, пропускаем парсинг атлетов.")
                else:
                    print(f"    🤷 Команды для турнира '{tournament_url}' не найдены или ошибка парсинга.")
            conn.commit()
            print(f"  ✅ Команды и атлеты сохранены.")
        else:
            print("  🤷 Нет турниров для парсинга команд и атлетов.")

    conn.close()
    print("\n--- Все структурные данные сохранены. ---")

# --- Точка входа в скрипт ---
if __name__ == "__main__":
    db_file = "prosport.db"
    init_db(db_file) # Вызываем централизованную функцию инициализации
    asyncio.run(main_data_loader(db_file))
