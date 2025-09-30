# athlete_parser_async.py

import sqlite3
import os
import yaml
import time
import asyncio
import aiohttp
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
import datetime # Для отметки времени

# --- Вспомогательные функции для работы с БД ---
def create_athletes_table_if_not_exists(cursor):
    """
    Создает таблицу 'athletes' в базе данных, если она еще не существует.
    Теперь 'tag_url' является UNIQUE, а 'url' - нет.
    """
    try:
        # Проверяем наличие таблицы
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='athletes'")
        if not cursor.fetchone():
            cursor.execute("""
                CREATE TABLE athletes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    url TEXT NOT NULL, -- Теперь не UNIQUE
                    team_id INTEGER,
                    external_id INTEGER,
                    tag_url TEXT UNIQUE, -- Теперь UNIQUE
                    tournament_id INTEGER,
                    type TEXT,
                    FOREIGN KEY (team_id) REFERENCES teams(id),
                    FOREIGN KEY (tournament_id) REFERENCES tournaments(id)
                )
            """)
            print("✅ Таблица 'athletes' успешно создана.")
        else:
            # Проверяем и обновляем схему, если необходимо (например, если UNIQUE был на url)
            cursor.execute("PRAGMA table_info(athletes)")
            columns = [info['name'] for info in cursor.fetchall()]
            
            # Проверяем, есть ли столбец 'type'
            if 'type' not in columns:
                cursor.execute("ALTER TABLE athletes ADD COLUMN type TEXT")
                print("⚠️ Добавлен столбец 'type' в таблицу 'athletes'.")

            # Проверяем, есть ли уникальный индекс на 'url'
            cursor.execute("PRAGMA index_list(athletes)")
            indexes = cursor.fetchall()
            url_unique_index_exists = False
            for idx in indexes:
                cursor.execute(f"PRAGMA index_info({idx['name']})")
                idx_info = cursor.fetchall()
                for col_info in idx_info:
                    if col_info['name'] == 'url' and idx['unique'] == 1:
                        url_unique_index_exists = True
                        break
                if url_unique_index_exists:
                    break
            
            if url_unique_index_exists:
                print("⚠️ Обнаружен уникальный индекс на 'url'. Рекомендуется вручную удалить его, если он мешает.")
                print("   (Например: DROP INDEX IF EXISTS idx_athletes_url_unique;)")
            
            # Проверяем, есть ли уникальный индекс на 'tag_url'
            tag_url_unique_index_exists = False
            for idx in indexes:
                cursor.execute(f"PRAGMA index_info({idx['name']})")
                idx_info = cursor.fetchall()
                for col_info in idx_info:
                    if col_info['name'] == 'tag_url' and idx['unique'] == 1:
                        tag_url_unique_index_exists = True
                        break
                if tag_url_unique_index_exists:
                    break

            if not tag_url_unique_index_exists:
                cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_athletes_tag_url_unique ON athletes (tag_url)")
                print("✅ Добавлен уникальный индекс на 'tag_url' в таблицу 'athletes'.")

            print("✅ Таблица 'athletes' проверена.")

    except sqlite3.Error as e:
        print(f"❌ Ошибка при проверке/создании таблицы 'athletes': {e}")
        raise

def create_failed_attempts_table_if_not_exists(cursor):
    """
    Создает таблицу 'failed_parsing_attempts' для логирования неудачных попыток парсинга.
    """
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS failed_parsing_attempts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                entity_type TEXT NOT NULL,    -- 'tournament' или 'team'
                entity_id INTEGER NOT NULL,   -- ID турнира или команды из соответствующей таблицы
                url TEXT NOT NULL,            -- URL, который не удалось обработать
                error_message TEXT,           -- Сообщение об ошибке
                timestamp TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        print("✅ Таблица 'failed_parsing_attempts' успешно проверена или создана.")
    except sqlite3.Error as e:
        print(f"❌ Ошибка при создании таблицы 'failed_parsing_attempts': {e}")
        raise

def log_failed_attempt(cursor, conn, entity_type, entity_id, url, error_message):
    """
    Логирует неудачную попытку парсинга в таблицу 'failed_parsing_attempts'.
    """
    try:
        cursor.execute(
            "INSERT INTO failed_parsing_attempts (entity_type, entity_id, url, error_message) VALUES (?, ?, ?, ?)",
            (entity_type, entity_id, url, error_message)
        )
        conn.commit()
        print(f"    ⚠️ Залогирована неудачная попытка: {entity_type} ID {entity_id}, URL: {url}, Ошибка: {error_message[:100]}...")
    except sqlite3.Error as e:
        print(f"    ❌ Ошибка при логировании неудачной попытки в БД: {e}")

def insert_athlete(cursor, name, url, tag_url, tournament_id, team_id, athlete_type):
    """
    Вставляет нового атлета в БД или обновляет существующего,
    используя tag_url как основной уникальный идентификатор.
    """
    try:
        existing_id = None
        existing_name = None
        existing_url = None
        existing_tag_url_db = None

        # 1. Попытка найти атлета по tag_url (если он известен)
        if tag_url:
            cursor.execute("SELECT id, name, url, tag_url FROM athletes WHERE tag_url = ?", (tag_url,))
            existing_data = cursor.fetchone()
            if existing_data:
                existing_id = existing_data['id']
                existing_name = existing_data['name']
                existing_url = existing_data['url']
                existing_tag_url_db = existing_data['tag_url']
                # print(f"    ℹ️ Атлет '{existing_name}' (ID: {existing_id}) найден по tag_url: {tag_url}.")

        # 2. Если tag_url неизвестен ИЛИ атлет не найден по tag_url,
        #    попытка найти по url (как резервный, временный идентификатор)
        if existing_id is None:
            cursor.execute("SELECT id, name, url, tag_url FROM athletes WHERE url = ?", (url,))
            existing_data_by_url = cursor.fetchone()
            if existing_data_by_url:
                existing_id = existing_data_by_url['id']
                existing_name = existing_data_by_url['name']
                existing_url = existing_data_by_url['url']
                existing_tag_url_db = existing_data_by_url['tag_url']
                # print(f"    ℹ️ Атлет '{existing_name}' (ID: {existing_id}) найден по url: {url}.")

        # 3. Выполнение UPDATE или INSERT
        if existing_id:
            # Атлет найден, обновляем данные
            update_needed = False
            if existing_name != name: update_needed = True
            if existing_url != url: update_needed = True # Обновляем основной URL, если он изменился
            if tag_url and existing_tag_url_db != tag_url: update_needed = True # Обновляем tag_url, если новый известен и отличается

            if update_needed:
                # COALESCE(?, tag_url) сохраняет старый tag_url, если новый tag_url равен NULL
                cursor.execute("UPDATE athletes SET name = ?, url = ?, tag_url = COALESCE(?, tag_url), tournament_id = ?, team_id = ?, type = ? WHERE id = ?",
                               (name, url, tag_url, tournament_id, team_id, athlete_type, existing_id))
                print(f"    🔄 Обновлен атлет '{existing_name}' (ID: {existing_id}). Новый URL: {url}, Новый tag_url: {tag_url}.")
            else:
                print(f"    ℹ️ Атлет '{name}' (ID: {existing_id}) уже существует. Данные не изменились.")
            return existing_id
        else:
            # Атлет не найден, вставляем новую запись
            cursor.execute("INSERT INTO athletes (name, url, tag_url, tournament_id, team_id, type) VALUES (?, ?, ?, ?, ?, ?)",
                           (name, url, tag_url, tournament_id, team_id, athlete_type))
            print(f"    ✅ Добавлен новый атлет '{name}' (ID: {cursor.lastrowid}). URL: {url}, Tag_url: {tag_url}.")
            return cursor.lastrowid
    except sqlite3.IntegrityError as e:
        print(f"❌ Ошибка целостности БД при вставке/обновлении атлета '{name}' (URL: {url}, Tag_url: {tag_url}): {e}.")
        return None
    except sqlite3.Error as e:
        print(f"❌ Ошибка БД при вставке/обновлении атлета '{name}': {e}")
        return None

# --- Асинхронные функции для парсинга ---

async def fetch_page_content_async(session, url, semaphore):
    """
    Асинхронно загружает HTML-содержимое страницы по URL с помощью aiohttp.
    Использует семафор для ограничения параллельных запросов.
    Возвращает кортеж (html_content, error_message).
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'
    }
    async with semaphore:
        try:
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as response:
                response.raise_for_status()
                return await response.text(), None # ИСПРАВЛЕНО: Всегда возвращаем кортеж из двух значений
        except aiohttp.ClientError as e:
            return None, str(e)
        except asyncio.TimeoutError:
            return None, "Таймаут загрузки страницы"
        except Exception as e:
            return None, str(e)

def parse_players_from_html(html_content, base_url, player_table_selector, player_link_selector):
    """
    Парсит HTML-содержимое страницы и извлекает базовую информацию об игроках.
    Возвращает кортеж (players_data, error_message).
    """
    if not html_content:
        return [], "HTML-контент пуст"

    soup = BeautifulSoup(html_content, 'html.parser')
    players_data = []

    try:
        player_table = soup.select_one(player_table_selector)
        if not player_table:
            return [], f"Не найдена таблица игроков по селектору '{player_table_selector}'."

        player_links = player_table.select(player_link_selector)
        
        if not player_links:
            return [], f"Не найдено ссылок на игроков по селектору '{player_link_selector}' в таблице."

        for link_element in player_links:
            athlete_name = link_element.get_text(strip=True)
            raw_athlete_url = link_element.get('href')

            if athlete_name and raw_athlete_url:
                athlete_url = urljoin(base_url, raw_athlete_url)
                players_data.append({
                    'name': athlete_name,
                    'url': athlete_url
                })
        return players_data, None
    except Exception as e:
        return [], f"Ошибка при парсинге HTML: {e}"

async def process_tournament_async(session, cursor, conn, tournament_row, parser_config, semaphore):
    """
    Асинхронно обрабатывает один турнир: определяет тип, загружает страницу игроков
    и сохраняет их в БД.
    """
    tournament_id = tournament_row['id']
    tournament_name = tournament_row['name']
    tournament_url = tournament_row['tournaments_url']
    tournament_type = tournament_row['type']
    
    player_table_selector = parser_config.get('player_table_selector')
    player_link_selector = parser_config.get('player_link_selector')

    if not tournament_url:
        print(f"  ⚠️ URL для турнира '{tournament_name}' (ID: {tournament_id}) отсутствует. Пропускаем.")
        return

    print(f"\n--- Обработка турнира: '{tournament_name}' (ID: {tournament_id}, тип: {tournament_type}) ---")
    
    players_page_url = None
    if tournament_type == 'individual':
        players_page_url = tournament_url
        if '/grid/' in tournament_url:
            players_page_url = tournament_url.replace('/grid/', '/players/')
        elif not tournament_url.endswith('/players/'):
            if not tournament_url.endswith('/'):
                players_page_url += '/'
            players_page_url += 'players/'

        print(f"  ➡️ Получаем HTML для страницы игроков: {players_page_url}")
        html_content, error_msg = await fetch_page_content_async(session, players_page_url, semaphore)
        
        if html_content:
            players_data, parse_error_msg = parse_players_from_html(
                html_content=html_content,
                base_url=players_page_url,
                player_table_selector=player_table_selector,
                player_link_selector=player_link_selector
            )
            if players_data:
                for player_data in players_data:
                    insert_athlete(
                        cursor=cursor,
                        name=player_data['name'],
                        url=player_data['url'],
                        tag_url=None, # tag_url не извлекается в этом парсере
                        tournament_id=tournament_id,
                        team_id=None,
                        athlete_type='individual'
                    )
                conn.commit()
                print(f"  ✅ Обработано {len(players_data)} атлетов для турнира '{tournament_name}'.")
            else:
                log_failed_attempt(cursor, conn, 'tournament', tournament_id, players_page_url, f"Парсинг HTML не удался: {parse_error_msg}")
        else:
            log_failed_attempt(cursor, conn, 'tournament', tournament_id, players_page_url, f"Не удалось загрузить страницу: {error_msg}")

    elif tournament_type == 'teams':
        cursor.execute("SELECT id, name, url FROM teams WHERE tournament_id = ?", (tournament_id,))
        db_teams_data = cursor.fetchall()
        
        if not db_teams_data:
            print(f"  ⚠️ Для турнира '{tournament_name}' (ID {tournament_id}) нет команд. Пропускаем.")
            return

        team_tasks = []
        for team_row in db_teams_data:
            team_id = team_row['id']
            team_name = team_row['name']
            team_url = team_row['url']
            
            if not team_url:
                print(f"  ⚠️ URL для команды '{team_name}' (ID {team_id}) отсутствует. Пропускаем.")
                log_failed_attempt(cursor, conn, 'team', team_id, "URL отсутствует", f"URL для команды '{team_name}' отсутствует.")
                continue
            
            players_page_url = team_url
            if '/result/' in team_url:
                players_page_url = team_url.replace('/result/', '/players/')
            else:
                if not team_url.endswith('/'):
                    players_page_url += '/'
                players_page_url += 'players/'

            team_tasks.append(
                process_team_players_async(
                    session=session,
                    cursor=cursor,
                    conn=conn,
                    players_page_url=players_page_url,
                    tournament_id=tournament_id,
                    team_id=team_id,
                    athlete_type='teams',
                    player_table_selector=player_table_selector,
                    player_link_selector=player_link_selector,
                    semaphore=semaphore
                )
            )
        
        if team_tasks:
            await asyncio.gather(*team_tasks)
            print(f"  ✅ Обработаны атлеты для всех команд турнира '{tournament_name}'.")

    else:
        print(f"  ℹ️ Неизвестный тип турнира '{tournament_type}' для ID {tournament_id}. Пропускаем.")
    
async def process_team_players_async(session, cursor, conn, players_page_url, tournament_id, team_id, athlete_type, player_table_selector, player_link_selector, semaphore):
    """
    Асинхронно обрабатывает страницу игроков для одной команды.
    """
    print(f"    ➡️ Получаем HTML для страницы игроков команды: {players_page_url}")
    html_content, error_msg = await fetch_page_content_async(session, players_page_url, semaphore)
    if html_content:
        players_data, parse_error_msg = parse_players_from_html(
            html_content=html_content,
            base_url=players_page_url,
            player_table_selector=player_table_selector,
            player_link_selector=player_link_selector
        )
        if players_data:
            for player_data in players_data:
                insert_athlete(
                    cursor=cursor,
                    name=player_data['name'],
                    url=player_data['url'],
                    tag_url=None, # tag_url не извлекается в этом парсере
                    tournament_id=tournament_id,
                    team_id=team_id,
                    athlete_type=athlete_type
                )
            conn.commit()
            print(f"    ✅ Обработано {len(players_data)} атлетов для команды ID {team_id}.")
        else:
            log_failed_attempt(cursor, conn, 'team', team_id, players_page_url, f"Парсинг HTML не удался: {parse_error_msg}")
    else:
        log_failed_attempt(cursor, conn, 'team', team_id, players_page_url, f"Не удалось загрузить страницу: {error_msg}")


async def main():
    """
    Основная асинхронная функция для парсинга атлетов.
    """
    # --- Начальная настройка: пути к файлам и конфигурация ---
    config_path = os.path.join(os.path.dirname(os.getcwd()), 'sport-news-bot','parsers', 'sources','championat', 'config', 'sources_config.yml')
    # Путь к базе данных
    db_path = os.path.join(os.path.dirname(os.getcwd()), 'sport-news-bot', 'database', 'prosport.db')
    
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        print(f"❌ Файл конфигурации не найден по пути: {config_path}")
        return

    parser_config = config['championat']['parser']
    player_table_selector = parser_config.get('player_table_selector')
    player_link_selector = parser_config.get('player_link_selector')
    
    if not all([player_table_selector, player_link_selector]):
        print("❌ Не удалось загрузить все необходимые селекторы из sources_config.yml. Проверьте имена ключей.")
        return
    
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Создаем/проверяем таблицы
        create_athletes_table_if_not_exists(cursor)
        create_failed_attempts_table_if_not_exists(cursor)
        conn.commit()

        print("\n--- Начинаем АСИНХРОННЫЙ парсинг атлетов (только имя и URL) ---")
        
        cursor.execute("SELECT id, name, tournaments_url, type FROM tournaments")
        db_tournaments_data = cursor.fetchall()
        
        if not db_tournaments_data:
            print("В таблице 'tournaments' нет данных. Парсинг атлетов невозможен.")
            return

        semaphore = asyncio.Semaphore(5) # Семафор для ограничения количества одновременных запросов

        async with aiohttp.ClientSession() as session:
            tournament_tasks = []
            for tournament_row in db_tournaments_data:
                tournament_tasks.append(
                    process_tournament_async(session, cursor, conn, tournament_row, parser_config, semaphore)
                )
            
            await asyncio.gather(*tournament_tasks)

    except Exception as main_e:
        print(f"❌ Произошла критическая ошибка в основной программе: {main_e}")
    finally:
        if conn:
            conn.close()
            print("\n✅ Соединение с базой данных закрыто.")

if __name__ == "__main__":
    asyncio.run(main())
