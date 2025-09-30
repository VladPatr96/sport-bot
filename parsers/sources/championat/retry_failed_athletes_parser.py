# retry_failed_athletes_parser.py

import sqlite3
import os
import yaml
import asyncio
import aiohttp
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
import datetime # Для отметки времени

# --- Вспомогательные функции для работы с БД (скопированы из athlete_parser_async.py) ---
def create_athletes_table_if_not_exists(cursor):
    """
    Создает таблицу 'athletes' в базе данных, если она еще не существует.
    """
    try:
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='athletes'")
        if not cursor.fetchone():
            cursor.execute("""
                CREATE TABLE athletes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    url TEXT NOT NULL,
                    team_id INTEGER,
                    external_id INTEGER,
                    tag_url TEXT UNIQUE,
                    tournament_id INTEGER,
                    type TEXT,
                    FOREIGN KEY (team_id) REFERENCES teams(id),
                    FOREIGN KEY (tournament_id) REFERENCES tournaments(id)
                )
            """)
            print("✅ Таблица 'athletes' успешно создана.")
        else:
            # Проверяем и обновляем схему, если необходимо
            cursor.execute("PRAGMA table_info(athletes)")
            columns = [info['name'] for info in cursor.fetchall()]
            if 'type' not in columns:
                cursor.execute("ALTER TABLE athletes ADD COLUMN type TEXT")
                print("⚠️ Добавлен столбец 'type' в таблицу 'athletes'.")
            
            # Проверяем и добавляем уникальный индекс на 'tag_url', если его нет
            cursor.execute("PRAGMA index_list(athletes)")
            indexes = cursor.fetchall()
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

def update_failed_attempt(cursor, conn, attempt_id, new_error_message):
    """
    Обновляет запись о неудачной попытке в таблице 'failed_parsing_attempts'.
    """
    try:
        cursor.execute(
            "UPDATE failed_parsing_attempts SET error_message = ?, timestamp = CURRENT_TIMESTAMP WHERE id = ?",
            (new_error_message, attempt_id)
        )
        conn.commit()
        print(f"    🔄 Обновлена запись о неудачной попытке ID {attempt_id}: {new_error_message[:100]}...")
    except sqlite3.Error as e:
        print(f"    ❌ Ошибка при обновлении записи о неудачной попытке в БД: {e}")

def delete_failed_attempt(cursor, conn, attempt_id):
    """
    Удаляет запись о неудачной попытке из таблицы 'failed_parsing_attempts'.
    """
    try:
        cursor.execute("DELETE FROM failed_parsing_attempts WHERE id = ?", (attempt_id,))
        conn.commit()
        print(f"    ✅ Удалена запись о неудачной попытке ID {attempt_id} (успешно обработана).")
    except sqlite3.Error as e:
        print(f"    ❌ Ошибка при удалении записи о неудачной попытке из БД: {e}")

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

        # 3. Выполнение UPDATE или INSERT
        if existing_id:
            update_needed = False
            if existing_name != name: update_needed = True
            if existing_url != url: update_needed = True
            if tag_url and existing_tag_url_db != tag_url: update_needed = True

            if update_needed:
                cursor.execute("UPDATE athletes SET name = ?, url = ?, tag_url = COALESCE(?, tag_url), tournament_id = ?, team_id = ?, type = ? WHERE id = ?",
                               (name, url, tag_url, tournament_id, team_id, athlete_type, existing_id))
                print(f"    🔄 Обновлен атлет '{existing_name}' (ID: {existing_id}). Новый URL: {url}, Новый tag_url: {tag_url}.")
            else:
                print(f"    ℹ️ Атлет '{name}' (ID: {existing_id}) уже существует. Данные не изменились.")
            return existing_id
        else:
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

# --- Асинхронные функции для парсинга (скопированы из athlete_parser_async.py) ---
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
                return await response.text(), None
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

async def retry_failed_attempt(session, cursor, conn, failed_attempt_row, parser_config, semaphore):
    """
    Пытается повторно обработать одну запись из failed_parsing_attempts.
    """
    attempt_id = failed_attempt_row['id']
    entity_type = failed_attempt_row['entity_type']
    entity_id = failed_attempt_row['entity_id']
    failed_url = failed_attempt_row['url'] # URL, который изначально не удалось обработать
    
    player_table_selector = parser_config.get('player_table_selector')
    player_link_selector = parser_config.get('player_link_selector')

    print(f"\n--- Повторная попытка обработки: {entity_type} ID {entity_id} (запись failed_id: {attempt_id}) ---")

    target_url = None
    if entity_type == 'tournament':
        cursor.execute("SELECT tournaments_url, type FROM tournaments WHERE id = ?", (entity_id,))
        tournament_data = cursor.fetchone()
        if tournament_data:
            base_url = tournament_data['tournaments_url']
            tournament_type = tournament_data['type']
            if tournament_type == 'individual':
                target_url = base_url
                if '/grid/' in base_url:
                    target_url = base_url.replace('/grid/', '/players/')
                elif not base_url.endswith('/players/'):
                    if not base_url.endswith('/'):
                        target_url += '/'
                    target_url += 'players/'
            else:
                # Для командных турниров, URL в failed_parsing_attempts уже должен быть URL страницы игроков команды
                target_url = failed_url 
            
            print(f"  ➡️ Получаем HTML для (повторно) турнира: {target_url}")
            html_content, error_msg = await fetch_page_content_async(session, target_url, semaphore)
            
            if html_content:
                players_data, parse_error_msg = parse_players_from_html(
                    html_content=html_content,
                    base_url=target_url,
                    player_table_selector=player_table_selector,
                    player_link_selector=player_link_selector
                )
                if players_data:
                    for player_data in players_data:
                        insert_athlete(
                            cursor=cursor,
                            name=player_data['name'],
                            url=player_data['url'],
                            tag_url=None,
                            tournament_id=entity_id,
                            team_id=None,
                            athlete_type='individual'
                        )
                    conn.commit()
                    print(f"  ✅ Успешно обработано {len(players_data)} атлетов для турнира ID {entity_id}.")
                    delete_failed_attempt(cursor, conn, attempt_id)
                else:
                    update_failed_attempt(cursor, conn, attempt_id, f"Повторный парсинг HTML не удался: {parse_error_msg}")
            else:
                update_failed_attempt(cursor, conn, attempt_id, f"Повторная загрузка страницы не удалась: {error_msg}")
        else:
            update_failed_attempt(cursor, conn, attempt_id, f"Турнир с ID {entity_id} не найден в таблице 'tournaments'.")

    elif entity_type == 'team':
        cursor.execute("SELECT name FROM teams WHERE id = ?", (entity_id,))
        team_data = cursor.fetchone()
        if team_data:
            team_name = team_data['name']
            # В этом случае failed_url уже является URL страницы игроков команды
            target_url = failed_url
            
            print(f"  ➡️ Получаем HTML для (повторно) команды '{team_name}': {target_url}")
            html_content, error_msg = await fetch_page_content_async(session, target_url, semaphore)
            
            if html_content:
                players_data, parse_error_msg = parse_players_from_html(
                    html_content=html_content,
                    base_url=target_url,
                    player_table_selector=player_table_selector,
                    player_link_selector=player_link_selector
                )
                if players_data:
                    for player_data in players_data:
                        insert_athlete(
                            cursor=cursor,
                            name=player_data['name'],
                            url=player_data['url'],
                            tag_url=None,
                            tournament_id=None, # Для командных игроков tournament_id может быть None или нужно получить его из teams
                            team_id=entity_id,
                            athlete_type='teams'
                        )
                    conn.commit()
                    print(f"  ✅ Успешно обработано {len(players_data)} атлетов для команды ID {entity_id}.")
                    delete_failed_attempt(cursor, conn, attempt_id)
                else:
                    update_failed_attempt(cursor, conn, attempt_id, f"Повторный парсинг HTML не удался: {parse_error_msg}")
            else:
                update_failed_attempt(cursor, conn, attempt_id, f"Повторная загрузка страницы не удалась: {error_msg}")
        else:
            update_failed_attempt(cursor, conn, attempt_id, f"Команда с ID {entity_id} не найдена в таблице 'teams'.")
    else:
        update_failed_attempt(cursor, conn, attempt_id, f"Неизвестный тип сущности: {entity_type}. Пропускаем.")


async def main():
    """
    Основная асинхронная функция для повторной обработки неудачных попыток парсинга атлетов.
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
    
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Убедимся, что таблицы существуют
        create_athletes_table_if_not_exists(cursor)
        create_failed_attempts_table_if_not_exists(cursor)
        conn.commit()

        print("\n--- Начинаем АСИНХРОННУЮ повторную обработку неудачных попыток парсинга атлетов ---")
        
        cursor.execute("SELECT * FROM failed_parsing_attempts")
        failed_attempts = cursor.fetchall()
        
        if not failed_attempts:
            print("В таблице 'failed_parsing_attempts' нет записей для повторной обработки. Завершение.")
            return

        print(f"Найдено {len(failed_attempts)} неудачных попыток для повторной обработки.")
        semaphore = asyncio.Semaphore(5) # Семафор для ограничения количества одновременных запросов

        async with aiohttp.ClientSession() as session:
            retry_tasks = []
            for attempt_row in failed_attempts:
                retry_tasks.append(
                    retry_failed_attempt(session, cursor, conn, attempt_row, parser_config, semaphore)
                )
            
            await asyncio.gather(*retry_tasks)

    except Exception as main_e:
        print(f"❌ Произошла критическая ошибка в основной программе: {main_e}")
    finally:
        if conn:
            conn.close()
            print("\n✅ Соединение с базой данных закрыто.")

if __name__ == "__main__":
    asyncio.run(main())
