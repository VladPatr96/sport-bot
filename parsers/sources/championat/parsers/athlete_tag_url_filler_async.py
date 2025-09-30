# athlete_tag_url_filler_async.py

import sqlite3
import os
import yaml
import asyncio
import aiohttp
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
import datetime

# --- Вспомогательные функции для работы с БД ---
def create_tables_if_not_exists(cursor):
    """
    Создает необходимые таблицы в базе данных, если они еще не существуют.
    Включает таблицу 'athletes' (с UNIQUE tag_url) и 'failed_tag_url_attempts'.
    """
    try:
        # Проверяем и создаем таблицу 'athletes'
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='athletes'")
        if not cursor.fetchone():
            cursor.execute("""
                CREATE TABLE athletes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    url TEXT NOT NULL,
                    team_id INTEGER,
                    external_id INTEGER,
                    tag_url TEXT UNIQUE, -- Убедимся, что это UNIQUE
                    tournament_id INTEGER,
                    type TEXT,
                    FOREIGN KEY (team_id) REFERENCES teams(id),
                    FOREIGN KEY (tournament_id) REFERENCES tournaments(id)
                )
            """)
            print("✅ Таблица 'athletes' успешно создана.")
        else:
            # Проверяем наличие уникального индекса на tag_url и добавляем, если его нет
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

        # Создаем таблицу для логирования неудачных попыток заполнения tag_url
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS failed_tag_url_attempts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                athlete_id INTEGER,
                athlete_name TEXT,
                athlete_url TEXT,
                error_message TEXT,
                timestamp TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        print("✅ Таблица 'failed_tag_url_attempts' успешно проверена или создана.")
        
    except sqlite3.Error as e:
        print(f"❌ Ошибка при проверке/создании таблиц: {e}")
        raise

def log_failed_tag_url_attempt(cursor, conn, athlete_id, athlete_name, athlete_url, error_message):
    """
    Логирует неудачную попытку заполнения tag_url для атлета.
    """
    try:
        cursor.execute(
            "INSERT INTO failed_tag_url_attempts (athlete_id, athlete_name, athlete_url, error_message) VALUES (?, ?, ?, ?)",
            (athlete_id, athlete_name, athlete_url, error_message)
        )
        conn.commit()
        print(f"    ⚠️ Залогирована неудачная попытка для атлета '{athlete_name}' (ID: {athlete_id}): {error_message[:150]}...")
    except sqlite3.Error as e:
        print(f"    ❌ Ошибка при логировании неудачной попытки в БД: {e}")

def update_athlete_tag_url(cursor, conn, athlete_id, new_tag_url, athlete_name):
    """
    Обновляет tag_url для атлета.
    Использует ON CONFLICT для обработки случаев, когда new_tag_url уже существует.
    В случае конфликта, мы не обновляем запись, а логируем это как проблему.
    """
    try:
        # Пытаемся обновить tag_url.
        # Если new_tag_url уже существует для другого атлета,
        # то произойдет UNIQUE constraint violation.
        # Мы не можем использовать ON CONFLICT DO UPDATE здесь,
        # так как это приведет к обновлению другого атлета, а не текущего.
        # Вместо этого мы ловим исключение и логируем его.
        cursor.execute("""
            UPDATE athletes
            SET tag_url = ?
            WHERE id = ?
        """, (new_tag_url, athlete_id))
        
        if cursor.rowcount > 0:
            print(f"    ✅ Обновлен tag_url для атлета '{athlete_name}' (ID: {athlete_id}): {new_tag_url}")
            conn.commit()
            return True
        else:
            print(f"    ℹ️ Не удалось обновить tag_url для атлета '{athlete_name}' (ID: {athlete_id}). Возможно, запись не найдена.")
            return False

    except sqlite3.IntegrityError as e:
        # Это исключение ловит UNIQUE constraint failed
        error_msg = f"UNIQUE constraint failed: tag_url '{new_tag_url}' уже используется другим атлетом."
        log_failed_tag_url_attempt(cursor, conn, athlete_id, athlete_name, None, error_msg)
        print(f"    ❌ Ошибка БД при обновлении tag_url для атлета ID {athlete_id}: {error_msg}")
        return False
    except sqlite3.Error as e:
        error_msg = f"Неизвестная ошибка БД при обновлении tag_url: {e}"
        log_failed_tag_url_attempt(cursor, conn, athlete_id, athlete_name, None, error_msg)
        print(f"    ❌ Ошибка БД при обновлении tag_url для атлета ID {athlete_id}: {error_msg}")
        return False


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
                return await response.text(), None
        except aiohttp.ClientError as e:
            return None, str(e)
        except asyncio.TimeoutError:
            return None, "Таймаут загрузки страницы"
        except Exception as e:
            return None, str(e)

def parse_tag_url_from_html(html_content, tag_url_selector, base_url):
    """
    Парсит HTML-содержимое страницы и извлекает tag_url.
    Возвращает tag_url или None, если не найден.
    """
    if not html_content:
        return None

    soup = BeautifulSoup(html_content, 'html.parser')
    
    try:
        tag_link_element = soup.select_one(tag_url_selector)
        if tag_link_element and tag_link_element.get('href'):
            raw_tag_url = tag_link_element.get('href')
            # Убедимся, что это абсолютный URL
            return urljoin(base_url, raw_tag_url)
        return None
    except Exception as e:
        print(f"    ❌ Ошибка при парсинге tag_url из HTML: {e}")
        return None

async def process_athlete_async(session, cursor, conn, athlete_row, parser_config, semaphore):
    """
    Асинхронно обрабатывает одного атлета: загружает его страницу
    и пытается извлечь и обновить tag_url.
    """
    athlete_id = athlete_row['id']
    athlete_name = athlete_row['name']
    athlete_url = athlete_row['url']
    
    tag_url_selector = parser_config.get('tag_url_selector')

    if not athlete_url:
        error_msg = "URL атлета отсутствует."
        log_failed_tag_url_attempt(cursor, conn, athlete_id, athlete_name, athlete_url, error_msg)
        print(f"  ⚠️ {error_msg} для атлета '{athlete_name}' (ID: {athlete_id}). Пропускаем.")
        return

    print(f"\n--- Обработка атлета: '{athlete_name}' (ID: {athlete_id}) ---")
    print(f"  ➡️ Загрузка страницы: {athlete_url}")
    
    html_content, error_msg = await fetch_page_content_async(session, athlete_url, semaphore)
    
    if html_content:
        new_tag_url = parse_tag_url_from_html(html_content, tag_url_selector, athlete_url)
        
        if new_tag_url:
            # Проверяем, отличается ли новый tag_url от текущего в БД (если он есть)
            current_tag_url = athlete_row['tag_url']
            if current_tag_url != new_tag_url:
                update_athlete_tag_url(cursor, conn, athlete_id, new_tag_url, athlete_name)
            else:
                print(f"    ℹ️ Tag_url для атлета '{athlete_name}' (ID: {athlete_id}) уже актуален: {new_tag_url}.")
        else:
            error_msg = f"Не удалось извлечь tag_url со страницы атлета по селектору '{tag_url_selector}'."
            log_failed_tag_url_attempt(cursor, conn, athlete_id, athlete_name, athlete_url, error_msg)
            print(f"    ❌ {error_msg}")
    else:
        log_failed_tag_url_attempt(cursor, conn, athlete_id, athlete_name, athlete_url, f"Не удалось загрузить страницу: {error_msg}")
        print(f"    ❌ Таймаут при загрузке страницы {athlete_url}")


async def main():
    """
    Основная асинхронная функция для заполнения tag_url для атлетов.
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
    tag_url_selector = parser_config.get('tag_url_selector')
    
    if not tag_url_selector:
        print("❌ Не удалось загрузить селектор 'tag_url_selector' из sources_config.yml. Проверьте имя ключа.")
        return
    
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Создаем/проверяем таблицы
        create_tables_if_not_exists(cursor)
        conn.commit()

        print("\n--- Начинаем АСИНХРОННОЕ заполнение tag_url для атлетов ---")
        
        # Выбираем только тех атлетов, у которых tag_url еще не заполнен
        cursor.execute("SELECT id, name, url, tag_url FROM athletes WHERE tag_url IS NULL OR tag_url = ''")
        athletes_to_process = cursor.fetchall()
        
        if not athletes_to_process:
            print("Все атлеты уже имеют заполненный tag_url. Завершение работы.")
            return

        semaphore = asyncio.Semaphore(5) # Семафор для ограничения количества одновременных запросов

        async with aiohttp.ClientSession() as session:
            athlete_tasks = []
            for athlete_row in athletes_to_process:
                athlete_tasks.append(
                    process_athlete_async(session, cursor, conn, athlete_row, parser_config, semaphore)
                )
            
            await asyncio.gather(*athlete_tasks)

    except Exception as main_e:
        print(f"❌ Произошла критическая ошибка в основной программе: {main_e}")
    finally:
        if conn:
            conn.close()
            print("\n✅ Соединение с базой данных закрыто.")

if __name__ == "__main__":
    asyncio.run(main())
