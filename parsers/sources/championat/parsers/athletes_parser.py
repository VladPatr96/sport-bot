# athlete_parser.py

import sqlite3
import os
import yaml
import time
from urllib.parse import urlparse, urljoin

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException

def create_athletes_table_if_not_exists(cursor):
    """
    Создает таблицу 'athletes' в базе данных, если она еще не существует.
    """
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS athletes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                url TEXT NOT NULL UNIQUE,
                team_id INTEGER,
                external_id INTEGER,
                tag_url TEXT,
                tournament_id INTEGER,
                type TEXT,
                FOREIGN KEY (team_id) REFERENCES teams(id),
                FOREIGN KEY (tournament_id) REFERENCES tournaments(id)
            )
        """)
        print("✅ Таблица 'athletes' успешно проверена или создана.")
    except sqlite3.Error as e:
        print(f"❌ Ошибка при создании таблицы 'athletes': {e}")
        raise

def insert_athlete(cursor, name, url, tag_url, tournament_id, team_id, athlete_type):
    """
    Вставляет нового атлета в БД или обновляет существующего.
    """
    try:
        cursor.execute("SELECT id, name FROM athletes WHERE url = ?", (url,))
        existing_data = cursor.fetchone()

        if existing_data:
            existing_id = existing_data['id']
            existing_name = existing_data['name']
            
            cursor.execute("SELECT tag_url FROM athletes WHERE id = ?", (existing_id,))
            current_tag_url = cursor.fetchone()['tag_url']

            if current_tag_url != tag_url:
                cursor.execute("UPDATE athletes SET name = ?, tag_url = ?, tournament_id = ?, team_id = ?, type = ? WHERE id = ?",
                               (name, tag_url, tournament_id, team_id, athlete_type, existing_id))
                print(f"    🔄 Обновлен tag_url для атлета '{existing_name}' (ID: {existing_id}).")
            else:
                print(f"    ℹ️ Атлет '{name}' уже существует (ID: {existing_id}). Данные не изменились.")
            return existing_id
        else:
            cursor.execute("INSERT INTO athletes (name, url, tag_url, tournament_id, team_id, type) VALUES (?, ?, ?, ?, ?, ?)",
                           (name, url, tag_url, tournament_id, team_id, athlete_type))
            print(f"    ✅ Добавлен атлет '{name}' (ID: {cursor.lastrowid}).")
            return cursor.lastrowid
    except sqlite3.IntegrityError as e:
        print(f"❌ Ошибка целостности БД при вставке/обновлении атлета '{name}': {e}.")
        return None
    except sqlite3.Error as e:
        print(f"❌ Ошибка БД при вставке/обновлении атлета '{name}': {e}")
        return None

def get_tag_url_for_athlete(driver, wait, athlete_url, tag_selector):
    """
    Переходит на страницу атлета и извлекает tag_url.
    """
    try:
        driver.get(athlete_url)
        wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, tag_selector)))
        
        tag_link_element = driver.find_element(By.CSS_SELECTOR, tag_selector)
        raw_tag_href = tag_link_element.get_attribute("href")
        if raw_tag_href:
            return urljoin(driver.current_url, raw_tag_href)
        return None
    except (TimeoutException, NoSuchElementException):
        print(f"    ❌ Не удалось найти элемент тега по селектору '{tag_selector}' для {athlete_url}.")
        return None
    except Exception as e:
        print(f"    ❌ Ошибка при извлечении tag_url для {athlete_url}: {e}")
        return None

def parse_players_from_page(driver, wait, cursor, conn, players_page_url, tournament_id, team_id, athlete_type, player_table_selector, player_link_selector, player_tag_selector):
    """
    Парсит страницу игроков и добавляет их в БД.
    """
    print(f"  ➡️ Переходим на страницу игроков: {players_page_url}")
    try:
        driver.get(players_page_url)
        wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, player_table_selector)))
        
        player_table = driver.find_element(By.CSS_SELECTOR, player_table_selector)
        player_link_elements = player_table.find_elements(By.CSS_SELECTOR, player_link_selector)
        
        if not player_link_elements:
            print("    ❌ Не найдено элементов игроков на странице.")
            return

        print(f"    ✅ Найдено {len(player_link_elements)} игроков. Начинаем обработку...")

        # Собираем данные об игроках в список, чтобы избежать StaleElementReferenceException
        player_data_list = []
        for link_element in player_link_elements:
            try:
                athlete_name = link_element.text.strip()
                if not athlete_name:
                    continue
                raw_athlete_url = link_element.get_attribute("href")
                athlete_url = urljoin(driver.current_url, raw_athlete_url)
                player_data_list.append({'name': athlete_name, 'url': athlete_url})
            except StaleElementReferenceException:
                # В этом случае просто пропускаем элемент, так как он мог измениться.
                # Новый подход должен минимизировать такие случаи.
                continue

        # Теперь итерируемся по собранному списку, а не по элементам DOM
        for player_data in player_data_list:
            athlete_name = player_data['name']
            athlete_url = player_data['url']

            print(f"      - Обработка атлета '{athlete_name}' (URL: {athlete_url})")

            athlete_tag_url = get_tag_url_for_athlete(driver, wait, athlete_url, player_tag_selector)
            
            insert_athlete(
                cursor=cursor,
                name=athlete_name,
                url=athlete_url,
                tag_url=athlete_tag_url,
                tournament_id=tournament_id,
                team_id=team_id,
                athlete_type=athlete_type
            )
            
            conn.commit()
            time.sleep(1) # Задержка, чтобы не перегружать сайт


    except (TimeoutException, NoSuchElementException):
        print("  ❌ Не удалось найти таблицу с игроками на странице.")
    except Exception as e:
        print(f"  ❌ Произошла ошибка при парсинге страницы игроков: {e}")

def main():
    """
    Основная функция для парсинга атлетов.
    """

    # --- Начальная настройка: пути к файлам и конфигурация ---
    config_path = os.path.join(os.path.dirname(os.getcwd()), 'sport-news-bot','parsers', 'sources','championat', 'config', 'sources_config.yml')
    # Путь к базе данных
    db_path = os.path.join(os.path.dirname(os.getcwd()), 'sport-news-bot', 'database', 'prosport.db')

    # --- Загрузка селекторов из файла конфигурации ---
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        print(f"❌ Файл конфигурации не найден по пути: {config_path}")
        return

    # Получаем селекторы для парсера атлетов
    parser_config = config['championat']['parser']
    player_table_selector = parser_config.get('player_table_selector')
    player_link_selector = parser_config.get('player_link_selector')
    player_tag_selector = parser_config.get('player_tag_selector')

    if not all([player_table_selector, player_link_selector, player_tag_selector]):
        print("❌ Не удалось загрузить все необходимые селекторы из sources_config.yml. Проверьте имена ключей.")
        return
    
    # --- Инициализация переменных перед блоком try ---
    driver = None
    conn = None
    try:
        # --- Инициализация Selenium WebDriver ---
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument(f"user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
        chrome_options.add_argument("--ignore-certificate-errors")
        chrome_options.add_argument("--allow-insecure-localhost")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)

        driver = webdriver.Chrome(options=chrome_options)
        wait = WebDriverWait(driver, 15)

        # --- Подключение к базе данных и создание таблиц ---
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        create_athletes_table_if_not_exists(cursor)
        conn.commit()

        print("\n--- Начинаем парсинг атлетов ---")
        
        cursor.execute("SELECT id, tournaments_url, type FROM tournaments")
        db_tournaments_data = cursor.fetchall()
        
        if not db_tournaments_data:
            print("В таблице 'tournaments' нет данных. Парсинг атлетов невозможен.")
            return

        for tournament_row in db_tournaments_data:
            tournament_id = tournament_row['id']
            tournament_url = tournament_row['tournaments_url']
            tournament_type = tournament_row['type']
            
            if not tournament_url:
                print(f"  ⚠️ URL для турнира ID {tournament_id} отсутствует. Пропускаем.")
                continue

            print(f"\n--- Обработка турнира ID {tournament_id} (тип: {tournament_type}) ---")
            
            if tournament_type == 'individual':
                players_page_url = tournament_url
                if '/grid/' in tournament_url:
                    players_page_url = tournament_url.replace('/grid/', '/players/')
                elif not tournament_url.endswith('/players/'):
                    if not tournament_url.endswith('/'):
                        players_page_url += '/'
                    players_page_url += 'players/'

                parse_players_from_page(
                    driver=driver,
                    wait=wait,
                    cursor=cursor,
                    conn=conn,
                    players_page_url=players_page_url,
                    tournament_id=tournament_id,
                    team_id=None,
                    athlete_type='individual',
                    player_table_selector=player_table_selector,
                    player_link_selector=player_link_selector,
                    player_tag_selector=player_tag_selector
                )

            elif tournament_type == 'teams':
                cursor.execute("SELECT id, name, url FROM teams WHERE tournament_id = ?", (tournament_id,))
                db_teams_data = cursor.fetchall()
                
                if not db_teams_data:
                    print(f"  ⚠️ Для турнира ID {tournament_id} нет команд. Пропускаем.")
                    continue

                for team_row in db_teams_data:
                    team_id = team_row['id']
                    team_name = team_row['name']
                    team_url = team_row['url']
                    
                    if not team_url:
                        print(f"  ⚠️ URL для команды '{team_name}' (ID {team_id}) отсутствует. Пропускаем.")
                        continue
                    
                    players_page_url = team_url
                    if '/result/' in team_url:
                        players_page_url = team_url.replace('/result/', '/players/')
                    else:
                        if not team_url.endswith('/'):
                            players_page_url += '/'
                        players_page_url += 'players/'

                    print(f"\n  --- Обработка команды '{team_name}' (ID {team_id}) ---")
                    
                    parse_players_from_page(
                        driver=driver,
                        wait=wait,
                        cursor=cursor,
                        conn=conn,
                        players_page_url=players_page_url,
                        tournament_id=tournament_id,
                        team_id=team_id,
                        athlete_type='teams',
                        player_table_selector=player_table_selector,
                        player_link_selector=player_link_selector,
                        player_tag_selector=player_tag_selector
                    )

            else:
                print(f"  ℹ️ Неизвестный тип турнира '{tournament_type}' для ID {tournament_id}. Пропускаем.")
            
            time.sleep(2) # Задержка между турнирами

    except Exception as main_e:
        print(f"❌ Произошла критическая ошибка в основной программе: {main_e}")
    finally:
        if conn:
            conn.close()
            print("\n✅ Соединение с базой данных закрыто.")
        if driver:
            driver.quit()
            print("✅ WebDriver закрыт.")

if __name__ == "__main__":
    main()
