# teams_parser.py

import sqlite3
import os
import yaml
import time
from urllib.parse import urljoin

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException

def insert_team(cursor, name, url, tag_url, tournament_id, alias=None, external_id=None):
    """
    Вставляет новую команду в БД или обновляет существующую.
    """
    try:
        # Проверяем, существует ли команда по URL
        cursor.execute("SELECT id, name FROM teams WHERE url = ?", (url,))
        existing_data = cursor.fetchone()

        if existing_data:
            existing_id = existing_data['id']
            existing_name = existing_data['name']
            
            # Получаем текущий tag_url из базы данных
            cursor.execute("SELECT tag_url FROM teams WHERE id = ?", (existing_id,))
            current_tag_url = cursor.fetchone()['tag_url']
            
            # Обновляем только если tag_url изменился или был пустым, а теперь найден
            if current_tag_url != tag_url:
                cursor.execute("UPDATE teams SET name = ?, tag_url = ? WHERE id = ?",
                               (name, tag_url, existing_id))
                print(f"    🔄 Обновлен tag_url для команды '{existing_name}' (ID: {existing_id}).")
            else:
                print(f"    ℹ️ Команда '{name}' уже существует (ID: {existing_id}). Данные не изменились.")
            return existing_id
        else:
            # Если команда не найдена, вставляем новую запись
            cursor.execute("INSERT INTO teams (name, url, tag_url, tournament_id, alias, external_id) VALUES (?, ?, ?, ?, ?, ?)",
                           (name, url, tag_url, tournament_id, alias, external_id))
            print(f"    ✅ Добавлена команда '{name}' (ID: {cursor.lastrowid}) для турнира ID: {tournament_id}.")
            return cursor.lastrowid
    except sqlite3.IntegrityError as e:
        print(f"❌ Ошибка целостности БД при вставке/обновлении команды '{name}': {e}.")
        return None
    except sqlite3.Error as e:
        print(f"❌ Ошибка БД при вставке/обновлении команды '{name}': {e}")
        return None

def check_and_update_db_schema(cursor):
    """
    Проверяет, есть ли в таблице 'tournaments' столбец 'type', и добавляет его, если нет.
    """
    try:
        cursor.execute("PRAGMA table_info(tournaments)")
        columns = [info['name'] for info in cursor.fetchall()]
        if 'type' not in columns:
            print("⚠️ Столбец 'type' не найден в таблице 'tournaments'. Добавляем его...")
            cursor.execute("ALTER TABLE tournaments ADD COLUMN type TEXT")
            print("✅ Столбец 'type' успешно добавлен.")
        else:
            print("✅ Столбец 'type' уже существует в таблице 'tournaments'.")
    except sqlite3.Error as e:
        print(f"❌ Ошибка при проверке/обновлении схемы БД: {e}")
        raise

def main():
    """
    Основная функция для парсинга команд со страниц всех турниров.
    """
    # --- Начальная настройка: пути к файлам и конфигурация ---
    config_path = os.path.join(os.path.dirname(os.getcwd()), 'sport-news-bot','parsers', 'sources','championat', 'config', 'sources_config.yml')
    # Путь к базе данных
    db_path = os.path.join(os.path.dirname(os.getcwd()), 'sport-news-bot', 'database', 'prosport.db')

    if not os.path.exists(config_path):
        print(f"Ошибка: sources_config.yml не найден по пути {config_path}")
        return
    try:
        with open(config_path, encoding="utf-8") as f:
            all_config = yaml.safe_load(f)
        championat_config = all_config["championat"]
        print("Конфигурация championat.com загружена успешно.")
    except Exception as e:
        print(f"Ошибка загрузки конфигурации: {e}")
        return

    parser_config = championat_config['selectors']
    team_item_selector = parser_config.get('team_item_selector')
    team_name_selector = parser_config.get('team_name_selector')
    team_link_selector = parser_config.get('team_link_selector')
    team_tag_link_selector_team_page = parser_config.get('team_tag_link_selector_team_page')
    no_teams_message_selector = parser_config.get('no_teams_message_selector')
    tournament_results_table_selector = parser_config.get('tournament_results_table_selector')
    tournament_table_team_link_selector = parser_config.get('tournament_table_team_link_selector')

    if not all([team_item_selector, team_name_selector, team_link_selector, team_tag_link_selector_team_page, tournament_results_table_selector, tournament_table_team_link_selector]):
        print("❌ Ошибка: Отсутствуют обязательные селекторы в конфигурации. Проверьте 'sources_config.yml'.")
        return

    # --- Инициализация Selenium WebDriver ---
    driver = None
    conn = None
    try:
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
        wait = WebDriverWait(driver, 10)

        # --- Подключение к базе данных и проверка/обновление схемы ---
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        check_and_update_db_schema(cursor)
        conn.commit()

        print("\n--- Начинаем парсинг команд для всех турниров ---")

        cursor.execute("SELECT id, name, tournaments_url, type FROM tournaments")
        db_tournaments_data = cursor.fetchall()

        if not db_tournaments_data:
            print("В таблице 'tournaments' нет данных. Парсинг команд невозможен.")
            return
        else:
            print(f"Найдено {len(db_tournaments_data)} турниров для обработки.")

        for tournament_row in db_tournaments_data:
            tournament_id = tournament_row['id']
            tournament_name = tournament_row['name']
            tournament_base_url = tournament_row['tournaments_url']
            
            if not tournament_base_url:
                print(f"  ⚠️ URL турнира '{tournament_name}' (ID: {tournament_id}) отсутствует. Пропускаем.")
                continue

            teams_page_url = f"{tournament_base_url}teams/"
            
            # Список для сбора команд
            teams_to_process = []
            
            print(f"\n--- Обработка турнира: '{tournament_name}' (ID: {tournament_id}) ---")
            
            # --- Этап 1: Попытка парсинга со страницы '/teams/' ---
            try:
                print(f"  Переходим на страницу команд: {teams_page_url}")
                driver.get(teams_page_url)
                
                # Проверяем, есть ли на странице сообщение о том, что команд нет
                try:
                    no_teams_message = driver.find_element(By.CSS_SELECTOR, no_teams_message_selector)
                    if "Команды не найдены" in no_teams_message.text:
                        raise NoSuchElementException("Сообщение 'Команды не найдены' обнаружено.")
                except NoSuchElementException:
                    pass

                wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, team_item_selector)))
                team_elements = driver.find_elements(By.CSS_SELECTOR, team_item_selector)
                
                if team_elements:
                    print(f"  ✅ Найдено {len(team_elements)} элементов команд на странице /teams/. Турнир определен как командный.")
                    cursor.execute("UPDATE tournaments SET type = 'teams' WHERE id = ?", (tournament_id,))
                    conn.commit()
                    
                    for el in team_elements:
                        try:
                            team_name = el.find_element(By.CSS_SELECTOR, team_name_selector).text.strip()
                            team_results_url = urljoin(driver.current_url, el.find_element(By.CSS_SELECTOR, team_link_selector).get_attribute("href"))
                            teams_to_process.append({'name': team_name, 'url': team_results_url})
                        except (NoSuchElementException, StaleElementReferenceException):
                            print("  ⚠️ Пропущена команда из-за ошибки в селекторе.")
                    
            except (TimeoutException, NoSuchElementException):
                print(f"❌ Не удалось найти команды на странице /teams/. Пробуем альтернативный способ...")
                
                # --- Этап 2: Попытка парсинга с главной страницы турнира ---
                try:
                    print(f"  Переходим на главную страницу турнира: {tournament_base_url}")
                    driver.get(tournament_base_url)
                    
                    wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, tournament_results_table_selector)))
                    team_elements = driver.find_elements(By.CSS_SELECTOR, tournament_table_team_link_selector)
                    
                    if team_elements:
                        print(f"  ✅ Найдено {len(team_elements)} элементов команд в турнирной таблице. Турнир определен как командный.")
                        cursor.execute("UPDATE tournaments SET type = 'teams' WHERE id = ?", (tournament_id,))
                        conn.commit()
                        
                        for el in team_elements:
                            try:
                                team_name = el.find_element(By.CSS_SELECTOR, "span.table-item__name").text.strip()
                                team_results_url = urljoin(driver.current_url, el.get_attribute("href"))
                                teams_to_process.append({'name': team_name, 'url': team_results_url})
                            except (NoSuchElementException, StaleElementReferenceException):
                                print("  ⚠️ Пропущена команда из-за ошибки в селекторе таблицы.")
                    else:
                        print(f"❌ Не удалось найти команды даже в турнирной таблице. Турнир определен как индивидуальный.")
                        cursor.execute("UPDATE tournaments SET type = 'individual' WHERE id = ?", (tournament_id,))
                        conn.commit()

                except (TimeoutException, NoSuchElementException):
                    print(f"❌ Не удалось найти турнирную таблицу на главной странице. Турнир определен как индивидуальный.")
                    cursor.execute("UPDATE tournaments SET type = 'individual' WHERE id = ?", (tournament_id,))
                    conn.commit()
                except Exception as e:
                    print(f"❌ Произошла ошибка при парсинге главной страницы турнира: {e}")
            
            except Exception as e:
                print(f"❌ Произошла непредвиденная ошибка при обработке страницы /teams/: {e}")

            # --- Этап 3: Парсинг каждой страницы команды для извлечения tag_url ---
            if teams_to_process:
                for team in teams_to_process:
                    team_name = team['name']
                    team_results_url = team['url']
                    team_tag_url = ""
                    
                    print(f"    ➡️ Переходим на страницу команды '{team_name}': {team_results_url}")
                    try:
                        driver.get(team_results_url)
                        wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, team_tag_link_selector_team_page)))
                        
                        tag_link_element = driver.find_element(By.CSS_SELECTOR, team_tag_link_selector_team_page)
                        raw_tag_href = tag_link_element.get_attribute("href")
                        if raw_tag_href:
                            team_tag_url = urljoin(driver.current_url, raw_tag_href)
                            print(f"    ✅ Найден tag_url для '{team_name}': {team_tag_url}")

                    except (TimeoutException, NoSuchElementException):
                        print(f"    ❌ Не удалось найти tag_url для '{team_name}'. Возможно, его нет на странице.")
                    except Exception as e_url:
                        print(f"    ❌ Ошибка при извлечении tag_url: {e_url}")
                    finally:
                        insert_team(
                            cursor=cursor,
                            name=team_name,
                            url=team_results_url,
                            tag_url=team_tag_url,
                            tournament_id=tournament_id
                        )
                        conn.commit()
                        time.sleep(1)
            else:
                 print(f"  ℹ️ Для турнира '{tournament_name}' не найдено команд для обработки.")

            time.sleep(2)

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
