# parsers/sources/championat/main_structural_data_loader.py

import asyncio
import aiohttp
import sqlite3
import os
import yaml

# Импортируем функцию для инициализации базы данных
from database.prosport_db import init_db

# Импортируем модули для парсинга структурных данных
from parsers.sources.championat.parsers.sports_parser import parse_sports, insert_sport
from parsers.sources.championat.parsers.tournaments_parser import parse_tournaments_for_sport, insert_tournament
from parsers.sources.championat.parsers.teams_parser import parse_teams_for_tournament, insert_team
from parsers.sources.championat.parsers.athletes_parser import parse_athletes_for_team, insert_athlete

# === Загрузка конфигурации ===
def load_config():
    """
    Загружает конфигурацию парсера из sources_config.yml.
    Возвращает словарь конфигурации или None в случае ошибки.
    """
    # Путь к файлу конфигурации относительно текущего скрипта
    # Предполагается, что main_structural_data_loader.py находится в parsers/sources/championat/
    config_path = os.path.join(
        os.path.dirname(__file__),
        'config', 'sources_config.yml'
    )

    if not os.path.exists(config_path):
        print(f"❌ Ошибка: sources_config.yml не найден по пути {config_path}")
        return None

    try:
        with open(config_path, encoding="utf-8") as f:
            all_config = yaml.safe_load(f)
        
        config = all_config.get("championat")
        if not config:
            print("❌ Ошибка: Секция 'championat' не найдена в sources_config.yml")
            return None

        print("✅ Конфигурация championat.com загружена успешно.")
        return config
    except Exception as e:
        print(f"❌ Ошибка загрузки конфигурации: {e}")
        return None

# === Основная функция загрузки всех структурных данных ===
async def load_championat_structure(db_path="database/prosport.db"):
    """
    Основная функция для загрузки всех структурных данных (спорт, турниры, команды, атлеты)
    с Championat.com и сохранения их в БД.
    """
    print("\n--- Запуск загрузки структурных данных Championat.com ---")
    
    # Убедимся, что база данных инициализирована
    init_db(db_path) 
    
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row # Для доступа к колонкам по имени
    cursor = conn.cursor()

    config = load_config()
    if not config:
        conn.close()
        return

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    }

    async with aiohttp.ClientSession(headers=headers) as session:
        # 1. Парсинг и сохранение видов спорта
        print("\n--- Начинаем парсинг видов спорта ---")
        sport_id_map = {} # slug -> id
        sport_url_to_id = {} # url -> id (для удобства)

        sports_list = await parse_sports(session, config["url"], config["parser"])
        if sports_list:
            print(f"  Найдено {len(sports_list)} видов спорта для вставки.")
            for sport in sports_list:
                sport_id = insert_sport(cursor, sport["name"], sport["slug"], sport["url"])
                if sport_id:
                    sport_id_map[sport["slug"]] = sport_id
                    sport_url_to_id[sport["url"]] = sport_id # Сохраняем URL -> ID
            conn.commit()
            print(f"  ✅ Виды спорта сохранены. Всего: {len(sport_id_map)}.")
        else:
            print("  🤷 Виды спорта не найдены или ошибка парсинга.")
            conn.close()
            return # Если нет видов спорта, нет смысла продолжать

        # 2. Парсинг и сохранение турниров
        print("\n--- Начинаем парсинг турниров ---")
        tournament_id_map = {} # url -> id
        if sports_list: # Продолжаем, только если есть виды спорта
            # Создаем список задач для асинхронного парсинга турниров для всех видов спорта
            tournament_tasks = []
            for sport in sports_list:
                sport_id = sport_id_map.get(sport["slug"])
                if sport_id:
                    tournament_tasks.append(
                        parse_tournaments_for_sport(session, sport["url"], config["parser"])
                    )
                else:
                    print(f"  ⚠️ Пропущен турнир для спорта '{sport['name']}' (ID не найден).")
            
            all_tournaments_lists = await asyncio.gather(*tournament_tasks)

            for i, tournaments_list in enumerate(all_tournaments_lists):
                sport = sports_list[i] # Соответствующий спорт
                sport_id = sport_id_map.get(sport["slug"]) # Получаем ID спорта
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
            conn.close()
            return

        # 3. Парсинг и сохранение команд и атлетов (для определенных турниров)
        print("\n--- Начинаем парсинг команд и атлетов ---")
        if tournament_id_map: # Продолжаем, только если есть турниры
            team_tasks = []
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

                # Пропускаем парсинг команд/атлетов для общих категорий
                if sport_slug in ["other", "lifestyle", "cybersport", "bets", "olympicwinter"]: # Добавьте другие общие категории, если нужно
                    print(f"    ℹ️ Пропускаем парсинг команд/атлетов для общей категории: '{sport_slug}' (Турнир: {tournament_url}).")
                    continue
                
                team_tasks.append(
                    parse_teams_for_tournament(session, tournament_url, config["parser"])
                )
            
            all_teams_lists = await asyncio.gather(*team_tasks)

            # Сопоставляем результаты с турнирами и сохраняем
            # Примечание: порядок в all_teams_lists соответствует порядку в team_tasks
            # Нужно быть осторожным, если какие-то задачи были пропущены (continue)
            # Лучше перестроить, чтобы передавать tournament_id в parse_teams_for_tournament
            # Для простоты пока будем считать, что порядок сохраняется и пропуски обрабатываются
            
            # В более надежной реализации можно было бы передавать tournament_id в parse_teams_for_tournament
            # и возвращать его вместе со списком команд.
            
            # Временно, будем итерироваться по исходным турнирам и получать команды
            # Это менее эффективно, но проще для текущей демонстрации
            
            # Пересобираем список турниров, для которых реально были запрошены команды
            actual_tournament_urls = [url for url, tid in tournament_id_map.items()]
            
            team_idx = 0
            for tournament_url, tournament_id in tournament_id_map.items():
                cursor.execute("SELECT sport_id FROM tournaments WHERE id = ?", (tournament_id,))
                result = cursor.fetchone()
                sport_id_for_tournament = result['sport_id'] if result else None
                sport_slug = None
                if sport_id_for_tournament:
                    cursor.execute("SELECT slug FROM sports WHERE id = ?", (sport_id_for_tournament,))
                    sport_slug_result = cursor.fetchone()
                    if sport_slug_result:
                        sport_slug = sport_slug_result['slug']
                
                if sport_slug in ["other", "lifestyle", "cybersport", "bets", "olympicwinter"]:
                    continue # Пропускаем, как и ранее

                # Получаем список команд для текущего турнира из all_teams_lists
                # Это предположение, что порядок в all_teams_lists соответствует итерации
                # В реальном проекте лучше использовать dictionary comprehension для all_teams_lists
                teams_list = all_teams_lists[team_idx]
                team_idx += 1 # Переходим к следующему списку команд

                if teams_list:
                    print(f"    Найдено {len(teams_list)} команд для турнира '{tournament_url}'.")
                    athlete_tasks = []
                    for team in teams_list:
                        team_id = insert_team(cursor, team["name"], team.get("alias"), team.get("url"), team.get("external_id"), tournament_id, team.get("tag_url"))
                        if team_id:
                            # Парсим атлетов, только если у команды есть URL
                            if team.get("url"):
                                athlete_tasks.append(
                                    parse_athletes_for_team(session, team["url"], config["parser"])
                                )
                                # Сохраняем информацию о команде и ее ID для связывания атлетов
                                team['db_id'] = team_id
                                team['url_for_athletes'] = team.get("url")
                            else:
                                print(f"        ℹ️ URL команды '{team['name']}' отсутствует, пропускаем парсинг атлетов.")
                    
                    all_athletes_lists = await asyncio.gather(*athlete_tasks)
                    
                    # Сохраняем атлетов
                    athlete_list_idx = 0
                    for team in teams_list: # Снова итерируемся по командам, чтобы сопоставить атлетов
                        if 'db_id' in team and 'url_for_athletes' in team: # Только для тех, для кого запрашивали атлетов
                            athletes_for_current_team = all_athletes_lists[athlete_list_idx]
                            athlete_list_idx += 1

                            if athletes_for_current_team:
                                print(f"        Найдено {len(athletes_for_current_team)} атлетов для команды '{team['name']}'.")
                                for athlete in athletes_for_current_team:
                                    insert_athlete(cursor, athlete["name"], athlete.get("url"), team['db_id'], athlete.get("external_id"), athlete.get("tag_url"))
                            else:
                                print(f"        🤷 Атлеты для команды '{team['name']}' не найдены или ошибка парсинга.")
                else:
                    print(f"    🤷 Команды для турнира '{tournament_url}' не найдены или ошибка парсинга.")
            conn.commit()
            print(f"  ✅ Команды и атлеты сохранены.")
        else:
            print("  🤷 Нет турниров для парсинга команд и атлетов.")

    conn.close()
    print("\n--- Все структурные данные Championat.com сохранены. ---")

# --- Точка входа в скрипт ---
if __name__ == "__main__":
    db_file = "database/prosport.db"
    asyncio.run(load_championat_structure(db_file))
