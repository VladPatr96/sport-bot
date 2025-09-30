# database/prosport_db.py

import sqlite3
import os

def init_db(db_path="database/prosport.db"):
    """
    Инициализирует базу данных SQLite, создавая необходимые таблицы, если они не существуют.
    """
    # Создаем директорию для БД, если она не существует
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Таблица для видов спорта
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            slug TEXT NOT NULL UNIQUE,
            url TEXT NOT NULL UNIQUE
        )
    """)

    # Таблица для турниров
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS tournaments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,          -- Основное название турнира (например, "РПЛ")
            url TEXT,             -- URL тега в новостях (например, "..._russiapl.html")
            sport_id INTEGER NOT NULL,
            season TEXT,                 -- Сезон (например, "2025/2026")
            tournaments_url TEXT UNIQUE, -- URL страницы данных турнира (например, ".../tournament/6594/")
            FOREIGN KEY (sport_id) REFERENCES sports(id)
        )
    """)

    # Таблица для команд
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS teams (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            alias TEXT,
            url TEXT UNIQUE,
            external_id TEXT,
            tournament_id INTEGER,
            tag_url TEXT,
            FOREIGN KEY (tournament_id) REFERENCES tournaments(id)
        )
    """)

    # Таблица для атлетов
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS athletes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            url TEXT UNIQUE,
            team_id INTEGER,
            external_id TEXT,
            tag_url TEXT,
            FOREIGN KEY (team_id) REFERENCES teams(id)
        )
    """)
    
    # Таблица для новостей
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS news (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            url TEXT UNIQUE NOT NULL,
            content TEXT,
            source TEXT,
            published_at TEXT,
            lang TEXT,
            is_published BOOLEAN DEFAULT 0,
            image_url TEXT,
            image_urls TEXT,
            video_urls TEXT,
            created_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%S', 'now', 'utc'))
        )
    """)

    # Таблица для тегов
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS tags (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            url TEXT UNIQUE,
            type TEXT,
            entity_id INTEGER
        )
    """)

    # Таблица для связей новостей с тегами
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS news_article_tags (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            news_id INTEGER NOT NULL,
            tag_id INTEGER NOT NULL,
            FOREIGN KEY(news_id) REFERENCES news(id),
            FOREIGN KEY(tag_id) REFERENCES tags(id),
            UNIQUE(news_id, tag_id)
        )
    """)

    # Таблица для сущностей (для NER)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS entities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            type TEXT,
            lang TEXT
        )
    """)

    # Таблица для связей новостей с сущностями
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS news_entities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            news_id INTEGER NOT NULL,
            entity_id INTEGER NOT NULL,
            confidence REAL DEFAULT 1.0,
            FOREIGN KEY(news_id) REFERENCES news(id),
            FOREIGN KEY(entity_id) REFERENCES entities(id)
        )
    """)

    # Таблица для связей между сущностями
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS entity_relations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            parent_id INTEGER NOT NULL,
            child_id INTEGER NOT NULL,
            relation TEXT DEFAULT 'member_of',
            FOREIGN KEY(parent_id) REFERENCES entities(id),
            FOREIGN KEY(child_id) REFERENCES entities(id)
        )
    """)

    # Таблица для псевдонимов сущностей
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS entity_aliases (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entity_id INTEGER NOT NULL,
            alias TEXT NOT NULL,
            lang TEXT,
            FOREIGN KEY(entity_id) REFERENCES entities(id)
        )
    """)

    # Таблица для отслеживания неудачных парсингов команд/турниров
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS failed_team_tournaments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            team_name TEXT,
            tournament_name TEXT,
            team_url TEXT,
            error_message TEXT,
            timestamp TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%S', 'now', 'utc'))
        )
    """)

    # Таблица для пользователей Telegram бота
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER NOT NULL UNIQUE,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            created_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%S', 'now', 'utc'))
        )
    """)

    # Таблица для просмотров новостей пользователями
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS user_views (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            news_id INTEGER NOT NULL,
            viewed_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%S', 'now', 'utc')),
            FOREIGN KEY(user_id) REFERENCES users(id),
            FOREIGN KEY(news_id) REFERENCES news(id),
            UNIQUE(user_id, news_id)
        )
    """)

    # Таблица для интересов пользователей
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS user_interests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            entity_id INTEGER NOT NULL, -- ID сущности (спорт, команда, атлет, турнир)
            interest_level INTEGER DEFAULT 1, -- Уровень интереса, если нужно
            FOREIGN KEY(user_id) REFERENCES users(id),
            FOREIGN KEY(entity_id) REFERENCES entities(id),
            UNIQUE(user_id, entity_id)
        )
    """)

    conn.commit()
    conn.close()
    print("✅ База данных prosport.db инициализирована/обновлена.")

if __name__ == "__main__":
    init_db("database/prosport.db") # Указываем путь к БД
