# database_utils.py

import sqlite3

def get_latest_news_url(cursor):
    """
    Получает URL последней добавленной новости из таблицы 'news',
    отсортированной по времени публикации.
    
    Args:
        cursor: Объект курсора SQLite.
        
    Returns:
        str: URL последней новости или None, если новостей нет.
    """
    try:
        cursor.execute("SELECT url FROM news ORDER BY published_at DESC LIMIT 1")
        result = cursor.fetchone()
        return result[0] if result else None
    except sqlite3.Error as e:
        print(f"❌ Ошибка при получении последней новости из БД: {e}")
        return None
