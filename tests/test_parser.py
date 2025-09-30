import asyncio
import yaml
import os
import pandas as pd # Для удобного вывода списка новостей
import sys

# Добавляем корневую директорию проекта в sys.path, чтобы импорты работали
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))

# Импортируем наш новый парсер на основе Selenium
# Убедитесь, что champ_parser.py находится в parsers/sources/championat/parsers/
from parsers.sources.championat.parsers.champ_parser import ChampParserSelenium # Изменено на ChampParserSelenium

async def main():
    """
    Основная асинхронная функция для тестирования парсера.
    """
    # === 1. Загрузка конфигурации ===
    # Путь к файлу конфигурации (относительно корневой директории проекта)
    config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'parsers', 'sources', 'championat', 'config', 'sources_config.yml'))
    
    if not os.path.exists(config_path):
        print(f"Ошибка: sources_config.yml не найден по пути {config_path}")
        return

    try:
        with open(config_path, encoding="utf-8") as f:
            all_config = yaml.safe_load(f)
        
        # Получаем конфигурацию для Championat
        config = all_config.get("championat")
        if not config:
            print("Ошибка: Секция 'championat' не найдена в sources_config.yml")
            return

        print("✅ Конфигурация championat.com загружена успешно.")
    except Exception as e:
        print(f"❌ Ошибка загрузки конфигурации: {e}")
        return

    # === 2. Инициализация и использование парсера ===
    # Используем асинхронный контекстный менеджер `async with` для ChampParserSelenium
    async with ChampParserSelenium(config) as parser: # Инициализируем ChampParserSelenium
        if parser.driver is None: # Проверяем, удалось ли инициализировать драйвер
            print("❌ WebDriver не был успешно инициализирован. Проверьте путь к драйверу и его установку.")
            return

        print("\n--- Тестирование fetch_list ---")
        news_list = await parser.fetch_list()

        if news_list:
            print(f"Найдено новостей в списке: {len(news_list)}")
            # Создаем DataFrame для удобного вывода
            df_display = pd.DataFrame(news_list)
            # Выбираем только нужные колонки для отображения
            df_display = df_display[['title', 'published', 'tag', 'url']]
            print("\nПервые 5 новостей из списка:")
            print(df_display.head(5).to_string(index=False)) # index=False для скрытия индекса pandas
        else:
            print("Список новостей пуст.")
            return # Если список пуст, нет смысла парсить статьи

        print("\n--- Тестирование fetch_article (первая новость из списка) ---")
        first_article_meta = news_list[0]
        parsed_article = await parser.fetch_article(first_article_meta)

        if parsed_article:
            print("\nДетали распарсенной статьи:")
            print(f"Заголовок: {parsed_article.get('title', 'N/A')}")
            print(f"URL: {parsed_article.get('url', 'N/A')}")
            print(f"Опубликовано: {parsed_article.get('published', 'N/A')}")
            print(f"Краткое содержание: {parsed_article.get('summary', 'N/A')[:200]}...")
            print("Теги:")
            for tag in parsed_article.get('tags', []):
                print(f"  - Имя: {tag.get('name', 'N/A')}, Ссылка: {tag.get('url', 'N/A')}")
            print(f"Количество изображений: {len(parsed_article.get('images', []))}")
            print(f"Количество видео: {len(parsed_article.get('videos', []))}")
            print(f"Начало тела статьи: {parsed_article.get('body', 'N/A')[:500]}...") # Выводим начало тела статьи
        else:
            print("Не удалось распарсить первую статью.")

if __name__ == "__main__":
    asyncio.run(main())

