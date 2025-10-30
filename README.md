# sport-news-bot (PROSPORT)

Автоматизированная система для парсинга, категоризации и публикации спортивных новостей в Telegram.

## Возможности

- **Парсинг**: Инкрементальная синхронизация новостей с Championat.com (Selenium)
- **Категоризация**: NLP-обработка через spaCy + pymorphy3, управление алиасами
- **Кластеризация**: Умная группировка связанных новостей в "истории"
- **Дедупликация**: Fingerprinting + антидубликаты (Jaccard similarity)
- **Публикация**: Telegram-бот с rate limiting, тихие часы, очередь
- **Веб-интерфейс**: FastAPI приложение для управления публикациями
- **Мониторинг**: Сбор метрик, алерты, дайджесты

## 🚀 Быстрый старт с Docker (рекомендуется)

### 1. Подготовка

```bash
# Клонировать репозиторий
git clone https://github.com/VladPatr96/sport-bot.git
cd sport-bot

# Создать .env файл из шаблона
cp .env.example .env

# Настроить переменные окружения
nano .env  # Укажи TG_BOT_TOKEN и TG_CHANNEL_ID
```

### 2. Запуск всех сервисов

```bash
# Запустить бота + веб-приложение
docker-compose up -d

# Проверить статус
docker-compose ps

# Посмотреть логи
docker-compose logs -f bot
docker-compose logs -f webapp
```

### 3. Доступ к веб-интерфейсу

Открой в браузере: http://localhost:8000

### 4. Парсинг новостей (вручную)

```bash
# Запустить парсер один раз
docker-compose run --rm parser

# Или запустить кластеризацию
docker-compose run --rm parser python cluster/build.py
```

### 5. Остановка

```bash
# Остановить все сервисы
docker-compose down

# Удалить контейнеры и volumes
docker-compose down -v
```

## 🛠️ Локальная разработка (без Docker)

### 1. Установка зависимостей

```bash
# Создать виртуальное окружение
python -m venv .venv
.venv\Scripts\activate  # Windows
source .venv/bin/activate  # Linux/Mac

# Установить зависимости
pip install -r requirements.txt
pip install -r requirements-dev.txt  # Для разработки

# Загрузить spaCy модели
python -m spacy download ru_core_news_sm
python -m spacy download en_core_web_sm
```

### 2. Настройка окружения

```bash
# Скопировать .env.example → .env
cp .env.example .env

# Применить миграции БД
python scripts/db_migrate.py

# Сгенерировать манифест БД
python db/gen_manifest.py database/prosport.db
```

### 3. Запуск компонентов

```bash
# Парсер новостей
python scripts/sync_champ_news.py

# Кластеризация
python cluster/build.py

# Telegram-бот (планировщик)
python bot/scheduler.py --loop

# Веб-приложение
python scripts/run_webapp_bot.py --port 8000

# Мониторинг
python scripts/monitor.py --loop
```

## 🧪 Проверки качества кода

```bash
# Линтер (автоисправление)
ruff check . --fix

# Проверка типов
mypy src

# Тесты
pytest -q

# Все проверки сразу
ruff . && mypy src && pytest -q
```

## 📊 Структура проекта

```text
sport-news-bot/
├── bot/                  # Telegram-бот, планировщик, рендеринг сообщений
├── categorizer/          # NLP категоризация, алиасы, нормализация тегов
├── cluster/              # Кластеризация новостей, fingerprinting, дедупликация
├── webapp/               # FastAPI веб-приложение, дайджесты, мониторинг
├── parsers/              # Парсеры новостей (Championat + структура для других)
├── scripts/              # Утилитарные скрипты (синхронизация, миграции, backfill)
├── db/                   # Миграции БД, утилиты, manifest.json
├── src/prosport/         # Core API-слой для работы с БД
├── mappings/             # YAML-словари канонических алиасов
├── docs/                 # Документация проекта
├── tests/                # Тесты (pytest)
├── database/             # SQLite БД (не в git, монтируется в Docker)
├── Dockerfile            # Docker образ приложения
├── docker-compose.yml    # Оркестрация сервисов
└── .env.example          # Шаблон конфигурации (без секретов)
```

## 🗄️ База данных

**SQLite** с 18 таблицами:

- **Сущности**: sports, tournaments, teams, athletes
- **Новости**: news, tags, news_article_tags (M2M)
- **Категоризация**: entity_aliases, news_entity_assignments
- **Кластеризация**: stories, story_articles, content_fingerprints
- **Публикация**: publish_queue, publish_map, publish_edits
- **Дайджесты**: digests, digest_items
- **Мониторинг**: monitor_logs

Применение миграций: `python scripts/db_migrate.py`

## 🔧 Полезные команды

```bash
# Парсинг новостей по конкретной сущности
python scripts/fetch_entity_news_on_demand.py --entity-id 123 --entity-type team

# Генерация и отправка дайджеста
python scripts/digest.py --limit 25

# Обогащение новостей алиасами
python scripts/alias_backfill.py

# Генерация fingerprints для старых новостей
python scripts/backfill_fingerprints.py

# Обновление заголовков историй
python cluster/refresh_titles.py
```

## 🐳 Docker команды

```bash
# Пересобрать образы после изменений
docker-compose build

# Запустить только веб-приложение
docker-compose up webapp

# Запустить мониторинг (профиль)
docker-compose --profile monitoring up

# Посмотреть логи конкретного сервиса
docker-compose logs -f bot --tail=100

# Выполнить команду внутри контейнера
docker-compose exec bot python scripts/monitor.py

# Очистить все (контейнеры + volumes + образы)
docker-compose down -v --rmi all
```

## 📝 CI/CD

GitHub Actions workflow [.github/workflows/project-health.yml](.github/workflows/project-health.yml):

- ✅ Ruff lint
- ✅ MyPy type checking
- ✅ Pytest tests
- 📦 Artifacts: `db/manifest.json`, `docs/PROMPT_AI.md`, `docs/TASKS_AI.md`

## 📚 Документация

- [CHANGELOG.md](docs/CHANGELOG.md) — история изменений
- [ROADMAP.md](docs/ROADMAP.md) — план развития
- [DB_SCHEMA.md](docs/DB_SCHEMA.md) — схема базы данных
- [PROJECT_TREE.md](docs/PROJECT_TREE.md) — структура файлов

## 🤝 Вклад в проект

1. Fork репозиторий
2. Создай ветку: `git checkout -b feature/amazing-feature`
3. Закоммить изменения: `git commit -m 'feat: add amazing feature'`
4. Запуш в ветку: `git push origin feature/amazing-feature`
5. Создай Pull Request

## 📄 Лицензия

Этот проект является частным. Все права защищены.
