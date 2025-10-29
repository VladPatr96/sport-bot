# sport-news-bot (PROSPORT)

Бот и парсеры спортивных новостей + Telegram + простая web-прослойка.

## Quickstart

```bash
python -m venv .venv
.venv\Scripts\activate (Win) / source .venv/bin/activate (Linux/Mac)
pip install -r requirements.txt
```

## Подготовка окружения

```bash
# 1) Генерация БД + промптов/тасков
python db/gen_manifest.py database/prosport.db
python tools/generate_prompt.py
python tools/generate_tasks.py

# 2) Прогон проверок
ruff .
mypy src
pytest -q
```

**Title:** chore: project health — README, gitignore, DB API, CI, AI docs, contracts test

**Summary:**

* Добавлен `README.md` (запуск, структура, команды).
* Обновлён `.gitignore` (SQLite, артефакты, драйверы).
* Создан единый слой БД: `src/prosport/db/api.py`.
* Добавлен тест-контракт БД: `tests/test_db_contract.py`.
* GitHub Actions: `project-health.yml` (ruff/mypy/pytest + артефакты).
* Интегрированы автопромт/таски: `docs/PROMPT_AI.md`, `docs/TASKS_AI.md`.
* `drivers/README.md` с инструкцией (убираем бинарники из репо).

**Checks:**

* [ ] `ruff .` без критичных ошибок
* [ ] `mypy src` без критичных ошибок
* [ ] `pytest -q` проходит локально (при наличии БД)
* [ ] `db/manifest.json` актуален
