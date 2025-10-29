# PROSPORT • Автопромт для ИИ
_сгенерировано: 2025-09-30T03:50:36Z_

## Готово (release)
_пусто_

## В работе (work)
- **categorization.py** — `work/categorization.py`
  - цель: -
  - тесты: -
  - deps: -
- **__init__.py** — `src/prosport/__init__.py`
  - цель: -
  - тесты: -
  - deps: -
- **api.py** — `src/prosport/db/api.py`
  - цель: -
  - тесты: -
  - deps: -

## Правила
1) Меняем только перечисленные файлы (сначала из work).
2) ≤ 60 строк diff (кроме tests). Формат: объяснение → патч → проверки.
3) БД: использовать API и поля из db/manifest.json.

Проверка:
uv run ruff .
uv run mypy src
uv run pytest -q

