# sport-news-bot (PROSPORT)

Бот и парсеры спортивных новостей + Telegram + простая web-прослойка.

## Быстрый старт

```bash
# 1) создать и активировать venv (Windows PowerShell)
python -m venv .venv
.venv\Scripts\Activate.ps1

# 2) зависимости
pip install -r requirements.txt

# 3) паспорт БД + автопромт/таски
python db/gen_manifest.py database/prosport.db
python tools/generate_prompt.py
python tools/generate_tasks.py

# 4) проверки качества
ruff .
mypy src
pytest -q
```
