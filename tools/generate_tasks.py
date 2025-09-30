from __future__ import annotations
import os, pathlib
from datetime import datetime
from generate_prompt import collect  # тот же каталог tools/

ROOT = pathlib.Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"

def render():
    items = collect()
    wip = [i for i in items if i["status"].startswith("work")]
    changed = os.popen("git status --porcelain").read().strip().splitlines()
    changed = [c.split(maxsplit=1)[-1] for c in changed if c]
    def bullet(i): return f"- **{i['title']}** (`{i['path']}`) — цель: {i['purpose'] or '-'}; тесты: {i['tests'] or '-'}"
    now = datetime.utcnow().isoformat(timespec="seconds")+"Z"
    return f"""# PROSPORT • ЗАДАЧИ ДЛЯ ИИ (автоген)
_сгенерировано: {now}_

## Изменённые файлы
{chr(10).join(f"- `{p}`" for p in changed) or "- (нет)"}

## Модули в работе
{chr(10).join(bullet(i) for i in wip) or "_пусто_"}

## Что сделать
1) Довести изменённые файлы до зелёных тестов.
2) Для каждого work-модуля — добавить 1 тест и довести до release.
3) Ограничение: ≤ 60 строк diff (кроме tests). Формат: объяснение → патч → проверки.
"""

if __name__ == "__main__":
    DOCS.mkdir(exist_ok=True)
    (DOCS/"TASKS_AI.md").write_text(render(), encoding="utf-8")
    print("✅ docs/TASKS_AI.md обновлён")
