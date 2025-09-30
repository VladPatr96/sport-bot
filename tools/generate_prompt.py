from __future__ import annotations
import re, pathlib
from datetime import datetime

ROOT = pathlib.Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"
HEADER_RE = re.compile(r"^[#/\\*\\s-]*---\\s*(.*?)\\s*---", re.S)

def parse_meta(text: str) -> dict[str,str]:
    m = HEADER_RE.search(text); 
    if not m: return {}
    meta={}
    for line in m.group(1).splitlines():
        if ":" in line:
            k,v = line.split(":",1); meta[k.strip().lower()]=v.strip()
    return meta

def collect():
    items=[]
    for folder in ["release","релиз","work","в_работе","src","webapp"]:
        p = ROOT / folder
        if not p.exists(): continue
        for f in p.rglob("*"):
            if f.is_file() and f.suffix.lower() in {".py",".ts",".tsx",".js",".sql"}:
                meta = parse_meta(f.read_text(encoding="utf-8", errors="ignore"))
                status = meta.get("status","work" if "work" in str(f) or "в_работе" in str(f) else "release" if "release" in str(f) or "релиз" in str(f) else "work")
                items.append({"path": str(f.relative_to(ROOT)).replace("\\","/"),
                              "title": meta.get("title", f.name),
                              "purpose": meta.get("purpose",""),
                              "status": status,
                              "tests": meta.get("tests",""),
                              "deps": meta.get("deps","")})
    return items

def render(items):
    rel = [i for i in items if i["status"].startswith("rel")]
    wip = [i for i in items if i["status"].startswith("work")]
    def block(rows):
        if not rows: return "_пусто_"
        out=[]
        for i in rows:
            out.append(f"- **{i['title']}** — `{i['path']}`\n  - цель: {i['purpose'] or '-'}\n  - тесты: {i['tests'] or '-'}\n  - deps: {i['deps'] or '-'}")
        return "\n".join(out)
    now = datetime.utcnow().isoformat(timespec="seconds")+"Z"
    return f"""# PROSPORT • Автопромт для ИИ
_сгенерировано: {now}_

## Готово (release)
{block(rel)}

## В работе (work)
{block(wip)}

## Правила
1) Меняем только перечисленные файлы (сначала из work).
2) ≤ 60 строк diff (кроме tests). Формат: объяснение → патч → проверки.
3) БД: использовать API и поля из db/manifest.json.

Проверка:
uv run ruff .
uv run mypy src
uv run pytest -q

"""

if __name__ == "__main__":
    DOCS.mkdir(exist_ok=True)
    (DOCS/"PROMPT_AI.md").write_text(render(collect()), encoding="utf-8")
    print("✅ docs/PROMPT_AI.md обновлён")

