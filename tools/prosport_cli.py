from __future__ import annotations
import json, os, re, shutil, sqlite3
from pathlib import Path
from datetime import datetime
import typer

app = typer.Typer(add_completion=False)
ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"
DB_DEFAULT = ROOT / "database" / "prosport.db"
HEADER_RE = re.compile(r"^[#/\\*\\s-]*---\\s*(.*?)\\s*---", re.S)
RELEASE_DIRS = ["release", "релиз"]
WORK_DIRS = ["work", "в_работе"]

def _read(p: Path) -> str: return p.read_text(encoding="utf-8", errors="ignore")
def _write(p: Path, s: str) -> None:
    p.parent.mkdir(parents=True, exist_ok=True); p.write_text(s, encoding="utf-8")

def parse_meta(text: str) -> dict[str, str]:
    m = HEADER_RE.search(text); 
    if not m: return {}
    meta = {}
    for line in m.group(1).splitlines():
        if ":" in line:
            k, v = line.split(":", 1); meta[k.strip().lower()] = v.strip()
    return meta

def upsert_meta(text: str, updates: dict[str, str]) -> str:
    if HEADER_RE.search(text):
        def repl(m):
            body = m.group(1); meta = {}
            for line in body.splitlines():
                if ":" in line:
                    k, v = line.split(":", 1); meta[k.strip().lower()] = v.strip()
            meta.update({k.lower(): v for k, v in updates.items() if v is not None})
            lines = [f"{k}: {meta[k]}" for k in sorted(meta)]
            return f"---\n" + "\n".join(lines) + "\n---"
        return HEADER_RE.sub(lambda m: repl(m), text, count=1)
    # новый блок (python/sql комментарии)
    block = "\n".join(f"# {line}" for line in ["---", *[f"{k}: {v}" for k, v in updates.items() if v is not None], "---", ""])
    return block + text

def _status_from_path(p: Path) -> str:
    s = str(p).lower()
    if any(d in s for d in RELEASE_DIRS): return "release"
    if any(d in s for d in WORK_DIRS): return "work"
    return "work"

def _collect_modules() -> list[dict]:
    exts = {".py",".ts",".tsx",".js",".sql"}
    items = []
    for folder in RELEASE_DIRS + WORK_DIRS + ["src","webapp"]:
        base = ROOT / folder
        if not base.exists(): continue
        for path in base.rglob("*"):
            if path.is_file() and path.suffix.lower() in exts:
                meta = parse_meta(_read(path))
                items.append({
                    "path": str(path.relative_to(ROOT)).replace("\\","/"),
                    "title": meta.get("title", path.name),
                    "purpose": meta.get("purpose",""),
                    "status": meta.get("status", _status_from_path(path)),
                    "tests": meta.get("tests",""),
                    "deps": meta.get("deps",""),
                })
    return items

@app.command("modules")
def modules(status: str = typer.Option(None, help="work|release")):
    items = _collect_modules()
    if status: items = [i for i in items if i["status"].startswith(status)]
    if not items: typer.echo("Пока пусто."); raise typer.Exit(0)
    for i in items:
        typer.echo(f"- {i['title']} — {i['path']} | status={i['status']} | tests={i['tests']}")

@app.command("status")
def set_status(file: str, status: str, tests: str = typer.Option(None), move: bool = typer.Option(False)):
    p = ROOT / file
    if not p.exists(): typer.echo(f"❌ Нет файла: {file}"); raise typer.Exit(1)
    new = upsert_meta(_read(p), {"status": status, "tests": tests} if tests else {"status": status})
    _write(p, new); typer.echo(f"✅ Обновлён фронт-маттер: {file} -> status={status}")
    if move:
        target = ROOT / ("release" if status.startswith("rel") else "work")
        target.mkdir(parents=True, exist_ok=True)
        dst = target / p.name
        if p.resolve() != dst.resolve():
            shutil.move(str(p), str(dst))
            typer.echo(f"🚚 Перемещён → {dst.relative_to(ROOT)}")

@app.command("db-manifest")
def db_manifest(db_path: str = str(DB_DEFAULT), out: str = "db/manifest.json"):
    db = Path(db_path)
    if not db.exists(): typer.echo(f"❌ Нет БД: {db}"); raise typer.Exit(1)
    conn = sqlite3.connect(str(db)); tables = []
    for (name,) in conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';"):
        cols = conn.execute(f"PRAGMA table_info({name})").fetchall()
        fks  = conn.execute(f"PRAGMA foreign_key_list({name})").fetchall()
        cnt  = conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
        tables.append({
            "table": name,
            "columns": [{"name": c[1], "type": c[2], "notnull": bool(c[3]), "pk": bool(c[5])} for c in cols],
            "foreign_keys": [{"table": fk[2], "from": fk[3], "to": fk[4]} for fk in fks],
            "rows": cnt,
        })
    manifest = {"generated_at": datetime.utcnow().isoformat()+"Z", "tables": tables}
    outp = ROOT / out; _write(outp, json.dumps(manifest, ensure_ascii=False, indent=2))
    typer.echo(f"✅ {out} обновлён")

@app.command("prompt")
def prompt():
    items = _collect_modules()
    rel = [i for i in items if i["status"].startswith("rel")]
    wip = [i for i in items if i["status"].startswith("work")]
    def block(rows): 
        if not rows: return "_пусто_"
        out=[]; 
        for i in rows:
            out.append(f"- **{i['title']}** — `{i['path']}`\n  - цель: {i['purpose'] or '-'}\n  - тесты: {i['tests'] or '-'}\n  - deps: {i['deps'] or '-'}")
        return "\n".join(out)
    content = f"""# PROSPORT • Автопромт для ИИ
_сгенерировано: {datetime.utcnow().isoformat(timespec="seconds")}Z_

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
    out = DOCS / "PROMPT_AI.md"; _write(out, content)
    typer.echo(f"✅ docs/PROMPT_AI.md обновлён")

@app.command("tasks")
def tasks():
    items = _collect_modules()
    wip = [i for i in items if i["status"].startswith("work")]
    changed = os.popen("git status --porcelain").read().strip().splitlines()
    changed = [c.split(maxsplit=1)[-1] for c in changed if c]
    def bullet(i): return f"- **{i['title']}** (`{i['path']}`) — цель: {i['purpose'] or '-'}; тесты: {i['tests'] or '-'}"
    content = f"""# PROSPORT • ЗАДАЧИ ДЛЯ ИИ
_сгенерировано: {datetime.utcnow().isoformat(timespec="seconds")}Z_

## Изменённые файлы
{chr(10).join(f"- `{p}`" for p in changed) or "- (нет)"}

## Модули в работе
{chr(10).join(bullet(i) for i in wip) or "_пусто_"}

## Что сделать
1) Довести изменённые файлы до зелёных тестов.
2) Для каждого модуля из work — добавить 1 тест и довести до release.
3) Ограничение: ≤ 60 строк diff (кроме tests). Формат: объяснение → патч → проверки.
"""
    out = DOCS / "TASKS_AI.md"; _write(out, content)
    typer.echo(f"✅ docs/TASKS_AI.md обновлён")

@app.command("all")
def all_():
    db_manifest(str(DB_DEFAULT))
    prompt()
    tasks()
    typer.echo("🎯 Готово.")

if __name__ == "__main__":
    app()
