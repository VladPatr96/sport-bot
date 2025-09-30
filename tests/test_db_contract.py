from __future__ import annotations
import json, sqlite3, pathlib, re

ROOT = pathlib.Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "database" / "prosport.db"
MANIFEST = ROOT / "db" / "manifest.json"

def test_manifest_matches_db():
    if not MANIFEST.exists() or not DB_PATH.exists():
        # Без реальной БД в CI пропускаем
        return
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    conn = sqlite3.connect(str(DB_PATH))
    tables = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';"
    )}
    assert tables == {t["table"] for t in manifest["tables"]}

def test_no_raw_sql_outside_api():
    # Разрешаем сырой SQL только в src/prosport/db/api.py
    files = list((ROOT / "src").rglob("*.py"))
    offenders = []
    for f in files:
        if "src/prosport/db/api.py" in str(f).replace("\\","/"):
            continue
        txt = f.read_text(encoding="utf-8", errors="ignore")
        if re.search(r"\bexecute\(", txt) or "SELECT " in txt:
            offenders.append(str(f.relative_to(ROOT)))
    assert not offenders, f"Raw SQL detected outside DB API: {offenders}"
