from __future__ import annotations
import sqlite3, json
from datetime import datetime
from pathlib import Path

DB = Path("database/prosport.db")
OUT = Path("db/manifest.json")

def dump_schema(conn: sqlite3.Connection):
    tables=[]
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
    return {"generated_at": datetime.utcnow().isoformat()+"Z", "tables": tables}

if __name__ == "__main__":
    conn = sqlite3.connect(str(DB))
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(dump_schema(conn), ensure_ascii=False, indent=2), encoding="utf-8")
    print("✅ db/manifest.json обновлён")
