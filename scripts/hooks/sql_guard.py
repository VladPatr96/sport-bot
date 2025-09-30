#!/usr/bin/env python3
import json, re, subprocess, sys, pathlib
manifest = json.loads(pathlib.Path("db/manifest.json").read_text(encoding="utf-8")) if pathlib.Path("db/manifest.json").exists() else {"tables":[]}
tables = {t["table"]: {c["name"] for c in t.get("columns",[])} for t in manifest.get("tables",[])}
changed = subprocess.check_output(["git","diff","--cached","--name-only"]).decode().splitlines()
changed = [p for p in changed if p.endswith((".py",".sql"))]
sql_re = re.compile(r"\b(SELECT|INSERT|UPDATE|DELETE|FROM|JOIN)\b", re.I)
name_re = re.compile(r"\b([A-Za-z_][A-Za-z0-9_]*)\b")
viol=[]
for path in changed:
  txt = pathlib.Path(path).read_text(encoding="utf-8", errors="ignore")
  if "execute(" in txt or path.endswith(".sql"):
    if not sql_re.search(txt): continue
    for w in set(name_re.findall(txt)):
      if w in {"SELECT","INSERT","UPDATE","DELETE","FROM","JOIN","ON","WHERE","AND","OR","AS","IN","NOT","NULL","LEFT","INNER","BY","GROUP","ORDER","DESC","ASC","LIMIT","VALUES","SET"}:
        continue
      if w in tables: continue
      if "." in w or w.isupper(): continue
      viol.append((path,w))
if viol:
  print("❌ Возможные несуществующие имена БД:")
  for p,w in viol[:50]: print(f" - {p}: «{w}»")
  print("👉 Проверь db/manifest.json или добавь миграцию.")
  sys.exit(1)
