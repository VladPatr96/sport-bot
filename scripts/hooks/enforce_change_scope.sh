#!/usr/bin/env bash
set -euo pipefail
SCOPE_FILE=".change_scope"
if [[ ! -f "$SCOPE_FILE" ]]; then
  echo "❌ Нет $SCOPE_FILE. Добавь разрешённые пути."; exit 1
fi
CHANGED=$(git diff --cached --name-only)
[[ -z "$CHANGED" ]] && exit 0
mapfile -t ALLOWED < <(grep -v '^\s*$' "$SCOPE_FILE" | grep -v '^\s*#')
violations=0
while IFS= read -r f; do
  ok=1
  for allow in "${ALLOWED[@]}"; do
    if [[ "$f" == "$allow" || "$f" == $allow/* ]]; then ok=0; break; fi
  done
  if [[ $ok -eq 1 ]]; then echo "❌ Запрещено изменять: $f (не в .change_scope)"; violations=1; fi
done <<< "$CHANGED"
if [[ $violations -eq 1 ]]; then echo "👉 Обнови .change_scope или убери лишнее."; exit 1; fi
