# ---
# title: Категоризация тегов
# purpose: Нормализация тегов и маппинг к сущностям БД
# owner: Владислав
# status: work
# tests: no
# deps: src/prosport/db/api.py
# ---

from __future__ import annotations

def normalize_tag_name(name: str) -> str:
    return " ".join(name.strip().split()).lower()
