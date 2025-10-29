
from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Dict, List

import yaml

from categorizer.normalize import normalize_token
from db.utils import get_conn

LOGGER = logging.getLogger(__name__)
BASE_DIR = Path(__file__).resolve().parents[1]
DEFAULT_SEED = BASE_DIR / "mappings" / "aliases_seed.yml"
ALLOWED_TYPES = {"sport", "tournament", "team", "player"}


def ensure_entity(conn, *, entity_type: str, canonical: str, entity_id_hint: int | None) -> int:
    cur = conn.cursor()
    if entity_id_hint:
        cur.execute('SELECT id FROM entities WHERE id = ?', (entity_id_hint,))
        row = cur.fetchone()
        if row:
            return entity_id_hint
        LOGGER.warning('entities.id=%s not found; inserting new row', entity_id_hint)

    canonical_norm = normalize_token(canonical)
    if not canonical_norm:
        canonical_norm = canonical.strip()

    cur.execute('SELECT id FROM entities WHERE name = ? AND type = ?', (canonical_norm, entity_type))
    row = cur.fetchone()
    if row:
        return row[0]

    cur.execute(
        'INSERT INTO entities (name, type, lang) VALUES (?, ?, ?)',
        (canonical_norm, entity_type, 'ru'),
    )
    return cur.lastrowid


def apply_aliases(conn, aliases: Dict[str, List[Dict[str, object]]]) -> None:
    cur = conn.cursor()
    for entity_type, entries in aliases.items():
        if entity_type not in ALLOWED_TYPES:
            LOGGER.warning('Unknown entity type in seed: %s', entity_type)
            continue
        for entry in entries or []:
            canonical = str(entry.get('canonical') or '').strip()
            if not canonical:
                continue
            alias_list = entry.get('aliases') or []
            alias_list = [str(a).strip() for a in alias_list if str(a).strip()]
            if not alias_list:
                continue
            entity_id = ensure_entity(
                conn,
                entity_type=entity_type,
                canonical=canonical,
                entity_id_hint=entry.get('entity_id'),
            )
            for alias in alias_list:
                alias_norm = normalize_token(alias)
                if not alias_norm:
                    continue
                cur.execute(
                    'INSERT OR IGNORE INTO entity_aliases (alias, alias_normalized, entity_type, entity_id, source, lang, created_at) VALUES (?, ?, ?, ?, ?, ?, datetime("now"))',
                    (alias, alias_norm, entity_type, entity_id, 'seed', 'ru'),
                )
    conn.commit()


def main() -> None:
    parser = argparse.ArgumentParser(description='Apply alias seed YAML to entity_aliases table')
    parser.add_argument('--seed', type=Path, default=DEFAULT_SEED)
    parser.add_argument('--verbose', action='store_true')
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

    if not args.seed.exists():
        raise FileNotFoundError(f'Seed file not found: {args.seed}')

    data = yaml.safe_load(args.seed.read_text(encoding='utf-8')) or {}
    aliases = data.get('aliases') or {}

    conn = get_conn()
    try:
        apply_aliases(conn, aliases)
    finally:
        conn.close()


if __name__ == '__main__':
    main()
