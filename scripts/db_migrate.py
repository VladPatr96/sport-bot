from __future__ import annotations
import logging
import sqlite3
from pathlib import Path

from db.utils import get_conn

logger = logging.getLogger(__name__)

MIGRATIONS_DIR = Path(__file__).resolve().parents[1] / 'db' / 'migrations'


def _iter_statements(sql: str):
    statement = ''
    for line in sql.splitlines():
        statement += line + '\n'
        if sqlite3.complete_statement(statement):
            cleaned = [ln for ln in statement.splitlines() if ln.strip() and not ln.strip().startswith('--')]
            if cleaned:
                yield '\n'.join(cleaned)
            statement = ''
    cleaned_tail = [ln for ln in statement.splitlines() if ln.strip() and not ln.strip().startswith('--')]
    if cleaned_tail:
        yield '\n'.join(cleaned_tail)


def _extract_added_column(statement: str) -> str | None:
    upper = statement.upper()
    if upper.startswith('ALTER TABLE ENTITY_ALIASES') and 'ADD COLUMN' in upper:
        after = statement.split('ADD COLUMN', 1)[1].strip()
        column = after.split()[0]
        return column.strip('"`[]')
    return None


def _log_success(statement: str) -> None:
    column = _extract_added_column(statement)
    if column:
        logger.info('Added column entity_aliases.%s', column)
        return
    upper = statement.upper()
    if 'CREATE UNIQUE INDEX IF NOT EXISTS IDX_ENTITY_ALIASES_NORM' in upper:
        logger.info('Ensured unique index idx_entity_aliases_norm on (alias_normalized, entity_type)')


def apply_migrations() -> int:
    if not MIGRATIONS_DIR.exists():
        logger.info('No migrations directory found: %s', MIGRATIONS_DIR)
        return 0

    migrations = sorted(p for p in MIGRATIONS_DIR.glob('*.sql'))
    if not migrations:
        logger.info('No migration files detected in %s', MIGRATIONS_DIR)
        return 0

    conn = get_conn()
    try:
        applied = 0
        for path in migrations:
            sql = path.read_text(encoding='utf-8')
            if not sql.strip():
                logger.info('Skipping empty migration: %s', path.name)
                continue
            logger.info('Applying migration: %s', path.name)
            for statement in _iter_statements(sql):
                try:
                    conn.execute(statement)
                except sqlite3.OperationalError as exc:
                    message = str(exc).lower()
                    column = _extract_added_column(statement)
                    if 'duplicate column name' in message and column:
                        logger.info('Column entity_aliases.%s already exists — skipping', column)
                        continue
                    if 'no such column' in message and 'alias_normalized' in message:
                        logger.warning(
                            'Skipping index for entity_aliases due to missing columns (statement: %s)',
                            statement.split('\n', 1)[0][:80],
                        )
                        continue
                    raise
                else:
                    _log_success(statement)
            applied += 1
        if conn.in_transaction:
            conn.commit()
        logger.info('Migrations completed (count=%s)', applied)
        return applied
    finally:
        conn.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    apply_migrations()


if __name__ == '__main__':
    main()
