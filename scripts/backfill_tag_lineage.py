import argparse, sqlite3, sys
from contextlib import closing

def col_exists(conn, table, col):
    return any(r[1] == col for r in conn.execute(f"PRAGMA table_info({table});"))

def ensure_columns(conn, verbose=False):
    adds = []
    for c in ("sport_id", "tournament_id", "team_id", "athlete_id"):
        if not col_exists(conn, "tags", c):
            if verbose:
                print(f"[migrate] ALTER TABLE tags ADD COLUMN {c} INTEGER")
            adds.append(f"ALTER TABLE tags ADD COLUMN {c} INTEGER;")
    for sql in adds:
        conn.execute(sql)

INDEXES = [
    "PRAGMA foreign_keys = ON;",
    "CREATE INDEX IF NOT EXISTS idx_tags_url            ON tags(url);",
    "CREATE INDEX IF NOT EXISTS idx_tags_sport_id       ON tags(sport_id);",
    "CREATE INDEX IF NOT EXISTS idx_tags_tournament_id  ON tags(tournament_id);",
    "CREATE INDEX IF NOT EXISTS idx_tags_team_id        ON tags(team_id);",
    "CREATE INDEX IF NOT EXISTS idx_tags_athlete_id     ON tags(athlete_id);",
]

def step(conn, sql, label, verbose=False):
    cur = conn.execute(sql)
    rc = cur.rowcount if cur.rowcount is not None else 0
    if verbose:
        print(f"[update] {label}: ~{rc}")
    return rc

def main():
    ap = argparse.ArgumentParser(description="Backfill tags lineage using existing FKs (no M2M)")
    ap.add_argument("--db", required=True)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--verbose", "-v", action="store_true")
    ap.add_argument("--normalize-urls", action="store_true", default=True,
                    help="Сгладить различия в URL: схема/www/utm/хвостовой слэш")
    args = ap.parse_args()

    with closing(sqlite3.connect(args.db)) as conn:
        conn.isolation_level = None
        conn.execute("PRAGMA foreign_keys = ON;")
        sp = "sp_tags_fk"
        conn.execute(f"SAVEPOINT {sp};")
        try:
            # Убедимся, что есть базовые таблицы
            for t in ("tags", "sports", "tournaments", "teams", "athletes"):
                conn.execute(f"SELECT 1 FROM {t} LIMIT 1;")

            # Добавим колонки при необходимости
            ensure_columns(conn, verbose=args.verbose)
            for sql in INDEXES:
                if args.verbose:
                    print(f"[index] {sql}")
                conn.execute(sql)

            # Нормализация URL на лету (SQLite выражения)
            def norm(alias, col):
                return (
                    f"lower(replace(replace(replace(substr({alias}.{col}, instr({alias}.{col}, '://')+3), 'www.', ''), '?', ''), ' ', ''))"
                )

            if args.normalize_urls:
                turl = norm("tags", "url")
                s_url = norm("s", "url")
                tr_url = norm("tr", "url")
                tm_tu = norm("tm", "tag_url")
                a_tu  = norm("a",  "tag_url")
                matches = [
                    (f"""
                        UPDATE tags
                        SET sport_id = (SELECT s.id FROM sports s WHERE {s_url} = {turl} LIMIT 1)
                        WHERE url IS NOT NULL AND sport_id IS NULL;
                    """, "tags ← sports.url (norm)"),
                    (f"""
                        UPDATE tags
                        SET tournament_id = (SELECT tr.id FROM tournaments tr WHERE {tr_url} = {turl} LIMIT 1)
                        WHERE url IS NOT NULL AND tournament_id IS NULL;
                    """, "tags ← tournaments.url (norm)"),
                    (f"""
                        UPDATE tags
                        SET team_id = (SELECT tm.id FROM teams tm WHERE {tm_tu} = {turl} LIMIT 1)
                        WHERE url IS NOT NULL AND team_id IS NULL;
                    """, "tags ← teams.tag_url (norm)"),
                    (f"""
                        UPDATE tags
                        SET athlete_id = (SELECT a.id FROM athletes a WHERE {a_tu} = {turl} LIMIT 1)
                        WHERE url IS NOT NULL AND athlete_id IS NULL;
                    """, "tags ← athletes.tag_url (norm)"),
                ]
            else:
                matches = [
                    ("""
                        UPDATE tags
                        SET sport_id = (SELECT s.id FROM sports s WHERE s.url = tags.url LIMIT 1)
                        WHERE url IS NOT NULL AND sport_id IS NULL;
                    """, "tags ← sports.url"),
                    ("""
                        UPDATE tags
                        SET tournament_id = (SELECT tr.id FROM tournaments tr WHERE tr.url = tags.url LIMIT 1)
                        WHERE url IS NOT NULL AND tournament_id IS NULL;
                    """, "tags ← tournaments.url"),
                    ("""
                        UPDATE tags
                        SET team_id = (SELECT tm.id FROM teams tm WHERE tm.tag_url = tags.url LIMIT 1)
                        WHERE url IS NOT NULL AND team_id IS NULL;
                    """, "tags ← teams.tag_url"),
                    ("""
                        UPDATE tags
                        SET athlete_id = (SELECT a.id FROM athletes a WHERE a.tag_url = tags.url LIMIT 1)
                        WHERE url IS NOT NULL AND athlete_id IS NULL;
                    """, "tags ← athletes.tag_url"),
                ]

            # 1) Прямые совпадения
            for sql, lbl in matches:
                step(conn, sql, lbl, args.verbose)

            # 2) Поднятие по цепочке FK
            step(conn, """
                UPDATE tags
                SET sport_id = COALESCE(
                  sport_id,
                  (SELECT tr.sport_id FROM tournaments tr WHERE tr.id = tags.tournament_id)
                )
                WHERE tournament_id IS NOT NULL;
            """, "tournament→sport", args.verbose)

            step(conn, """
                UPDATE tags
                SET tournament_id = COALESCE(
                  tournament_id,
                  (SELECT tm.tournament_id FROM teams tm WHERE tm.id = tags.team_id)
                )
                WHERE team_id IS NOT NULL;
            """, "team→tournament", args.verbose)

            step(conn, """
                UPDATE tags
                SET sport_id = COALESCE(
                  sport_id,
                  (SELECT tr.sport_id FROM tournaments tr WHERE tr.id = tags.tournament_id)
                )
                WHERE team_id IS NOT NULL;  -- после заполнения tournament_id
            """, "team→sport (via tournament)", args.verbose)

            step(conn, """
                UPDATE tags
                SET team_id = COALESCE(
                  team_id,
                  (SELECT a.team_id FROM athletes a WHERE a.id = tags.athlete_id)
                )
                WHERE athlete_id IS NOT NULL;
            """, "athlete→team", args.verbose)

            step(conn, """
                UPDATE tags
                SET tournament_id = COALESCE(
                  tournament_id,
                  (SELECT a.tournament_id FROM athletes a WHERE a.id = tags.athlete_id)
                )
                WHERE athlete_id IS NOT NULL;
            """, "athlete→tournament (direct)", args.verbose)

            step(conn, """
                UPDATE tags
                SET tournament_id = COALESCE(
                  tournament_id,
                  (SELECT tm.tournament_id FROM teams tm WHERE tm.id = tags.team_id)
                )
                WHERE athlete_id IS NOT NULL;  -- fallback через команду
            """, "athlete→tournament (via team)", args.verbose)

            step(conn, """
                UPDATE tags
                SET sport_id = COALESCE(
                  sport_id,
                  (SELECT tr.sport_id FROM tournaments tr WHERE tr.id = tags.tournament_id)
                )
                WHERE athlete_id IS NOT NULL;  -- финально поднять спорт
            """, "athlete→sport", args.verbose)

            # 3) Выставить type по нижнему уровню
            step(conn, """
                UPDATE tags
                SET type = CASE
                  WHEN athlete_id    IS NOT NULL THEN 'athlete'
                  WHEN team_id       IS NOT NULL THEN 'team'
                  WHEN tournament_id IS NOT NULL THEN 'tournament'
                  WHEN sport_id      IS NOT NULL THEN 'sport'
                  ELSE type
                END
                WHERE type IS NULL OR type = '';
            """, "set type", args.verbose)

            # Отчёты
            filled = conn.execute("""
                SELECT COUNT(*) FROM tags
                WHERE sport_id IS NOT NULL
                   OR tournament_id IS NOT NULL
                   OR team_id IS NOT NULL
                   OR athlete_id IS NOT NULL;
            """).fetchone()[0]
            print(f"✅ Заполнено (есть хотя бы одно *_id): {filled}")

            print("\nℹ️ Примеры размеченных тегов:")
            for row in conn.execute("""
                SELECT id, name, url, type, sport_id, tournament_id, team_id, athlete_id
                FROM tags
                WHERE sport_id IS NOT NULL
                   OR tournament_id IS NOT NULL
                   OR team_id IS NOT NULL
                   OR athlete_id IS NOT NULL
                ORDER BY id DESC LIMIT 20;
            """):
                print(row)

            print("\nℹ️ Примеры неразмеченных тегов:")
            for row in conn.execute("""
                SELECT id, name, url
                FROM tags
                WHERE sport_id IS NULL AND tournament_id IS NULL AND team_id IS NULL AND athlete_id IS NULL
                ORDER BY id DESC LIMIT 20;
            """):
                print(row)

            if args.dry_run:
                print("\n🧪 DRY-RUN: откат savepoint.")
                conn.execute(f"ROLLBACK TO {sp};")
                conn.execute(f"RELEASE {sp};")
            else:
                conn.execute(f"RELEASE {sp};")
                print("\n💾 COMMIT (release savepoint).")

        except sqlite3.Error as e:
            print(f"❌ SQLite error: {e}")
            conn.execute(f"ROLLBACK TO {sp};")
            conn.execute(f"RELEASE {sp};")
            sys.exit(1)

if __name__ == "__main__":
    main()