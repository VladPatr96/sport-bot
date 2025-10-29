# scripts/backfill_m2m_and_tags.py
import argparse, sqlite3, sys
from contextlib import closing


def step(conn, sql, label, verbose=False):
    cur = conn.execute(sql)
    rc = cur.rowcount if cur.rowcount is not None else 0
    if verbose:
        print(f"[update] {label}: ~{rc}")
    return rc

def main():
    ap = argparse.ArgumentParser(description="Backfill team/athlete M2M tournaments and update tags")
    ap.add_argument("--db", required=True)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--verbose", "-v", action="store_true")
    ap.add_argument("--from-cotags", action="store_true", help="достраивать связи по ко-тегам новостей")
    args = ap.parse_args()

    with closing(sqlite3.connect(args.db)) as conn:
        conn.isolation_level = None
        conn.execute("PRAGMA foreign_keys = ON;")
        sp = "sp_m2m"
        conn.execute(f"SAVEPOINT {sp};")
        try:
            # Проверим базовые таблицы
            for t in ("teams","athletes","tournaments","tags","news","news_article_tags"):
                conn.execute(f"SELECT 1 FROM {t} LIMIT 1;")

            # Индексы, чтобы не тормозило
            for sql in [
                "CREATE INDEX IF NOT EXISTS idx_news_published ON news(published_at);",
                "CREATE INDEX IF NOT EXISTS idx_nat_tag ON news_article_tags(tag_id);",
                "CREATE INDEX IF NOT EXISTS idx_tags_team_sport ON tags(team_id, sport_id);",
                "CREATE INDEX IF NOT EXISTS idx_tags_tournament ON tags(tournament_id);",
            ]:
                conn.execute(sql)

            # 1) Засеять M2M из канонических FK
            step(conn, """
                INSERT OR IGNORE INTO team_tournaments (team_id, tournament_id, is_primary, source, confidence)
                SELECT id, tournament_id, 1, 'import', 1.0
                FROM teams
                WHERE tournament_id IS NOT NULL;
            """, "seed team_tournaments from teams.tournament_id", args.verbose)

            step(conn, """
                INSERT OR IGNORE INTO athlete_tournaments (athlete_id, tournament_id, is_primary, source, confidence)
                SELECT id, tournament_id, 1, 'import', 1.0
                FROM athletes
                WHERE tournament_id IS NOT NULL;
            """, "seed athlete_tournaments from athletes.tournament_id", args.verbose)

            # 2) Достроить по ко‑тегам (опционально)
            if args.from_cotags:
                step(conn, """
                    INSERT OR IGNORE INTO team_tournaments (team_id, tournament_id, is_primary, source, confidence)
                    SELECT DISTINCT t_team.entity_id, t_tour.entity_id, 0, 'co_tags', 0.6
                    FROM news_article_tags nat1
                    JOIN tags t_team ON t_team.id = nat1.tag_id AND t_team.type='team' AND t_team.entity_id IS NOT NULL
                    JOIN news_article_tags nat2 ON nat2.news_id = nat1.news_id
                    JOIN tags t_tour ON t_tour.id = nat2.tag_id AND t_tour.type='tournament' AND t_tour.entity_id IS NOT NULL;
                """, "team↔tournament from co-tags", args.verbose)

                step(conn, """
                    INSERT OR IGNORE INTO athlete_tournaments (athlete_id, tournament_id, is_primary, source, confidence)
                    SELECT DISTINCT t_ath.entity_id, t_tour.entity_id, 0, 'co_tags', 0.6
                    FROM news_article_tags nat1
                    JOIN tags t_ath  ON t_ath.id  = nat1.tag_id AND t_ath.type='athlete'   AND t_ath.entity_id IS NOT NULL
                    JOIN news_article_tags nat2 ON nat2.news_id = nat1.news_id
                    JOIN tags t_tour ON t_tour.id = nat2.tag_id AND t_tour.type='tournament' AND t_tour.entity_id IS NOT NULL;
                """, "athlete↔tournament from co-tags", args.verbose)

            # 3) Убедиться, что primary помечен там, где есть канонический FK
            step(conn, """
                UPDATE team_tournaments SET is_primary = 1
                WHERE (team_id, tournament_id) IN (
                    SELECT id, tournament_id FROM teams WHERE tournament_id IS NOT NULL
                );
            """, "mark team primary from teams.tournament_id", args.verbose)

            step(conn, """
                UPDATE athlete_tournaments SET is_primary = 1
                WHERE (athlete_id, tournament_id) IN (
                    SELECT id, tournament_id FROM athletes WHERE tournament_id IS NOT NULL
                );
            """, "mark athlete primary from athletes.tournament_id", args.verbose)

            # 4) Обновить tags: primary tournament и sport
            step(conn, """
                UPDATE tags
                SET tournament_id = COALESCE(tournament_id,
                    (SELECT tt.tournament_id
                       FROM team_tournaments tt
                      WHERE tt.team_id = tags.team_id
                      ORDER BY tt.is_primary DESC, tt.tournament_id ASC
                      LIMIT 1))
                WHERE team_id IS NOT NULL;
            """, "tags: team→primary tournament", args.verbose)

            step(conn, """
                UPDATE tags
                SET tournament_id = COALESCE(tournament_id,
                    (SELECT at.tournament_id
                       FROM athlete_tournaments at
                      WHERE at.athlete_id = tags.athlete_id
                      ORDER BY at.is_primary DESC, at.tournament_id ASC
                      LIMIT 1))
                WHERE athlete_id IS NOT NULL;
            """, "tags: athlete→primary tournament", args.verbose)

            step(conn, """
                UPDATE tags
                SET sport_id = COALESCE(sport_id,
                    (SELECT tr.sport_id FROM tournaments tr WHERE tr.id = tags.tournament_id))
                WHERE tournament_id IS NOT NULL;
            """, "tags: tournament→sport", args.verbose)

            step(conn, """
                UPDATE tags
                SET sport_id = COALESCE(sport_id,
                    (SELECT tr.sport_id
                       FROM team_tournaments tt
                       JOIN tournaments tr ON tr.id = tt.tournament_id
                      WHERE tt.team_id = tags.team_id
                      ORDER BY tt.is_primary DESC, tt.tournament_id ASC
                      LIMIT 1))
                WHERE team_id IS NOT NULL AND sport_id IS NULL;
            """, "tags: team→sport via M2M", args.verbose)

            step(conn, """
                UPDATE tags
                SET sport_id = COALESCE(sport_id,
                    (SELECT tr.sport_id
                       FROM athlete_tournaments at
                       JOIN tournaments tr ON tr.id = at.tournament_id
                      WHERE at.athlete_id = tags.athlete_id
                      ORDER BY at.is_primary DESC, at.tournament_id ASC
                      LIMIT 1))
                WHERE athlete_id IS NOT NULL AND sport_id IS NULL;
            """, "tags: athlete→sport via M2M", args.verbose)

            # 5) type и entity_id на всякий случай
            step(conn, """
                UPDATE tags
                SET type = CASE
                  WHEN athlete_id    IS NOT NULL THEN 'athlete'
                  WHEN team_id       IS NOT NULL THEN 'team'
                  WHEN tournament_id IS NOT NULL THEN 'tournament'
                  WHEN sport_id      IS NOT NULL THEN 'sport'
                  ELSE type END
                WHERE type IS NULL OR type = '';
            """, "tags: set type by depth", args.verbose)

            step(conn, """
                UPDATE tags SET entity_id = COALESCE(entity_id, athlete_id)
                WHERE (type='athlete' OR (type IS NULL OR type='')) AND athlete_id IS NOT NULL;
            """, "tags: entity_id from athlete", args.verbose)
            step(conn, """
                UPDATE tags SET entity_id = COALESCE(entity_id, team_id)
                WHERE (type='team' OR (type IS NULL OR type='')) AND team_id IS NOT NULL;
            """, "tags: entity_id from team", args.verbose)
            step(conn, """
                UPDATE tags SET entity_id = COALESCE(entity_id, tournament_id)
                WHERE (type='tournament' OR (type IS NULL OR type='')) AND tournament_id IS NOT NULL;
            """, "tags: entity_id from tournament", args.verbose)
            step(conn, """
                UPDATE tags SET entity_id = COALESCE(entity_id, sport_id)
                WHERE (type='sport' OR (type IS NULL OR type='')) AND sport_id IS NOT NULL;
            """, "tags: entity_id from sport", args.verbose)

            # 6) Диагностика
            cnt = conn.execute("""
                SELECT COUNT(*) FROM tags
                WHERE sport_id IS NOT NULL OR tournament_id IS NOT NULL OR team_id IS NOT NULL OR athlete_id IS NOT NULL;
            """).fetchone()[0]
            print(f"✅ В tags заполнено хотя бы одно *_id: {cnt}")

            print("\nℹ️ Примеры обновлённых тегов:")
            for row in conn.execute("""
                SELECT id, name, type, sport_id, tournament_id, team_id, athlete_id, entity_id
                FROM tags
                WHERE sport_id IS NOT NULL OR tournament_id IS NOT NULL OR team_id IS NOT NULL OR athlete_id IS NOT NULL
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
            print("❌ SQLite:", e)
            conn.execute(f"ROLLBACK TO {sp};")
            conn.execute(f"RELEASE {sp};")
            sys.exit(1)

if __name__ == "__main__":
    main()