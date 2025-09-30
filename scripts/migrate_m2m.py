# scripts/migrate_m2m.py
import argparse, sqlite3, sys
from contextlib import closing

SQL_MIGRATION = r"""
PRAGMA foreign_keys = ON;

-- Команда ↔ Турниры (многие-ко-многим)
CREATE TABLE IF NOT EXISTS team_tournaments (
  team_id       INTEGER NOT NULL,
  tournament_id INTEGER NOT NULL,
  season        TEXT,
  is_primary    INTEGER DEFAULT 0,
  source        TEXT,
  confidence    REAL DEFAULT 0.0,
  created_at    TEXT DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (team_id, tournament_id),
  FOREIGN KEY (team_id) REFERENCES teams(id) ON DELETE CASCADE,
  FOREIGN KEY (tournament_id) REFERENCES tournaments(id) ON DELETE CASCADE
);

-- Атлет ↔ Турниры (многие-ко-многим)
CREATE TABLE IF NOT EXISTS athlete_tournaments (
  athlete_id    INTEGER NOT NULL,
  tournament_id INTEGER NOT NULL,
  season        TEXT,
  is_primary    INTEGER DEFAULT 0,
  source        TEXT,
  confidence    REAL DEFAULT 0.0,
  created_at    TEXT DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (athlete_id, tournament_id),
  FOREIGN KEY (athlete_id) REFERENCES athletes(id) ON DELETE CASCADE,
  FOREIGN KEY (tournament_id) REFERENCES tournaments(id) ON DELETE CASCADE
);

-- Индексы
CREATE INDEX IF NOT EXISTS idx_tt_team ON team_tournaments(team_id);
CREATE INDEX IF NOT EXISTS idx_tt_tour ON team_tournaments(tournament_id);
CREATE INDEX IF NOT EXISTS idx_at_ath  ON athlete_tournaments(athlete_id);
CREATE INDEX IF NOT EXISTS idx_at_tour ON athlete_tournaments(tournament_id);

-- Представления
DROP VIEW IF EXISTS v_tournament_teams;
CREATE VIEW v_tournament_teams AS
SELECT t.tournament_id AS tournament_id, t.id AS team_id, 1 AS is_primary
FROM teams t
WHERE t.tournament_id IS NOT NULL
UNION
SELECT tt.tournament_id, tt.team_id, tt.is_primary
FROM team_tournaments tt;

DROP VIEW IF EXISTS v_team_tournaments;
CREATE VIEW v_team_tournaments AS
SELECT t.id AS team_id, t.tournament_id AS tournament_id, 1 AS is_primary
FROM teams t
WHERE t.tournament_id IS NOT NULL
UNION
SELECT tt.team_id, tt.tournament_id, tt.is_primary
FROM team_tournaments tt;

DROP VIEW IF EXISTS v_athlete_main_tournament;
CREATE VIEW v_athlete_main_tournament AS
SELECT at.athlete_id,
       (SELECT at2.tournament_id
          FROM athlete_tournaments at2
         WHERE at2.athlete_id = at.athlete_id
         ORDER BY at2.is_primary DESC, at2.tournament_id ASC
         LIMIT 1) AS tournament_id
FROM (SELECT DISTINCT athlete_id FROM athlete_tournaments) at;
"""

def main():
    ap = argparse.ArgumentParser(description="Migrate SQLite: create M2M tables and views")
    ap.add_argument("--db", required=True)
    ap.add_argument("--verbose", "-v", action="store_true")
    args = ap.parse_args()

    with closing(sqlite3.connect(args.db)) as conn:
        conn.isolation_level = None
        conn.execute("PRAGMA foreign_keys = ON;")
        sp = "sp_migrate"
        conn.execute(f"SAVEPOINT {sp};")
        try:
            if args.verbose:
                print("[sql] applying migration…")
            for stmt in filter(None, SQL_MIGRATION.split(";\n")):
                sql = stmt.strip()
                if not sql:
                    continue
                if args.verbose:
                    print("[exec]", sql[:120].replace("\n"," ") + ("…" if len(sql) > 120 else ""))
                conn.execute(sql)
            conn.execute(f"RELEASE {sp};")
            print("✅ Migration applied.")
        except sqlite3.Error as e:
            print("❌ SQLite:", e)
            conn.execute(f"ROLLBACK TO {sp};")
            conn.execute(f"RELEASE {sp};")
            sys.exit(1)

if __name__ == "__main__":
    main()