PRAGMA foreign_keys = ON;
BEGIN TRANSACTION;


-- Команда ↔ Турниры (многие-ко-многим)
CREATE TABLE IF NOT EXISTS team_tournaments (
team_id INTEGER NOT NULL,
tournament_id INTEGER NOT NULL,
season TEXT,
is_primary INTEGER DEFAULT 0, -- 1 = предпочтительный турнир для команды
PRIMARY KEY (team_id, tournament_id),
FOREIGN KEY (team_id) REFERENCES teams(id) ON DELETE CASCADE,
FOREIGN KEY (tournament_id) REFERENCES tournaments(id) ON DELETE CASCADE
);


-- Спортсмен ↔ Турниры (многие-ко-многим)
CREATE TABLE IF NOT EXISTS athlete_tournaments (
athlete_id INTEGER NOT NULL,
tournament_id INTEGER NOT NULL,
season TEXT,
is_primary INTEGER DEFAULT 0, -- 1 = предпочтительный турнир спортсмена
PRIMARY KEY (athlete_id, tournament_id),
FOREIGN KEY (athlete_id) REFERENCES athletes(id) ON DELETE CASCADE,
FOREIGN KEY (tournament_id) REFERENCES tournaments(id) ON DELETE CASCADE
);


-- Индексы
CREATE INDEX IF NOT EXISTS idx_tt_team ON team_tournaments(team_id);
CREATE INDEX IF NOT EXISTS idx_tt_tour ON team_tournaments(tournament_id);
CREATE INDEX IF NOT EXISTS idx_at_ath ON athlete_tournaments(athlete_id);
CREATE INDEX IF NOT EXISTS idx_at_tour ON athlete_tournaments(tournament_id);


COMMIT;