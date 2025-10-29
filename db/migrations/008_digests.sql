-- 008_digests.sql
-- Digest storage for daily/weekly exports and Telegram sharing.

CREATE TABLE IF NOT EXISTS digests (
  id           INTEGER PRIMARY KEY,
  period       TEXT NOT NULL CHECK (period IN ('daily','weekly')),
  since_utc    TEXT NOT NULL,
  until_utc    TEXT NOT NULL,
  title        TEXT NOT NULL,
  created_at   TEXT DEFAULT (STRFTIME('%Y-%m-%dT%H:%M:%SZ','now')),
  status       TEXT NOT NULL DEFAULT 'ready' CHECK (status IN ('ready','sent','error')),
  message_id   TEXT,
  notes        TEXT
);

CREATE TABLE IF NOT EXISTS digest_items (
  digest_id      INTEGER NOT NULL,
  rank           INTEGER NOT NULL,
  story_id       INTEGER NOT NULL,
  total_articles INTEGER,
  PRIMARY KEY (digest_id, rank)
);

CREATE INDEX IF NOT EXISTS idx_digests_period_since ON digests(period, since_utc);
CREATE INDEX IF NOT EXISTS idx_digest_items_story ON digest_items(story_id);
