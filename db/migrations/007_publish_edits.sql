-- 007_publish_edits.sql
-- Tracking edits and threaded updates for published items.

CREATE TABLE IF NOT EXISTS publish_edits (
  id           INTEGER PRIMARY KEY,
  item_type    TEXT NOT NULL CHECK(item_type IN ('story','article')),
  item_id      INTEGER NOT NULL,
  action       TEXT NOT NULL CHECK(action IN ('edit','append')),
  message_id   TEXT NOT NULL,
  reply_msg_id TEXT,
  old_text     TEXT,
  new_text     TEXT,
  mode         TEXT DEFAULT 'html',
  created_at   TEXT DEFAULT (STRFTIME('%Y-%m-%dT%H:%M:%SZ','now')),
  error        TEXT
);

CREATE INDEX IF NOT EXISTS idx_publish_edits_item
ON publish_edits(item_type, item_id, created_at DESC);

CREATE TABLE IF NOT EXISTS publish_map (
  item_type   TEXT NOT NULL CHECK(item_type IN ('story','article')),
  item_id     INTEGER NOT NULL,
  message_id  TEXT NOT NULL,
  sent_at     TEXT,
  text        TEXT,
  mode        TEXT DEFAULT 'html',
  PRIMARY KEY (item_type, item_id)
);
