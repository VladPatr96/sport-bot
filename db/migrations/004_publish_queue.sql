-- 004_publish_queue.sql
-- Publishing queue table for Telegram scheduler

CREATE TABLE IF NOT EXISTS publish_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    item_type TEXT NOT NULL CHECK(item_type IN ('story', 'article')),
    item_id INTEGER NOT NULL,
    priority INTEGER DEFAULT 0,
    scheduled_at TEXT,
    enqueued_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%S', 'now', 'utc')),
    status TEXT NOT NULL DEFAULT 'queued' CHECK(status IN ('queued', 'sent', 'skipped', 'error')),
    dedup_key TEXT UNIQUE,
    error TEXT,
    message_id TEXT,
    sent_at TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_publish_queue_dedup ON publish_queue(dedup_key);
CREATE INDEX IF NOT EXISTS idx_publish_queue_status_scheduled ON publish_queue(status, scheduled_at, priority, enqueued_at);
CREATE INDEX IF NOT EXISTS idx_publish_queue_sent_at ON publish_queue(sent_at);
