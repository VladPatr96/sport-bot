-- 005_content_fingerprints.sql
-- Fingerprints for content near-duplicate detection

CREATE TABLE IF NOT EXISTS content_fingerprints (
    news_id INTEGER PRIMARY KEY,
    title_sig TEXT NOT NULL,
    entity_sig TEXT,
    created_at TEXT DEFAULT (STRFTIME('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_cf_title ON content_fingerprints(title_sig);
CREATE INDEX IF NOT EXISTS idx_cf_entity ON content_fingerprints(entity_sig);
