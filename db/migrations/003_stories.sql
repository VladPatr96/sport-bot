-- 003_stories.sql
-- Stories clustering support tables

CREATE TABLE IF NOT EXISTS stories (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%S', 'now', 'utc')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%S', 'now', 'utc'))
);

CREATE TABLE IF NOT EXISTS story_articles (
    story_id INTEGER NOT NULL,
    news_id INTEGER NOT NULL,
    PRIMARY KEY (story_id, news_id),
    FOREIGN KEY (story_id) REFERENCES stories(id) ON DELETE CASCADE,
    FOREIGN KEY (news_id) REFERENCES news(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_story_articles_news_id ON story_articles(news_id);
CREATE INDEX IF NOT EXISTS idx_story_articles_story_id ON story_articles(story_id);
