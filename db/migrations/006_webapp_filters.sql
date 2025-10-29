-- 006_webapp_filters.sql
-- Indexes to support webapp filtering and queue lookups

CREATE INDEX IF NOT EXISTS idx_stories_updated_at ON stories(updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_story_articles_story ON story_articles(story_id);
CREATE INDEX IF NOT EXISTS idx_story_articles_news ON story_articles(news_id);
CREATE INDEX IF NOT EXISTS idx_publish_queue_status_sched ON publish_queue(status, scheduled_at DESC);
CREATE INDEX IF NOT EXISTS idx_publish_queue_sent_at ON publish_queue(sent_at DESC);
