-- 009_monitor.sql
-- Monitoring logs for key pipeline metrics.

CREATE TABLE IF NOT EXISTS monitor_logs (
  id       INTEGER PRIMARY KEY,
  ts_utc   TEXT NOT NULL DEFAULT (STRFTIME('%Y-%m-%dT%H:%M:%SZ','now')),
  metric   TEXT NOT NULL,
  value    REAL NOT NULL,
  meta     TEXT
);

CREATE INDEX IF NOT EXISTS idx_monitor_ts ON monitor_logs(ts_utc);
CREATE INDEX IF NOT EXISTS idx_monitor_metric_ts ON monitor_logs(metric, ts_utc);
