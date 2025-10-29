-- 002_entity_aliases_patch.sql
-- Align entity_aliases schema for normalization and indexing without touching existing data.
ALTER TABLE entity_aliases ADD COLUMN alias_normalized TEXT;
ALTER TABLE entity_aliases ADD COLUMN type TEXT;
ALTER TABLE entity_aliases ADD COLUMN entity_type TEXT;
ALTER TABLE entity_aliases ADD COLUMN source TEXT;
ALTER TABLE entity_aliases ADD COLUMN lang TEXT;
ALTER TABLE entity_aliases ADD COLUMN created_at TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS idx_entity_aliases_norm
ON entity_aliases(alias_normalized, entity_type)
WHERE alias_normalized IS NOT NULL AND entity_type IS NOT NULL;
