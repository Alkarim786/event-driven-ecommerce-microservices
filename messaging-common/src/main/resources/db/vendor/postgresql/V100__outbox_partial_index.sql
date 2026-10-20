-- Partial index keeps the relay's "find unpublished" query O(pending) instead of O(table size).
-- Postgres only (H2, used by the fast unit tests, has no partial indexes).
CREATE INDEX outbox_unpublished_idx ON outbox (seq) WHERE published_at IS NULL;
