-- Rollback initial schema

DROP INDEX IF EXISTS idx_bookmarks_last_used;
DROP INDEX IF EXISTS idx_bookmarks_title;
DROP TABLE IF EXISTS bookmarks;

DROP INDEX IF EXISTS idx_objects_is_delete_marker;
DROP INDEX IF EXISTS idx_objects_last_modified;
DROP INDEX IF EXISTS idx_objects_size;
DROP INDEX IF EXISTS idx_objects_bucket_id;
DROP TABLE IF EXISTS objects;

DROP INDEX IF EXISTS idx_buckets_creation_date;
DROP INDEX IF EXISTS idx_buckets_name;
DROP INDEX IF EXISTS idx_buckets_session_id;
DROP TABLE IF EXISTS buckets;

DROP INDEX IF EXISTS idx_sessions_config_str;
DROP TABLE IF EXISTS sessions;

