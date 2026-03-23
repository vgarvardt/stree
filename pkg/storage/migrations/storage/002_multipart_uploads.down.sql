-- Rollback multipart uploads migration

DROP INDEX IF EXISTS idx_multipart_upload_parts_size;
DROP INDEX IF EXISTS idx_multipart_upload_parts_upload_id;
DROP INDEX IF EXISTS idx_multipart_upload_parts_bucket_id;
DROP TABLE IF EXISTS multipart_upload_parts;

DROP INDEX IF EXISTS idx_multipart_uploads_initiated;
DROP INDEX IF EXISTS idx_multipart_uploads_upload_id;
DROP INDEX IF EXISTS idx_multipart_uploads_bucket_id;
DROP TABLE IF EXISTS multipart_uploads;

