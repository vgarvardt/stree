-- Restore indexes removed in 006
CREATE INDEX IF NOT EXISTS idx_objects_bucket_id ON objects(bucket_id);
CREATE INDEX IF NOT EXISTS idx_multipart_uploads_bucket_id ON multipart_uploads(bucket_id);
CREATE INDEX IF NOT EXISTS idx_multipart_upload_parts_bucket_id ON multipart_upload_parts(bucket_id);
CREATE INDEX IF NOT EXISTS idx_multipart_upload_parts_upload_id ON multipart_upload_parts(bucket_id, upload_id);
