-- Migration for multipart uploads support

CREATE TABLE IF NOT EXISTS multipart_uploads (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    bucket_id INTEGER NOT NULL,
    properties TEXT NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (bucket_id) REFERENCES buckets(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_multipart_uploads_bucket_id ON multipart_uploads(bucket_id);
CREATE INDEX IF NOT EXISTS idx_multipart_uploads_upload_id ON multipart_uploads(json_extract(properties, '$.upload_id'));
CREATE INDEX IF NOT EXISTS idx_multipart_uploads_initiated ON multipart_uploads(json_extract(properties, '$.initiated'));

CREATE TABLE IF NOT EXISTS multipart_upload_parts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    bucket_id INTEGER NOT NULL,
    upload_id TEXT NOT NULL,
    properties TEXT NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (bucket_id) REFERENCES buckets(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_multipart_upload_parts_bucket_id ON multipart_upload_parts(bucket_id);
CREATE INDEX IF NOT EXISTS idx_multipart_upload_parts_upload_id ON multipart_upload_parts(upload_id);
CREATE INDEX IF NOT EXISTS idx_multipart_upload_parts_size ON multipart_upload_parts(json_extract(properties, '$.size'));

