-- Revert to JSON properties columns

DROP INDEX IF EXISTS idx_multipart_upload_parts_size;
DROP INDEX IF EXISTS idx_multipart_upload_parts_upload_id;
DROP INDEX IF EXISTS idx_multipart_upload_parts_bucket_id;
DROP TABLE IF EXISTS multipart_upload_parts;

DROP INDEX IF EXISTS idx_multipart_uploads_initiated;
DROP INDEX IF EXISTS idx_multipart_uploads_bucket_id;
DROP TABLE IF EXISTS multipart_uploads;

DROP INDEX IF EXISTS idx_objects_is_delete_marker;
DROP INDEX IF EXISTS idx_objects_last_modified;
DROP INDEX IF EXISTS idx_objects_size;
DROP INDEX IF EXISTS idx_objects_bucket_id;
DROP TABLE IF EXISTS objects;

CREATE TABLE objects (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    bucket_id INTEGER NOT NULL,
    properties TEXT NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (bucket_id) REFERENCES buckets(id) ON DELETE CASCADE
);

CREATE INDEX idx_objects_bucket_id ON objects(bucket_id);
CREATE INDEX idx_objects_size ON objects(json_extract(properties, '$.size'));
CREATE INDEX idx_objects_last_modified ON objects(json_extract(properties, '$.last_modified'));
CREATE INDEX idx_objects_is_delete_marker ON objects(json_extract(properties, '$.is_delete_marker'));

CREATE TABLE multipart_uploads (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    bucket_id INTEGER NOT NULL,
    properties TEXT NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (bucket_id) REFERENCES buckets(id) ON DELETE CASCADE
);

CREATE INDEX idx_multipart_uploads_bucket_id ON multipart_uploads(bucket_id);
CREATE INDEX idx_multipart_uploads_upload_id ON multipart_uploads(json_extract(properties, '$.upload_id'));
CREATE INDEX idx_multipart_uploads_initiated ON multipart_uploads(json_extract(properties, '$.initiated'));

CREATE TABLE multipart_upload_parts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    bucket_id INTEGER NOT NULL,
    upload_id TEXT NOT NULL,
    properties TEXT NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (bucket_id) REFERENCES buckets(id) ON DELETE CASCADE
);

CREATE INDEX idx_multipart_upload_parts_bucket_id ON multipart_upload_parts(bucket_id);
CREATE INDEX idx_multipart_upload_parts_upload_id ON multipart_upload_parts(upload_id);
CREATE INDEX idx_multipart_upload_parts_size ON multipart_upload_parts(json_extract(properties, '$.size'));
