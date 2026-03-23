-- Replace JSON properties columns with typed columns and WITHOUT ROWID for space efficiency.
-- This is a destructive migration — cached data will be re-fetched from S3 on next refresh.

-- Drop old objects table and its indexes
DROP INDEX IF EXISTS idx_objects_is_delete_marker;
DROP INDEX IF EXISTS idx_objects_last_modified;
DROP INDEX IF EXISTS idx_objects_size;
DROP INDEX IF EXISTS idx_objects_bucket_id;
DROP TABLE IF EXISTS objects;

-- Recreate objects with typed columns and composite PK
CREATE TABLE objects (
    bucket_id        INTEGER  NOT NULL,
    key              TEXT     NOT NULL,
    version_id       TEXT     NOT NULL DEFAULT '',
    is_latest        INTEGER  NOT NULL DEFAULT 0,
    size             INTEGER  NOT NULL DEFAULT 0,
    last_modified    DATETIME NOT NULL,
    is_delete_marker INTEGER  NOT NULL DEFAULT 0,
    etag             TEXT     NOT NULL DEFAULT '',
    storage_class    TEXT     NOT NULL DEFAULT '',
    PRIMARY KEY (bucket_id, key, version_id, is_delete_marker),
    FOREIGN KEY (bucket_id) REFERENCES buckets(id) ON DELETE CASCADE
) WITHOUT ROWID;

CREATE INDEX idx_objects_bucket_id ON objects(bucket_id);
CREATE INDEX idx_objects_size ON objects(bucket_id, size);
CREATE INDEX idx_objects_last_modified ON objects(bucket_id, last_modified);
CREATE INDEX idx_objects_is_delete_marker ON objects(bucket_id, is_delete_marker);

-- Drop old multipart_uploads table and its indexes
DROP INDEX IF EXISTS idx_multipart_uploads_initiated;
DROP INDEX IF EXISTS idx_multipart_uploads_upload_id;
DROP INDEX IF EXISTS idx_multipart_uploads_bucket_id;
DROP TABLE IF EXISTS multipart_uploads;

-- Recreate multipart_uploads with typed columns and composite PK
CREATE TABLE multipart_uploads (
    bucket_id     INTEGER  NOT NULL,
    key           TEXT     NOT NULL,
    upload_id     TEXT     NOT NULL,
    initiator     TEXT     NOT NULL DEFAULT '',
    owner         TEXT     NOT NULL DEFAULT '',
    storage_class TEXT     NOT NULL DEFAULT '',
    initiated     DATETIME NOT NULL,
    PRIMARY KEY (bucket_id, upload_id),
    FOREIGN KEY (bucket_id) REFERENCES buckets(id) ON DELETE CASCADE
) WITHOUT ROWID;

CREATE INDEX idx_multipart_uploads_bucket_id ON multipart_uploads(bucket_id);
CREATE INDEX idx_multipart_uploads_initiated ON multipart_uploads(bucket_id, initiated);

-- Drop old multipart_upload_parts table and its indexes
DROP INDEX IF EXISTS idx_multipart_upload_parts_size;
DROP INDEX IF EXISTS idx_multipart_upload_parts_upload_id;
DROP INDEX IF EXISTS idx_multipart_upload_parts_bucket_id;
DROP TABLE IF EXISTS multipart_upload_parts;

-- Recreate multipart_upload_parts with typed columns and composite PK
CREATE TABLE multipart_upload_parts (
    bucket_id     INTEGER  NOT NULL,
    upload_id     TEXT     NOT NULL,
    part_number   INTEGER  NOT NULL,
    size          INTEGER  NOT NULL DEFAULT 0,
    etag          TEXT     NOT NULL DEFAULT '',
    last_modified DATETIME NOT NULL,
    PRIMARY KEY (bucket_id, upload_id, part_number),
    FOREIGN KEY (bucket_id) REFERENCES buckets(id) ON DELETE CASCADE
) WITHOUT ROWID;

CREATE INDEX idx_multipart_upload_parts_bucket_id ON multipart_upload_parts(bucket_id);
CREATE INDEX idx_multipart_upload_parts_upload_id ON multipart_upload_parts(bucket_id, upload_id);
CREATE INDEX idx_multipart_upload_parts_size ON multipart_upload_parts(bucket_id, size);
