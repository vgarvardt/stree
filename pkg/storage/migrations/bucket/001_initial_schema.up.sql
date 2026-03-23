CREATE TABLE IF NOT EXISTS objects (
    key TEXT NOT NULL,
    version_id TEXT NOT NULL DEFAULT '',
    is_latest INTEGER NOT NULL DEFAULT 0,
    size INTEGER NOT NULL DEFAULT 0,
    last_modified DATETIME NOT NULL,
    is_delete_marker INTEGER NOT NULL DEFAULT 0,
    etag TEXT NOT NULL DEFAULT '',
    storage_class TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (key, version_id, is_delete_marker)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_objects_size ON objects (size);
CREATE INDEX IF NOT EXISTS idx_objects_last_modified ON objects (last_modified);
CREATE INDEX IF NOT EXISTS idx_objects_is_delete_marker ON objects (is_delete_marker);

CREATE TABLE IF NOT EXISTS multipart_uploads (
    key TEXT NOT NULL,
    upload_id TEXT NOT NULL,
    initiator TEXT NOT NULL DEFAULT '',
    owner TEXT NOT NULL DEFAULT '',
    storage_class TEXT NOT NULL DEFAULT '',
    initiated DATETIME NOT NULL,
    PRIMARY KEY (upload_id)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_multipart_uploads_initiated ON multipart_uploads (initiated);

CREATE TABLE IF NOT EXISTS multipart_upload_parts (
    upload_id TEXT NOT NULL,
    part_number INTEGER NOT NULL,
    size INTEGER NOT NULL DEFAULT 0,
    etag TEXT NOT NULL DEFAULT '',
    last_modified DATETIME NOT NULL,
    PRIMARY KEY (upload_id, part_number)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_multipart_upload_parts_size ON multipart_upload_parts (size);
