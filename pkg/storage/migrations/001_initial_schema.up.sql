-- Initial schema migration

CREATE TABLE IF NOT EXISTS sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    config_str TEXT NOT NULL UNIQUE,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_sessions_config_str ON sessions(config_str);

CREATE TABLE IF NOT EXISTS buckets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    creation_date DATETIME NOT NULL,
    details TEXT NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (session_id) REFERENCES sessions(id) ON DELETE CASCADE,
    UNIQUE(session_id, name)
);

CREATE INDEX IF NOT EXISTS idx_buckets_session_id ON buckets(session_id);
CREATE INDEX IF NOT EXISTS idx_buckets_name ON buckets(name);
CREATE INDEX IF NOT EXISTS idx_buckets_creation_date ON buckets(creation_date);

CREATE TABLE IF NOT EXISTS objects (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    bucket_id INTEGER NOT NULL,
    properties TEXT NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (bucket_id) REFERENCES buckets(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_objects_bucket_id ON objects(bucket_id);
CREATE INDEX IF NOT EXISTS idx_objects_size ON objects(json_extract(properties, '$.size'));
CREATE INDEX IF NOT EXISTS idx_objects_last_modified ON objects(json_extract(properties, '$.last_modified'));
CREATE INDEX IF NOT EXISTS idx_objects_is_delete_marker ON objects(json_extract(properties, '$.is_delete_marker'));

CREATE TABLE IF NOT EXISTS bookmarks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    region TEXT NOT NULL,
    access_key_id TEXT NOT NULL,
    session_token TEXT,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_used_at DATETIME
);

CREATE INDEX IF NOT EXISTS idx_bookmarks_title ON bookmarks(title);
CREATE INDEX IF NOT EXISTS idx_bookmarks_last_used ON bookmarks(last_used_at);

