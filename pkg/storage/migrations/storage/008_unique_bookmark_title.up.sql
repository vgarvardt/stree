DROP INDEX IF EXISTS idx_bookmarks_title;

CREATE UNIQUE INDEX idx_bookmarks_title ON bookmarks (title);
