-- Drop indexes that duplicate the leading columns of WITHOUT ROWID primary keys.
-- On WITHOUT ROWID tables the PK *is* the B-tree, so an index on its prefix is
-- pure overhead: it stores a full copy of the PK per row for no query benefit.
--
-- This is a cache-only database, so a VACUUM after this migration will reclaim
-- the disk space.

-- objects PK: (bucket_id, key, version_id, is_delete_marker)
DROP INDEX IF EXISTS idx_objects_bucket_id;           -- prefix (bucket_id) ⊂ PK

-- multipart_uploads PK: (bucket_id, upload_id)
DROP INDEX IF EXISTS idx_multipart_uploads_bucket_id; -- prefix (bucket_id) ⊂ PK

-- multipart_upload_parts PK: (bucket_id, upload_id, part_number)
DROP INDEX IF EXISTS idx_multipart_upload_parts_bucket_id;  -- prefix (bucket_id) ⊂ PK
DROP INDEX IF EXISTS idx_multipart_upload_parts_upload_id;  -- prefix (bucket_id, upload_id) ⊂ PK
