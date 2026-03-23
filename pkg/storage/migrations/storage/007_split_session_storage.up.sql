DROP TABLE IF EXISTS multipart_upload_parts;
DROP TABLE IF EXISTS multipart_uploads;
DROP TABLE IF EXISTS objects;

-- Reset bucket metadata to force re-fetch (data moved to per-bucket DB files)
UPDATE buckets SET details = CAST('{}' AS BLOB);
