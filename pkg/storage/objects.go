package storage

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/vgarvardt/stree/pkg/models"
)

// InsertObject inserts an object version into the cache
func (s *Storage) InsertObject(ctx context.Context, bucketID int64, obj models.ObjectVersion) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT OR REPLACE INTO objects (bucket_id, key, version_id, is_latest, size, last_modified, is_delete_marker, etag, storage_class)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		bucketID, obj.Key, obj.VersionID, obj.IsLatest, obj.Size, obj.LastModified, obj.IsDeleteMarker, obj.ETag, obj.StorageClass,
	)
	if err != nil {
		return fmt.Errorf("failed to insert object: %w", err)
	}
	return nil
}

// GetObjectsByBucket retrieves all objects for a bucket
func (s *Storage) GetObjectsByBucket(ctx context.Context, bucketID int64) ([]models.ObjectVersion, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT key, version_id, is_latest, size, last_modified, is_delete_marker, etag, storage_class
		 FROM objects WHERE bucket_id = ? ORDER BY key`,
		bucketID,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to query objects: %w", err)
	}
	defer rows.Close()

	var objects []models.ObjectVersion
	for rows.Next() {
		var obj models.ObjectVersion
		if err := rows.Scan(&obj.Key, &obj.VersionID, &obj.IsLatest, &obj.Size, &obj.LastModified, &obj.IsDeleteMarker, &obj.ETag, &obj.StorageClass); err != nil {
			return nil, fmt.Errorf("failed to scan object: %w", err)
		}
		objects = append(objects, obj)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	return objects, nil
}

// DeleteObjectsByBucket deletes all objects for a specific bucket
func (s *Storage) DeleteObjectsByBucket(ctx context.Context, bucketID int64) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM objects WHERE bucket_id = ?`, bucketID)
	if err != nil {
		return fmt.Errorf("failed to delete objects: %w", err)
	}
	return nil
}

// DeleteObjectsByBucketBatch deletes up to limit objects for a bucket and returns
// the number of rows actually deleted.
func (s *Storage) DeleteObjectsByBucketBatch(ctx context.Context, bucketID int64, limit int) (int64, error) {
	result, err := s.db.ExecContext(ctx,
		`DELETE FROM objects WHERE (bucket_id, key, version_id, is_delete_marker) IN (
			SELECT bucket_id, key, version_id, is_delete_marker FROM objects WHERE bucket_id = ? LIMIT ?
		)`,
		bucketID, limit)
	if err != nil {
		return 0, fmt.Errorf("failed to delete objects batch: %w", err)
	}
	return result.RowsAffected()
}

// Vacuum runs VACUUM on the database to reclaim disk space after large deletions.
func (s *Storage) Vacuum(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `VACUUM`)
	if err != nil {
		return fmt.Errorf("failed to vacuum database: %w", err)
	}
	return nil
}

// OrderByField specifies which field to order objects by
type OrderByField string

const (
	OrderByKey          OrderByField = ""
	OrderBySize         OrderByField = "size"
	OrderByLastModified OrderByField = "last_modified"
)

// ObjectListOptions specifies filtering, ordering, and pagination options for listing objects
type ObjectListOptions struct {
	Limit              int          // Maximum number of objects to return (0 = no limit)
	OrderBy            OrderByField // Field to order by
	OrderDesc          bool         // Order descending (true) or ascending (false)
	FilterDeleteMarker *bool        // Filter by is_delete_marker: nil = all, true = only delete markers, false = exclude delete markers
}

// ListObjectsByBucket retrieves objects for a bucket with filtering, ordering, and pagination
func (s *Storage) ListObjectsByBucket(ctx context.Context, bucketID int64, opts ObjectListOptions) ([]models.ObjectVersion, error) {
	query := `SELECT key, version_id, is_latest, size, last_modified, is_delete_marker, etag, storage_class FROM objects WHERE bucket_id = ?`
	args := []any{bucketID}

	// Add filter for delete markers if specified
	if opts.FilterDeleteMarker != nil {
		if *opts.FilterDeleteMarker {
			query += ` AND is_delete_marker = 1`
		} else {
			query += ` AND is_delete_marker = 0`
		}
	}

	// Add ordering
	orderDir := "ASC"
	if opts.OrderDesc {
		orderDir = "DESC"
	}

	switch opts.OrderBy {
	case OrderBySize:
		query += ` ORDER BY size ` + orderDir
	case OrderByLastModified:
		query += ` ORDER BY last_modified ` + orderDir
	case OrderByKey:
		fallthrough
	default:
		query += ` ORDER BY key ` + orderDir + `, version_id ` + orderDir
	}

	// Add limit
	if opts.Limit > 0 {
		query += ` LIMIT ?`
		args = append(args, opts.Limit)
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query objects: %w", err)
	}
	defer rows.Close()

	var objects []models.ObjectVersion
	for rows.Next() {
		var obj models.ObjectVersion
		if err := rows.Scan(&obj.Key, &obj.VersionID, &obj.IsLatest, &obj.Size, &obj.LastModified, &obj.IsDeleteMarker, &obj.ETag, &obj.StorageClass); err != nil {
			return nil, fmt.Errorf("failed to scan object: %w", err)
		}
		objects = append(objects, obj)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	return objects, nil
}

// BulkInsertObjectVersions inserts multiple object versions in a single transaction
func (s *Storage) BulkInsertObjectVersions(ctx context.Context, bucketID int64, versions []models.ObjectVersion) error {
	if len(versions) == 0 {
		return nil
	}

	return transactional(ctx, s.db, func(ctx context.Context, tx *sql.Tx) error {
		stmt, err := tx.PrepareContext(ctx,
			`INSERT OR REPLACE INTO objects (bucket_id, key, version_id, is_latest, size, last_modified, is_delete_marker, etag, storage_class)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		)
		if err != nil {
			return fmt.Errorf("failed to prepare statement: %w", err)
		}
		defer stmt.Close()

		for _, version := range versions {
			_, err = stmt.ExecContext(ctx, bucketID, version.Key, version.VersionID, version.IsLatest, version.Size, version.LastModified, version.IsDeleteMarker, version.ETag, version.StorageClass)
			if err != nil {
				return fmt.Errorf("failed to insert object version: %w", err)
			}
		}

		return nil
	})
}
