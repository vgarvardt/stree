package storage

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/vgarvardt/stree/pkg/models"
)

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

// InsertObject inserts an object version into the bucket's cache
func (b *BucketDB) InsertObject(ctx context.Context, obj models.ObjectVersion) error {
	_, err := b.db.ExecContext(ctx,
		`INSERT OR REPLACE INTO objects (key, version_id, is_latest, size, last_modified, is_delete_marker, etag, storage_class)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		obj.Key, obj.VersionID, obj.IsLatest, obj.Size, obj.LastModified, obj.IsDeleteMarker, obj.ETag, obj.StorageClass,
	)
	if err != nil {
		return fmt.Errorf("failed to insert object: %w", err)
	}
	return nil
}

// GetObjects retrieves all objects for the bucket
func (b *BucketDB) GetObjects(ctx context.Context) ([]models.ObjectVersion, error) {
	rows, err := b.db.QueryContext(ctx,
		`SELECT key, version_id, is_latest, size, last_modified, is_delete_marker, etag, storage_class
		 FROM objects ORDER BY key`,
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

// ListObjects retrieves objects with filtering, ordering, and pagination
func (b *BucketDB) ListObjects(ctx context.Context, opts ObjectListOptions) ([]models.ObjectVersion, error) {
	query := `SELECT key, version_id, is_latest, size, last_modified, is_delete_marker, etag, storage_class FROM objects WHERE 1=1`
	var args []any

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

	rows, err := b.db.QueryContext(ctx, query, args...)
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
func (b *BucketDB) BulkInsertObjectVersions(ctx context.Context, versions []models.ObjectVersion) error {
	if len(versions) == 0 {
		return nil
	}

	return transactional(ctx, b.db, func(ctx context.Context, tx *sql.Tx) error {
		stmt, err := tx.PrepareContext(ctx,
			`INSERT OR REPLACE INTO objects (key, version_id, is_latest, size, last_modified, is_delete_marker, etag, storage_class)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		)
		if err != nil {
			return fmt.Errorf("failed to prepare statement: %w", err)
		}
		defer stmt.Close()

		for _, version := range versions {
			_, err = stmt.ExecContext(ctx, version.Key, version.VersionID, version.IsLatest, version.Size, version.LastModified, version.IsDeleteMarker, version.ETag, version.StorageClass)
			if err != nil {
				return fmt.Errorf("failed to insert object version: %w", err)
			}
		}

		return nil
	})
}
