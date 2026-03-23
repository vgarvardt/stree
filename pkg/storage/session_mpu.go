package storage

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/vgarvardt/stree/pkg/models"
)

// MPUListOptions specifies filtering, ordering, and pagination options for listing multipart uploads
type MPUListOptions struct {
	Limit     int  // Maximum number of uploads to return (0 = no limit)
	OrderDesc bool // Order by initiated descending (true) or ascending (false)
}

// DeleteMultipartUploads deletes all multipart uploads and their parts
func (b *BucketDB) DeleteMultipartUploads(ctx context.Context) error {
	if _, err := b.db.ExecContext(ctx, `DELETE FROM multipart_uploads`); err != nil {
		return fmt.Errorf("failed to delete multipart uploads: %w", err)
	}
	if _, err := b.db.ExecContext(ctx, `DELETE FROM multipart_upload_parts`); err != nil {
		return fmt.Errorf("failed to delete multipart upload parts: %w", err)
	}
	return nil
}

// BulkInsertMultipartUploads inserts multiple multipart uploads in a single transaction
func (b *BucketDB) BulkInsertMultipartUploads(ctx context.Context, uploads []models.MultipartUpload) error {
	if len(uploads) == 0 {
		return nil
	}

	return transactional(ctx, b.db, func(ctx context.Context, tx *sql.Tx) error {
		stmt, err := tx.PrepareContext(ctx,
			`INSERT INTO multipart_uploads (key, upload_id, initiator, owner, storage_class, initiated)
			 VALUES (?, ?, ?, ?, ?, ?)`,
		)
		if err != nil {
			return fmt.Errorf("failed to prepare statement: %w", err)
		}
		defer stmt.Close()

		for _, upload := range uploads {
			_, err = stmt.ExecContext(ctx, upload.Key, upload.UploadID, upload.Initiator, upload.Owner, upload.StorageClass, upload.Initiated)
			if err != nil {
				return fmt.Errorf("failed to insert multipart upload: %w", err)
			}
		}

		return nil
	})
}

// BulkInsertMultipartUploadParts inserts multiple multipart upload parts in a single transaction
func (b *BucketDB) BulkInsertMultipartUploadParts(ctx context.Context, uploadID string, parts []models.MultipartUploadPart) error {
	if len(parts) == 0 {
		return nil
	}

	return transactional(ctx, b.db, func(ctx context.Context, tx *sql.Tx) error {
		stmt, err := tx.PrepareContext(ctx,
			`INSERT INTO multipart_upload_parts (upload_id, part_number, size, etag, last_modified)
			 VALUES (?, ?, ?, ?, ?)`,
		)
		if err != nil {
			return fmt.Errorf("failed to prepare statement: %w", err)
		}
		defer stmt.Close()

		for _, part := range parts {
			_, err = stmt.ExecContext(ctx, uploadID, part.PartNumber, part.Size, part.ETag, part.LastModified)
			if err != nil {
				return fmt.Errorf("failed to insert multipart upload part: %w", err)
			}
		}

		return nil
	})
}

// ListMultipartUploads retrieves multipart uploads with aggregated part stats
func (b *BucketDB) ListMultipartUploads(ctx context.Context, opts MPUListOptions) ([]models.MultipartUploadWithParts, error) {
	orderDir := "ASC"
	if opts.OrderDesc {
		orderDir = "DESC"
	}

	query := `
		SELECT
			m.key, m.upload_id, m.initiator, m.owner, m.storage_class, m.initiated,
			COALESCE(COUNT(p.part_number), 0) as parts_count,
			COALESCE(SUM(p.size), 0) as total_size
		FROM multipart_uploads m
		LEFT JOIN multipart_upload_parts p ON m.upload_id = p.upload_id
		GROUP BY m.upload_id
		ORDER BY m.initiated ` + orderDir

	var args []any

	if opts.Limit > 0 {
		query += ` LIMIT ?`
		args = append(args, opts.Limit)
	}

	rows, err := b.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query multipart uploads: %w", err)
	}
	defer rows.Close()

	var uploads []models.MultipartUploadWithParts
	for rows.Next() {
		var upload models.MultipartUploadWithParts

		if err := rows.Scan(
			&upload.Key, &upload.UploadID, &upload.Initiator, &upload.Owner,
			&upload.StorageClass, &upload.Initiated,
			&upload.PartsCount, &upload.TotalSize,
		); err != nil {
			return nil, fmt.Errorf("failed to scan multipart upload: %w", err)
		}

		uploads = append(uploads, upload)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	return uploads, nil
}

// GetMultipartUploadStats returns aggregated statistics for multipart uploads
func (b *BucketDB) GetMultipartUploadStats(ctx context.Context) (*models.MultipartUploadStats, error) {
	var stats models.MultipartUploadStats

	err := b.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM multipart_uploads`,
	).Scan(&stats.UploadsCount)
	if err != nil {
		return nil, fmt.Errorf("failed to count multipart uploads: %w", err)
	}

	err = b.db.QueryRowContext(ctx,
		`SELECT COALESCE(COUNT(*), 0), COALESCE(SUM(size), 0)
		 FROM multipart_upload_parts`,
	).Scan(&stats.TotalPartsCount, &stats.TotalPartsSize)
	if err != nil {
		return nil, fmt.Errorf("failed to get parts stats: %w", err)
	}

	return &stats, nil
}
