package storage

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/goccy/go-json"

	"github.com/vgarvardt/stree/pkg/models"
)

// MultipartUpload represents a cached multipart upload
type MultipartUpload struct {
	ID         int64
	BucketID   int64
	Properties json.RawMessage // JSON field for upload properties
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

// MultipartUploadPart represents a cached multipart upload part
type MultipartUploadPart struct {
	ID         int64
	BucketID   int64
	UploadID   string
	Properties json.RawMessage // JSON field for part properties
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

// DeleteMultipartUploadsByBucket deletes all multipart uploads and their parts for a specific bucket
func (s *Storage) DeleteMultipartUploadsByBucket(ctx context.Context, bucketID int64) error {
	// Parts are deleted via CASCADE when multipart_uploads are deleted
	_, err := s.db.ExecContext(ctx, `DELETE FROM multipart_uploads WHERE bucket_id = ?`, bucketID)
	if err != nil {
		return fmt.Errorf("failed to delete multipart uploads: %w", err)
	}

	// Also delete parts directly (in case the reference is not via foreign key)
	_, err = s.db.ExecContext(ctx, `DELETE FROM multipart_upload_parts WHERE bucket_id = ?`, bucketID)
	if err != nil {
		return fmt.Errorf("failed to delete multipart upload parts: %w", err)
	}

	return nil
}

// BulkInsertMultipartUploads inserts multiple multipart uploads in a single transaction
func (s *Storage) BulkInsertMultipartUploads(ctx context.Context, bucketID int64, uploads []models.MultipartUpload) error {
	if len(uploads) == 0 {
		return nil
	}

	return transactional(ctx, s.db, func(ctx context.Context, tx *sql.Tx) error {
		stmt, err := tx.PrepareContext(ctx,
			`INSERT INTO multipart_uploads (bucket_id, properties, created_at, updated_at) VALUES (?, ?, ?, ?)`,
		)
		if err != nil {
			return fmt.Errorf("failed to prepare statement: %w", err)
		}
		defer stmt.Close()

		now := time.Now()
		for _, upload := range uploads {
			propertiesJSON, err := json.Marshal(upload)
			if err != nil {
				return fmt.Errorf("failed to marshal multipart upload: %w", err)
			}

			_, err = stmt.ExecContext(ctx, bucketID, propertiesJSON, now, now)
			if err != nil {
				return fmt.Errorf("failed to insert multipart upload: %w", err)
			}
		}

		return nil
	})
}

// BulkInsertMultipartUploadParts inserts multiple multipart upload parts in a single transaction
func (s *Storage) BulkInsertMultipartUploadParts(ctx context.Context, bucketID int64, uploadID string, parts []models.MultipartUploadPart) error {
	if len(parts) == 0 {
		return nil
	}

	return transactional(ctx, s.db, func(ctx context.Context, tx *sql.Tx) error {
		stmt, err := tx.PrepareContext(ctx,
			`INSERT INTO multipart_upload_parts (bucket_id, upload_id, properties, created_at, updated_at) VALUES (?, ?, ?, ?, ?)`,
		)
		if err != nil {
			return fmt.Errorf("failed to prepare statement: %w", err)
		}
		defer stmt.Close()

		now := time.Now()
		for _, part := range parts {
			propertiesJSON, err := json.Marshal(part)
			if err != nil {
				return fmt.Errorf("failed to marshal multipart upload part: %w", err)
			}

			_, err = stmt.ExecContext(ctx, bucketID, uploadID, propertiesJSON, now, now)
			if err != nil {
				return fmt.Errorf("failed to insert multipart upload part: %w", err)
			}
		}

		return nil
	})
}

// MPUListOptions specifies filtering, ordering, and pagination options for listing multipart uploads
type MPUListOptions struct {
	Limit     int  // Maximum number of uploads to return (0 = no limit)
	OrderDesc bool // Order by initiated descending (true) or ascending (false)
}

// ListMultipartUploadsByBucket retrieves multipart uploads for a bucket with aggregated part stats
func (s *Storage) ListMultipartUploadsByBucket(ctx context.Context, bucketID int64, opts MPUListOptions) ([]models.MultipartUploadWithParts, error) {
	orderDir := "ASC"
	if opts.OrderDesc {
		orderDir = "DESC"
	}

	query := `
		SELECT 
			m.properties,
			COALESCE(COUNT(p.id), 0) as parts_count,
			COALESCE(SUM(json_extract(p.properties, '$.size')), 0) as total_size
		FROM multipart_uploads m
		LEFT JOIN multipart_upload_parts p ON json_extract(m.properties, '$.upload_id') = p.upload_id AND m.bucket_id = p.bucket_id
		WHERE m.bucket_id = ?
		GROUP BY m.id
		ORDER BY json_extract(m.properties, '$.initiated') ` + orderDir

	args := []any{bucketID}

	if opts.Limit > 0 {
		query += ` LIMIT ?`
		args = append(args, opts.Limit)
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query multipart uploads: %w", err)
	}
	defer rows.Close()

	var uploads []models.MultipartUploadWithParts
	for rows.Next() {
		var props json.RawMessage
		var partsCount int32
		var totalSize int64

		if err := rows.Scan(&props, &partsCount, &totalSize); err != nil {
			return nil, fmt.Errorf("failed to scan multipart upload: %w", err)
		}

		var upload models.MultipartUpload
		if err := json.Unmarshal(props, &upload); err != nil {
			return nil, fmt.Errorf("failed to unmarshal multipart upload: %w", err)
		}

		uploads = append(uploads, models.MultipartUploadWithParts{
			MultipartUpload: upload,
			PartsCount:      partsCount,
			TotalSize:       totalSize,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	return uploads, nil
}

// GetMultipartUploadStats returns aggregated statistics for multipart uploads in a bucket
func (s *Storage) GetMultipartUploadStats(ctx context.Context, bucketID int64) (*models.MultipartUploadStats, error) {
	var stats models.MultipartUploadStats

	// Get upload count
	err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM multipart_uploads WHERE bucket_id = ?`,
		bucketID,
	).Scan(&stats.UploadsCount)
	if err != nil {
		return nil, fmt.Errorf("failed to count multipart uploads: %w", err)
	}

	// Get parts count and total size
	err = s.db.QueryRowContext(ctx,
		`SELECT COALESCE(COUNT(*), 0), COALESCE(SUM(json_extract(properties, '$.size')), 0) 
		 FROM multipart_upload_parts WHERE bucket_id = ?`,
		bucketID,
	).Scan(&stats.TotalPartsCount, &stats.TotalPartsSize)
	if err != nil {
		return nil, fmt.Errorf("failed to get parts stats: %w", err)
	}

	return &stats, nil
}
