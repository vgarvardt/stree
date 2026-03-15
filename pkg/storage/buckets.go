package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/goccy/go-json"

	"github.com/vgarvardt/stree/pkg/models"
)

// UpsertBucket creates or updates a bucket with its encryption configuration
func (s *Storage) UpsertBucket(ctx context.Context, sessionID int64, name string, creationDate time.Time, details models.BucketDetails, encryption *models.BucketEncryption) error {
	detailsJSON, err := json.Marshal(details)
	if err != nil {
		return fmt.Errorf("failed to marshal bucket details: %w", err)
	}

	var encryptionJSON []byte
	if encryption != nil {
		encryptionJSON, err = json.Marshal(encryption)
		if err != nil {
			return fmt.Errorf("failed to marshal bucket encryption: %w", err)
		}
	}

	now := time.Now()

	// Try to update existing bucket
	result, err := s.db.ExecContext(ctx,
		`UPDATE buckets SET details = ?, encryption = ?, updated_at = ?, creation_date = ? WHERE session_id = ? AND name = ?`,
		detailsJSON, encryptionJSON, now, creationDate, sessionID, name,
	)
	if err != nil {
		return fmt.Errorf("failed to update bucket: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected > 0 {
		return nil
	}

	// Insert new bucket
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO buckets (session_id, name, creation_date, details, encryption, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		sessionID, name, creationDate, detailsJSON, encryptionJSON, now, now,
	)
	if err != nil {
		return fmt.Errorf("failed to insert bucket: %w", err)
	}

	return nil
}

// GetBucketsBySession retrieves all buckets for a session
func (s *Storage) GetBucketsBySession(ctx context.Context, sessionID int64) ([]Bucket, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, session_id, name, creation_date, details, encryption, created_at, updated_at FROM buckets WHERE session_id = ? ORDER BY name`,
		sessionID,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to query buckets: %w", err)
	}
	defer rows.Close()

	var buckets []Bucket
	for rows.Next() {
		var bucket Bucket
		var encryption []byte
		if err := rows.Scan(&bucket.ID, &bucket.SessionID, &bucket.Name, &bucket.CreationDate, &bucket.Details, &encryption, &bucket.CreatedAt, &bucket.UpdatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan bucket: %w", err)
		}
		if encryption != nil {
			bucket.Encryption = new(models.BucketEncryption)
			if err := json.Unmarshal(encryption, bucket.Encryption); err != nil {
				return nil, fmt.Errorf("failed to unmarshal bucket encryption: %w", err)
			}
		}
		buckets = append(buckets, bucket)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	return buckets, nil
}

// GetBucket retrieves a specific bucket
func (s *Storage) GetBucket(ctx context.Context, sessionID int64, name string) (*Bucket, error) {
	var bucket Bucket
	var encryption []byte
	err := s.db.QueryRowContext(ctx,
		`SELECT id, session_id, name, creation_date, details, encryption, created_at, updated_at FROM buckets WHERE session_id = ? AND name = ?`,
		sessionID, name,
	).Scan(&bucket.ID, &bucket.SessionID, &bucket.Name, &bucket.CreationDate, &bucket.Details, &encryption, &bucket.CreatedAt, &bucket.UpdatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get bucket: %w", err)
	}
	if encryption != nil {
		bucket.Encryption = new(models.BucketEncryption)
		if err := json.Unmarshal(encryption, bucket.Encryption); err != nil {
			return nil, fmt.Errorf("failed to unmarshal bucket encryption: %w", err)
		}
	}

	return &bucket, nil
}

// DeleteBucket deletes a specific bucket and all associated objects (cascades)
func (s *Storage) DeleteBucket(ctx context.Context, sessionID int64, name string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM buckets WHERE session_id = ? AND name = ?`, sessionID, name)
	if err != nil {
		return fmt.Errorf("failed to delete bucket: %w", err)
	}
	return nil
}

// DeleteStaleBuckets deletes buckets for a session whose names are NOT in the given set.
// Associated objects, multipart uploads, and parts are removed via CASCADE.
func (s *Storage) DeleteStaleBuckets(ctx context.Context, sessionID int64, freshNames []string) (int64, error) {
	if len(freshNames) == 0 {
		// No fresh buckets — delete all buckets for this session
		result, err := s.db.ExecContext(ctx, `DELETE FROM buckets WHERE session_id = ?`, sessionID)
		if err != nil {
			return 0, fmt.Errorf("failed to delete all buckets: %w", err)
		}
		return result.RowsAffected()
	}

	// Build placeholders for the IN clause
	placeholders := make([]byte, 0, len(freshNames)*2)
	args := make([]any, 0, len(freshNames)+1)
	args = append(args, sessionID)
	for i, name := range freshNames {
		if i > 0 {
			placeholders = append(placeholders, ',')
		}
		placeholders = append(placeholders, '?')
		args = append(args, name)
	}

	query := fmt.Sprintf(`DELETE FROM buckets WHERE session_id = ? AND name NOT IN (%s)`, placeholders)
	result, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("failed to delete stale buckets: %w", err)
	}
	return result.RowsAffected()
}
