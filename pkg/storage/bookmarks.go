package storage

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/vgarvardt/stree/pkg/models"
)

// UpsertBookmark creates or updates a bookmark in the database
// Note: Sensitive data (secret keys) should be stored separately in the OS keychain
func (s *Storage) UpsertBookmark(ctx context.Context, bookmark *models.Bookmark) error {
	if err := bookmark.Validate(); err != nil {
		return err
	}

	now := time.Now()
	bookmark.UpdatedAt = now

	if bookmark.ID == 0 {
		// Insert new bookmark
		bookmark.CreatedAt = now
		query := `
			INSERT INTO bookmarks (title, endpoint, region, access_key_id, session_token, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?)
		`
		result, err := s.db.ExecContext(ctx, query,
			bookmark.Title,
			bookmark.Endpoint,
			bookmark.Region,
			bookmark.AccessKeyID,
			bookmark.SessionToken,
			bookmark.CreatedAt,
			bookmark.UpdatedAt,
		)
		if err != nil {
			return fmt.Errorf("failed to insert bookmark: %w", err)
		}
		id, err := result.LastInsertId()
		if err != nil {
			return fmt.Errorf("failed to get inserted bookmark ID: %w", err)
		}
		bookmark.ID = id
	} else {
		// Update existing bookmark
		query := `
			UPDATE bookmarks
			SET title = ?, endpoint = ?, region = ?, access_key_id = ?, session_token = ?, updated_at = ?
			WHERE id = ?
		`
		_, err := s.db.ExecContext(ctx, query,
			bookmark.Title,
			bookmark.Endpoint,
			bookmark.Region,
			bookmark.AccessKeyID,
			bookmark.SessionToken,
			bookmark.UpdatedAt,
			bookmark.ID,
		)
		if err != nil {
			return fmt.Errorf("failed to update bookmark: %w", err)
		}
	}

	return nil
}

// GetBookmark retrieves a bookmark by ID
func (s *Storage) GetBookmark(ctx context.Context, id int64) (*models.Bookmark, error) {
	query := `
		SELECT id, title, endpoint, region, access_key_id, session_token, created_at, updated_at, last_used_at
		FROM bookmarks
		WHERE id = ?
	`
	var bookmark models.Bookmark
	var lastUsedAt sql.NullTime

	err := s.db.QueryRowContext(ctx, query, id).Scan(
		&bookmark.ID,
		&bookmark.Title,
		&bookmark.Endpoint,
		&bookmark.Region,
		&bookmark.AccessKeyID,
		&bookmark.SessionToken,
		&bookmark.CreatedAt,
		&bookmark.UpdatedAt,
		&lastUsedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get bookmark: %w", err)
	}

	if lastUsedAt.Valid {
		bookmark.LastUsedAt = &lastUsedAt.Time
	}

	return &bookmark, nil
}

// ListBookmarks retrieves all bookmarks ordered by title ascending
func (s *Storage) ListBookmarks(ctx context.Context) ([]models.Bookmark, error) {
	const query = `
SELECT id, title, endpoint, region, access_key_id, session_token, created_at, updated_at, last_used_at
FROM bookmarks
ORDER BY title`

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to list bookmarks: %w", err)
	}
	defer rows.Close()

	var bookmarks []models.Bookmark
	for rows.Next() {
		var bookmark models.Bookmark
		var lastUsedAt sql.NullTime

		err := rows.Scan(
			&bookmark.ID,
			&bookmark.Title,
			&bookmark.Endpoint,
			&bookmark.Region,
			&bookmark.AccessKeyID,
			&bookmark.SessionToken,
			&bookmark.CreatedAt,
			&bookmark.UpdatedAt,
			&lastUsedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan bookmark: %w", err)
		}

		if lastUsedAt.Valid {
			bookmark.LastUsedAt = &lastUsedAt.Time
		}

		bookmarks = append(bookmarks, bookmark)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate bookmarks: %w", err)
	}

	return bookmarks, nil
}

// DeleteBookmark deletes a bookmark by ID
func (s *Storage) DeleteBookmark(ctx context.Context, id int64) error {
	query := `DELETE FROM bookmarks WHERE id = ?`
	result, err := s.db.ExecContext(ctx, query, id)
	if err != nil {
		return fmt.Errorf("failed to delete bookmark: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("bookmark not found: %d", id)
	}

	return nil
}

// UpdateBookmarkLastUsed updates the last_used_at timestamp for a bookmark
func (s *Storage) UpdateBookmarkLastUsed(ctx context.Context, id int64) error {
	query := `UPDATE bookmarks SET last_used_at = ? WHERE id = ?`
	_, err := s.db.ExecContext(ctx, query, time.Now(), id)
	if err != nil {
		return fmt.Errorf("failed to update bookmark last used: %w", err)
	}
	return nil
}
