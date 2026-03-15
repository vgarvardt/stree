package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// UpsertSession creates or updates a session, returning the session ID
func (s *Storage) UpsertSession(ctx context.Context, configStr string) (int64, error) {
	now := time.Now()

	// Try to update existing session
	result, err := s.db.ExecContext(ctx,
		`UPDATE sessions SET updated_at = ? WHERE config_str = ?`,
		now, configStr,
	)
	if err != nil {
		return 0, fmt.Errorf("failed to update session: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected > 0 {
		// Get the existing session ID
		var sessionID int64
		err := s.db.QueryRowContext(ctx,
			`SELECT id FROM sessions WHERE config_str = ?`,
			configStr,
		).Scan(&sessionID)
		if err != nil {
			return 0, fmt.Errorf("failed to get session ID: %w", err)
		}
		return sessionID, nil
	}

	// Insert new session
	result, err = s.db.ExecContext(ctx,
		`INSERT INTO sessions (config_str, created_at, updated_at) VALUES (?, ?, ?)`,
		configStr, now, now,
	)
	if err != nil {
		return 0, fmt.Errorf("failed to insert session: %w", err)
	}

	sessionID, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("failed to get last insert ID: %w", err)
	}

	return sessionID, nil
}

// GetSession retrieves a session by config string
func (s *Storage) GetSession(ctx context.Context, configStr string) (*Session, error) {
	var session Session
	err := s.db.QueryRowContext(ctx,
		`SELECT id, config_str, buckets_refreshed_at, created_at, updated_at FROM sessions WHERE config_str = ?`,
		configStr,
	).Scan(&session.ID, &session.ConfigStr, &session.BucketsRefreshedAt, &session.CreatedAt, &session.UpdatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}

		return nil, fmt.Errorf("failed to get session: %w", err)
	}

	return &session, nil
}

// GetSessionByID retrieves a session by its ID
func (s *Storage) GetSessionByID(ctx context.Context, sessionID int64) (*Session, error) {
	var session Session
	err := s.db.QueryRowContext(ctx,
		`SELECT id, config_str, buckets_refreshed_at, created_at, updated_at FROM sessions WHERE id = ?`,
		sessionID,
	).Scan(&session.ID, &session.ConfigStr, &session.BucketsRefreshedAt, &session.CreatedAt, &session.UpdatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}

		return nil, fmt.Errorf("failed to get session by ID: %w", err)
	}

	return &session, nil
}

// UpdateSessionBucketsRefreshedAt updates the buckets_refreshed_at timestamp for a session
func (s *Storage) UpdateSessionBucketsRefreshedAt(ctx context.Context, sessionID int64, refreshedAt time.Time) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE sessions SET buckets_refreshed_at = ?, updated_at = ? WHERE id = ?`,
		refreshedAt, time.Now(), sessionID,
	)
	if err != nil {
		return fmt.Errorf("failed to update session buckets_refreshed_at: %w", err)
	}
	return nil
}

// InvalidateSession deletes a session and all associated data (cascades to buckets and objects),
// creates a new session with the same config string, and returns the new session ID.
func (s *Storage) InvalidateSession(ctx context.Context, sessionID int64) (int64, error) {
	// Get the config string of the existing session
	var configStr string
	err := s.db.QueryRowContext(ctx,
		`SELECT config_str FROM sessions WHERE id = ?`,
		sessionID,
	).Scan(&configStr)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, fmt.Errorf("session not found")
		}
		return 0, fmt.Errorf("failed to get session config: %w", err)
	}

	now := time.Now()

	var newSessionID int64
	return newSessionID, transactional(ctx, s.db, func(ctx context.Context, tx *sql.Tx) error {
		// Delete the existing session (cascades to buckets and objects)
		_, err := tx.ExecContext(ctx, `DELETE FROM sessions WHERE id = ?`, sessionID)
		if err != nil {
			return fmt.Errorf("failed to delete session: %w", err)
		}

		// Insert a new session with the same config string
		result, err := tx.ExecContext(ctx,
			`INSERT INTO sessions (config_str, created_at, updated_at) VALUES (?, ?, ?)`,
			configStr, now, now,
		)
		if err != nil {
			return fmt.Errorf("failed to insert new session: %w", err)
		}

		newSessionID, err = result.LastInsertId()
		if err != nil {
			return fmt.Errorf("failed to get last insert ID: %w", err)
		}

		return nil
	})
}

// DeleteSession deletes a session and all associated data (cascades to buckets and objects)
func (s *Storage) DeleteSession(ctx context.Context, sessionID int64) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM sessions WHERE id = ?`, sessionID)
	if err != nil {
		return fmt.Errorf("failed to delete session: %w", err)
	}
	return nil
}
