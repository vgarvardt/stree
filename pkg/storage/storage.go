package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/goccy/go-json"
	_ "modernc.org/sqlite" // CGO-free SQLite driver
)

// Storage manages the SQLite database for caching S3 data
type Storage struct {
	db *sql.DB
}

// Session represents an S3 connection session
type Session struct {
	ID        int64
	ConfigStr string
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Bucket represents a cached S3 bucket
type Bucket struct {
	ID           int64
	SessionID    int64
	Name         string
	CreationDate time.Time       // Bucket creation date from S3
	Details      json.RawMessage // JSON field for bucket metadata
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// Object represents a cached S3 object
type Object struct {
	ID         int64
	BucketID   int64
	Properties json.RawMessage // JSON field for object properties
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

// New creates a new Storage instance with an in-memory SQLite database
func New(ctx context.Context) (*Storage, error) {
	// TODO: Replace with persistent file storage
	// For now, using in-memory database: ":memory:"
	// Future: use file path like "file:stree.db?cache=shared&mode=rwc"
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Enable foreign key constraints (required for CASCADE deletes)
	if _, err := db.ExecContext(ctx, "PRAGMA foreign_keys = ON"); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to enable foreign keys: %w", err)
	}

	// Test connection
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	storage := &Storage{db: db}

	// Initialize schema
	if err := storage.initSchema(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	return storage, nil
}

// initSchema creates the database tables
func (s *Storage) initSchema(ctx context.Context) error {
	schema := `
	CREATE TABLE IF NOT EXISTS sessions (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		config_str TEXT NOT NULL UNIQUE,
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	);

	CREATE INDEX IF NOT EXISTS idx_sessions_config_str ON sessions(config_str);

	CREATE TABLE IF NOT EXISTS buckets (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		session_id INTEGER NOT NULL,
		name TEXT NOT NULL,
		creation_date DATETIME NOT NULL, -- Bucket creation date from S3
		details TEXT NOT NULL, -- JSON field
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		FOREIGN KEY (session_id) REFERENCES sessions(id) ON DELETE CASCADE,
		UNIQUE(session_id, name)
	);

	CREATE INDEX IF NOT EXISTS idx_buckets_session_id ON buckets(session_id);
	CREATE INDEX IF NOT EXISTS idx_buckets_name ON buckets(name);
	CREATE INDEX IF NOT EXISTS idx_buckets_creation_date ON buckets(creation_date);

	CREATE TABLE IF NOT EXISTS objects (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		bucket_id INTEGER NOT NULL,
		properties TEXT NOT NULL, -- JSON field
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		FOREIGN KEY (bucket_id) REFERENCES buckets(id) ON DELETE CASCADE
	);

	CREATE INDEX IF NOT EXISTS idx_objects_bucket_id ON objects(bucket_id);
	`

	_, err := s.db.ExecContext(ctx, schema)
	return err
}

// Close closes the database connection
func (s *Storage) Close() error {
	return s.db.Close()
}

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
		`SELECT id, config_str, created_at, updated_at FROM sessions WHERE config_str = ?`,
		configStr,
	).Scan(&session.ID, &session.ConfigStr, &session.CreatedAt, &session.UpdatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}

		return nil, fmt.Errorf("failed to get session: %w", err)
	}

	return &session, nil
}

// UpsertBucket creates or updates a bucket
func (s *Storage) UpsertBucket(ctx context.Context, sessionID int64, name string, creationDate time.Time, details any) error {
	detailsJSON, err := json.Marshal(details)
	if err != nil {
		return fmt.Errorf("failed to marshal bucket details: %w", err)
	}

	now := time.Now()

	// Try to update existing bucket
	result, err := s.db.ExecContext(ctx,
		`UPDATE buckets SET details = ?, updated_at = ?, creation_date = ? WHERE session_id = ? AND name = ?`,
		detailsJSON, now, creationDate, sessionID, name,
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
		`INSERT INTO buckets (session_id, name, creation_date, details, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)`,
		sessionID, name, creationDate, detailsJSON, now, now,
	)
	if err != nil {
		return fmt.Errorf("failed to insert bucket: %w", err)
	}

	return nil
}

// GetBucketsBySession retrieves all buckets for a session
func (s *Storage) GetBucketsBySession(ctx context.Context, sessionID int64) ([]Bucket, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, session_id, name, creation_date, details, created_at, updated_at FROM buckets WHERE session_id = ? ORDER BY name`,
		sessionID,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to query buckets: %w", err)
	}
	defer rows.Close()

	var buckets []Bucket
	for rows.Next() {
		var bucket Bucket
		if err := rows.Scan(&bucket.ID, &bucket.SessionID, &bucket.Name, &bucket.CreationDate, &bucket.Details, &bucket.CreatedAt, &bucket.UpdatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan bucket: %w", err)
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
	err := s.db.QueryRowContext(ctx,
		`SELECT id, session_id, name, creation_date, details, created_at, updated_at FROM buckets WHERE session_id = ? AND name = ?`,
		sessionID, name,
	).Scan(&bucket.ID, &bucket.SessionID, &bucket.Name, &bucket.CreationDate, &bucket.Details, &bucket.CreatedAt, &bucket.UpdatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get bucket: %w", err)
	}

	return &bucket, nil
}

// UpsertObject creates or updates an object
func (s *Storage) UpsertObject(ctx context.Context, bucketID int64, properties any) (int64, error) {
	propertiesJSON, err := json.Marshal(properties)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal object properties: %w", err)
	}

	now := time.Now()

	// For now, just insert a new object
	// TODO: In the future, we might want to update based on object key
	result, err := s.db.ExecContext(ctx,
		`INSERT INTO objects (bucket_id, properties, created_at, updated_at) VALUES (?, ?, ?, ?)`,
		bucketID, propertiesJSON, now, now,
	)
	if err != nil {
		return 0, fmt.Errorf("failed to insert object: %w", err)
	}

	objectID, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("failed to get last insert ID: %w", err)
	}

	return objectID, nil
}

// GetObjectsByBucket retrieves all objects for a bucket
func (s *Storage) GetObjectsByBucket(ctx context.Context, bucketID int64) ([]Object, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, bucket_id, properties, created_at, updated_at FROM objects WHERE bucket_id = ? ORDER BY created_at DESC`,
		bucketID,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to query objects: %w", err)
	}
	defer rows.Close()

	var objects []Object
	for rows.Next() {
		var obj Object
		if err := rows.Scan(&obj.ID, &obj.BucketID, &obj.Properties, &obj.CreatedAt, &obj.UpdatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan object: %w", err)
		}
		objects = append(objects, obj)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	return objects, nil
}

// DeleteSession deletes a session and all associated data (cascades to buckets and objects)
func (s *Storage) DeleteSession(ctx context.Context, sessionID int64) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM sessions WHERE id = ?`, sessionID)
	if err != nil {
		return fmt.Errorf("failed to delete session: %w", err)
	}
	return nil
}
