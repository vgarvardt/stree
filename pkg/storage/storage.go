package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/goccy/go-json"
	_ "modernc.org/sqlite" // CGO-free SQLite driver

	"github.com/vgarvardt/stree/pkg/models"
)

// schemaVersion represents the current database schema version.
// Increment this constant when making backward-incompatible changes to the schema.
// When the version doesn't match, the database will be recreated from scratch.
const schemaVersion = 2

// Config holds configuration for storage initialization
type Config struct {
	DSN   string // Database connection string (e.g., "./storage.db" or ":memory:")
	Purge bool   // If true, removes the storage file before initialization (only for file-based storage)
}

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

// New creates a new Storage instance with the provided configuration
func New(ctx context.Context, config Config) (*Storage, error) {
	// Purge database file if needed (version mismatch or explicit purge request)
	if err := purgeIfNeeded(ctx, config); err != nil {
		return nil, fmt.Errorf("failed to purge database: %w", err)
	}

	db, err := sql.Open("sqlite", config.DSN)
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
		db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	storage := &Storage{db: db}

	// Initialize schema
	if err := storage.initSchema(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	// Set the schema version
	if err := storage.setSchemaVersion(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to set schema version: %w", err)
	}

	return storage, nil
}

// purgeIfNeeded checks if the database file needs to be purged and removes it if necessary
// Returns nil if no purge is needed or if purge was successful
func purgeIfNeeded(ctx context.Context, config Config) error {
	// In-memory databases cannot be purged, skip all checks
	if config.DSN == ":memory:" {
		return nil
	}

	// Check if we need to purge the database file
	shouldPurge := config.Purge

	// If not explicitly purging, check if database version mismatches
	if !shouldPurge {
		mismatch, err := isDBVersionMismatch(ctx, config.DSN)
		if err != nil {
			return fmt.Errorf("failed to check database version: %w", err)
		}
		shouldPurge = mismatch
	}

	// Purge the database file if needed
	if shouldPurge {
		if err := os.Remove(config.DSN); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("failed to remove storage file: %w", err)
		}
	}

	return nil
}

// isDBVersionMismatch checks if the database file exists and has a mismatched version
// Returns true if the database should be purged and recreated
func isDBVersionMismatch(ctx context.Context, dsn string) (bool, error) {
	// Check if file exists
	if _, err := os.Stat(dsn); os.IsNotExist(err) {
		// File doesn't exist, no mismatch (will be created fresh)
		return false, nil
	}

	// Open database temporarily to check version
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return false, fmt.Errorf("failed to open database for version check: %w", err)
	}
	defer db.Close()

	// Try to read the version
	var version int
	err = db.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version)
	if err != nil {
		// If we can't read the version, assume it's corrupted or incompatible
		return true, nil
	}

	// Return true if version doesn't match (needs purge)
	return version != schemaVersion, nil
}

// setSchemaVersion sets the current schema version in the database
func (s *Storage) setSchemaVersion(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, fmt.Sprintf("PRAGMA user_version = %d", schemaVersion))
	if err != nil {
		return fmt.Errorf("failed to set schema version: %w", err)
	}
	return nil
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

-- JSON field indexes for efficient filtering and ordering
CREATE INDEX IF NOT EXISTS idx_objects_size ON objects(json_extract(properties, '$.size'));
CREATE INDEX IF NOT EXISTS idx_objects_last_modified ON objects(json_extract(properties, '$.last_modified'));
CREATE INDEX IF NOT EXISTS idx_objects_is_delete_marker ON objects(json_extract(properties, '$.is_delete_marker'));

CREATE TABLE IF NOT EXISTS bookmarks (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	session_id INTEGER NOT NULL,
	name TEXT NOT NULL,
	url TEXT NOT NULL,
	created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
	updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
	FOREIGN KEY (session_id) REFERENCES sessions(id) ON DELETE CASCADE,
	UNIQUE(session_id, name)
);

CREATE INDEX IF NOT EXISTS idx_bookmarks_session_id ON bookmarks(session_id);
CREATE INDEX IF NOT EXISTS idx_bookmarks_name ON bookmarks(name);
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
func (s *Storage) UpsertBucket(ctx context.Context, sessionID int64, name string, creationDate time.Time, details models.BucketDetails) error {
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

// DeleteBucket deletes a specific bucket and all associated objects (cascades)
func (s *Storage) DeleteBucket(ctx context.Context, sessionID int64, name string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM buckets WHERE session_id = ? AND name = ?`, sessionID, name)
	if err != nil {
		return fmt.Errorf("failed to delete bucket: %w", err)
	}
	return nil
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

// DeleteObjectsByBucket deletes all objects for a specific bucket
func (s *Storage) DeleteObjectsByBucket(ctx context.Context, bucketID int64) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM objects WHERE bucket_id = ?`, bucketID)
	if err != nil {
		return fmt.Errorf("failed to delete objects: %w", err)
	}
	return nil
}

// OrderByField specifies which field to order objects by
type OrderByField string

const (
	OrderByID           OrderByField = ""
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
func (s *Storage) ListObjectsByBucket(ctx context.Context, bucketID int64, opts ObjectListOptions) ([]Object, error) {
	query := `SELECT id, bucket_id, properties, created_at, updated_at FROM objects WHERE bucket_id = ?`
	args := []any{bucketID}

	// Add filter for delete markers if specified
	if opts.FilterDeleteMarker != nil {
		if *opts.FilterDeleteMarker {
			query += ` AND json_extract(properties, '$.is_delete_marker') = 1`
		} else {
			query += ` AND json_extract(properties, '$.is_delete_marker') = 0`
		}
	}

	// Add ordering
	orderDir := "ASC"
	if opts.OrderDesc {
		orderDir = "DESC"
	}

	switch opts.OrderBy {
	case OrderBySize:
		query += ` ORDER BY json_extract(properties, '$.size') ` + orderDir
	case OrderByLastModified:
		query += ` ORDER BY json_extract(properties, '$.last_modified') ` + orderDir
	case OrderByID:
		fallthrough
	default:
		query += ` ORDER BY id ` + orderDir
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

// BulkInsertObjectVersions inserts multiple object versions in a single transaction
func (s *Storage) BulkInsertObjectVersions(ctx context.Context, bucketID int64, versions []models.ObjectVersion) error {
	if len(versions) == 0 {
		return nil
	}

	return transactional(ctx, s.db, func(ctx context.Context, tx *sql.Tx) error {
		stmt, err := tx.PrepareContext(ctx,
			`INSERT INTO objects (bucket_id, properties, created_at, updated_at) VALUES (?, ?, ?, ?)`,
		)
		if err != nil {
			return fmt.Errorf("failed to prepare statement: %w", err)
		}
		defer stmt.Close()

		now := time.Now()
		for _, version := range versions {
			propertiesJSON, err := json.Marshal(version)
			if err != nil {
				return fmt.Errorf("failed to marshal object version: %w", err)
			}

			_, err = stmt.ExecContext(ctx, bucketID, propertiesJSON, now, now)
			if err != nil {
				return fmt.Errorf("failed to insert object version: %w", err)
			}
		}

		return nil
	})
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
