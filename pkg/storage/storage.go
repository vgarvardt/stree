package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"time"

	"github.com/goccy/go-json"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/sqlite"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	_ "modernc.org/sqlite" // CGO-free SQLite driver

	"github.com/vgarvardt/stree/pkg/models"
	"github.com/vgarvardt/stree/pkg/storage/migrations"
)

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
	ID                 int64
	ConfigStr          string
	BucketsRefreshedAt *time.Time
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

// Bucket represents a cached S3 bucket
type Bucket struct {
	ID           int64
	SessionID    int64
	Name         string
	CreationDate time.Time                // Bucket creation date from S3
	Details      json.RawMessage          // JSON field for bucket metadata
	Encryption   *models.BucketEncryption // Bucket encryption configuration (nullable)
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// buildDSN constructs a SQLite DSN with per-connection PRAGMAs.
// The modernc driver applies _pragma parameters on every new connection,
// ensuring all connections in the pool are consistently configured.
func buildDSN(dsn string) string {
	pragmas := url.Values{
		"_pragma": {
			"busy_timeout(5000)",       // 5s timeout for locked database
			"synchronous(normal)",      // safe with WAL, much faster writes
			"cache_size(-64000)",       // 64MB cache (default is ~2MB)
			"mmap_size(268435456)",     // 256MB memory-mapped I/O
			"foreign_keys(on)",         // required for CASCADE deletes
		},
	}

	if dsn == ":memory:" {
		return "file::memory:?" + pragmas.Encode()
	}
	return "file:" + dsn + "?" + pragmas.Encode()
}

// New creates a new Storage instance with the provided configuration
func New(ctx context.Context, config Config) (*Storage, error) {
	// Purge database file if explicitly requested
	if config.Purge && config.DSN != ":memory:" {
		if err := os.Remove(config.DSN); err != nil && !os.IsNotExist(err) {
			return nil, fmt.Errorf("failed to remove storage file: %w", err)
		}
	}

	dsn := buildDSN(config.DSN)

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Configure connection pool for SQLite with WAL mode:
	// multiple readers can run concurrently, but only one writer at a time.
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(0)

	// journal_mode is database-wide (not per-connection), so set it once after opening.
	if _, err := db.ExecContext(ctx, "PRAGMA journal_mode = WAL"); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to enable WAL journal mode: %w", err)
	}

	// Test connection
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	storage := &Storage{db: db}

	// Run migrations
	if err := storage.runMigrations(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to run migrations: %w", err)
	}

	return storage, nil
}

// runMigrations applies all pending database migrations
func (s *Storage) runMigrations() error {
	// Create migration source from embedded files
	source, err := iofs.New(migrations.FS, ".")
	if err != nil {
		return fmt.Errorf("failed to create migration source: %w", err)
	}

	// Create database driver for sqlite
	driver, err := sqlite.WithInstance(s.db, &sqlite.Config{})
	if err != nil {
		return fmt.Errorf("failed to create migration driver: %w", err)
	}

	// Create migrate instance
	m, err := migrate.NewWithInstance("iofs", source, "sqlite", driver)
	if err != nil {
		return fmt.Errorf("failed to create migrate instance: %w", err)
	}

	// Run migrations
	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("failed to apply migrations: %w", err)
	}

	return nil
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
