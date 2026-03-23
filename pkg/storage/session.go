package storage

import (
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/sqlite"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	_ "modernc.org/sqlite" // CGO-free SQLite driver

	"github.com/vgarvardt/stree/pkg/storage/migrations"
)

// SessionManager manages per-bucket SQLite databases organized under session directories.
// Directory layout: {baseDir}/{sessionID}/bucket-{bucketID}.db
type SessionManager struct {
	baseDir string
	mu      sync.Mutex
	open    map[bucketKey]*BucketDB
}

type bucketKey struct {
	sessionID int64
	bucketID  int64
}

// BucketDB wraps a SQLite database for a single bucket's cached objects and multipart uploads.
type BucketDB struct {
	db   *sql.DB
	path string
}

// NewSessionManager creates a new SessionManager rooted at the given base directory.
func NewSessionManager(baseDir string) *SessionManager {
	return &SessionManager{
		baseDir: baseDir,
		open:    make(map[bucketKey]*BucketDB),
	}
}

// bucketDBPath returns the file path for a bucket's database.
func (m *SessionManager) bucketDBPath(sessionID, bucketID int64) string {
	return filepath.Join(m.baseDir, strconv.FormatInt(sessionID, 10), fmt.Sprintf("bucket-%d.db", bucketID))
}

// sessionDir returns the directory path for a session.
func (m *SessionManager) sessionDir(sessionID int64) string {
	return filepath.Join(m.baseDir, strconv.FormatInt(sessionID, 10))
}

// buildBucketDSN constructs a SQLite DSN with per-connection PRAGMAs optimized for cache data.
func buildBucketDSN(path string) string {
	pragmas := url.Values{
		"_pragma": {
			"busy_timeout(5000)",   // 5s timeout for locked database
			"synchronous(off)",     // cache data — re-fetchable from S3, no durability needed
			"cache_size(-16000)",   // 16MB cache
			"mmap_size(134217728)", // 128MB memory-mapped I/O
			"page_size(8192)",      // consistent with main DB
		},
	}
	return "file:" + path + "?" + pragmas.Encode()
}

// Open returns a BucketDB for the given session and bucket, creating the database file
// and running migrations if needed. If the BucketDB is already open, it is returned directly.
func (m *SessionManager) Open(sessionID, bucketID int64) (*BucketDB, error) {
	key := bucketKey{sessionID: sessionID, bucketID: bucketID}

	m.mu.Lock()
	defer m.mu.Unlock()

	if bdb, ok := m.open[key]; ok {
		return bdb, nil
	}

	dbPath := m.bucketDBPath(sessionID, bucketID)

	// Ensure session directory exists
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o700); err != nil {
		return nil, fmt.Errorf("failed to create session directory: %w", err)
	}

	dsn := buildBucketDSN(dbPath)

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open bucket database: %w", err)
	}

	// Single connection is sufficient for a per-bucket cache DB
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)

	// Enable WAL mode
	if _, err := db.Exec("PRAGMA journal_mode = WAL"); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to enable WAL journal mode: %w", err)
	}

	// Run bucket migrations
	if err := runBucketMigrations(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to run bucket migrations: %w", err)
	}

	bdb := &BucketDB{db: db, path: dbPath}
	m.open[key] = bdb
	return bdb, nil
}

// runBucketMigrations applies pending bucket-level migrations to the given database.
func runBucketMigrations(db *sql.DB) error {
	source, err := iofs.New(migrations.BucketFS, "bucket")
	if err != nil {
		return fmt.Errorf("failed to create migration source: %w", err)
	}

	driver, err := sqlite.WithInstance(db, &sqlite.Config{})
	if err != nil {
		return fmt.Errorf("failed to create migration driver: %w", err)
	}

	m, err := migrate.NewWithInstance("iofs", source, "sqlite", driver)
	if err != nil {
		return fmt.Errorf("failed to create migrate instance: %w", err)
	}

	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("failed to apply bucket migrations: %w", err)
	}

	return nil
}

// DeleteBucket closes the bucket's database connection (if open) and removes its file.
func (m *SessionManager) DeleteBucket(sessionID, bucketID int64) error {
	key := bucketKey{sessionID: sessionID, bucketID: bucketID}

	m.mu.Lock()
	defer m.mu.Unlock()

	if bdb, ok := m.open[key]; ok {
		if err := bdb.db.Close(); err != nil {
			slog.Warn("Failed to close bucket DB before deletion",
				slog.Int64("session-id", sessionID),
				slog.Int64("bucket-id", bucketID))
		}
		delete(m.open, key)
	}

	dbPath := m.bucketDBPath(sessionID, bucketID)

	// Remove the database file and WAL/SHM files
	for _, suffix := range []string{"", "-wal", "-shm"} {
		if err := os.Remove(dbPath + suffix); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("failed to remove bucket database file %s: %w", dbPath+suffix, err)
		}
	}

	return nil
}

// DeleteSession closes all bucket databases for a session and removes the session directory.
func (m *SessionManager) DeleteSession(sessionID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Close all open bucket DBs for this session
	for key, bdb := range m.open {
		if key.sessionID == sessionID {
			if err := bdb.db.Close(); err != nil {
				slog.Warn("Failed to close bucket DB during session deletion",
					slog.Int64("session-id", sessionID),
					slog.Int64("bucket-id", key.bucketID))
			}
			delete(m.open, key)
		}
	}

	// Remove the entire session directory
	dir := m.sessionDir(sessionID)
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("failed to remove session directory: %w", err)
	}

	return nil
}

// Close closes all open bucket database connections.
func (m *SessionManager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var errs []error
	for key, bdb := range m.open {
		if err := bdb.db.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close bucket DB (session=%d, bucket=%d): %w", key.sessionID, key.bucketID, err))
		}
		delete(m.open, key)
	}

	return errors.Join(errs...)
}
