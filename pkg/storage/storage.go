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
			"busy_timeout(5000)",   // 5s timeout for locked database
			"synchronous(normal)",  // safe with WAL, much faster writes
			"cache_size(-64000)",   // 64MB cache (default is ~2MB)
			"mmap_size(268435456)", // 256MB memory-mapped I/O
			"foreign_keys(on)",     // required for CASCADE deletes
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
