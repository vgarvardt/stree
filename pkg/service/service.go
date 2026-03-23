package service

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/cappuccinotm/slogx"

	"github.com/vgarvardt/stree/pkg/models"
	"github.com/vgarvardt/stree/pkg/s3client"
	"github.com/vgarvardt/stree/pkg/storage"
)

// Service encapsulates business logic for S3 operations, storage caching, and credential management.
// It is independent of any UI framework.
type Service struct {
	storage   *storage.Storage
	sessions  *storage.SessionManager
	credStore *storage.CredentialStore
	verbose   bool
	version   string

	s3Client       *s3client.Client
	sessionID      int64
	activeBookmark *models.Bookmark

	ctx      context.Context
	opCtx    context.Context
	opCancel context.CancelFunc
}

// New creates a new Service instance.
func New(stor *storage.Storage, sessions *storage.SessionManager, credStore *storage.CredentialStore, verbose bool, version string) *Service {
	return &Service{
		storage:   stor,
		sessions:  sessions,
		credStore: credStore,
		verbose:   verbose,
		version:   version,
	}
}

// SetContext sets the root context and creates an initial operation context.
func (s *Service) SetContext(ctx context.Context) {
	s.ctx = ctx
	s.resetOperationContext()
}

// OpCtx returns the current cancellable operation context.
func (s *Service) OpCtx() context.Context {
	return s.opCtx
}

// IsConnected returns true if there is an active S3 connection.
func (s *Service) IsConnected() bool {
	return s.s3Client != nil
}

// ActiveBookmark returns the currently active bookmark, or nil if disconnected.
func (s *Service) ActiveBookmark() *models.Bookmark {
	return s.activeBookmark
}

// SessionID returns the current storage session ID.
func (s *Service) SessionID() int64 {
	return s.sessionID
}

// resetOperationContext cancels any in-flight operations and creates a fresh context.
func (s *Service) resetOperationContext() {
	if s.opCancel != nil {
		s.opCancel()
	}
	s.opCtx, s.opCancel = context.WithCancel(s.ctx)
}

// Connect establishes a connection to an S3 endpoint using the specified bookmark title.
func (s *Service) Connect(ctx context.Context, bookmarkTitle string) error {
	// Find the bookmark by title
	bookmarks, err := s.storage.ListBookmarks(ctx)
	if err != nil {
		return fmt.Errorf("failed to load bookmarks: %w", err)
	}

	var bookmark *models.Bookmark
	for i := range bookmarks {
		if bookmarks[i].Title == bookmarkTitle {
			bookmark = &bookmarks[i]
			break
		}
	}

	if bookmark == nil {
		return fmt.Errorf("bookmark not found: %s", bookmarkTitle)
	}

	// Get secret key from credential store
	secretKey := ""
	if s.credStore != nil {
		secretKey, err = s.credStore.GetSecretKey(ctx, bookmark.ID)
		if err != nil {
			return fmt.Errorf("failed to retrieve secret key: %w", err)
		}
	}

	// Create S3 client
	s3Cfg := s3client.Config{
		Endpoint:     bookmark.Endpoint,
		Region:       bookmark.Region,
		AccessKey:    bookmark.AccessKeyID,
		SecretKey:    secretKey,
		SessionToken: bookmark.SessionToken,
		Debug:        s.verbose,
	}

	client, err := s3client.NewClient(context.Background(), s3Cfg, s.version)
	if err != nil {
		return fmt.Errorf("failed to create S3 client: %w", err)
	}

	// Store session
	sessionID, err := s.storage.UpsertSession(ctx, s3Cfg.String())
	if err != nil {
		return fmt.Errorf("failed to store session: %w", err)
	}

	// Update last used timestamp
	if err := s.storage.UpdateBookmarkLastUsed(ctx, bookmark.ID); err != nil {
		slog.Warn("Failed to update bookmark last used", slogx.Error(err))
	}

	// Cancel any in-flight operations from the previous connection
	s.resetOperationContext()

	// Update state
	s.s3Client = client
	s.sessionID = sessionID
	s.activeBookmark = bookmark

	slog.Info("Connected to bookmark",
		slog.String("title", bookmark.Title),
		slog.String("endpoint", bookmark.Endpoint),
		slog.Int64("session-id", sessionID),
	)

	return nil
}

// Disconnect closes the current S3 connection and clears state.
func (s *Service) Disconnect() {
	if s.opCancel != nil {
		s.opCancel()
	}

	s.s3Client = nil
	s.sessionID = 0
	s.activeBookmark = nil

	slog.Info("Disconnected from S3")
}
