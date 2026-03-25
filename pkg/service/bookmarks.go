package service

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/aws/smithy-go/ptr"
	"github.com/cappuccinotm/slogx"

	"github.com/vgarvardt/stree/pkg/models"
	"github.com/vgarvardt/stree/pkg/s3client"
)

// ListBookmarks returns all saved bookmarks.
func (s *Service) ListBookmarks(ctx context.Context) ([]models.Bookmark, error) {
	return s.storage.ListBookmarks(ctx)
}

// BookmarkTitleExists checks if a bookmark with the given title already exists,
// optionally excluding a specific bookmark ID (for edit scenarios).
func (s *Service) BookmarkTitleExists(ctx context.Context, title string, excludeID int64) (bool, error) {
	return s.storage.BookmarkTitleExists(ctx, title, excludeID)
}

// SaveBookmark creates or updates a bookmark and stores its secret key.
func (s *Service) SaveBookmark(ctx context.Context, bookmark *models.Bookmark, secretKey string) error {
	if err := s.storage.UpsertBookmark(ctx, bookmark); err != nil {
		return fmt.Errorf("failed to save bookmark: %w", err)
	}

	if s.credStore != nil {
		if err := s.credStore.StoreSecretKey(ctx, bookmark.ID, secretKey); err != nil {
			return fmt.Errorf("failed to save secret key: %w", err)
		}
	}

	slog.Info("Bookmark saved", slog.Int64("id", bookmark.ID), slog.String("title", bookmark.Title))
	return nil
}

// DeleteBookmark deletes a bookmark and its secret key.
// Returns true if the deleted bookmark was the active one.
func (s *Service) DeleteBookmark(ctx context.Context, bookmarkID int64) (wasActive bool, err error) {
	if err := s.storage.DeleteBookmark(ctx, bookmarkID); err != nil {
		return false, fmt.Errorf("failed to delete bookmark: %w", err)
	}

	if s.credStore != nil {
		if err := s.credStore.DeleteSecretKey(ctx, bookmarkID); err != nil {
			slog.Warn("Failed to delete secret key from credential store", slogx.Error(err))
		}
	}

	wasActive = s.activeBookmark != nil && s.activeBookmark.ID == bookmarkID

	slog.Info("Bookmark deleted", slog.Int64("id", bookmarkID))
	return wasActive, nil
}

// GetSecretKey retrieves the secret key for a bookmark from the credential store.
func (s *Service) GetSecretKey(ctx context.Context, bookmarkID int64) (string, error) {
	if s.credStore == nil {
		return "", nil
	}
	return s.credStore.GetSecretKey(ctx, bookmarkID)
}

// TestConnection tests connectivity to an S3 endpoint using the provided credentials.
func (s *Service) TestConnection(ctx context.Context, endpoint, region, accessKey, secretKey, sessionToken string) error {
	cfg := s3client.Config{
		Endpoint:     endpoint,
		Region:       region,
		AccessKey:    accessKey,
		SecretKey:    secretKey,
		SessionToken: sessionToken,
		Debug:        s.verbose,
	}

	client, err := s3client.NewClient(ctx, cfg, s.version)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}

	_, err = client.ListBuckets(ctx, ptr.Int32(1))
	return err
}
