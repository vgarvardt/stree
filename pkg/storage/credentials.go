package storage

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/zalando/go-keyring"
)

const (
	// KeyringService is the service name used for storing credentials in the OS keychain
	KeyringService = "stree-s3-browser"
)

// CredentialStore provides secure storage for sensitive credentials using OS keychain
type CredentialStore struct {
	service string
}

// NewCredentialStore creates a new credential store
func NewCredentialStore() *CredentialStore {
	return &CredentialStore{
		service: KeyringService,
	}
}

// StoreSecretKey stores the S3 secret key for a bookmark in the OS keychain
// The key is stored as "bookmark-{bookmarkID}"
func (cs *CredentialStore) StoreSecretKey(_ context.Context, bookmarkID int64, secretKey string) error {
	key := fmt.Sprintf("bookmark-%d", bookmarkID)
	if err := keyring.Set(cs.service, key, secretKey); err != nil {
		return fmt.Errorf("failed to store secret key in keychain: %w", err)
	}
	slog.Debug("Stored secret key in keychain", slog.Int64("bookmark_id", bookmarkID))
	return nil
}

// GetSecretKey retrieves the S3 secret key for a bookmark from the OS keychain
func (cs *CredentialStore) GetSecretKey(_ context.Context, bookmarkID int64) (string, error) {
	key := fmt.Sprintf("bookmark-%d", bookmarkID)
	secret, err := keyring.Get(cs.service, key)
	if err != nil {
		if errors.Is(err, keyring.ErrNotFound) {
			return "", fmt.Errorf("secret key not found for bookmark %d", bookmarkID)
		}
		return "", fmt.Errorf("failed to get secret key from keychain: %w", err)
	}
	slog.Debug("Retrieved secret key from keychain", slog.Int64("bookmark_id", bookmarkID))
	return secret, nil
}

// DeleteSecretKey removes the S3 secret key for a bookmark from the OS keychain
func (cs *CredentialStore) DeleteSecretKey(_ context.Context, bookmarkID int64) error {
	key := fmt.Sprintf("bookmark-%d", bookmarkID)
	if err := keyring.Delete(cs.service, key); err != nil {
		// Don't fail if the key doesn't exist
		if errors.Is(err, keyring.ErrNotFound) {
			slog.Debug("Secret key not found in keychain (already deleted?)", slog.Int64("bookmark_id", bookmarkID))
			return nil
		}
		return fmt.Errorf("failed to delete secret key from keychain: %w", err)
	}
	slog.Debug("Deleted secret key from keychain", slog.Int64("bookmark_id", bookmarkID))
	return nil
}

// TestKeychain tests if the keychain is accessible and working
func (cs *CredentialStore) TestKeychain(_ context.Context) error {
	testKey := "test-key"
	testValue := "test-value"

	// Try to set a test value
	if err := keyring.Set(cs.service, testKey, testValue); err != nil {
		return fmt.Errorf("keychain set test failed: %w", err)
	}

	// Try to get it back
	value, err := keyring.Get(cs.service, testKey)
	if err != nil {
		return fmt.Errorf("keychain get test failed: %w", err)
	}

	if value != testValue {
		return fmt.Errorf("keychain test failed: expected %q, got %q", testValue, value)
	}

	// Clean up
	_ = keyring.Delete(cs.service, testKey)

	slog.Info("Keychain test successful")
	return nil
}
