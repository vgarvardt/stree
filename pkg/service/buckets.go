package service

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/cappuccinotm/slogx"
	"github.com/goccy/go-json"

	"github.com/vgarvardt/stree/pkg/models"
)

// SortMode represents the bucket sorting mode.
type SortMode int

const (
	SortNameAsc SortMode = iota
	SortNameDesc
	SortDateAsc
	SortDateDesc
)

const (
	LabelSortByNameAsc  = "Name ↓"
	LabelSortByNameDesc = "Name ↑"
	LabelSortByDateAsc  = "Date ↓"
	LabelSortByDateDesc = "Date ↑"
)

// String returns the display name for the sort mode.
func (s SortMode) String() string {
	switch s {
	case SortNameAsc:
		return LabelSortByNameAsc
	case SortNameDesc:
		return LabelSortByNameDesc
	case SortDateAsc:
		return LabelSortByDateAsc
	case SortDateDesc:
		return LabelSortByDateDesc
	default:
		return LabelSortByNameAsc
	}
}

// BucketsLoadResult represents the result of loading buckets.
type BucketsLoadResult struct {
	Buckets     []models.Bucket
	FromCache   bool
	RefreshedAt *time.Time
}

// BucketsProgress represents the current progress of the encryption enrichment.
type BucketsProgress struct {
	CurrentIdx int
	TotalCount int
}

// LoadBuckets loads buckets from cache or S3. If from cache, returns immediately.
// If from S3, returns the raw bucket list (without encryption info).
func (s *Service) LoadBuckets(ctx context.Context) (*BucketsLoadResult, error) {
	// Check if the session already has a cached buckets list
	session, err := s.storage.GetSessionByID(ctx, s.sessionID)
	if err != nil {
		slog.Warn("Failed to get session", slogx.Error(err))
	}

	if session != nil && session.BucketsRefreshedAt != nil {
		// Buckets were previously loaded - use cached data from DB
		slog.Info("Loading buckets from DB cache", slog.Time("refreshed-at", *session.BucketsRefreshedAt))

		storedBuckets, err := s.storage.GetBucketsBySession(ctx, s.sessionID)
		if err != nil {
			return nil, fmt.Errorf("failed to load buckets from cache: %w", err)
		}

		buckets := make([]models.Bucket, 0, len(storedBuckets))
		for _, sb := range storedBuckets {
			bucket := models.Bucket{
				Name:         sb.Name,
				CreationDate: sb.CreationDate,
				Encryption:   sb.Encryption,
			}
			buckets = append(buckets, bucket)
		}

		slog.Info("Loaded buckets from DB cache",
			slog.Int("count", len(buckets)),
			slog.Time("refreshed-at", *session.BucketsRefreshedAt),
		)

		return &BucketsLoadResult{
			Buckets:     buckets,
			FromCache:   true,
			RefreshedAt: session.BucketsRefreshedAt,
		}, nil
	}

	// No cached data - fetch from S3
	slog.Info("Loading S3 buckets")

	buckets, err := s.s3Client.ListBuckets(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to load buckets: %w", err)
	}

	return &BucketsLoadResult{
		Buckets:   buckets,
		FromCache: false,
	}, nil
}

// StoreBucketsWithEncryption fetches encryption info for each bucket, stores them in the DB,
// and updates the session timestamp. The progress callback is called periodically.
// Returns the enriched buckets slice (same slice, mutated in place).
func (s *Service) StoreBucketsWithEncryption(ctx context.Context, buckets []models.Bucket, progress func(BucketsProgress)) ([]models.Bucket, error) {
	slog.Info("Reading encryption information and storing buckets to the storage", slog.Int("count", len(buckets)))

	lastProgressUpdate := time.Now()

	for i := range buckets {
		if ctx.Err() != nil {
			return buckets, ctx.Err()
		}

		// Fetch encryption configuration from S3
		encryptionCfg, err := s.s3Client.GetBucketEncryption(ctx, buckets[i].Name)
		if err != nil {
			slog.Error("Failed to get bucket encryption", slogx.Error(err), slog.String("bucket", buckets[i].Name))
		}
		buckets[i].Encryption = encryptionCfg

		// Try to load existing bucket details from storage to preserve metadata
		var details models.BucketDetails
		storedBucket, err := s.storage.GetBucket(ctx, s.sessionID, buckets[i].Name)
		if err == nil && storedBucket != nil {
			if err := json.Unmarshal(storedBucket.Details, &details); err == nil {
				details.Bucket = buckets[i]
			} else {
				details = models.NewBucketDetails(buckets[i], nil)
			}
		} else {
			details = models.NewBucketDetails(buckets[i], nil)
		}

		if err := s.storage.UpsertBucket(ctx, s.sessionID, buckets[i].Name, buckets[i].CreationDate, details, encryptionCfg); err != nil {
			slog.Warn("Failed to store bucket to storage", slogx.Error(err), slog.String("bucket", buckets[i].Name))
		}

		if progress != nil && time.Since(lastProgressUpdate) >= time.Second {
			progress(BucketsProgress{
				CurrentIdx: i + 1,
				TotalCount: len(buckets),
			})
			lastProgressUpdate = time.Now()
		}
	}

	// Update session's buckets_refreshed_at timestamp
	refreshedAt := time.Now()
	if err := s.storage.UpdateSessionBucketsRefreshedAt(ctx, s.sessionID, refreshedAt); err != nil {
		slog.Warn("Failed to update session buckets_refreshed_at", slogx.Error(err))
	} else {
		slog.Info("Updated session buckets_refreshed_at", slog.Time("refreshed-at", refreshedAt))
	}

	slog.Info("Loaded buckets", slog.Int("count", len(buckets)))

	return buckets, nil
}

// InvalidateSession deletes the current session data and creates a new session.
// Returns the new session ID.
func (s *Service) InvalidateSession(ctx context.Context) (int64, error) {
	newSessionID, err := s.storage.InvalidateSession(ctx, s.sessionID)
	if err != nil {
		return s.sessionID, fmt.Errorf("failed to invalidate session: %w", err)
	}
	s.sessionID = newSessionID
	slog.Info("Invalidated storage session", slog.Int64("session-id", s.sessionID))
	return newSessionID, nil
}

// LoadBucketMetadata loads metadata for a specific bucket from cache or S3.
// Returns the metadata and whether it came from cache.
func (s *Service) LoadBucketMetadata(ctx context.Context, bucket models.Bucket) (*models.BucketMetadata, bool, error) {
	slog.Info("Loading metadata for bucket", slog.String("bucket", bucket.Name))

	// First, try to load from storage
	storedBucket, err := s.storage.GetBucket(ctx, s.sessionID, bucket.Name)
	if err != nil {
		slog.Warn("Failed to get bucket from storage", slogx.Error(err), slog.String("bucket", bucket.Name))
	} else if storedBucket != nil {
		var details models.BucketDetails
		if err := json.Unmarshal(storedBucket.Details, &details); err == nil {
			if details.HasMetadata() {
				slog.Info("Loading bucket metadata from storage", slog.String("bucket", bucket.Name))
				metadata := details.ToMetadata()
				return metadata, true, nil
			}
		}
	}

	// Not in storage or no metadata, fetch from S3
	slog.Info("Fetching bucket metadata from S3", slog.String("bucket", bucket.Name))
	metadata, err := s.s3Client.GetBucketMetadata(ctx, bucket.Name)
	if err != nil {
		return nil, false, fmt.Errorf("failed to load bucket metadata: %w", err)
	}

	// Store the metadata in storage
	details := models.NewBucketDetails(bucket, metadata)
	if err := s.storage.UpsertBucket(ctx, s.sessionID, bucket.Name, bucket.CreationDate, details, bucket.Encryption); err != nil {
		slog.Warn("Failed to store bucket metadata to storage", slogx.Error(err), slog.String("bucket", bucket.Name))
	}

	slog.Info("Loaded bucket metadata", slog.String("bucket", bucket.Name))
	return metadata, false, nil
}

// RefreshSingleBucket invalidates and reloads metadata for a specific bucket.
func (s *Service) RefreshSingleBucket(ctx context.Context, bucket models.Bucket) (*models.BucketMetadata, error) {
	slog.Info("Refreshing bucket", slog.String("bucket", bucket.Name))

	// Delete bucket from storage to force refresh
	if err := s.storage.DeleteBucket(ctx, s.sessionID, bucket.Name); err != nil {
		slog.Warn("Failed to delete bucket from storage", slogx.Error(err), slog.String("bucket", bucket.Name))
	}

	// Fetch fresh metadata from S3
	metadata, err := s.s3Client.GetBucketMetadata(ctx, bucket.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to load bucket metadata: %w", err)
	}

	// Store the metadata in storage
	details := models.NewBucketDetails(bucket, metadata)
	if err := s.storage.UpsertBucket(ctx, s.sessionID, bucket.Name, bucket.CreationDate, details, bucket.Encryption); err != nil {
		slog.Warn("Failed to store bucket metadata to storage", slogx.Error(err), slog.String("bucket", bucket.Name))
	}

	slog.Info("Refreshed bucket metadata", slog.String("bucket", bucket.Name))
	return metadata, nil
}

// SortBuckets sorts buckets in place according to the given sort mode.
func SortBuckets(buckets []models.Bucket, mode SortMode) {
	switch mode {
	case SortNameAsc:
		sort.Slice(buckets, func(i, j int) bool {
			return buckets[i].Name < buckets[j].Name
		})
	case SortNameDesc:
		sort.Slice(buckets, func(i, j int) bool {
			return buckets[i].Name > buckets[j].Name
		})
	case SortDateAsc:
		sort.Slice(buckets, func(i, j int) bool {
			return buckets[i].CreationDate.Before(buckets[j].CreationDate)
		})
	case SortDateDesc:
		sort.Slice(buckets, func(i, j int) bool {
			return buckets[i].CreationDate.After(buckets[j].CreationDate)
		})
	}
}

// FilterBuckets returns buckets whose names contain the query string.
func FilterBuckets(buckets []models.Bucket, query string) []models.Bucket {
	if query == "" {
		return buckets
	}

	filtered := make([]models.Bucket, 0)
	for _, bucket := range buckets {
		if strings.Contains(bucket.Name, query) {
			filtered = append(filtered, bucket)
		}
	}
	return filtered
}
