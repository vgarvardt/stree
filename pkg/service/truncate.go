package service

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/dustin/go-humanize"

	"github.com/vgarvardt/stree/pkg/models"
)

// TruncateProgress represents the current progress of a truncate operation.
type TruncateProgress struct {
	Phase        string
	DeletedCount int64
}

// TruncateResult represents the result of a truncate operation.
type TruncateResult struct {
	DeletedCount int64
	Elapsed      time.Duration
	Stalled      bool // true if the loop was broken due to identical consecutive lists
}

// objectKey uniquely identifies an object version for set-based comparison.
type objectKey struct {
	key       string
	versionID string
}

// buildObjectKeySet builds a set of object keys from a list of object versions.
func buildObjectKeySet(versions []models.ObjectVersion) map[objectKey]struct{} {
	set := make(map[objectKey]struct{}, len(versions))
	for _, v := range versions {
		set[objectKey{key: v.Key, versionID: v.VersionID}] = struct{}{}
	}
	return set
}

// countIntersection returns how many items from current also exist in prev.
func countIntersection(current []models.ObjectVersion, prev map[objectKey]struct{}) int {
	count := 0
	for _, v := range current {
		if _, ok := prev[objectKey{key: v.Key, versionID: v.VersionID}]; ok {
			count++
		}
	}
	return count
}

// TruncateBucket deletes all objects from an S3 bucket by listing and batch-deleting.
// It does NOT use pagination — it always lists from the beginning and deletes what it finds.
// If two consecutive lists return identical objects, it breaks to avoid an infinite loop.
// When some objects survive deletion (retention, permissions, legal hold), the overlap
// is logged as a warning but the process continues as long as progress is being made.
func (s *Service) TruncateBucket(ctx context.Context, bucketName string, progress func(TruncateProgress)) (*TruncateResult, error) {
	logger := slog.With(slog.String("bucket", bucketName))
	logger.Info("Starting bucket truncation")

	startedAt := time.Now()
	var totalDeleted int64
	var prevKeySet map[objectKey]struct{}

	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		// List first batch of objects (no pagination — always from the beginning)
		versions, _, err := s.s3Client.ListObjectVersions(ctx, bucketName, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to list objects: %w", err)
		}

		if len(versions) == 0 {
			logger.Info("Bucket is empty, truncation complete",
				slog.Int64("total-deleted", totalDeleted),
			)
			break
		}

		// Check for stalled progress by comparing actual object identities
		if prevKeySet != nil {
			overlap := countIntersection(versions, prevKeySet)
			if overlap > 0 && overlap < len(versions) {
				// Some objects survived deletion but new ones appeared — progress is being made
				logger.Warn("Some objects survived deletion",
					slog.Int("listed", len(versions)),
					slog.Int("surviving-from-previous", overlap),
					slog.Int("new", len(versions)-overlap),
				)
			} else if overlap == len(versions) {
				// Every single object in this list was already in the previous list —
				// nothing was deleted, we're stuck in an infinite loop
				logger.Error("Truncation stalled: list returned identical objects after deletion attempt, breaking to avoid infinite loop",
					slog.Int("undeletable-objects", overlap),
					slog.Int64("total-deleted-so-far", totalDeleted),
				)
				return &TruncateResult{
					DeletedCount: totalDeleted,
					Elapsed:      time.Since(startedAt),
					Stalled:      true,
				}, nil
			}
		}

		prevKeySet = buildObjectKeySet(versions)

		logger.Info("Listed objects for deletion",
			slog.Int("count", len(versions)),
		)

		result, err := s.s3Client.DeleteObjects(ctx, bucketName, versions)
		if err != nil {
			return nil, fmt.Errorf("failed to delete objects: %w", err)
		}

		totalDeleted += int64(result.Deleted)

		if len(result.Errors) > 0 {
			for _, e := range result.Errors {
				logger.Warn("Object delete failed",
					slog.String("key", e.Key),
					slog.String("version-id", e.VersionID),
					slog.String("code", e.Code),
					slog.String("message", e.Message),
				)
			}
			logger.Warn("Some objects failed to delete",
				slog.Int("requested", len(versions)),
				slog.Int("deleted", result.Deleted),
				slog.Int("errors", len(result.Errors)),
			)
		}

		if progress != nil {
			progress(TruncateProgress{
				Phase:        fmt.Sprintf("Deleting objects... %s deleted", humanize.Comma(totalDeleted)),
				DeletedCount: totalDeleted,
			})
		}
	}

	return &TruncateResult{
		DeletedCount: totalDeleted,
		Elapsed:      time.Since(startedAt),
	}, nil
}
