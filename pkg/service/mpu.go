package service

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/cappuccinotm/slogx"

	"github.com/vgarvardt/stree/pkg/models"
	"github.com/vgarvardt/stree/pkg/storage"
)

// MPUProgress represents the current progress of an MPU refresh operation.
type MPUProgress struct {
	Phase          string
	UploadsCount   int
	PartsCount     int64
	TotalPartsSize int64
}

// MPURefreshResult represents the result of an MPU refresh operation.
type MPURefreshResult struct {
	UpdatedMetadata *models.BucketMetadata
	UploadsCount    int64
	PartsCount      int64
	TotalSize       int64
}

// RefreshMPUsMetadata refreshes multipart uploads data for a bucket.
// The progress callback is called periodically with current status.
func (s *Service) RefreshMPUsMetadata(ctx context.Context, bucketName string, currentMetadata *models.BucketMetadata, bucket models.Bucket, progress func(MPUProgress)) (*MPURefreshResult, error) {
	slog.Info("Refreshing MPUs metadata", slog.String("bucket", bucketName))

	// Get the bucket from storage to get its ID
	storedBucket, err := s.storage.GetBucket(ctx, s.sessionID, bucketName)
	if err != nil {
		return nil, fmt.Errorf("failed to get bucket from storage: %w", err)
	}
	if storedBucket == nil {
		return nil, fmt.Errorf("bucket not found in storage")
	}

	if progress != nil {
		progress(MPUProgress{Phase: "Deleting existing multipart uploads data..."})
	}

	// Delete all existing MPUs for this bucket
	if err := s.storage.DeleteMultipartUploadsByBucket(ctx, storedBucket.ID); err != nil {
		return nil, fmt.Errorf("failed to delete existing MPUs: %w", err)
	}

	slog.Info("Deleted existing MPUs from storage", slog.String("bucket", bucketName))

	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	if progress != nil {
		progress(MPUProgress{Phase: "Fetching multipart uploads from S3..."})
	}

	// Fetch and process multipart uploads
	stats, err := s.fetchAndStoreMPUs(ctx, bucketName, storedBucket.ID, progress)
	if err != nil {
		return nil, err
	}

	slog.Info("Fetched MPU stats",
		slog.String("bucket", bucketName),
		slog.Int64("uploads-count", stats.uploadsCount),
		slog.Int64("parts-count", stats.partsCount),
		slog.Int64("total-size", stats.totalSize),
	)

	// Update metadata
	metadata := currentMetadata
	if metadata == nil {
		metadata = &models.BucketMetadata{}
	}

	now := time.Now()
	metadata.MPUsRefreshedAt = &now
	metadata.MPUsCount = stats.uploadsCount
	metadata.MPUsTotalParts = stats.partsCount
	metadata.MPUsTotalSize = stats.totalSize

	// Store the updated metadata in storage
	details := models.NewBucketDetails(bucket, metadata)
	if err := s.storage.UpsertBucket(ctx, s.sessionID, bucketName, bucket.CreationDate, details, bucket.Encryption); err != nil {
		return nil, fmt.Errorf("failed to update metadata: %w", err)
	}

	slog.Info("Updated bucket MPU metadata", slog.String("bucket", bucketName))

	return &MPURefreshResult{
		UpdatedMetadata: metadata,
		UploadsCount:    stats.uploadsCount,
		PartsCount:      stats.partsCount,
		TotalSize:       stats.totalSize,
	}, nil
}

type mpuStats struct {
	uploadsCount int64
	partsCount   int64
	totalSize    int64
}

func (s *Service) fetchAndStoreMPUs(ctx context.Context, bucketName string, bucketID int64, progress func(MPUProgress)) (*mpuStats, error) {
	stats := &mpuStats{}
	lastProgressUpdate := time.Now()
	var pagination *models.Pagination

	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		uploads, nextPagination, err := s.s3Client.ListMultipartUploads(ctx, bucketName, pagination)
		if err != nil {
			return nil, fmt.Errorf("failed to list multipart uploads: %w", err)
		}

		if len(uploads) > 0 {
			if err := s.storage.BulkInsertMultipartUploads(ctx, bucketID, uploads); err != nil {
				return nil, fmt.Errorf("failed to store multipart uploads: %w", err)
			}

			for _, upload := range uploads {
				parts, err := s.s3Client.ListParts(ctx, bucketName, upload.Key, upload.UploadID)
				if err != nil {
					slog.Warn("Failed to list parts for upload", slogx.Error(err), slog.String("upload-id", upload.UploadID))
					continue
				}

				if len(parts) > 0 {
					if err := s.storage.BulkInsertMultipartUploadParts(ctx, bucketID, upload.UploadID, parts); err != nil {
						return nil, fmt.Errorf("failed to store multipart upload parts: %w", err)
					}

					for _, part := range parts {
						stats.partsCount++
						stats.totalSize += part.Size
					}
				}
			}

			stats.uploadsCount += int64(len(uploads))
		}

		if progress != nil && time.Since(lastProgressUpdate) > 500*time.Millisecond {
			progress(MPUProgress{
				UploadsCount:   int(stats.uploadsCount),
				PartsCount:     stats.partsCount,
				TotalPartsSize: stats.totalSize,
				Phase:          "Fetching multipart uploads and parts from S3...",
			})
			lastProgressUpdate = time.Now()
		}

		if nextPagination == nil || !nextPagination.IsTruncated {
			break
		}
		pagination = nextPagination
	}

	return stats, nil
}

// ListMPUs loads multipart uploads from the storage cache for a bucket.
func (s *Service) ListMPUs(ctx context.Context, bucketName string, sortDesc bool, limit int) ([]models.MultipartUploadWithParts, error) {
	slog.Info("Loading MPU list", slog.String("bucket", bucketName))

	storedBucket, err := s.storage.GetBucket(ctx, s.sessionID, bucketName)
	if err != nil {
		return nil, fmt.Errorf("failed to get bucket from storage: %w", err)
	}
	if storedBucket == nil {
		return nil, fmt.Errorf("bucket not found in storage")
	}

	opts := storage.MPUListOptions{
		Limit:     limit,
		OrderDesc: sortDesc,
	}

	uploads, err := s.storage.ListMultipartUploadsByBucket(ctx, storedBucket.ID, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to list MPUs: %w", err)
	}

	slog.Info("Loaded MPUs", slog.String("bucket", bucketName), slog.Int("count", len(uploads)))
	return uploads, nil
}
