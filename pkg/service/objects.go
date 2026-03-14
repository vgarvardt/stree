package service

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/cappuccinotm/slogx"
	"github.com/dustin/go-humanize"
	"github.com/goccy/go-json"

	"github.com/vgarvardt/stree/pkg/models"
	"github.com/vgarvardt/stree/pkg/storage"
)

// ObjectsProgress represents the current progress of an objects refresh operation.
type ObjectsProgress struct {
	Phase              string
	FetchedCount       int
	TotalCount         int64
	TotalSize          int64
	LatestVersionCount int64
	LatestVersionSize  int64
	DeleteMarkerCount  int64
}

// ObjectsRefreshResult represents the result of an objects refresh operation.
type ObjectsRefreshResult struct {
	UpdatedMetadata    *models.BucketMetadata
	TotalVersionCount  int64
	LatestVersionCount int64
	LatestVersionSize  int64
}

// RefreshObjectsMetadata refreshes object versions data for a bucket.
// The progress callback is called periodically with current status.
func (s *Service) RefreshObjectsMetadata(ctx context.Context, bucketName string, currentMetadata *models.BucketMetadata, bucket models.Bucket, progress func(ObjectsProgress)) (*ObjectsRefreshResult, error) {
	slog.Info("Refreshing objects metadata", slog.String("bucket", bucketName))

	// Get the bucket from storage to get its ID
	storedBucket, err := s.storage.GetBucket(ctx, s.sessionID, bucketName)
	if err != nil {
		return nil, fmt.Errorf("failed to get bucket from storage: %w", err)
	}
	if storedBucket == nil {
		return nil, fmt.Errorf("bucket not found in storage")
	}

	if progress != nil {
		progress(ObjectsProgress{Phase: "Deleting existing objects..."})
	}

	// Delete all existing objects for this bucket
	if err := s.storage.DeleteObjectsByBucket(ctx, storedBucket.ID); err != nil {
		return nil, fmt.Errorf("failed to delete existing objects: %w", err)
	}

	slog.Info("Deleted existing objects from storage", slog.String("bucket", bucketName))

	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	if progress != nil {
		progress(ObjectsProgress{Phase: "Fetching and storing object versions from S3..."})
	}

	// Fetch and process object versions in batches
	aggregates, err := s.fetchAndStoreObjectVersions(ctx, bucketName, storedBucket.ID, progress)
	if err != nil {
		return nil, err
	}

	slog.Info("Calculated aggregates",
		slog.String("bucket", bucketName),
		slog.Int64("total-count", aggregates.totalCount),
		slog.String("total-count-fmt", humanize.Comma(aggregates.totalCount)),
		slog.Int64("total-size", aggregates.totalSize),
		slog.String("total-size-fmt", humanize.Bytes(uint64(aggregates.totalSize))),
		slog.Int64("latest-version-count", aggregates.latestVersionCount),
		slog.String("latest-version-count-fmt", humanize.Comma(aggregates.latestVersionCount)),
		slog.Int64("latest-version-size", aggregates.latestVersionSize),
		slog.String("latest-version-size-fmt", humanize.Bytes(uint64(aggregates.latestVersionSize))),
		slog.Int64("delete-marker-count", aggregates.deleteMarkerCount),
		slog.String("delete-marker-count-fmt", humanize.Comma(aggregates.deleteMarkerCount)),
	)

	// Update metadata
	metadata := currentMetadata
	if metadata == nil {
		metadata = &models.BucketMetadata{}
	}

	now := time.Now()
	metadata.ObjectsRefreshedAt = &now
	metadata.ObjectsCount = aggregates.latestVersionCount
	metadata.ObjectsSize = aggregates.latestVersionSize
	metadata.DeleteMarkersCount = aggregates.deleteMarkerCount

	// Store the updated metadata in storage
	details := models.NewBucketDetails(bucket, metadata)
	if err := s.storage.UpsertBucket(ctx, s.sessionID, bucketName, bucket.CreationDate, details, bucket.Encryption); err != nil {
		return nil, fmt.Errorf("failed to update metadata: %w", err)
	}

	slog.Info("Updated bucket metadata", slog.String("bucket", bucketName))

	return &ObjectsRefreshResult{
		UpdatedMetadata:    metadata,
		TotalVersionCount:  aggregates.totalCount,
		LatestVersionCount: aggregates.latestVersionCount,
		LatestVersionSize:  aggregates.latestVersionSize,
	}, nil
}

type objectAggregates struct {
	totalCount         int64
	totalSize          int64
	latestVersionCount int64
	latestVersionSize  int64
	deleteMarkerCount  int64
	fetchedCount       int
}

func (s *Service) fetchAndStoreObjectVersions(ctx context.Context, bucketName string, bucketID int64, progress func(ObjectsProgress)) (*objectAggregates, error) {
	aggregates := &objectAggregates{}
	lastProgressUpdate := time.Now()
	var pagination *models.Pagination

	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		versions, nextPagination, err := s.s3Client.ListObjectVersions(ctx, bucketName, pagination)
		if err != nil {
			return nil, fmt.Errorf("failed to list object versions: %w", err)
		}

		for _, version := range versions {
			aggregates.fetchedCount++
			aggregates.totalCount++

			if version.IsDeleteMarker {
				aggregates.deleteMarkerCount++
			} else {
				aggregates.totalSize += version.Size
				if version.IsLatest {
					aggregates.latestVersionCount++
					aggregates.latestVersionSize += version.Size
				}
			}
		}

		if len(versions) > 0 {
			if err := s.storage.BulkInsertObjectVersions(ctx, bucketID, versions); err != nil {
				return nil, fmt.Errorf("failed to store object versions batch: %w", err)
			}
			slog.Debug("Stored batch of object versions",
				slog.String("bucket", bucketName),
				slog.Int("batch-size", len(versions)),
				slog.Int("total-fetched", aggregates.fetchedCount))
		}

		if progress != nil && time.Since(lastProgressUpdate) >= time.Second {
			progress(ObjectsProgress{
				FetchedCount:       aggregates.fetchedCount,
				TotalCount:         aggregates.totalCount,
				TotalSize:          aggregates.totalSize,
				LatestVersionCount: aggregates.latestVersionCount,
				LatestVersionSize:  aggregates.latestVersionSize,
				DeleteMarkerCount:  aggregates.deleteMarkerCount,
				Phase:              fmt.Sprintf("Fetching and storing object versions... (%d processed)", aggregates.fetchedCount),
			})
			lastProgressUpdate = time.Now()
		}

		if !nextPagination.IsTruncated {
			break
		}
		pagination = nextPagination
	}

	slog.Info("Completed fetching and storing object versions",
		slog.String("bucket", bucketName),
		slog.Int("total-count", aggregates.fetchedCount))

	return aggregates, nil
}

// ObjectSort specifies how to sort objects.
type ObjectSort int

const (
	ObjectSortSizeDesc ObjectSort = iota
	ObjectSortSizeAsc
	ObjectSortDateDesc
	ObjectSortDateAsc
)

// ObjectFilter specifies which objects to include.
type ObjectFilter int

const (
	ObjectFilterAll ObjectFilter = iota
	ObjectFilterFilesOnly
	ObjectFilterDeleteMarkersOnly
)

// ListObjects loads objects from the storage cache for a bucket.
func (s *Service) ListObjects(ctx context.Context, bucketName string, sortMode ObjectSort, filter ObjectFilter, limit int) ([]models.ObjectVersion, error) {
	slog.Info("Loading objects list", slog.String("bucket", bucketName))

	// Get bucket from storage
	storedBucket, err := s.storage.GetBucket(ctx, s.sessionID, bucketName)
	if err != nil {
		return nil, fmt.Errorf("failed to get bucket from storage: %w", err)
	}
	if storedBucket == nil {
		return nil, fmt.Errorf("bucket not found in storage")
	}

	opts := storage.ObjectListOptions{
		Limit: limit,
	}

	switch sortMode {
	case ObjectSortSizeDesc:
		opts.OrderBy = storage.OrderBySize
		opts.OrderDesc = true
	case ObjectSortSizeAsc:
		opts.OrderBy = storage.OrderBySize
		opts.OrderDesc = false
	case ObjectSortDateDesc:
		opts.OrderBy = storage.OrderByLastModified
		opts.OrderDesc = true
	case ObjectSortDateAsc:
		opts.OrderBy = storage.OrderByLastModified
		opts.OrderDesc = false
	}

	switch filter {
	case ObjectFilterFilesOnly:
		f := false
		opts.FilterDeleteMarker = &f
	case ObjectFilterDeleteMarkersOnly:
		f := true
		opts.FilterDeleteMarker = &f
	}

	storageObjects, err := s.storage.ListObjectsByBucket(ctx, storedBucket.ID, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to list objects: %w", err)
	}

	objects := make([]models.ObjectVersion, 0, len(storageObjects))
	for _, obj := range storageObjects {
		var version models.ObjectVersion
		if err := json.Unmarshal(obj.Properties, &version); err != nil {
			slog.Warn("Failed to unmarshal object version", slogx.Error(err))
			continue
		}
		objects = append(objects, version)
	}

	slog.Info("Loaded objects", slog.String("bucket", bucketName), slog.Int("count", len(objects)))
	return objects, nil
}

// EnsureBucketInStorage ensures a bucket exists in storage, creating it if needed.
// Returns an error only if the bucket cannot be found or stored.
func (s *Service) EnsureBucketInStorage(ctx context.Context, bucket models.Bucket, metadata *models.BucketMetadata) error {
	storedBucket, err := s.storage.GetBucket(ctx, s.sessionID, bucket.Name)
	if err != nil {
		return fmt.Errorf("failed to get bucket from storage: %w", err)
	}
	if storedBucket != nil {
		return nil
	}

	slog.Warn("Bucket not found in storage, storing it", slog.String("bucket", bucket.Name))
	details := models.NewBucketDetails(bucket, metadata)
	if err := s.storage.UpsertBucket(ctx, s.sessionID, bucket.Name, bucket.CreationDate, details, bucket.Encryption); err != nil {
		return fmt.Errorf("failed to store bucket: %w", err)
	}

	return nil
}
