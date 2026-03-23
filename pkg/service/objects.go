package service

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/cappuccinotm/slogx"
	"github.com/dustin/go-humanize"

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

	// Delete the bucket's cache database file (instant) and open a fresh one
	if err := s.sessions.DeleteBucket(s.sessionID, storedBucket.ID); err != nil {
		return nil, fmt.Errorf("failed to delete existing objects: %w", err)
	}

	slog.Info("Deleted existing objects from storage", slog.String("bucket", bucketName))

	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	bucketDB, err := s.sessions.Open(s.sessionID, storedBucket.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to open bucket database: %w", err)
	}

	if progress != nil {
		progress(ObjectsProgress{Phase: "Fetching and storing object versions from S3..."})
	}

	checkpoint := s.makeCheckpointFn(bucketName, bucket, currentMetadata)

	// Fetch and process object versions in batches
	aggregates, err := s.fetchAndStoreObjectVersions(ctx, bucketName, bucketDB, nil, nil, checkpoint, progress)
	if err != nil {
		return nil, err
	}

	return s.finalizeObjectsRefresh(ctx, bucketName, bucket, currentMetadata, aggregates)
}

// ResumeObjectsMetadata resumes an interrupted objects refresh from the last checkpoint.
func (s *Service) ResumeObjectsMetadata(ctx context.Context, bucketName string, currentMetadata *models.BucketMetadata, bucket models.Bucket, progress func(ObjectsProgress)) (*ObjectsRefreshResult, error) {
	if currentMetadata == nil || currentMetadata.ObjectsContinuation == nil {
		return nil, fmt.Errorf("no continuation state available for resuming")
	}

	cont := currentMetadata.ObjectsContinuation
	slog.Info("Resuming objects metadata refresh",
		slog.String("bucket", bucketName),
		slog.Int("fetched-so-far", cont.FetchedCount),
		slog.String("key-marker", cont.NextKeyMarker))

	storedBucket, err := s.storage.GetBucket(ctx, s.sessionID, bucketName)
	if err != nil {
		return nil, fmt.Errorf("failed to get bucket from storage: %w", err)
	}
	if storedBucket == nil {
		return nil, fmt.Errorf("bucket not found in storage")
	}

	if progress != nil {
		progress(ObjectsProgress{
			Phase:              fmt.Sprintf("Resuming from %s fetched objects...", humanize.Comma(int64(cont.FetchedCount))),
			FetchedCount:       cont.FetchedCount,
			TotalCount:         cont.TotalCount,
			TotalSize:          cont.TotalSize,
			LatestVersionCount: cont.LatestVersionCount,
			LatestVersionSize:  cont.LatestVersionSize,
			DeleteMarkerCount:  cont.DeleteMarkerCount,
		})
	}

	startPagination := &models.Pagination{
		IsTruncated:         true,
		NextKeyMarker:       cont.NextKeyMarker,
		NextVersionIDMarker: cont.NextVersionIDMarker,
	}

	startAggregates := &objectAggregates{
		totalCount:         cont.TotalCount,
		totalSize:          cont.TotalSize,
		latestVersionCount: cont.LatestVersionCount,
		latestVersionSize:  cont.LatestVersionSize,
		deleteMarkerCount:  cont.DeleteMarkerCount,
		fetchedCount:       cont.FetchedCount,
	}

	bucketDB, err := s.sessions.Open(s.sessionID, storedBucket.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to open bucket database: %w", err)
	}

	checkpoint := s.makeCheckpointFn(bucketName, bucket, currentMetadata)

	aggregates, err := s.fetchAndStoreObjectVersions(ctx, bucketName, bucketDB, startPagination, startAggregates, checkpoint, progress)
	if err != nil {
		return nil, err
	}

	return s.finalizeObjectsRefresh(ctx, bucketName, bucket, currentMetadata, aggregates)
}

type objectAggregates struct {
	totalCount         int64
	totalSize          int64
	latestVersionCount int64
	latestVersionSize  int64
	deleteMarkerCount  int64
	fetchedCount       int
}

// makeCheckpointFn creates a function that persists the current continuation state
// so the refresh can be resumed if interrupted.
func (s *Service) makeCheckpointFn(bucketName string, bucket models.Bucket, baseMetadata *models.BucketMetadata) func(ctx context.Context, nextPagination *models.Pagination, agg *objectAggregates) {
	return func(ctx context.Context, nextPagination *models.Pagination, agg *objectAggregates) {
		metadata := &models.BucketMetadata{}
		if baseMetadata != nil {
			metadataCopy := *baseMetadata
			metadata = &metadataCopy
		}

		metadata.ObjectsContinuation = &models.ObjectsContinuation{
			NextKeyMarker:       nextPagination.NextKeyMarker,
			NextVersionIDMarker: nextPagination.NextVersionIDMarker,
			TotalCount:          agg.totalCount,
			TotalSize:           agg.totalSize,
			LatestVersionCount:  agg.latestVersionCount,
			LatestVersionSize:   agg.latestVersionSize,
			DeleteMarkerCount:   agg.deleteMarkerCount,
			FetchedCount:        agg.fetchedCount,
		}

		details := models.NewBucketDetails(bucket, metadata)
		if err := s.storage.UpsertBucket(ctx, s.sessionID, bucketName, bucket.CreationDate, details, bucket.Encryption); err != nil {
			slog.Warn("Failed to save continuation checkpoint",
				slog.String("bucket", bucketName),
				slogx.Error(err))
		}
	}
}

// finalizeObjectsRefresh updates metadata after a successful refresh or resume.
func (s *Service) finalizeObjectsRefresh(ctx context.Context, bucketName string, bucket models.Bucket, currentMetadata *models.BucketMetadata, aggregates *objectAggregates) (*ObjectsRefreshResult, error) {
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

	metadata := currentMetadata
	if metadata == nil {
		metadata = &models.BucketMetadata{}
	}

	now := time.Now()
	metadata.ObjectsRefreshedAt = &now
	metadata.ObjectsCount = aggregates.latestVersionCount
	metadata.ObjectsSize = aggregates.latestVersionSize
	metadata.DeleteMarkersCount = aggregates.deleteMarkerCount
	metadata.ObjectsContinuation = nil // Clear continuation on success

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

func (s *Service) fetchAndStoreObjectVersions(
	ctx context.Context,
	bucketName string,
	bucketDB *storage.BucketDB,
	startPagination *models.Pagination,
	startAggregates *objectAggregates,
	checkpoint func(ctx context.Context, nextPagination *models.Pagination, aggregates *objectAggregates),
	progress func(ObjectsProgress),
) (*objectAggregates, error) {
	aggregates := startAggregates
	if aggregates == nil {
		aggregates = &objectAggregates{}
	}
	lastProgressUpdate := time.Now()
	pagination := startPagination

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
			if err := bucketDB.BulkInsertObjectVersions(ctx, versions); err != nil {
				return nil, fmt.Errorf("failed to store object versions batch: %w", err)
			}
			slog.Debug("Stored batch of object versions",
				slog.String("bucket", bucketName),
				slog.Int("batch-size", len(versions)),
				slog.Int("total-fetched", aggregates.fetchedCount))
		}

		// Save checkpoint for resumability when there are more pages
		if checkpoint != nil && nextPagination.IsTruncated {
			checkpoint(ctx, nextPagination, aggregates)
		}

		if progress != nil && time.Since(lastProgressUpdate) >= time.Second {
			progress(ObjectsProgress{
				FetchedCount:       aggregates.fetchedCount,
				TotalCount:         aggregates.totalCount,
				TotalSize:          aggregates.totalSize,
				LatestVersionCount: aggregates.latestVersionCount,
				LatestVersionSize:  aggregates.latestVersionSize,
				DeleteMarkerCount:  aggregates.deleteMarkerCount,
				Phase:              fmt.Sprintf("Fetching and storing object versions... (%s processed)", humanize.Comma(int64(aggregates.fetchedCount))),
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

	bucketDB, err := s.sessions.Open(s.sessionID, storedBucket.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to open bucket database: %w", err)
	}

	objects, err := bucketDB.ListObjects(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to list objects: %w", err)
	}

	slog.Info("Loaded objects", slog.String("bucket", bucketName), slog.Int("count", len(objects)))
	return objects, nil
}

// ForgetProgress represents the current progress of a forget operation.
type ForgetProgress struct {
	Phase        string
	TotalCount   int64
	DeletedCount int64
}

// ForgetBucketObjects deletes all cached objects for a bucket by removing
// its per-bucket database file, then resets the objects metadata.
func (s *Service) ForgetBucketObjects(ctx context.Context, bucketName string, currentMetadata *models.BucketMetadata, bucket models.Bucket, progress func(ForgetProgress)) (*models.BucketMetadata, error) {
	slog.Info("Forgetting objects for bucket", slog.String("bucket", bucketName))

	storedBucket, err := s.storage.GetBucket(ctx, s.sessionID, bucketName)
	if err != nil {
		return nil, fmt.Errorf("failed to get bucket from storage: %w", err)
	}
	if storedBucket == nil {
		return nil, fmt.Errorf("bucket not found in storage")
	}

	// Delete the bucket's cache database file — instant, no batch loop or vacuum needed
	if err := s.sessions.DeleteBucket(s.sessionID, storedBucket.ID); err != nil {
		return nil, fmt.Errorf("failed to delete bucket cache: %w", err)
	}

	slog.Info("Deleted bucket cache file", slog.String("bucket", bucketName))

	// Reset objects metadata
	metadata := currentMetadata
	if metadata == nil {
		metadata = &models.BucketMetadata{}
	}
	metadata.ObjectsRefreshedAt = nil
	metadata.ObjectsCount = 0
	metadata.ObjectsSize = 0
	metadata.DeleteMarkersCount = 0
	metadata.ObjectsContinuation = nil

	details := models.NewBucketDetails(bucket, metadata)
	if err := s.storage.UpsertBucket(ctx, s.sessionID, bucketName, bucket.CreationDate, details, bucket.Encryption); err != nil {
		return nil, fmt.Errorf("failed to update metadata: %w", err)
	}

	slog.Info("Reset bucket objects metadata", slog.String("bucket", bucketName))

	return metadata, nil
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
