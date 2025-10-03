package gui

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/widget"
	"github.com/cappuccinotm/slogx"
	"github.com/dustin/go-humanize"

	"github.com/vgarvardt/stree/pkg/models"
)

// refreshProgress represents the current progress of the refresh operation
type refreshProgress struct {
	elapsed            time.Duration
	fetchedCount       int
	totalCount         int64
	totalSize          int64
	latestVersionCount int64
	latestVersionSize  int64
	deleteMarkerCount  int64
	currentPhase       string
}

// refreshResult represents the final result of the refresh operation
type refreshResult struct {
	success            bool
	cancelled          bool
	err                error
	totalCount         int64
	latestVersionCount int64
	latestVersionSize  int64
	elapsed            time.Duration
}

// refreshObjectsMetadata refreshes object versions data for a bucket
func (a *App) refreshObjectsMetadata(bucketName string) {
	startedAt := time.Now()

	slog.Info("Refreshing objects metadata", slog.String("bucket", bucketName))

	// Create a cancellable context for this operation
	ctx, cancel := context.WithCancel(a.ctx)
	// Don't use defer cancel() here - let the cancel button handle it
	// The context will be cleaned up when the function returns

	// Create progress tracking channels
	progressChan := make(chan refreshProgress, 1)
	doneChan := make(chan refreshResult, 1)

	// Start the refresh operation in a goroutine
	go a.performObjectsRefresh(ctx, bucketName, startedAt, progressChan, doneChan)

	// Show progress modal on UI thread
	a.fyneApp.Driver().DoFromGoroutine(func() {
		a.showRefreshProgressModal(bucketName, cancel, progressChan, doneChan)
	}, false)
}

// performObjectsRefresh performs the actual refresh operation with cancellation support
func (a *App) performObjectsRefresh(ctx context.Context, bucketName string, startedAt time.Time, progressChan chan<- refreshProgress, doneChan chan<- refreshResult) {
	// Get the bucket from storage to get its ID
	storedBucket, err := a.storage.GetBucket(ctx, a.sessionID, bucketName)
	if err != nil {
		slog.Error("Failed to get bucket from storage", slogx.Error(err), slog.String("bucket", bucketName))
		doneChan <- refreshResult{success: false, err: fmt.Errorf("failed to get bucket from storage: %w", err)}
		return
	}

	if storedBucket == nil {
		slog.Error("Bucket not found in storage", slog.String("bucket", bucketName))
		doneChan <- refreshResult{success: false, err: fmt.Errorf("bucket not found in storage")}
		return
	}

	// Send initial progress
	progressChan <- refreshProgress{
		elapsed:      time.Since(startedAt),
		currentPhase: "Deleting existing objects...",
	}

	// Delete all existing objects for this bucket
	if err := a.storage.DeleteObjectsByBucket(ctx, storedBucket.ID); err != nil {
		slog.Error("Failed to delete existing objects", slogx.Error(err), slog.String("bucket", bucketName))
		doneChan <- refreshResult{success: false, err: fmt.Errorf("failed to delete existing objects: %w", err)}
		return
	}

	slog.Info("Deleted existing objects from storage", slog.String("bucket", bucketName))

	// Check for cancellation
	if ctx.Err() != nil {
		doneChan <- refreshResult{success: false, cancelled: true}
		return
	}

	progressChan <- refreshProgress{
		elapsed:      time.Since(startedAt),
		currentPhase: "Fetching object versions from S3...",
	}

	// Fetch all object versions from S3 in batches
	versions, err := a.listObjectVersionsWithProgress(ctx, bucketName, startedAt, progressChan)
	if err != nil {
		if ctx.Err() != nil {
			doneChan <- refreshResult{success: false, cancelled: true}
			return
		}
		slog.Error("Failed to list object versions", slogx.Error(err), slog.String("bucket", bucketName))
		doneChan <- refreshResult{success: false, err: fmt.Errorf("failed to list object versions: %w", err)}
		return
	}

	slog.Info("Fetched object versions from S3", slog.String("bucket", bucketName), slog.Int("count", len(versions)))

	// Check for cancellation
	if ctx.Err() != nil {
		doneChan <- refreshResult{success: false, cancelled: true}
		return
	}

	progressChan <- refreshProgress{
		elapsed:      time.Since(startedAt),
		fetchedCount: len(versions),
		currentPhase: "Storing object versions...",
	}

	// Insert object versions into storage in batches
	const batchSize = 1000
	for i := 0; i < len(versions); i += batchSize {
		// Check for cancellation before each batch
		if ctx.Err() != nil {
			doneChan <- refreshResult{success: false, cancelled: true}
			return
		}

		end := i + batchSize
		if end > len(versions) {
			end = len(versions)
		}

		batch := versions[i:end]
		if err := a.storage.BulkInsertObjectVersions(ctx, storedBucket.ID, batch); err != nil {
			slog.Error("Failed to insert object versions batch", slogx.Error(err), slog.String("bucket", bucketName), slog.Int("batch-start", i))
			doneChan <- refreshResult{success: false, err: fmt.Errorf("failed to store object versions: %w", err)}
			return
		}
	}

	slog.Info("Stored object versions in storage", slog.String("bucket", bucketName), slog.Int("count", len(versions)))

	// Check for cancellation
	if ctx.Err() != nil {
		doneChan <- refreshResult{success: false, cancelled: true}
		return
	}

	progressChan <- refreshProgress{
		elapsed:      time.Since(startedAt),
		fetchedCount: len(versions),
		currentPhase: "Calculating aggregates...",
	}

	// Calculate aggregates
	var totalCount int64
	var totalSize int64
	var latestVersionCount int64
	var latestVersionSize int64
	var deleteMarkerCount int64

	for _, version := range versions {
		totalCount++

		if version.IsDeleteMarker {
			deleteMarkerCount++
		} else {
			totalSize += version.Size

			if version.IsLatest {
				latestVersionCount++
				latestVersionSize += version.Size
			}
		}
	}

	slog.Info("Calculated aggregates",
		slog.String("bucket", bucketName),
		slog.Int64("total-count", totalCount),
		slog.Int64("total-size", totalSize),
		slog.Int64("latest-version-count", latestVersionCount),
		slog.Int64("latest-version-size", latestVersionSize),
		slog.Int64("delete-marker-count", deleteMarkerCount),
	)

	// Update metadata in storage
	metadata, exists := a.treeData.bucketMetadata[bucketName]
	if !exists {
		slog.Warn("Bucket metadata not found in tree data", slog.String("bucket", bucketName))
		metadata = &models.BucketMetadata{}
		a.treeData.bucketMetadata[bucketName] = metadata
	}

	// Update the metadata with new aggregates
	now := time.Now()
	metadata.ObjectsRefreshedAt = &now
	metadata.ObjectsCount = latestVersionCount
	metadata.ObjectsSize = latestVersionSize

	// Find the bucket to get its creation date
	var bucket models.Bucket
	for _, b := range a.treeData.buckets {
		if b.Name == bucketName {
			bucket = b
			break
		}
	}

	// Store the updated metadata in storage
	details := models.NewBucketDetails(bucket, metadata)
	if err := a.storage.UpsertBucket(ctx, a.sessionID, bucketName, bucket.CreationDate, details); err != nil {
		slog.Error("Failed to update bucket metadata", slogx.Error(err), slog.String("bucket", bucketName))
		doneChan <- refreshResult{success: false, err: fmt.Errorf("failed to update metadata: %w", err)}
		return
	}

	elapsed := time.Since(startedAt)
	slog.Info("Updated bucket metadata", slog.String("bucket", bucketName), slog.Duration("elapsed", elapsed))

	// Send success result
	doneChan <- refreshResult{
		success:            true,
		totalCount:         totalCount,
		latestVersionCount: latestVersionCount,
		latestVersionSize:  latestVersionSize,
		elapsed:            elapsed,
	}
}

// listObjectVersionsWithProgress lists object versions and sends progress updates
func (a *App) listObjectVersionsWithProgress(ctx context.Context, bucketName string, startedAt time.Time, progressChan chan<- refreshProgress) ([]models.ObjectVersion, error) {
	var allVersions []models.ObjectVersion

	lastProgressUpdate := time.Now()
	var totalCount int64
	var totalSize int64
	var latestVersionCount int64
	var latestVersionSize int64
	var deleteMarkerCount int64

	var pagination *models.Pagination

	for {
		// Check for cancellation
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		// Fetch a batch of object versions
		versions, nextPagination, err := a.s3Client.ListObjectVersions(ctx, bucketName, pagination)
		if err != nil {
			return nil, fmt.Errorf("failed to list object versions: %w", err)
		}

		// Process the batch
		for _, version := range versions {
			allVersions = append(allVersions, version)

			totalCount++
			if version.IsDeleteMarker {
				deleteMarkerCount++
			} else {
				totalSize += version.Size
				if version.IsLatest {
					latestVersionCount++
					latestVersionSize += version.Size
				}
			}
		}

		// Send progress update every 1 second
		if time.Since(lastProgressUpdate) >= time.Second {
			progressChan <- refreshProgress{
				elapsed:            time.Since(startedAt),
				fetchedCount:       len(allVersions),
				totalCount:         totalCount,
				totalSize:          totalSize,
				latestVersionCount: latestVersionCount,
				latestVersionSize:  latestVersionSize,
				deleteMarkerCount:  deleteMarkerCount,
				currentPhase:       fmt.Sprintf("Fetching object versions... (%d fetched)", len(allVersions)),
			}
			lastProgressUpdate = time.Now()
		}

		// Check if there are more results
		if !nextPagination.IsTruncated {
			break
		}

		pagination = nextPagination
	}

	slog.Info("Listed object versions", slog.String("bucket", bucketName), slog.Int("count", len(allVersions)))
	return allVersions, nil
}

// showRefreshProgressModal displays a modal dialog with progress information
func (a *App) showRefreshProgressModal(bucketName string, cancel context.CancelFunc, progressChan <-chan refreshProgress, doneChan <-chan refreshResult) {
	// Create progress labels
	phaseLabel := widget.NewLabel("Initializing...")
	elapsedLabel := widget.NewLabel("Elapsed: 0s")
	statsLabel := widget.NewLabel("")

	// Create cancel button
	var dialog *widget.PopUp
	cancelButton := widget.NewButton("Cancel", func() {
		slog.Info("User cancelled refresh operation", slog.String("bucket", bucketName))
		cancel()
		// Dialog will be closed when done channel receives
	})

	// Create content
	content := container.NewVBox(
		widget.NewLabel(fmt.Sprintf("Refreshing objects for: %s", bucketName)),
		widget.NewSeparator(),
		phaseLabel,
		elapsedLabel,
		statsLabel,
		widget.NewSeparator(),
		cancelButton,
	)

	// Create modal dialog
	dialog = widget.NewModalPopUp(content, a.window.Canvas())
	dialog.Show()

	// Start goroutine to handle progress updates
	go func() {
		for {
			select {
			case progress := <-progressChan:
				// Update UI on main thread
				a.fyneApp.Driver().DoFromGoroutine(func() {
					phaseLabel.SetText(progress.currentPhase)
					elapsedLabel.SetText(fmt.Sprintf("Elapsed: %s", progress.elapsed.Round(time.Second)))

					if progress.fetchedCount > 0 {
						statsText := fmt.Sprintf("Fetched: %s versions\nLatest: %s objects (%s)\nDelete markers: %s",
							humanize.Comma(int64(progress.fetchedCount)),
							humanize.Comma(progress.latestVersionCount),
							humanize.Bytes(uint64(progress.latestVersionSize)),
							humanize.Comma(progress.deleteMarkerCount),
						)
						statsLabel.SetText(statsText)
					} else {
						statsLabel.SetText("")
					}
					dialog.Refresh()
				}, false)

			case result := <-doneChan:
				// Close dialog and update status
				a.fyneApp.Driver().DoFromGoroutine(func() {
					dialog.Hide()

					if result.cancelled {
						a.statusBar.SetText(fmt.Sprintf("Refresh cancelled for %s (incomplete data)", bucketName))
						slog.Warn("Refresh operation was cancelled", slog.String("bucket", bucketName))
					} else if result.success {
						a.tree.Refresh()
						a.statusBar.SetText(fmt.Sprintf("Refreshed objects for %s: %s objects, %s in %s",
							bucketName,
							humanize.Comma(result.latestVersionCount),
							humanize.Bytes(uint64(result.latestVersionSize)),
							result.elapsed.Round(time.Millisecond),
						))
					} else {
						errMsg := "unknown error"
						if result.err != nil {
							errMsg = result.err.Error()
						}
						a.statusBar.SetText(fmt.Sprintf("Error refreshing %s: %s", bucketName, errMsg))
						slog.Error("Refresh operation failed", slog.String("bucket", bucketName), slogx.Error(result.err))
					}
				}, false)
				return
			}
		}
	}()
}
