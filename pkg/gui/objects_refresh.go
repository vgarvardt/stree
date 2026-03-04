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

	// Close objects window if it's open to prevent conflicts with stale data
	a.closeObjectsWindow()

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
		currentPhase: "Fetching and storing object versions from S3...",
	}

	// Fetch and process object versions in batches, calculating aggregates on the fly
	aggregates, err := a.fetchAndStoreObjectVersions(ctx, bucketName, storedBucket.ID, startedAt, progressChan)
	if err != nil {
		if ctx.Err() != nil {
			doneChan <- refreshResult{success: false, cancelled: true}
			return
		}
		slog.Error("Failed to fetch and store object versions", slogx.Error(err), slog.String("bucket", bucketName))
		doneChan <- refreshResult{success: false, err: fmt.Errorf("failed to fetch and store object versions: %w", err)}
		return
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
	metadata.ObjectsCount = aggregates.latestVersionCount
	metadata.ObjectsSize = aggregates.latestVersionSize
	metadata.DeleteMarkersCount = aggregates.deleteMarkerCount

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
	if err := a.storage.UpsertBucket(ctx, a.sessionID, bucketName, bucket.CreationDate, details, bucket.Encryption); err != nil {
		slog.Error("Failed to update bucket metadata", slogx.Error(err), slog.String("bucket", bucketName))
		doneChan <- refreshResult{success: false, err: fmt.Errorf("failed to update metadata: %w", err)}
		return
	}

	elapsed := time.Since(startedAt)
	slog.Info("Updated bucket metadata", slog.String("bucket", bucketName), slog.Duration("elapsed", elapsed))

	// Send success result
	doneChan <- refreshResult{
		success:            true,
		totalCount:         aggregates.totalCount,
		latestVersionCount: aggregates.latestVersionCount,
		latestVersionSize:  aggregates.latestVersionSize,
		elapsed:            elapsed,
	}
}

// objectAggregates holds aggregate statistics calculated during object version processing
type objectAggregates struct {
	totalCount         int64
	totalSize          int64
	latestVersionCount int64
	latestVersionSize  int64
	deleteMarkerCount  int64
	fetchedCount       int
}

// fetchAndStoreObjectVersions fetches object versions from S3 and stores them in batches,
// calculating aggregates on the fly without keeping all versions in memory
func (a *App) fetchAndStoreObjectVersions(ctx context.Context, bucketName string, bucketID int64, startedAt time.Time, progressChan chan<- refreshProgress) (*objectAggregates, error) {
	aggregates := &objectAggregates{}
	lastProgressUpdate := time.Now()
	var pagination *models.Pagination

	for {
		// Check for cancellation
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		// Fetch a batch of object versions from S3
		versions, nextPagination, err := a.s3Client.ListObjectVersions(ctx, bucketName, pagination)
		if err != nil {
			return nil, fmt.Errorf("failed to list object versions: %w", err)
		}

		// Process and calculate aggregates for this batch
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

		// Store this batch immediately to the database
		if len(versions) > 0 {
			if err := a.storage.BulkInsertObjectVersions(ctx, bucketID, versions); err != nil {
				return nil, fmt.Errorf("failed to store object versions batch: %w", err)
			}
			slog.Debug("Stored batch of object versions",
				slog.String("bucket", bucketName),
				slog.Int("batch-size", len(versions)),
				slog.Int("total-fetched", aggregates.fetchedCount))
		}

		// Send progress update every 1 second
		if time.Since(lastProgressUpdate) >= time.Second {
			progressChan <- refreshProgress{
				elapsed:            time.Since(startedAt),
				fetchedCount:       aggregates.fetchedCount,
				totalCount:         aggregates.totalCount,
				totalSize:          aggregates.totalSize,
				latestVersionCount: aggregates.latestVersionCount,
				latestVersionSize:  aggregates.latestVersionSize,
				deleteMarkerCount:  aggregates.deleteMarkerCount,
				currentPhase:       fmt.Sprintf("Fetching and storing object versions... (%d processed)", aggregates.fetchedCount),
			}
			lastProgressUpdate = time.Now()
		}

		// Check if there are more results
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

// showRefreshProgressModal displays a modal dialog with progress information
func (a *App) showRefreshProgressModal(bucketName string, cancel context.CancelFunc, progressChan <-chan refreshProgress, doneChan <-chan refreshResult) {
	startTime := time.Now()

	// Create progress labels
	phaseLabel := widget.NewLabel("Initializing...")
	elapsedLabel := widget.NewLabel("Elapsed: 0s")
	statsLabel := widget.NewLabel("")

	// Track the latest progress state
	var latestProgress refreshProgress

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

	// Create a ticker that fires every second for UI updates
	ticker := time.NewTicker(time.Second)

	// Helper function to update the UI with current state
	updateUI := func() {
		a.fyneApp.Driver().DoFromGoroutine(func() {
			// Always update elapsed time with current time
			elapsed := time.Since(startTime)
			elapsedLabel.SetText(fmt.Sprintf("Elapsed: %s", elapsed.Round(time.Second)))

			// Update phase if we have it
			if latestProgress.currentPhase != "" {
				phaseLabel.SetText(latestProgress.currentPhase)
			}

			// Update stats if we have data
			if latestProgress.fetchedCount > 0 {
				statsText := fmt.Sprintf("Fetched: %s versions\nLatest: %s objects (%s)\nDelete markers: %s",
					humanize.Comma(int64(latestProgress.fetchedCount)),
					humanize.Comma(latestProgress.latestVersionCount),
					humanize.Bytes(uint64(latestProgress.latestVersionSize)),
					humanize.Comma(latestProgress.deleteMarkerCount),
				)
				statsLabel.SetText(statsText)
			} else {
				statsLabel.SetText("")
			}
			dialog.Refresh()
		}, false)
	}

	// Start goroutine to handle progress updates and ticker
	go func() {
		defer ticker.Stop() // Stop ticker when goroutine exits

		for {
			select {
			case progress := <-progressChan:
				// Store latest progress data
				latestProgress = progress
				// Update UI immediately when new data arrives
				updateUI()

			case <-ticker.C:
				// Update UI every second even if no new data arrived
				updateUI()

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
				return // This exits the goroutine, triggering defer ticker.Stop()
			}
		}
	}()
}
