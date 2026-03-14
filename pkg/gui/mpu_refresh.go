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

// mpuRefreshProgress represents the current progress of the MPU refresh operation
type mpuRefreshProgress struct {
	elapsed        time.Duration
	uploadsCount   int
	partsCount     int64
	totalPartsSize int64
	currentPhase   string
}

// mpuRefreshResult represents the final result of the MPU refresh operation
type mpuRefreshResult struct {
	success      bool
	cancelled    bool
	err          error
	uploadsCount int64
	partsCount   int64
	totalSize    int64
	elapsed      time.Duration
}

// refreshMPUsMetadata refreshes multipart uploads data for a bucket
func (a *App) refreshMPUsMetadata(bucketName string) {
	startedAt := time.Now()

	slog.Info("Refreshing MPUs metadata", slog.String("bucket", bucketName))

	// Close MPU window if it's open to prevent conflicts with stale data
	a.closeMPUWindow()

	// Create a cancellable context for this operation
	ctx, cancel := context.WithCancel(a.opCtx)

	// Create progress tracking channels
	progressChan := make(chan mpuRefreshProgress, 1)
	doneChan := make(chan mpuRefreshResult, 1)

	// Start the refresh operation in a goroutine
	go a.performMPURefresh(ctx, bucketName, startedAt, progressChan, doneChan)

	// Show progress modal on UI thread
	a.fyneApp.Driver().DoFromGoroutine(func() {
		a.showMPURefreshProgressModal(bucketName, cancel, progressChan, doneChan)
	}, false)
}

// performMPURefresh performs the actual MPU refresh operation with cancellation support
func (a *App) performMPURefresh(ctx context.Context, bucketName string, startedAt time.Time, progressChan chan<- mpuRefreshProgress, doneChan chan<- mpuRefreshResult) {
	// Get the bucket from storage to get its ID
	storedBucket, err := a.storage.GetBucket(ctx, a.sessionID, bucketName)
	if err != nil {
		slog.Error("Failed to get bucket from storage", slogx.Error(err), slog.String("bucket", bucketName))
		doneChan <- mpuRefreshResult{success: false, err: fmt.Errorf("failed to get bucket from storage: %w", err)}
		return
	}

	if storedBucket == nil {
		slog.Error("Bucket not found in storage", slog.String("bucket", bucketName))
		doneChan <- mpuRefreshResult{success: false, err: fmt.Errorf("bucket not found in storage")}
		return
	}

	// Send initial progress
	progressChan <- mpuRefreshProgress{
		elapsed:      time.Since(startedAt),
		currentPhase: "Deleting existing multipart uploads data...",
	}

	// Delete all existing MPUs for this bucket
	if err := a.storage.DeleteMultipartUploadsByBucket(ctx, storedBucket.ID); err != nil {
		slog.Error("Failed to delete existing MPUs", slogx.Error(err), slog.String("bucket", bucketName))
		doneChan <- mpuRefreshResult{success: false, err: fmt.Errorf("failed to delete existing MPUs: %w", err)}
		return
	}

	slog.Info("Deleted existing MPUs from storage", slog.String("bucket", bucketName))

	// Check for cancellation
	if ctx.Err() != nil {
		doneChan <- mpuRefreshResult{success: false, cancelled: true}
		return
	}

	progressChan <- mpuRefreshProgress{
		elapsed:      time.Since(startedAt),
		currentPhase: "Fetching multipart uploads from S3...",
	}

	// Fetch and process multipart uploads
	stats, err := a.fetchAndStoreMPUs(ctx, bucketName, storedBucket.ID, startedAt, progressChan)
	if err != nil {
		if ctx.Err() != nil {
			doneChan <- mpuRefreshResult{success: false, cancelled: true}
			return
		}
		slog.Error("Failed to fetch and store MPUs", slogx.Error(err), slog.String("bucket", bucketName))
		doneChan <- mpuRefreshResult{success: false, err: fmt.Errorf("failed to fetch and store MPUs: %w", err)}
		return
	}

	slog.Info("Fetched MPU stats",
		slog.String("bucket", bucketName),
		slog.Int64("uploads-count", stats.uploadsCount),
		slog.Int64("parts-count", stats.partsCount),
		slog.Int64("total-size", stats.totalSize),
	)

	// Update metadata in storage
	metadata, exists := a.treeData.bucketMetadata[bucketName]
	if !exists {
		slog.Warn("Bucket metadata not found in tree data", slog.String("bucket", bucketName))
		metadata = &models.BucketMetadata{}
		a.treeData.bucketMetadata[bucketName] = metadata
	}

	// Update the metadata with new MPU stats
	now := time.Now()
	metadata.MPUsRefreshedAt = &now
	metadata.MPUsCount = stats.uploadsCount
	metadata.MPUsTotalParts = stats.partsCount
	metadata.MPUsTotalSize = stats.totalSize

	// Find the bucket to get its creation date
	var bucket models.Bucket
	if b := a.treeData.bucketIndex[bucketName]; b != nil {
		bucket = *b
	}

	// Store the updated metadata in storage
	details := models.NewBucketDetails(bucket, metadata)
	if err := a.storage.UpsertBucket(ctx, a.sessionID, bucketName, bucket.CreationDate, details, bucket.Encryption); err != nil {
		slog.Error("Failed to update bucket metadata", slogx.Error(err), slog.String("bucket", bucketName))
		doneChan <- mpuRefreshResult{success: false, err: fmt.Errorf("failed to update metadata: %w", err)}
		return
	}

	elapsed := time.Since(startedAt)
	slog.Info("Updated bucket MPU metadata", slog.String("bucket", bucketName), slog.Duration("elapsed", elapsed))

	// Send success result
	doneChan <- mpuRefreshResult{
		success:      true,
		uploadsCount: stats.uploadsCount,
		partsCount:   stats.partsCount,
		totalSize:    stats.totalSize,
		elapsed:      elapsed,
	}
}

// mpuStats holds aggregate statistics calculated during MPU processing
type mpuStats struct {
	uploadsCount int64
	partsCount   int64
	totalSize    int64
}

// fetchAndStoreMPUs fetches multipart uploads from S3 and stores them in batches
func (a *App) fetchAndStoreMPUs(ctx context.Context, bucketName string, bucketID int64, startedAt time.Time, progressChan chan<- mpuRefreshProgress) (*mpuStats, error) {
	stats := &mpuStats{}
	lastProgressUpdate := time.Now()
	var pagination *models.Pagination

	for {
		// Check for cancellation
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		// Fetch batch of multipart uploads
		uploads, nextPagination, err := a.s3Client.ListMultipartUploads(ctx, bucketName, pagination)
		if err != nil {
			return nil, fmt.Errorf("failed to list multipart uploads: %w", err)
		}

		if len(uploads) > 0 {
			// Store uploads to database
			if err := a.storage.BulkInsertMultipartUploads(ctx, bucketID, uploads); err != nil {
				return nil, fmt.Errorf("failed to store multipart uploads: %w", err)
			}

			// For each upload, fetch its parts
			for _, upload := range uploads {
				parts, err := a.s3Client.ListParts(ctx, bucketName, upload.Key, upload.UploadID)
				if err != nil {
					slog.Warn("Failed to list parts for upload", slogx.Error(err), slog.String("upload-id", upload.UploadID))
					continue
				}

				if len(parts) > 0 {
					// Store parts to database
					if err := a.storage.BulkInsertMultipartUploadParts(ctx, bucketID, upload.UploadID, parts); err != nil {
						return nil, fmt.Errorf("failed to store multipart upload parts: %w", err)
					}

					// Update stats
					for _, part := range parts {
						stats.partsCount++
						stats.totalSize += part.Size
					}
				}
			}

			stats.uploadsCount += int64(len(uploads))
		}

		// Update progress periodically (max every 500ms)
		if time.Since(lastProgressUpdate) > 500*time.Millisecond {
			progressChan <- mpuRefreshProgress{
				elapsed:        time.Since(startedAt),
				uploadsCount:   int(stats.uploadsCount),
				partsCount:     stats.partsCount,
				totalPartsSize: stats.totalSize,
				currentPhase:   "Fetching multipart uploads and parts from S3...",
			}
			lastProgressUpdate = time.Now()
		}

		// Check if there are more pages
		if nextPagination == nil || !nextPagination.IsTruncated {
			break
		}
		pagination = nextPagination
	}

	return stats, nil
}

// showMPURefreshProgressModal displays a modal dialog showing refresh progress
func (a *App) showMPURefreshProgressModal(bucketName string, cancel context.CancelFunc, progressChan <-chan mpuRefreshProgress, doneChan <-chan mpuRefreshResult) {
	// Create progress labels
	phaseLabel := widget.NewLabel("Starting...")
	statsLabel := widget.NewLabel("")
	elapsedLabel := widget.NewLabel("Elapsed: 0s")

	// Create cancel button
	cancelButton := widget.NewButton("Cancel", func() {
		cancel()
		phaseLabel.SetText("Cancelling...")
	})

	// Create content
	content := container.NewVBox(
		widget.NewLabel(fmt.Sprintf("Refreshing MPUs for %s", bucketName)),
		widget.NewSeparator(),
		phaseLabel,
		statsLabel,
		elapsedLabel,
		widget.NewSeparator(),
		cancelButton,
	)

	// Create dialog
	dialog := widget.NewModalPopUp(content, a.window.Canvas())
	dialog.Show()

	// Start goroutine to update progress
	go func() {
		for {
			select {
			case progress := <-progressChan:
				a.fyneApp.Driver().DoFromGoroutine(func() {
					phaseLabel.SetText(progress.currentPhase)
					statsLabel.SetText(fmt.Sprintf("Uploads: %s, Parts: %s, Size: %s",
						humanize.Comma(int64(progress.uploadsCount)),
						humanize.Comma(progress.partsCount),
						humanize.Bytes(uint64(progress.totalPartsSize))))
					elapsedLabel.SetText(fmt.Sprintf("Elapsed: %s", progress.elapsed.Round(time.Second)))
				}, false)

			case result := <-doneChan:
				a.fyneApp.Driver().DoFromGoroutine(func() {
					dialog.Hide()

					if result.cancelled {
						a.statusBar.SetText(fmt.Sprintf("MPU refresh cancelled for %s", bucketName))
					} else if result.err != nil {
						a.statusBar.SetText(fmt.Sprintf("Error refreshing MPUs: %v", result.err))
					} else {
						a.tree.Refresh()
						a.statusBar.SetText(fmt.Sprintf("Refreshed %s MPU(s), %s part(s), %s in %s",
							humanize.Comma(result.uploadsCount),
							humanize.Comma(result.partsCount),
							humanize.Bytes(uint64(result.totalSize)),
							result.elapsed.Round(time.Millisecond)))
					}
				}, false)
				return
			}
		}
	}()
}
