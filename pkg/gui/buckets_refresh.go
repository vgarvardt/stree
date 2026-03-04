package gui

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/widget"
	"github.com/cappuccinotm/slogx"
	"github.com/goccy/go-json"

	"github.com/vgarvardt/stree/pkg/models"
)

// bucketsLoadProgress represents the current progress of the buckets loading operation
type bucketsLoadProgress struct {
	elapsed    time.Duration
	currentIdx int
	totalCount int
}

// bucketsLoadResult represents the final result of the buckets loading operation
type bucketsLoadResult struct {
	success bool
	err     error
	count   int
	elapsed time.Duration
}

// refreshBuckets clears cached data and reloads the buckets list
func (a *App) refreshBuckets() {
	slog.Info("Refreshing S3 buckets")

	// Close objects window if it's open to prevent conflicts
	a.closeObjectsWindow()

	// Close MPU window if it's open to prevent conflicts
	a.closeMPUWindow()

	// Close all open branches to reset the tree state
	a.fyneApp.Driver().DoFromGoroutine(func() {
		a.tree.CloseAllBranches()
	}, true)

	// Clear cached bucket metadata
	a.treeData.bucketMetadata = make(map[string]*models.BucketMetadata)

	// Invalidate storage cache by deleting the current session
	newSessionID, err := a.storage.InvalidateSession(a.ctx, a.sessionID)
	if err != nil {
		slog.Warn("Failed to invalidate storage cache", slogx.Error(err))
	}
	a.sessionID = newSessionID
	slog.Info("Invalidated storage session", slog.Int64("session-id", a.sessionID))

	// Reload buckets
	a.loadBuckets()
}

// refreshSingleBucket invalidates and reloads metadata for a specific bucket
func (a *App) refreshSingleBucket(bucketName string) {
	slog.Info("Refreshing bucket", slog.String("bucket", bucketName))

	a.fyneApp.Driver().DoFromGoroutine(func() {
		a.statusBar.SetText(fmt.Sprintf("Refreshing %s...", bucketName))
	}, true)

	// Remove cached metadata
	delete(a.treeData.bucketMetadata, bucketName)

	// Delete bucket from storage to force refresh
	if err := a.storage.DeleteBucket(a.ctx, a.sessionID, bucketName); err != nil {
		slog.Warn("Failed to delete bucket from storage", slogx.Error(err), slog.String("bucket", bucketName))
	}

	// Reload bucket metadata from S3
	a.loadBucketMetadata(bucketName)
}

// loadBuckets loads the list of S3 buckets
func (a *App) loadBuckets() {
	startedAt := time.Now()

	slog.Info("Loading S3 buckets")
	a.fyneApp.Driver().DoFromGoroutine(func() {
		a.statusBar.SetText("Loading buckets...")
	}, true)

	buckets, err := a.s3Client.ListBuckets(a.ctx, nil)
	if err != nil {
		slog.Error("Failed to load buckets", slogx.Error(err))
		a.fyneApp.Driver().DoFromGoroutine(func() {
			a.statusBar.SetText(fmt.Sprintf("Error: %v", err))
		}, true)
		return
	}

	a.treeData.buckets = buckets

	// Sort buckets according to current sort mode
	a.sortBuckets()

	// Create a cancellable context for this operation
	ctx, cancel := context.WithCancel(a.ctx)

	// Create progress tracking channels
	progressChan := make(chan bucketsLoadProgress, 1)
	doneChan := make(chan bucketsLoadResult, 1)

	// Start the store operation in a goroutine
	go a.performBucketsStore(ctx, startedAt, progressChan, doneChan)

	// Show progress modal on UI thread
	a.fyneApp.Driver().DoFromGoroutine(func() {
		a.showBucketsLoadProgressModal(cancel, progressChan, doneChan)
	}, false)
}

// performBucketsStore fetches encryption info and stores buckets to the database
func (a *App) performBucketsStore(ctx context.Context, startedAt time.Time, progressChan chan<- bucketsLoadProgress, doneChan chan<- bucketsLoadResult) {
	buckets := a.treeData.buckets

	slog.Info("Reading encryption information and storing buckets to the storage", slog.Int("count", len(buckets)))

	lastProgressUpdate := time.Now()

	for i := range buckets {
		// Check for cancellation
		if ctx.Err() != nil {
			doneChan <- bucketsLoadResult{success: false}
			return
		}

		// Fetch encryption configuration from S3
		encryptionCfg, err := a.s3Client.GetBucketEncryption(ctx, buckets[i].Name)
		if err != nil {
			slog.Error("Failed to get bucket encryption", slogx.Error(err), slog.String("bucket", buckets[i].Name))
		}
		buckets[i].Encryption = encryptionCfg

		// Try to load existing bucket details from storage to preserve metadata
		var details models.BucketDetails
		storedBucket, err := a.storage.GetBucket(ctx, a.sessionID, buckets[i].Name)
		if err == nil && storedBucket != nil {
			// Bucket exists in storage - deserialize and preserve existing metadata
			if err := json.Unmarshal(storedBucket.Details, &details); err == nil {
				// Update the basic bucket info but keep the metadata
				details.Bucket = buckets[i]
			} else {
				// Failed to unmarshal, create new details
				details = models.NewBucketDetails(buckets[i], nil)
			}
		} else {
			// Bucket doesn't exist in storage yet, create new details
			details = models.NewBucketDetails(buckets[i], nil)
		}

		if err := a.storage.UpsertBucket(ctx, a.sessionID, buckets[i].Name, buckets[i].CreationDate, details, encryptionCfg); err != nil {
			slog.Warn("Failed to store bucket to storage", slogx.Error(err), slog.String("bucket", buckets[i].Name))
		}

		// Send progress update at most once per second
		if time.Since(lastProgressUpdate) >= time.Second {
			progressChan <- bucketsLoadProgress{
				elapsed:    time.Since(startedAt),
				currentIdx: i + 1,
				totalCount: len(buckets),
			}
			lastProgressUpdate = time.Now()
		}
	}

	elapsed := time.Since(startedAt)
	slog.Info("Loaded buckets", slog.Int("count", len(buckets)), slog.Duration("elapsed", elapsed))

	doneChan <- bucketsLoadResult{
		success: true,
		count:   len(buckets),
		elapsed: elapsed,
	}
}

// showBucketsLoadProgressModal displays a modal dialog showing buckets loading progress
func (a *App) showBucketsLoadProgressModal(cancel context.CancelFunc, progressChan <-chan bucketsLoadProgress, doneChan <-chan bucketsLoadResult) {
	startTime := time.Now()

	// Create progress labels
	progressLabel := widget.NewLabel("")
	elapsedLabel := widget.NewLabel("Elapsed: 0s")
	progressBar := widget.NewProgressBar()

	// Track the latest progress state
	var latestProgress bucketsLoadProgress

	// Create cancel button
	cancelButton := widget.NewButton("Cancel", func() {
		slog.Info("User cancelled buckets loading")
		cancel()
	})

	// Create content
	content := container.NewVBox(
		widget.NewLabel("Loading buckets"),
		widget.NewSeparator(),
		progressBar,
		progressLabel,
		elapsedLabel,
		widget.NewSeparator(),
		cancelButton,
	)

	// Create modal dialog
	dialog := widget.NewModalPopUp(content, a.window.Canvas())
	dialog.Show()

	// Create a ticker that fires every second for UI updates
	ticker := time.NewTicker(time.Second)

	// Helper function to update the UI with current state
	updateUI := func() {
		a.fyneApp.Driver().DoFromGoroutine(func() {
			elapsed := time.Since(startTime)
			elapsedLabel.SetText(fmt.Sprintf("Elapsed: %s", elapsed.Round(time.Second)))

			if latestProgress.totalCount > 0 {
				progressBar.SetValue(float64(latestProgress.currentIdx) / float64(latestProgress.totalCount))
				progressLabel.SetText(fmt.Sprintf("%d / %d buckets", latestProgress.currentIdx, latestProgress.totalCount))
			}

			dialog.Refresh()
		}, false)
	}

	// Start goroutine to handle progress updates and ticker
	go func() {
		defer ticker.Stop()

		for {
			select {
			case progress := <-progressChan:
				latestProgress = progress
				updateUI()

			case <-ticker.C:
				updateUI()

			case result := <-doneChan:
				a.fyneApp.Driver().DoFromGoroutine(func() {
					dialog.Hide()

					if !result.success {
						if result.err != nil {
							a.statusBar.SetText(fmt.Sprintf("Error loading buckets: %v", result.err))
							slog.Error("Buckets loading failed", slogx.Error(result.err))
						} else {
							a.statusBar.SetText("Buckets loading cancelled")
							slog.Warn("Buckets loading was cancelled")
						}
					} else {
						a.tree.Refresh()
						a.statusBar.SetText(fmt.Sprintf("Loaded %d bucket(s) in %s", result.count, result.elapsed.Round(time.Millisecond)))
					}
				}, false)
				return
			}
		}
	}()
}

// loadBucketMetadata loads metadata for a specific bucket
func (a *App) loadBucketMetadata(bucketName string) {
	slog.Info("Loading metadata for bucket", slog.String("bucket", bucketName))
	a.fyneApp.Driver().DoFromGoroutine(func() {
		a.statusBar.SetText(fmt.Sprintf("Loading metadata for %s...", bucketName))
	}, true)

	// First, try to load from storage
	storedBucket, err := a.storage.GetBucket(a.ctx, a.sessionID, bucketName)
	if err != nil {
		slog.Warn("Failed to get bucket from storage", slogx.Error(err), slog.String("bucket", bucketName))
	} else if storedBucket != nil {
		// Deserialize stored details into BucketDetails
		var details models.BucketDetails
		if err := json.Unmarshal(storedBucket.Details, &details); err == nil {
			// Check if the details contain metadata (not just basic bucket info)
			if details.HasMetadata() {
				slog.Info("Loading bucket metadata from storage", slog.String("bucket", bucketName))

				// Convert to BucketMetadata
				metadata := details.ToMetadata()
				a.treeData.bucketMetadata[bucketName] = metadata

				// Refresh tree on UI thread
				a.fyneApp.Driver().DoFromGoroutine(func() {
					a.tree.Refresh()
					a.statusBar.SetText(fmt.Sprintf("Loaded metadata for %s (from cache)", bucketName))
				}, false)
				return
			}
		}
	}

	// Not in storage or no metadata, fetch from S3
	slog.Info("Fetching bucket metadata from S3", slog.String("bucket", bucketName))
	metadata, err := a.s3Client.GetBucketMetadata(a.ctx, bucketName)
	if err != nil {
		slog.Error("Failed to load bucket metadata", slogx.Error(err), slog.String("bucket", bucketName))
		a.fyneApp.Driver().DoFromGoroutine(func() {
			a.statusBar.SetText(fmt.Sprintf("Error loading %s: %v", bucketName, err))
		}, true)
		return
	}

	a.treeData.bucketMetadata[bucketName] = metadata

	// Find the bucket to get its creation date
	var bucket models.Bucket
	for _, b := range a.treeData.buckets {
		if b.Name == bucketName {
			bucket = b
			break
		}
	}

	// Store the metadata in storage using BucketDetails
	details := models.NewBucketDetails(bucket, metadata)
	if err := a.storage.UpsertBucket(a.ctx, a.sessionID, bucketName, bucket.CreationDate, details, bucket.Encryption); err != nil {
		slog.Warn("Failed to store bucket metadata to storage", slogx.Error(err), slog.String("bucket", bucketName))
	}

	// Refresh tree on UI thread
	a.fyneApp.Driver().DoFromGoroutine(func() {
		a.tree.Refresh()
		a.statusBar.SetText(fmt.Sprintf("Loaded metadata for %s", bucketName))
	}, false)

	slog.Info("Loaded bucket metadata", slog.String("bucket", bucketName))
}
