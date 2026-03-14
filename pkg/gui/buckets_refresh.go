package gui

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/widget"
	"github.com/cappuccinotm/slogx"
	"github.com/dustin/go-humanize"
	"github.com/goccy/go-json"

	"github.com/vgarvardt/stree/pkg/models"
	"github.com/vgarvardt/stree/pkg/service"
)

// refreshBuckets clears cached data and reloads the buckets list
func (a *App) refreshBuckets() {
	slog.Info("Refreshing S3 buckets")

	// Close objects window if it's open to prevent conflicts
	a.closeObjectsWindow()

	// Close MPU window if it's open to prevent conflicts
	a.closeMPUWindow()

	// Close all open branches to reset the tree state
	a.doUI(func() {
		a.tree.CloseAllBranches()
	})

	// Clear cached bucket metadata
	a.treeData.bucketMetadata = make(map[string]*models.BucketMetadata)

	// Invalidate storage cache
	if _, err := a.svc.InvalidateSession(a.svc.OpCtx()); err != nil {
		slog.Warn("Failed to invalidate storage cache", slogx.Error(err))
	}

	// Reload buckets
	a.loadBuckets()
}

// refreshSingleBucket invalidates and reloads metadata for a specific bucket
func (a *App) refreshSingleBucket(bucketName string) {
	slog.Info("Refreshing bucket", slog.String("bucket", bucketName))

	a.doUI(func() {
		a.statusBar.SetText(fmt.Sprintf("Refreshing %s...", bucketName))
	})

	// Remove cached metadata
	delete(a.treeData.bucketMetadata, bucketName)

	// Find the bucket
	var bucket models.Bucket
	if b := a.treeData.bucketIndex[bucketName]; b != nil {
		bucket = *b
	}

	metadata, err := a.svc.RefreshSingleBucket(a.svc.OpCtx(), bucket)
	if err != nil {
		slog.Error("Failed to refresh bucket", slogx.Error(err), slog.String("bucket", bucketName))
		a.doUI(func() {
			a.statusBar.SetText(fmt.Sprintf("Error refreshing %s: %v", bucketName, err))
		})
		return
	}

	a.treeData.bucketMetadata[bucketName] = metadata

	a.doUIAsync(func() {
		a.tree.Refresh()
		a.statusBar.SetText(fmt.Sprintf("Refreshed metadata for %s", bucketName))
	})
}

// loadBuckets loads the list of S3 buckets
func (a *App) loadBuckets() {
	a.doUI(func() {
		a.statusBar.SetText("Loading buckets...")
	})

	result, err := a.svc.LoadBuckets(a.svc.OpCtx())
	if err != nil {
		slog.Error("Failed to load buckets", slogx.Error(err))
		a.doUI(func() {
			a.statusBar.SetText(fmt.Sprintf("Error: %v", err))
		})
		return
	}

	a.treeData.setBuckets(result.Buckets)
	a.sortBuckets()

	if result.FromCache {
		refreshedAt := result.RefreshedAt.Format(time.RFC3339)
		statusMsg := fmt.Sprintf("Loaded %d bucket(s) from cache (last refreshed: %s)", len(result.Buckets), refreshedAt)
		slog.Info("Loaded buckets from DB cache", slog.Int("count", len(result.Buckets)), slog.String("refreshed-at", refreshedAt))

		a.doUIAsync(func() {
			a.tree.Refresh()
			a.statusBar.SetText(statusMsg)
		})
		return
	}

	// From S3 - need to enrich with encryption info
	startedAt := time.Now()

	// Create a cancellable context for this operation
	ctx, cancel := context.WithCancel(a.svc.OpCtx())

	// Create progress tracking channels
	progressChan := make(chan service.BucketsProgress, 1)
	doneChan := make(chan bucketsLoadResult, 1)

	// Start the enrichment in a goroutine
	go func() {
		enriched, err := a.svc.StoreBucketsWithEncryption(ctx, result.Buckets, func(p service.BucketsProgress) {
			progressChan <- p
		})
		if err != nil {
			doneChan <- bucketsLoadResult{success: false, err: err}
			return
		}
		a.treeData.setBuckets(enriched)
		doneChan <- bucketsLoadResult{
			success: true,
			count:   len(enriched),
			elapsed: time.Since(startedAt),
		}
	}()

	// Show progress modal on UI thread
	a.doUIAsync(func() {
		a.showBucketsLoadProgressModal(cancel, progressChan, doneChan)
	})
}

// bucketsLoadResult represents the final result of the buckets loading operation
type bucketsLoadResult struct {
	success bool
	err     error
	count   int
	elapsed time.Duration
}

// showBucketsLoadProgressModal displays a modal dialog showing buckets loading progress
func (a *App) showBucketsLoadProgressModal(cancel context.CancelFunc, progressChan <-chan service.BucketsProgress, doneChan <-chan bucketsLoadResult) {
	startTime := time.Now()

	// Create progress labels
	progressLabel := widget.NewLabel("")
	elapsedLabel := widget.NewLabel("Elapsed: 0s")
	progressBar := widget.NewProgressBar()

	// Track the latest progress state
	var latestProgress service.BucketsProgress

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
		a.doUIAsync(func() {
			elapsed := time.Since(startTime)
			elapsedLabel.SetText(fmt.Sprintf("Elapsed: %s", elapsed.Round(time.Second)))

			if latestProgress.TotalCount > 0 {
				progressBar.SetValue(float64(latestProgress.CurrentIdx) / float64(latestProgress.TotalCount))
				progressLabel.SetText(fmt.Sprintf("%s / %s", humanize.Comma(int64(latestProgress.CurrentIdx)), humanize.Comma(int64(latestProgress.TotalCount))))
			}

			dialog.Refresh()
		})
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
				a.doUIAsync(func() {
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
				})
				return
			}
		}
	}()
}

// loadBucketMetadata loads metadata for a specific bucket
func (a *App) loadBucketMetadata(bucketName string) {
	slog.Info("Loading metadata for bucket", slog.String("bucket", bucketName))
	a.doUI(func() {
		a.statusBar.SetText(fmt.Sprintf("Loading metadata for %s...", bucketName))
	})

	// Find the bucket
	var bucket models.Bucket
	if b := a.treeData.bucketIndex[bucketName]; b != nil {
		bucket = *b
	}

	metadata, fromCache, err := a.svc.LoadBucketMetadata(a.svc.OpCtx(), bucket)
	if err != nil {
		slog.Error("Failed to load bucket metadata", slogx.Error(err), slog.String("bucket", bucketName))
		a.doUI(func() {
			a.statusBar.SetText(fmt.Sprintf("Error loading %s: %v", bucketName, err))
		})
		return
	}

	a.treeData.bucketMetadata[bucketName] = metadata

	statusSuffix := ""
	if fromCache {
		statusSuffix = " (from cache)"
	}

	a.doUIAsync(func() {
		a.tree.Refresh()
		a.statusBar.SetText(fmt.Sprintf("Loaded metadata for %s%s", bucketName, statusSuffix))
	})
}

// showEncryptionDetails opens a window displaying the bucket encryption configuration as formatted JSON
func (a *App) showEncryptionDetails(bucketName string) {
	// Find the bucket to get its encryption config
	var encryption *models.BucketEncryption
	if b := a.treeData.bucketIndex[bucketName]; b != nil {
		encryption = b.Encryption
	}

	if encryption == nil {
		return
	}

	// Marshal to formatted JSON
	formatted, err := json.MarshalIndent(encryption, "", "  ")
	if err != nil {
		slog.Error("Failed to format encryption config", slogx.Error(err), slog.String("bucket", bucketName))
		return
	}

	a.doUIAsync(func() {
		w := a.fyneApp.NewWindow(fmt.Sprintf("Encryption: %s", bucketName))
		w.Resize(fyne.NewSize(600, 400))

		text := widget.NewLabel(string(formatted))
		text.Wrapping = fyne.TextWrapBreak
		text.TextStyle = fyne.TextStyle{Monospace: true}

		w.SetContent(container.NewScroll(text))
		w.Show()
	})
}
