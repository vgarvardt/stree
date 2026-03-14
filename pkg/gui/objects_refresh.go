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
	"github.com/vgarvardt/stree/pkg/service"
)

// refreshObjectsMetadata refreshes object versions data for a bucket
func (a *App) refreshObjectsMetadata(bucketName string) {
	startedAt := time.Now()

	slog.Info("Refreshing objects metadata", slog.String("bucket", bucketName))

	// Close objects window if it's open to prevent conflicts with stale data
	a.closeObjectsWindow()

	// Create a cancellable context for this operation
	ctx, cancel := context.WithCancel(a.svc.OpCtx())

	// Create progress tracking channels
	progressChan := make(chan service.ObjectsProgress, 1)
	doneChan := make(chan objectsRefreshResult, 1)

	// Get current metadata and bucket info for the service call
	currentMetadata := a.treeData.bucketMetadata[bucketName]
	var bucket models.Bucket
	if b := a.treeData.bucketIndex[bucketName]; b != nil {
		bucket = *b
	}

	// Start the refresh operation in a goroutine
	go func() {
		result, err := a.svc.RefreshObjectsMetadata(ctx, bucketName, currentMetadata, bucket, func(p service.ObjectsProgress) {
			progressChan <- p
		})
		if err != nil {
			if ctx.Err() != nil {
				doneChan <- objectsRefreshResult{cancelled: true}
				return
			}
			doneChan <- objectsRefreshResult{err: err}
			return
		}

		// Update tree data with new metadata
		a.treeData.bucketMetadata[bucketName] = result.UpdatedMetadata

		doneChan <- objectsRefreshResult{
			success:            true,
			latestVersionCount: result.LatestVersionCount,
			latestVersionSize:  result.LatestVersionSize,
			elapsed:            time.Since(startedAt),
		}
	}()

	// Show progress modal on UI thread
	a.doUIAsync(func() {
		a.showRefreshProgressModal(bucketName, cancel, progressChan, doneChan)
	})
}

// objectsRefreshResult represents the final result of the refresh operation
type objectsRefreshResult struct {
	success            bool
	cancelled          bool
	err                error
	latestVersionCount int64
	latestVersionSize  int64
	elapsed            time.Duration
}

// showRefreshProgressModal displays a modal dialog with progress information
func (a *App) showRefreshProgressModal(bucketName string, cancel context.CancelFunc, progressChan <-chan service.ObjectsProgress, doneChan <-chan objectsRefreshResult) {
	startTime := time.Now()

	// Create progress labels
	phaseLabel := widget.NewLabel("Initializing...")
	elapsedLabel := widget.NewLabel("Elapsed: 0s")
	statsLabel := widget.NewLabel("")

	// Track the latest progress state
	var latestProgress service.ObjectsProgress

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
		a.doUIAsync(func() {
			// Always update elapsed time with current time
			elapsed := time.Since(startTime)
			elapsedLabel.SetText(fmt.Sprintf("Elapsed: %s", elapsed.Round(time.Second)))

			// Update phase if we have it
			if latestProgress.Phase != "" {
				phaseLabel.SetText(latestProgress.Phase)
			}

			// Update stats if we have data
			if latestProgress.FetchedCount > 0 {
				statsText := fmt.Sprintf("Fetched: %s versions\nLatest: %s objects (%s)\nDelete markers: %s",
					humanize.Comma(int64(latestProgress.FetchedCount)),
					humanize.Comma(latestProgress.LatestVersionCount),
					humanize.Bytes(uint64(latestProgress.LatestVersionSize)),
					humanize.Comma(latestProgress.DeleteMarkerCount),
				)
				statsLabel.SetText(statsText)
			} else {
				statsLabel.SetText("")
			}
			dialog.Refresh()
		})
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
				a.doUIAsync(func() {
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
				})
				return // This exits the goroutine, triggering defer ticker.Stop()
			}
		}
	}()
}
