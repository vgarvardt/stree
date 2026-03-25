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

// forgetBucketObjects deletes all cached objects for a bucket and reclaims disk space
func (a *App) forgetBucketObjects(bucketName string) {
	slog.Info("Forgetting objects for bucket", slog.String("bucket", bucketName))

	// Close objects window if it's open
	a.closeObjectsWindow()

	currentMetadata, _ := a.treeData.bucketMetadata.Get(bucketName)
	var bucket models.Bucket
	if b, ok := a.treeData.bucketIndex.Get(bucketName); ok {
		bucket = *b
	}

	// Create a cancellable context for this operation
	ctx, cancel := context.WithCancel(a.svc.OpCtx())

	progressChan := make(chan service.ForgetProgress, 1)
	doneChan := make(chan forgetResult, 1)

	startedAt := time.Now()

	go func() {
		updatedMetadata, err := a.svc.ForgetBucketObjects(ctx, bucketName, currentMetadata, bucket, func(p service.ForgetProgress) {
			progressChan <- p
		})
		if err != nil {
			if ctx.Err() != nil {
				doneChan <- forgetResult{cancelled: true}
				return
			}
			doneChan <- forgetResult{err: err}
			return
		}

		doneChan <- forgetResult{success: true, elapsed: time.Since(startedAt), updatedMetadata: updatedMetadata}
	}()

	a.doUIAsync(func() {
		a.showForgetProgressModal(bucketName, cancel, progressChan, doneChan)
	})
}

// forgetResult represents the final result of the forget operation
type forgetResult struct {
	success         bool
	cancelled       bool
	err             error
	elapsed         time.Duration
	updatedMetadata *models.BucketMetadata
}

// showForgetProgressModal displays a modal dialog with forget progress
func (a *App) showForgetProgressModal(bucketName string, cancel context.CancelFunc, progressChan <-chan service.ForgetProgress, doneChan <-chan forgetResult) {
	startTime := time.Now()

	phaseLabel := widget.NewLabel("Initializing...")
	elapsedLabel := widget.NewLabel("Elapsed: 0s")
	statsLabel := widget.NewLabel("")

	var latestProgress service.ForgetProgress

	var dialog *widget.PopUp
	cancelButton := widget.NewButton("Cancel", func() {
		slog.Info("User cancelled forget operation", slog.String("bucket", bucketName))
		cancel()
	})

	content := container.NewVBox(
		widget.NewLabel(fmt.Sprintf("Forgetting objects for: %s", bucketName)),
		widget.NewSeparator(),
		phaseLabel,
		elapsedLabel,
		statsLabel,
		widget.NewSeparator(),
		cancelButton,
	)

	dialog = widget.NewModalPopUp(content, a.window.Canvas())
	dialog.Show()

	ticker := time.NewTicker(time.Second)

	updateUI := func() {
		a.doUIAsync(func() {
			elapsed := time.Since(startTime)
			elapsedLabel.SetText(fmt.Sprintf("Elapsed: %s", elapsed.Round(time.Second)))

			if latestProgress.Phase != "" {
				phaseLabel.SetText(latestProgress.Phase)
			}

			if latestProgress.TotalCount > 0 && latestProgress.DeletedCount > 0 {
				pct := float64(latestProgress.DeletedCount) / float64(latestProgress.TotalCount) * 100
				statsLabel.SetText(fmt.Sprintf("Deleted: %s / %s (%.1f%%)",
					humanize.Comma(latestProgress.DeletedCount),
					humanize.Comma(latestProgress.TotalCount),
					pct,
				))
			} else {
				statsLabel.SetText("")
			}
			dialog.Refresh()
		})
	}

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

					if result.cancelled {
						a.statusBar.SetText(fmt.Sprintf("Forget cancelled for %s", bucketName))
						slog.Warn("Forget operation was cancelled", slog.String("bucket", bucketName))
					} else if result.success {
						a.treeData.bucketMetadata.Set(bucketName, result.updatedMetadata)
						a.tree.Refresh()
						a.statusBar.SetText(fmt.Sprintf("Forgot objects for %s — database vacuumed in %s",
							bucketName, result.elapsed.Round(time.Millisecond)))
					} else {
						errMsg := "unknown error"
						if result.err != nil {
							errMsg = result.err.Error()
						}
						a.statusBar.SetText(fmt.Sprintf("Error forgetting %s: %s", bucketName, errMsg))
						slog.Error("Forget operation failed", slog.String("bucket", bucketName), slogx.Error(result.err))
					}
				})
				return
			}
		}
	}()
}

// refreshObjectsMetadata refreshes object versions data for a bucket
func (a *App) refreshObjectsMetadata(bucketName string) {
	startedAt := time.Now()

	slog.Info("Refreshing objects metadata", slog.String("bucket", bucketName))

	// Close objects window if it's open to prevent conflicts with stale data
	a.closeObjectsWindow()

	currentMetadata, _ := a.treeData.bucketMetadata.Get(bucketName)
	var bucket models.Bucket
	if b, ok := a.treeData.bucketIndex.Get(bucketName); ok {
		bucket = *b
	}

	// Create a cancellable context for this operation
	ctx, cancel := context.WithCancel(a.svc.OpCtx())

	// Create progress tracking channels
	progressChan := make(chan service.ObjectsProgress, 1)
	doneChan := make(chan objectsRefreshResult, 1)

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

		doneChan <- objectsRefreshResult{
			success:         true,
			objectsCount:    result.ObjectsCount,
			totalSize:       result.TotalSize,
			elapsed:         time.Since(startedAt),
			updatedMetadata: result.UpdatedMetadata,
		}
	}()

	// Show progress modal on UI thread
	a.doUIAsync(func() {
		a.showRefreshProgressModal(bucketName, cancel, progressChan, doneChan)
	})
}

// resumeObjectsMetadata resumes an interrupted objects refresh from the last checkpoint
func (a *App) resumeObjectsMetadata(bucketName string) {
	startedAt := time.Now()

	slog.Info("Resuming objects metadata refresh", slog.String("bucket", bucketName))

	// Close objects window if it's open to prevent conflicts with stale data
	a.closeObjectsWindow()

	currentMetadata, _ := a.treeData.bucketMetadata.Get(bucketName)
	var bucket models.Bucket
	if b, ok := a.treeData.bucketIndex.Get(bucketName); ok {
		bucket = *b
	}

	ctx, cancel := context.WithCancel(a.svc.OpCtx())

	progressChan := make(chan service.ObjectsProgress, 1)
	doneChan := make(chan objectsRefreshResult, 1)

	go func() {
		result, err := a.svc.ResumeObjectsMetadata(ctx, bucketName, currentMetadata, bucket, func(p service.ObjectsProgress) {
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

		doneChan <- objectsRefreshResult{
			success:         true,
			objectsCount:    result.ObjectsCount,
			totalSize:       result.TotalSize,
			elapsed:         time.Since(startedAt),
			updatedMetadata: result.UpdatedMetadata,
		}
	}()

	a.doUIAsync(func() {
		a.showRefreshProgressModal(bucketName, cancel, progressChan, doneChan)
	})
}

// objectsRefreshResult represents the final result of the refresh operation
type objectsRefreshResult struct {
	success         bool
	cancelled       bool
	err             error
	objectsCount    int64
	totalSize       int64
	elapsed         time.Duration
	updatedMetadata *models.BucketMetadata
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
			if latestProgress.TotalCount > 0 {
				objectsCount := latestProgress.TotalCount - latestProgress.DeleteMarkerCount
				statsText := fmt.Sprintf("Versions: %s\nObjects: %s (%s)\nDelete markers: %s",
					humanize.Comma(latestProgress.TotalCount),
					humanize.Comma(objectsCount),
					humanize.Bytes(uint64(latestProgress.TotalSize)),
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
						a.treeData.bucketMetadata.Set(bucketName, result.updatedMetadata)
						a.tree.Refresh()
						a.statusBar.SetText(fmt.Sprintf("Refreshed objects for %s: %s objects, %s in %s",
							bucketName,
							humanize.Comma(result.objectsCount),
							humanize.Bytes(uint64(result.totalSize)),
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
