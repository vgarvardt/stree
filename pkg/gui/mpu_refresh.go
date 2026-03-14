package gui

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/widget"
	"github.com/dustin/go-humanize"

	"github.com/vgarvardt/stree/pkg/models"
	"github.com/vgarvardt/stree/pkg/service"
)

// refreshMPUsMetadata refreshes multipart uploads data for a bucket
func (a *App) refreshMPUsMetadata(bucketName string) {
	startedAt := time.Now()

	slog.Info("Refreshing MPUs metadata", slog.String("bucket", bucketName))

	// Close MPU window if it's open to prevent conflicts with stale data
	a.closeMPUWindow()

	// Create a cancellable context for this operation
	ctx, cancel := context.WithCancel(a.svc.OpCtx())

	// Create progress tracking channels
	progressChan := make(chan service.MPUProgress, 1)
	doneChan := make(chan mpuRefreshDone, 1)

	// Get current metadata and bucket info for the service call
	currentMetadata := a.treeData.bucketMetadata[bucketName]
	var bucket models.Bucket
	if b := a.treeData.bucketIndex[bucketName]; b != nil {
		bucket = *b
	}

	// Start the refresh operation in a goroutine
	go func() {
		result, err := a.svc.RefreshMPUsMetadata(ctx, bucketName, currentMetadata, bucket, func(p service.MPUProgress) {
			progressChan <- p
		})
		if err != nil {
			if ctx.Err() != nil {
				doneChan <- mpuRefreshDone{cancelled: true}
				return
			}
			doneChan <- mpuRefreshDone{err: err}
			return
		}

		// Update tree data with new metadata
		a.treeData.bucketMetadata[bucketName] = result.UpdatedMetadata

		doneChan <- mpuRefreshDone{
			success:      true,
			uploadsCount: result.UploadsCount,
			partsCount:   result.PartsCount,
			totalSize:    result.TotalSize,
			elapsed:      time.Since(startedAt),
		}
	}()

	// Show progress modal on UI thread
	a.doUIAsync(func() {
		a.showMPURefreshProgressModal(bucketName, cancel, progressChan, doneChan)
	})
}

// mpuRefreshDone represents the final result of the MPU refresh operation
type mpuRefreshDone struct {
	success      bool
	cancelled    bool
	err          error
	uploadsCount int64
	partsCount   int64
	totalSize    int64
	elapsed      time.Duration
}

// showMPURefreshProgressModal displays a modal dialog showing refresh progress
func (a *App) showMPURefreshProgressModal(bucketName string, cancel context.CancelFunc, progressChan <-chan service.MPUProgress, doneChan <-chan mpuRefreshDone) {
	startTime := time.Now()

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
				a.doUIAsync(func() {
					phaseLabel.SetText(progress.Phase)
					statsLabel.SetText(fmt.Sprintf("Uploads: %s, Parts: %s, Size: %s",
						humanize.Comma(int64(progress.UploadsCount)),
						humanize.Comma(progress.PartsCount),
						humanize.Bytes(uint64(progress.TotalPartsSize))))
					elapsedLabel.SetText(fmt.Sprintf("Elapsed: %s", time.Since(startTime).Round(time.Second)))
				})

			case result := <-doneChan:
				a.doUIAsync(func() {
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
				})
				return
			}
		}
	}()
}
