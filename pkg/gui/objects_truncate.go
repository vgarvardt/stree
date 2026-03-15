package gui

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/dialog"
	"fyne.io/fyne/v2/widget"
	"github.com/cappuccinotm/slogx"
	"github.com/dustin/go-humanize"

	"github.com/vgarvardt/stree/pkg/service"
)

// truncateBucketObjects shows confirmation dialog and then truncates the bucket
func (a *App) truncateBucketObjects(bucketName string) {
	a.doUIAsync(func() {
		a.showTruncateConfirmDialog(bucketName)
	})
}

// showTruncateConfirmDialog shows a confirmation dialog requiring the user to type the bucket name
func (a *App) showTruncateConfirmDialog(bucketName string) {
	confirmEntry := widget.NewEntry()
	confirmEntry.SetPlaceHolder("Type bucket name to confirm")

	content := container.NewVBox(
		widget.NewLabel(fmt.Sprintf("This will permanently delete ALL objects in bucket %q.", bucketName)),
		widget.NewLabel("This action cannot be undone."),
		widget.NewSeparator(),
		widget.NewLabel("Type the bucket name to confirm:"),
		confirmEntry,
	)

	d := dialog.NewCustomConfirm("Truncate Bucket", "Truncate", "Cancel", content,
		func(confirmed bool) {
			if !confirmed {
				return
			}
			if confirmEntry.Text != bucketName {
				a.statusBar.SetText(fmt.Sprintf("Truncate cancelled: bucket name does not match"))
				return
			}
			go a.executeTruncate(bucketName)
		},
		a.window,
	)
	d.Resize(fyne.NewSize(500, 0))
	d.Show()
}

// truncateResult represents the final result of the truncate operation
type truncateResult struct {
	success   bool
	cancelled bool
	stalled   bool
	err       error
	deleted   int64
	elapsed   time.Duration
}

// executeTruncate runs the truncate operation with a progress dialog
func (a *App) executeTruncate(bucketName string) {
	slog.Info("Truncating bucket", slog.String("bucket", bucketName))

	ctx, cancel := context.WithCancel(a.svc.OpCtx())

	progressChan := make(chan service.TruncateProgress, 1)
	doneChan := make(chan truncateResult, 1)

	go func() {
		result, err := a.svc.TruncateBucket(ctx, bucketName, func(p service.TruncateProgress) {
			progressChan <- p
		})
		if err != nil {
			if ctx.Err() != nil {
				doneChan <- truncateResult{cancelled: true}
				return
			}
			doneChan <- truncateResult{err: err}
			return
		}

		doneChan <- truncateResult{
			success: true,
			stalled: result.Stalled,
			deleted: result.DeletedCount,
			elapsed: result.Elapsed,
		}
	}()

	a.doUIAsync(func() {
		a.showTruncateProgressModal(bucketName, cancel, progressChan, doneChan)
	})
}

// showTruncateProgressModal displays a modal dialog with truncate progress
func (a *App) showTruncateProgressModal(bucketName string, cancel context.CancelFunc, progressChan <-chan service.TruncateProgress, doneChan <-chan truncateResult) {
	startTime := time.Now()

	phaseLabel := widget.NewLabel("Initializing...")
	elapsedLabel := widget.NewLabel("Elapsed: 0s")
	statsLabel := widget.NewLabel("")

	var latestProgress service.TruncateProgress

	var modal *widget.PopUp
	cancelButton := widget.NewButton("Cancel", func() {
		slog.Info("User cancelled truncate operation", slog.String("bucket", bucketName))
		cancel()
	})

	content := container.NewVBox(
		widget.NewLabel(fmt.Sprintf("Truncating bucket: %s", bucketName)),
		widget.NewSeparator(),
		phaseLabel,
		elapsedLabel,
		statsLabel,
		widget.NewSeparator(),
		cancelButton,
	)

	modal = widget.NewModalPopUp(content, a.window.Canvas())
	modal.Show()

	ticker := time.NewTicker(time.Second)

	updateUI := func() {
		a.doUIAsync(func() {
			elapsed := time.Since(startTime)
			elapsedLabel.SetText(fmt.Sprintf("Elapsed: %s", elapsed.Round(time.Second)))

			if latestProgress.Phase != "" {
				phaseLabel.SetText(latestProgress.Phase)
			}

			if latestProgress.DeletedCount > 0 {
				statsLabel.SetText(fmt.Sprintf("Deleted: %s objects", humanize.Comma(latestProgress.DeletedCount)))
			}
			modal.Refresh()
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
					modal.Hide()

					if result.cancelled {
						a.statusBar.SetText(fmt.Sprintf("Truncate cancelled for %s — %s objects deleted before cancellation",
							bucketName, humanize.Comma(result.deleted)))
						slog.Warn("Truncate operation was cancelled", slog.String("bucket", bucketName))
					} else if result.stalled {
						msg := fmt.Sprintf("Truncate stalled for %s — %s objects deleted, but remaining objects could not be deleted (check permissions/locks)",
							bucketName, humanize.Comma(result.deleted))
						a.statusBar.SetText(msg)
						slog.Warn("Truncate stalled", slog.String("bucket", bucketName),
							slog.Int64("deleted", result.deleted))
					} else if result.success {
						a.statusBar.SetText(fmt.Sprintf("Truncated %s — %s objects deleted in %s",
							bucketName,
							humanize.Comma(result.deleted),
							result.elapsed.Round(time.Millisecond),
						))
					} else {
						errMsg := "unknown error"
						if result.err != nil {
							errMsg = result.err.Error()
						}
						a.statusBar.SetText(fmt.Sprintf("Error truncating %s: %s", bucketName, errMsg))
						slog.Error("Truncate operation failed", slog.String("bucket", bucketName), slogx.Error(result.err))
					}
				})
				return
			}
		}
	}()
}
