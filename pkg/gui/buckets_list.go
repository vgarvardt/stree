package gui

import (
	"fmt"
	"log/slog"
	"sort"
	"strings"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/theme"
	"fyne.io/fyne/v2/widget"
	"github.com/cappuccinotm/slogx"
	"github.com/dustin/go-humanize"
	"github.com/goccy/go-json"

	"github.com/vgarvardt/stree/pkg/models"
)

// getFilteredBuckets returns buckets filtered by the search query
func (a *App) getFilteredBuckets() []models.Bucket {
	if a.treeData.searchFilter == "" {
		return a.treeData.buckets
	}

	filtered := make([]models.Bucket, 0)
	for _, bucket := range a.treeData.buckets {
		// Case-sensitive substring matching
		if strings.Contains(bucket.Name, a.treeData.searchFilter) {
			filtered = append(filtered, bucket)
		}
	}
	return filtered
}

// sortBuckets sorts the buckets based on the current sort mode
func (a *App) sortBuckets() {
	switch a.treeData.sortMode {
	case sortNameAsc:
		sort.Slice(a.treeData.buckets, func(i, j int) bool {
			return a.treeData.buckets[i].Name < a.treeData.buckets[j].Name
		})
	case sortNameDesc:
		sort.Slice(a.treeData.buckets, func(i, j int) bool {
			return a.treeData.buckets[i].Name > a.treeData.buckets[j].Name
		})
	case sortDateAsc:
		sort.Slice(a.treeData.buckets, func(i, j int) bool {
			return a.treeData.buckets[i].CreationDate.Before(a.treeData.buckets[j].CreationDate)
		})
	case sortDateDesc:
		sort.Slice(a.treeData.buckets, func(i, j int) bool {
			return a.treeData.buckets[i].CreationDate.After(a.treeData.buckets[j].CreationDate)
		})
	}
}

// refreshBuckets clears cached data and reloads the buckets list
func (a *App) refreshBuckets() {
	slog.Info("Refreshing S3 buckets")

	// Close objects window if it's open to prevent conflicts
	a.closeObjectsWindow()

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

// showBucketContextMenu displays a context menu for a bucket
func (a *App) showBucketContextMenu(bucketName string, position fyne.Position) {
	// Create menu items
	copyNameItem := fyne.NewMenuItem("Copy name", func() {
		a.window.Clipboard().SetContent(bucketName)
		slog.Info("Copied bucket name to clipboard", slog.String("bucket", bucketName))
		a.fyneApp.Driver().DoFromGoroutine(func() {
			a.statusBar.SetText(fmt.Sprintf("Copied %q to clipboard", bucketName))
		}, true)
	})
	copyNameItem.Icon = theme.ContentCopyIcon()

	refreshItem := fyne.NewMenuItem("Refresh", func() {
		go a.refreshSingleBucket(bucketName)
	})
	refreshItem.Icon = theme.ViewRefreshIcon()

	// Create and show the popup menu
	menu := fyne.NewMenu("", copyNameItem, refreshItem)
	popUpMenu := widget.NewPopUpMenu(menu, a.window.Canvas())
	popUpMenu.ShowAtPosition(position)
}

// showObjectsContextMenu displays a context menu for the objects metadata
func (a *App) showObjectsContextMenu(bucketName string, metadata *models.BucketMetadata, position fyne.Position) {
	// Create list item to open the objects list window
	listItem := fyne.NewMenuItem("List", func() {
		a.showObjectsList(bucketName)
	})
	listItem.Icon = theme.ListIcon()

	// Create menu items for copying objects count
	copyObjectsAsIsItem := fyne.NewMenuItem("Copy objects as is", func() {
		objectsCount := fmt.Sprintf("%d", metadata.ObjectsCount)
		a.window.Clipboard().SetContent(objectsCount)
		slog.Info("Copied objects count to clipboard", slog.String("bucket", bucketName), slog.Int64("count", metadata.ObjectsCount))
		a.statusBar.SetText(fmt.Sprintf("Copied objects count: %s", objectsCount))
	})
	copyObjectsAsIsItem.Icon = theme.ContentCopyIcon()

	copyObjectsFormattedItem := fyne.NewMenuItem("Copy objects formatted", func() {
		objectsCount := humanize.Comma(metadata.ObjectsCount)
		a.window.Clipboard().SetContent(objectsCount)
		slog.Info("Copied formatted objects count to clipboard", slog.String("bucket", bucketName), slog.Int64("count", metadata.ObjectsCount))
		a.statusBar.SetText(fmt.Sprintf("Copied objects count: %s", objectsCount))
	})
	copyObjectsFormattedItem.Icon = theme.ContentCopyIcon()

	// Create menu items for copying size
	copySizeAsIsItem := fyne.NewMenuItem("Copy size as is", func() {
		size := fmt.Sprintf("%d", metadata.ObjectsSize)
		a.window.Clipboard().SetContent(size)
		slog.Info("Copied size to clipboard", slog.String("bucket", bucketName), slog.Int64("size", metadata.ObjectsSize))
		a.statusBar.SetText(fmt.Sprintf("Copied size: %s bytes", size))
	})
	copySizeAsIsItem.Icon = theme.ContentCopyIcon()

	copySizeFormattedItem := fyne.NewMenuItem("Copy size formatted", func() {
		size := humanize.Bytes(uint64(metadata.ObjectsSize))
		a.window.Clipboard().SetContent(size)
		slog.Info("Copied formatted size to clipboard", slog.String("bucket", bucketName), slog.Int64("size", metadata.ObjectsSize))
		a.statusBar.SetText(fmt.Sprintf("Copied size: %s", size))
	})
	copySizeFormattedItem.Icon = theme.ContentCopyIcon()

	// Create refresh item - now with actual implementation
	refreshItem := fyne.NewMenuItem("Refresh", func() {
		go a.refreshObjectsMetadata(bucketName)
	})
	refreshItem.Icon = theme.ViewRefreshIcon()

	// Create and show the popup menu with separator
	menu := fyne.NewMenu("",
		listItem,
		fyne.NewMenuItemSeparator(),
		copyObjectsAsIsItem,
		copyObjectsFormattedItem,
		copySizeAsIsItem,
		copySizeFormattedItem,
		fyne.NewMenuItemSeparator(),
		refreshItem,
	)
	popUpMenu := widget.NewPopUpMenu(menu, a.window.Canvas())
	popUpMenu.ShowAtPosition(position)
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

	slog.Info("Storing buckets to the storage", slog.Int("count", len(buckets)))
	// Store all buckets to storage, preserving existing metadata
	for _, bucket := range buckets {
		// Try to load existing bucket details from storage to preserve metadata
		var details models.BucketDetails
		storedBucket, err := a.storage.GetBucket(a.ctx, a.sessionID, bucket.Name)
		if err == nil && storedBucket != nil {
			// Bucket exists in storage - deserialize and preserve existing metadata
			if err := json.Unmarshal(storedBucket.Details, &details); err == nil {
				// Update the basic bucket info but keep the metadata
				details.Name = bucket.Name
				details.CreationDate = bucket.CreationDate
			} else {
				// Failed to unmarshal, create new details
				details = models.NewBucketDetails(bucket, nil)
			}
		} else {
			// Bucket doesn't exist in storage yet, create new details
			details = models.NewBucketDetails(bucket, nil)
		}

		if err := a.storage.UpsertBucket(a.ctx, a.sessionID, bucket.Name, bucket.CreationDate, details); err != nil {
			slog.Warn("Failed to store bucket to storage", slogx.Error(err), slog.String("bucket", bucket.Name))
		}
	}

	// Refresh tree on UI thread
	a.fyneApp.Driver().DoFromGoroutine(func() {
		a.tree.Refresh()
		a.statusBar.SetText(fmt.Sprintf("Loaded %d bucket(s)", len(buckets)))
	}, false)

	slog.Info("Loaded buckets", slog.Int("count", len(buckets)))
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
	if err := a.storage.UpsertBucket(a.ctx, a.sessionID, bucketName, bucket.CreationDate, details); err != nil {
		slog.Warn("Failed to store bucket metadata to storage", slogx.Error(err), slog.String("bucket", bucketName))
	}

	// Refresh tree on UI thread
	a.fyneApp.Driver().DoFromGoroutine(func() {
		a.tree.Refresh()
		a.statusBar.SetText(fmt.Sprintf("Loaded metadata for %s", bucketName))
	}, false)

	slog.Info("Loaded bucket metadata", slog.String("bucket", bucketName))
}
