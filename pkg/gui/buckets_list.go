package gui

import (
	"fmt"
	"log/slog"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/theme"
	"fyne.io/fyne/v2/widget"
	"github.com/dustin/go-humanize"

	"github.com/vgarvardt/stree/pkg/models"
	"github.com/vgarvardt/stree/pkg/service"
)

// getFilteredBuckets returns buckets filtered by the search query
func (a *App) getFilteredBuckets() []models.Bucket {
	return service.FilterBuckets(a.treeData.buckets, a.treeData.searchFilter)
}

// sortBuckets sorts the buckets based on the current sort mode
func (a *App) sortBuckets() {
	service.SortBuckets(a.treeData.buckets, a.treeData.sortMode)
	// Rebuild the index since sorting shuffles the backing array
	a.treeData.rebuildBucketIndex()
}

// showBucketContextMenu displays a context menu for a bucket
func (a *App) showBucketContextMenu(bucketName string, position fyne.Position) {
	// Create menu items
	copyNameItem := fyne.NewMenuItem("Copy name", func() {
		a.window.Clipboard().SetContent(bucketName)
		slog.Info("Copied bucket name to clipboard", slog.String("bucket", bucketName))
		a.doUI(func() {
			a.statusBar.SetText(fmt.Sprintf("Copied %q to clipboard", bucketName))
		})
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

	// Create forget item to delete cached objects and reclaim disk space
	forgetItem := fyne.NewMenuItem("Forget", func() {
		go a.forgetBucketObjects(bucketName)
	})
	forgetItem.Icon = theme.DeleteIcon()

	// Create truncate item to permanently delete all objects from the bucket
	truncateItem := fyne.NewMenuItem("Truncate", func() {
		a.truncateBucketObjects(bucketName)
	})
	truncateItem.Icon = theme.WarningIcon()

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
		forgetItem,
		fyne.NewMenuItemSeparator(),
		truncateItem,
	)
	popUpMenu := widget.NewPopUpMenu(menu, a.window.Canvas())
	popUpMenu.ShowAtPosition(position)
}

// showMPUsContextMenu displays a context menu for the MPUs metadata
func (a *App) showMPUsContextMenu(bucketName string, metadata *models.BucketMetadata, position fyne.Position) {
	// Create list item to open the MPU list window
	listItem := fyne.NewMenuItem("List", func() {
		a.showMPUList(bucketName)
	})
	listItem.Icon = theme.ListIcon()

	// Create menu items for copying MPU count
	copyMPUsAsIsItem := fyne.NewMenuItem("Copy MPUs count as is", func() {
		mpusCount := fmt.Sprintf("%d", metadata.MPUsCount)
		a.window.Clipboard().SetContent(mpusCount)
		slog.Info("Copied MPUs count to clipboard", slog.String("bucket", bucketName), slog.Int64("count", metadata.MPUsCount))
		a.statusBar.SetText(fmt.Sprintf("Copied MPUs count: %s", mpusCount))
	})
	copyMPUsAsIsItem.Icon = theme.ContentCopyIcon()

	copyMPUsFormattedItem := fyne.NewMenuItem("Copy MPUs count formatted", func() {
		mpusCount := humanize.Comma(metadata.MPUsCount)
		a.window.Clipboard().SetContent(mpusCount)
		slog.Info("Copied formatted MPUs count to clipboard", slog.String("bucket", bucketName), slog.Int64("count", metadata.MPUsCount))
		a.statusBar.SetText(fmt.Sprintf("Copied MPUs count: %s", mpusCount))
	})
	copyMPUsFormattedItem.Icon = theme.ContentCopyIcon()

	// Create menu items for copying total size
	copySizeAsIsItem := fyne.NewMenuItem("Copy total size as is", func() {
		size := fmt.Sprintf("%d", metadata.MPUsTotalSize)
		a.window.Clipboard().SetContent(size)
		slog.Info("Copied MPUs size to clipboard", slog.String("bucket", bucketName), slog.Int64("size", metadata.MPUsTotalSize))
		a.statusBar.SetText(fmt.Sprintf("Copied MPUs size: %s bytes", size))
	})
	copySizeAsIsItem.Icon = theme.ContentCopyIcon()

	copySizeFormattedItem := fyne.NewMenuItem("Copy total size formatted", func() {
		size := humanize.Bytes(uint64(metadata.MPUsTotalSize))
		a.window.Clipboard().SetContent(size)
		slog.Info("Copied formatted MPUs size to clipboard", slog.String("bucket", bucketName), slog.Int64("size", metadata.MPUsTotalSize))
		a.statusBar.SetText(fmt.Sprintf("Copied MPUs size: %s", size))
	})
	copySizeFormattedItem.Icon = theme.ContentCopyIcon()

	// Create refresh item
	refreshItem := fyne.NewMenuItem("Refresh", func() {
		go a.refreshMPUsMetadata(bucketName)
	})
	refreshItem.Icon = theme.ViewRefreshIcon()

	// Create and show the popup menu with separator
	menu := fyne.NewMenu("",
		listItem,
		fyne.NewMenuItemSeparator(),
		copyMPUsAsIsItem,
		copyMPUsFormattedItem,
		copySizeAsIsItem,
		copySizeFormattedItem,
		fyne.NewMenuItemSeparator(),
		refreshItem,
	)
	popUpMenu := widget.NewPopUpMenu(menu, a.window.Canvas())
	popUpMenu.ShowAtPosition(position)
}
