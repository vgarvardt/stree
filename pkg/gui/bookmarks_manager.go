package gui

import (
	"fmt"
	"log/slog"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/dialog"
	"fyne.io/fyne/v2/theme"
	"fyne.io/fyne/v2/widget"
	"github.com/cappuccinotm/slogx"

	"github.com/vgarvardt/stree/pkg/models"
)

const (
	bookmarkAddNew    = "➕ Add New..."
	bookmarkManage    = "⚙️ Manage Bookmarks..."
	bookmarkSeparator = "───────────────"
)

// initBookmarkSelector creates and initializes the bookmark dropdown
func (a *App) initBookmarkSelector() *widget.Select {
	a.bookmarkSelect = widget.NewSelect([]string{}, func(selected string) {
		switch selected {
		case bookmarkAddNew:
			a.showBookmarkDialog(nil)
			// Reset selection
			a.bookmarkSelect.SetSelected("")
		case bookmarkManage:
			a.showManageBookmarksDialog()
			// Reset selection
			a.bookmarkSelect.SetSelected("")
		case bookmarkSeparator:
			// Ignore separator selection
			a.bookmarkSelect.SetSelected("")
		case "":
			// Empty selection, ignore
		default:
			// Connect to selected bookmark
			a.connectToBookmark(selected)
		}
	})

	a.bookmarkSelect.PlaceHolder = "Select connection..."
	return a.bookmarkSelect
}

// refreshBookmarksList reloads bookmarks from service and updates the dropdown
func (a *App) refreshBookmarksList() {
	bookmarks, err := a.svc.ListBookmarks(a.svc.OpCtx())
	if err != nil {
		slog.Error("Failed to load bookmarks", slogx.Error(err))
		return
	}

	options := []string{bookmarkAddNew, bookmarkManage}

	if len(bookmarks) > 0 {
		options = append(options, bookmarkSeparator)
		for _, bookmark := range bookmarks {
			options = append(options, bookmark.Title)
		}
	}

	a.bookmarkSelect.Options = options
	a.bookmarkSelect.Refresh()

	// Update current selection if we have an active bookmark
	if a.svc.ActiveBookmark() != nil {
		a.bookmarkSelect.SetSelected(a.svc.ActiveBookmark().Title)
	} else {
		a.bookmarkSelect.SetSelected("")
	}
}

// connectToBookmark establishes connection using the specified bookmark
func (a *App) connectToBookmark(bookmarkTitle string) {
	// Check if already connected to this bookmark
	if a.svc.ActiveBookmark() != nil && a.svc.ActiveBookmark().Title == bookmarkTitle {
		slog.Debug("Already connected to bookmark", slog.String("title", bookmarkTitle))
		return
	}

	a.doUIAsync(func() {
		a.statusBar.SetText(fmt.Sprintf("Connecting to %s...", bookmarkTitle))
	})

	if err := a.svc.Connect(a.svc.OpCtx(), bookmarkTitle); err != nil {
		slog.Error("Failed to connect to bookmark", slogx.Error(err))
		a.doUIAsync(func() {
			a.statusBar.SetText(fmt.Sprintf("Connection failed: %v", err))
		})
		return
	}

	// Update selection
	a.bookmarkSelect.SetSelected(bookmarkTitle)

	// Load buckets
	go a.loadBuckets()
}

// disconnect closes the current S3 connection and clears buckets
func (a *App) disconnect() {
	a.svc.Disconnect()

	// Clear buckets and metadata
	a.treeData.setBuckets([]models.Bucket{})
	a.treeData.bucketMetadata = make(map[string]*models.BucketMetadata)

	// Clear selection
	a.bookmarkSelect.SetSelected("")

	// Close objects window if open
	a.closeObjectsWindow()

	// Close MPU window if open
	a.closeMPUWindow()

	// Refresh tree
	a.doUIAsync(func() {
		a.tree.Refresh()
		a.statusBar.SetText("Disconnected")
	})
}

// showManageBookmarksDialog displays a dialog for managing bookmarks
func (a *App) showManageBookmarksDialog() {
	bookmarks, err := a.svc.ListBookmarks(a.svc.OpCtx())
	if err != nil {
		dialog.ShowError(fmt.Errorf("failed to load bookmarks: %w", err), a.window)
		return
	}

	if len(bookmarks) == 0 {
		dialog.ShowInformation("No Bookmarks", "No bookmarks found. Create one using 'Add New'.", a.window)
		return
	}

	// Track selected bookmark
	selectedID := -1

	// Create list of bookmarks
	list := widget.NewList(
		func() int {
			return len(bookmarks)
		},
		func() fyne.CanvasObject {
			return widget.NewLabel("Template")
		},
		func(id widget.ListItemID, obj fyne.CanvasObject) {
			label := obj.(*widget.Label)
			label.SetText(bookmarks[id].Title)
		},
	)

	list.OnSelected = func(id widget.ListItemID) {
		selectedID = id
	}

	// Create dialog window
	dialogWindow := a.fyneApp.NewWindow("Manage Bookmarks")
	dialogWindow.Resize(fyne.NewSize(400, 300))
	dialogWindow.CenterOnScreen()

	// Create buttons
	editButton := widget.NewButtonWithIcon("Edit", theme.DocumentCreateIcon(), func() {
		if selectedID < 0 || selectedID >= len(bookmarks) {
			dialog.ShowInformation("No Selection", "Please select a bookmark to edit.", dialogWindow)
			return
		}
		dialogWindow.Close()
		a.showBookmarkDialog(&bookmarks[selectedID])
	})

	deleteButton := widget.NewButtonWithIcon("Delete", theme.DeleteIcon(), func() {
		if selectedID < 0 || selectedID >= len(bookmarks) {
			dialog.ShowInformation("No Selection", "Please select a bookmark to delete.", dialogWindow)
			return
		}
		a.showDeleteBookmarkConfirm(&bookmarks[selectedID], dialogWindow)
		dialogWindow.Close()
	})
	deleteButton.Importance = widget.DangerImportance

	closeButton := widget.NewButton("Close", func() {
		dialogWindow.Close()
	})

	buttonBar := container.NewHBox(editButton, deleteButton, closeButton)

	content := container.NewBorder(
		nil,
		buttonBar,
		nil,
		nil,
		list,
	)

	dialogWindow.SetContent(content)
	dialogWindow.Show()
}
