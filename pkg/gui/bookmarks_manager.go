package gui

import (
	"context"
	"fmt"
	"log/slog"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/dialog"
	"fyne.io/fyne/v2/theme"
	"fyne.io/fyne/v2/widget"
	"github.com/cappuccinotm/slogx"

	"github.com/vgarvardt/stree/pkg/models"
	"github.com/vgarvardt/stree/pkg/s3client"
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

// refreshBookmarksList reloads bookmarks from storage and updates the dropdown
func (a *App) refreshBookmarksList() {
	bookmarks, err := a.storage.ListBookmarks(a.ctx)
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
	if a.activeBookmark != nil {
		a.bookmarkSelect.SetSelected(a.activeBookmark.Title)
	} else {
		a.bookmarkSelect.SetSelected("")
	}
}

// connectToBookmark establishes connection using the specified bookmark
func (a *App) connectToBookmark(bookmarkTitle string) {
	// Check if already connected to this bookmark
	if a.activeBookmark != nil && a.activeBookmark.Title == bookmarkTitle {
		slog.Debug("Already connected to bookmark", slog.String("title", bookmarkTitle))
		return
	}

	// Find the bookmark by title
	bookmarks, err := a.storage.ListBookmarks(a.ctx)
	if err != nil {
		a.fyneApp.Driver().DoFromGoroutine(func() {
			a.statusBar.SetText(fmt.Sprintf("Error loading bookmarks: %v", err))
		}, false)
		return
	}

	var bookmark *models.Bookmark
	for i := range bookmarks {
		if bookmarks[i].Title == bookmarkTitle {
			bookmark = &bookmarks[i]
			break
		}
	}

	if bookmark == nil {
		a.fyneApp.Driver().DoFromGoroutine(func() {
			a.statusBar.SetText("Bookmark not found: " + bookmarkTitle)
		}, false)
		return
	}

	// Get secret key from credential store
	secretKey := ""
	if a.credStore != nil {
		var err error
		secretKey, err = a.credStore.GetSecretKey(a.ctx, bookmark.ID)
		if err != nil {
			slog.Error("Failed to retrieve secret key", slogx.Error(err))
			a.fyneApp.Driver().DoFromGoroutine(func() {
				a.statusBar.SetText(fmt.Sprintf("Error: %v", err))
			}, false)
			return
		}
	}

	a.fyneApp.Driver().DoFromGoroutine(func() {
		a.statusBar.SetText(fmt.Sprintf("Connecting to %s...", bookmark.Title))
	}, false)

	// Create S3 client
	s3Cfg := s3client.Config{
		Endpoint:     bookmark.Endpoint,
		Region:       bookmark.Region,
		AccessKey:    bookmark.AccessKeyID,
		SecretKey:    secretKey,
		SessionToken: bookmark.SessionToken,
		Debug:        a.verbose,
	}

	s3Client, err := s3client.NewClient(context.Background(), s3Cfg, a.version)
	if err != nil {
		slog.Error("Failed to create S3 client", slogx.Error(err))
		a.fyneApp.Driver().DoFromGoroutine(func() {
			a.statusBar.SetText(fmt.Sprintf("Connection failed: %v", err))
		}, false)
		return
	}

	// Store session
	sessionID, err := a.storage.UpsertSession(a.ctx, s3Cfg.String())
	if err != nil {
		slog.Error("Failed to store session", slogx.Error(err))
		a.fyneApp.Driver().DoFromGoroutine(func() {
			a.statusBar.SetText(fmt.Sprintf("Error: %v", err))
		}, false)
		return
	}

	// Update last used timestamp
	if err := a.storage.UpdateBookmarkLastUsed(a.ctx, bookmark.ID); err != nil {
		slog.Warn("Failed to update bookmark last used", slogx.Error(err))
	}

	// Update app state
	a.s3Client = s3Client
	a.sessionID = sessionID
	a.activeBookmark = bookmark

	slog.Info("Connected to bookmark",
		slog.String("title", bookmark.Title),
		slog.String("endpoint", bookmark.Endpoint),
		slog.Int64("session-id", sessionID),
	)

	// Update selection (this may trigger OnChanged, but the check at the top will prevent reconnection)
	a.bookmarkSelect.SetSelected(bookmark.Title)

	// Load buckets
	go a.loadBuckets()
}

// disconnect closes the current S3 connection and clears buckets
func (a *App) disconnect() {
	a.s3Client = nil
	a.sessionID = 0
	a.activeBookmark = nil

	// Clear buckets and metadata
	a.treeData.buckets = []models.Bucket{}
	a.treeData.bucketMetadata = make(map[string]*models.BucketMetadata)

	// Clear selection
	a.bookmarkSelect.SetSelected("")

	// Close objects window if open
	a.closeObjectsWindow()

	// Close MPU window if open
	a.closeMPUWindow()

	// Refresh tree
	a.fyneApp.Driver().DoFromGoroutine(func() {
		a.tree.Refresh()
		a.statusBar.SetText("Disconnected")
	}, false)

	slog.Info("Disconnected from S3")
}

// showManageBookmarksDialog displays a dialog for managing bookmarks
func (a *App) showManageBookmarksDialog() {
	bookmarks, err := a.storage.ListBookmarks(a.ctx)
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
