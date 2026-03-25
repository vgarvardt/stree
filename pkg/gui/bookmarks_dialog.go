package gui

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/dialog"
	"fyne.io/fyne/v2/theme"
	"fyne.io/fyne/v2/widget"
	"github.com/cappuccinotm/slogx"

	"github.com/vgarvardt/stree/pkg/models"
)

// showBookmarkDialog displays a dialog for creating or editing a bookmark
func (a *App) showBookmarkDialog(existingBookmark *models.Bookmark) {
	isEdit := existingBookmark != nil
	title := "Add New Bookmark"
	if isEdit {
		title = "Edit Bookmark"
	}

	// Create the dialog window
	dialogWindow := a.fyneApp.NewWindow(title)
	dialogWindow.Resize(fyne.NewSize(500, 330))
	dialogWindow.CenterOnScreen()

	// Create form fields
	titleEntry := widget.NewEntry()
	titleEntry.SetPlaceHolder("My S3 Server")

	endpointEntry := widget.NewEntry()
	endpointEntry.SetPlaceHolder("http://localhost:9000")

	regionEntry := widget.NewEntry()
	regionEntry.SetPlaceHolder("eu-central-2")

	accessKeyEntry := widget.NewEntry()
	accessKeyEntry.SetPlaceHolder("")

	secretKeyEntry := widget.NewPasswordEntry()
	secretKeyEntry.SetPlaceHolder("")

	sessionTokenEntry := widget.NewEntry()
	sessionTokenEntry.SetPlaceHolder("Optional session token")

	// Pre-fill fields if editing
	if isEdit {
		titleEntry.SetText(existingBookmark.Title)
		endpointEntry.SetText(existingBookmark.Endpoint)
		regionEntry.SetText(existingBookmark.Region)
		accessKeyEntry.SetText(existingBookmark.AccessKeyID)
		sessionTokenEntry.SetText(existingBookmark.SessionToken)

		// Load secret key from credential store
		secretKey, err := a.svc.GetSecretKey(a.svc.OpCtx(), existingBookmark.ID)
		if err != nil {
			slog.Warn("Failed to load secret key from credential store", slogx.Error(err))
		} else {
			secretKeyEntry.SetText(secretKey)
		}
	}

	// Create status label for test connection
	statusLabel := widget.NewLabel("")

	// Create form
	form := container.NewVBox(
		widget.NewForm(
			widget.NewFormItem("Title", titleEntry),
			widget.NewFormItem("Endpoint", endpointEntry),
			widget.NewFormItem("Region", regionEntry),
			widget.NewFormItem("Access Key ID", accessKeyEntry),
			widget.NewFormItem("Secret Key", secretKeyEntry),
			widget.NewFormItem("Session Token", sessionTokenEntry),
		),
		statusLabel,
	)

	// Validation function
	validateFields := func() error {
		if titleEntry.Text == "" {
			return fmt.Errorf("title is required")
		}
		if endpointEntry.Text == "" {
			return fmt.Errorf("endpoint is required")
		}
		if regionEntry.Text == "" {
			return fmt.Errorf("region is required")
		}
		if accessKeyEntry.Text == "" {
			return fmt.Errorf("access key ID is required")
		}
		if secretKeyEntry.Text == "" {
			return fmt.Errorf("secret key is required")
		}
		return nil
	}

	// Test connection button - declare first so it can reference itself
	var testButton *widget.Button
	testButton = widget.NewButtonWithIcon("Test Connection", theme.SearchIcon(), func() {
		if err := validateFields(); err != nil {
			statusLabel.SetText("❌ " + err.Error())
			statusLabel.Refresh()
			return
		}

		// Disable button during test
		testButton.Disable()
		statusLabel.SetText("⏳ Testing connection...")
		statusLabel.Refresh()

		go func() {
			// Create a context with 30-second timeout
			testCtx, cancel := context.WithTimeout(a.svc.OpCtx(), 30*time.Second)
			defer cancel()

			err := a.svc.TestConnection(testCtx,
				endpointEntry.Text,
				regionEntry.Text,
				accessKeyEntry.Text,
				secretKeyEntry.Text,
				sessionTokenEntry.Text,
			)

			a.doUIAsync(func() {
				if err != nil {
					if errors.Is(testCtx.Err(), context.DeadlineExceeded) {
						statusLabel.SetText("❌ Connection test timed out (30s)")
					} else {
						statusLabel.SetText("❌ Connection failed: " + err.Error())
					}
				} else {
					statusLabel.SetText("✅ Connection successful!")
				}
				statusLabel.Refresh()
				testButton.Enable()
			})
		}()
	})

	// Cancel button
	cancelButton := widget.NewButton("Cancel", func() {
		dialogWindow.Close()
	})

	// Save button
	saveButton := widget.NewButtonWithIcon("Save", theme.ConfirmIcon(), func() {
		if err := validateFields(); err != nil {
			dialog.ShowError(err, dialogWindow)
			return
		}

		// Check for duplicate title
		var excludeID int64
		if isEdit {
			excludeID = existingBookmark.ID
		}
		exists, err := a.svc.BookmarkTitleExists(a.svc.OpCtx(), titleEntry.Text, excludeID)
		if err != nil {
			dialog.ShowError(fmt.Errorf("failed to check title uniqueness: %w", err), dialogWindow)
			return
		}
		if exists {
			dialog.ShowError(fmt.Errorf("a bookmark with title %q already exists", titleEntry.Text), dialogWindow)
			return
		}

		bookmark := &models.Bookmark{
			Title:        titleEntry.Text,
			Endpoint:     endpointEntry.Text,
			Region:       regionEntry.Text,
			AccessKeyID:  accessKeyEntry.Text,
			SessionToken: sessionTokenEntry.Text,
		}

		if isEdit {
			bookmark.ID = existingBookmark.ID
		}

		if err := a.svc.SaveBookmark(a.svc.OpCtx(), bookmark, secretKeyEntry.Text); err != nil {
			dialog.ShowError(err, dialogWindow)
			return
		}

		// Refresh bookmarks list
		a.refreshBookmarksList()

		dialogWindow.Close()
	})
	saveButton.Importance = widget.HighImportance

	// Create button bar
	buttonBar := container.NewBorder(
		nil,
		nil,
		testButton,
		container.NewHBox(cancelButton, saveButton),
		nil,
	)

	// Create main content
	content := container.NewBorder(
		nil,
		buttonBar,
		nil,
		nil,
		container.NewScroll(form),
	)

	dialogWindow.SetContent(content)
	dialogWindow.Show()
}

// showDeleteBookmarkConfirm shows a confirmation dialog before deleting a bookmark
func (a *App) showDeleteBookmarkConfirm(bookmark *models.Bookmark, parentWindow fyne.Window) {
	dialog.ShowConfirm(
		"Delete Bookmark",
		fmt.Sprintf("Are you sure you want to delete the bookmark '%s'?", bookmark.Title),
		func(confirmed bool) {
			if !confirmed {
				return
			}

			wasActive, err := a.svc.DeleteBookmark(a.svc.OpCtx(), bookmark.ID)
			if err != nil {
				dialog.ShowError(err, parentWindow)
				return
			}

			slog.Info("Bookmark deleted", slog.Int64("id", bookmark.ID), slog.String("title", bookmark.Title))

			// Refresh bookmarks list
			a.refreshBookmarksList()

			// If this was the active bookmark, disconnect
			if wasActive {
				a.disconnect()
			}
		},
		parentWindow,
	)
}
