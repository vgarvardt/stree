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

// showBookmarkDialog displays a dialog for creating or editing a bookmark
func (a *App) showBookmarkDialog(existingBookmark *models.Bookmark) {
	isEdit := existingBookmark != nil
	title := "Add New Bookmark"
	if isEdit {
		title = "Edit Bookmark"
	}

	// Create the dialog window
	dialogWindow := a.fyneApp.NewWindow(title)
	dialogWindow.Resize(fyne.NewSize(500, 300))
	dialogWindow.CenterOnScreen()

	// Create form fields
	titleEntry := widget.NewEntry()
	titleEntry.SetPlaceHolder("My S3 Server")

	endpointEntry := widget.NewEntry()
	endpointEntry.SetPlaceHolder("http://localhost:9000")

	regionEntry := widget.NewEntry()
	regionEntry.SetPlaceHolder("us-east-1")

	accessKeyEntry := widget.NewEntry()
	accessKeyEntry.SetPlaceHolder("AKIAIOSFODNN7EXAMPLE")

	secretKeyEntry := widget.NewPasswordEntry()
	secretKeyEntry.SetPlaceHolder("wJalrXUtnFEMI/K7MDENG/bPxRfiCY...")

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
		if a.credStore != nil {
			secretKey, err := a.credStore.GetSecretKey(a.ctx, existingBookmark.ID)
			if err != nil {
				slog.Warn("Failed to load secret key from credential store", slogx.Error(err))
			} else {
				secretKeyEntry.SetText(secretKey)
			}
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

	// Test connection button
	testButton := widget.NewButtonWithIcon("Test Connection", theme.SearchIcon(), func() {
		if err := validateFields(); err != nil {
			statusLabel.SetText("❌ " + err.Error())
			statusLabel.Refresh()
			return
		}

		statusLabel.SetText("⏳ Testing connection...")
		statusLabel.Refresh()

		go func() {
			cfg := s3client.Config{
				Endpoint:     endpointEntry.Text,
				Region:       regionEntry.Text,
				AccessKey:    accessKeyEntry.Text,
				SecretKey:    secretKeyEntry.Text,
				SessionToken: sessionTokenEntry.Text,
				Debug:        false,
			}

			client, err := s3client.NewClient(context.Background(), cfg, a.version)
			if err != nil {
				a.fyneApp.Driver().DoFromGoroutine(func() {
					statusLabel.SetText("❌ Failed to create client: " + err.Error())
					statusLabel.Refresh()
				}, false)
				return
			}

			// Test with ListBuckets
			ctx := context.Background()
			_, err = client.ListBuckets(ctx)

			a.fyneApp.Driver().DoFromGoroutine(func() {
				if err != nil {
					statusLabel.SetText("❌ Connection failed: " + err.Error())
				} else {
					statusLabel.SetText("✅ Connection successful!")
				}
				statusLabel.Refresh()
			}, false)
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

		// Save bookmark to database
		if err := a.storage.UpsertBookmark(a.ctx, bookmark); err != nil {
			dialog.ShowError(fmt.Errorf("failed to save bookmark: %w", err), dialogWindow)
			return
		}

		// Save secret key to credential store
		if a.credStore != nil {
			if err := a.credStore.StoreSecretKey(a.ctx, bookmark.ID, secretKeyEntry.Text); err != nil {
				dialog.ShowError(fmt.Errorf("failed to save secret key: %w", err), dialogWindow)
				return
			}
		}

		slog.Info("Bookmark saved", slog.Int64("id", bookmark.ID), slog.String("title", bookmark.Title))

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

			// Delete from database
			if err := a.storage.DeleteBookmark(a.ctx, bookmark.ID); err != nil {
				dialog.ShowError(fmt.Errorf("failed to delete bookmark: %w", err), parentWindow)
				return
			}

			// Delete from credential store
			if a.credStore != nil {
				if err := a.credStore.DeleteSecretKey(a.ctx, bookmark.ID); err != nil {
					slog.Warn("Failed to delete secret key from credential store", slogx.Error(err))
				}
			}

			slog.Info("Bookmark deleted", slog.Int64("id", bookmark.ID), slog.String("title", bookmark.Title))

			// Refresh bookmarks list
			a.refreshBookmarksList()

			// If this was the active bookmark, disconnect
			if a.activeBookmark != nil && a.activeBookmark.ID == bookmark.ID {
				a.disconnect()
			}
		},
		parentWindow,
	)
}
