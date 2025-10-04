package gui

import (
	"fmt"
	"log/slog"
	"time"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/theme"
	"fyne.io/fyne/v2/widget"
	"github.com/cappuccinotm/slogx"
	"github.com/dustin/go-humanize"
	"github.com/goccy/go-json"

	"github.com/vgarvardt/stree/pkg/models"
	"github.com/vgarvardt/stree/pkg/storage"
)

const objectsListLimit = 1000

// showObjectsList opens a modal window displaying objects in the bucket
func (a *App) showObjectsList(bucketName string) {
	// Create and show the window on the Fyne UI thread
	a.fyneApp.Driver().DoFromGoroutine(func() {
		// If an objects window is already open, close it first
		if a.objectsWindow != nil {
			a.objectsWindow.Close()
			a.objectsWindow = nil
		}

		// Create the modal window
		modalWindow := a.fyneApp.NewWindow(fmt.Sprintf("Objects @ %s", bucketName))
		modalWindow.Resize(fyne.NewSize(1200, 700))

		// Store reference to the objects window
		a.objectsWindow = modalWindow

		// Set up close handler to clean up reference
		modalWindow.SetOnClosed(func() {
			a.objectsWindow = nil
		})

		// Create the objects list view
		objectsView := newObjectsListView(a, modalWindow, bucketName)

		// Set window content
		modalWindow.SetContent(objectsView.buildUI())

		// Show window
		modalWindow.Show()

		// Initialize dropdown selections after window is shown to avoid triggering callbacks during setup
		objectsView.initializeSelections()

		// Load objects asynchronously after the window is shown
		go objectsView.loadObjects()
	}, false)
}

// objectsListView manages the objects list UI state
type objectsListView struct {
	app        *App
	window     fyne.Window
	bucketName string
	bucketID   int64

	// UI components
	table         *widget.Table
	statusBar     *widget.Label
	sortSelect    *widget.Select
	filterSelect  *widget.Select
	refreshButton *widget.Button

	// Data
	objects []models.ObjectVersion
}

// newObjectsListView creates a new objects list view
func newObjectsListView(app *App, window fyne.Window, bucketName string) *objectsListView {
	return &objectsListView{
		app:        app,
		window:     window,
		bucketName: bucketName,
		objects:    []models.ObjectVersion{},
	}
}

// buildUI constructs the UI layout
func (v *objectsListView) buildUI() fyne.CanvasObject {
	// Create status bar
	v.statusBar = widget.NewLabel("Loading...")
	statusContainer := container.NewBorder(nil, nil, widget.NewIcon(theme.InfoIcon()), nil, v.statusBar)

	// Create sort dropdown with initial selection but NO callback yet
	sortOptions := []string{
		"Size ↓",
		"Size ↑",
		"Date ↓",
		"Date ↑",
	}
	v.sortSelect = widget.NewSelect(sortOptions, nil) // No callback during construction
	v.sortSelect.SetSelected("Date ↓")                // Set initial value before attaching callback

	// Create filter dropdown with initial selection but NO callback yet
	filterOptions := []string{
		"All",
		"Files only",
		"Delete markers only",
	}
	v.filterSelect = widget.NewSelect(filterOptions, nil) // No callback during construction
	v.filterSelect.SetSelected("All")                     // Set initial value before attaching callback

	// Create refresh button
	v.refreshButton = widget.NewButtonWithIcon("", theme.ViewRefreshIcon(), func() {
		go v.refreshObjects()
	})

	// Create toolbar with proper layout
	sortLabel := widget.NewLabel("Sort:")
	filterLabel := widget.NewLabel("Filter:")

	toolbar := container.NewHBox(
		v.refreshButton,
		sortLabel,
		v.sortSelect,
		filterLabel,
		v.filterSelect,
	)

	// Create table
	v.table = v.createTable()

	// Create main layout
	content := container.NewBorder(
		toolbar,                      // top
		statusContainer,              // bottom
		nil,                          // left
		nil,                          // right
		container.NewScroll(v.table), // center
	)

	return content
}

// createTable creates the objects table widget
func (v *objectsListView) createTable() *widget.Table {
	table := widget.NewTable(
		// Length
		func() (int, int) {
			return len(v.objects) + 1, 7 // +1 for header row, 7 columns
		},
		// Create
		func() fyne.CanvasObject {
			label := widget.NewLabel("Template")
			label.Truncation = fyne.TextTruncateEllipsis
			return label
		},
		// Update
		func(cell widget.TableCellID, obj fyne.CanvasObject) {
			label := obj.(*widget.Label)

			// Header row
			if cell.Row == 0 {
				headers := []string{"Key", "Version ID", "Latest", "Size", "Last Modified", "Delete Marker", "Storage Class"}
				if cell.Col < len(headers) {
					label.SetText(headers[cell.Col])
					label.TextStyle = fyne.TextStyle{Bold: true}
				}
				return
			}

			// Data rows
			rowIndex := cell.Row - 1
			if rowIndex >= len(v.objects) {
				label.SetText("")
				return
			}

			objectVersion := v.objects[rowIndex]
			label.TextStyle = fyne.TextStyle{Bold: false}

			switch cell.Col {
			case 0: // Key
				label.SetText(objectVersion.Key)
			case 1: // Version ID
				versionID := objectVersion.VersionID
				if versionID == "" {
					versionID = "null"
				}
				label.SetText(versionID)
			case 2: // Latest
				if objectVersion.IsLatest {
					label.SetText("Yes")
				} else {
					label.SetText("No")
				}
			case 3: // Size
				label.SetText(humanize.Bytes(uint64(objectVersion.Size)))
			case 4: // Last Modified
				label.SetText(objectVersion.LastModified.Format(time.RFC3339))
			case 5: // Delete Marker
				if objectVersion.IsDeleteMarker {
					label.SetText("Yes")
				} else {
					label.SetText("No")
				}
			case 6: // Storage Class
				storageClass := objectVersion.StorageClass
				if storageClass == "" {
					storageClass = "-"
				}
				label.SetText(storageClass)
			default:
				label.SetText("")
			}
		},
	)

	// Set column widths
	table.SetColumnWidth(0, 300) // Key
	table.SetColumnWidth(1, 150) // Version ID
	table.SetColumnWidth(2, 60)  // Latest
	table.SetColumnWidth(3, 100) // Size
	table.SetColumnWidth(4, 200) // Last Modified
	table.SetColumnWidth(5, 100) // Delete Marker
	table.SetColumnWidth(6, 120) // Storage Class

	return table
}

// loadObjects loads objects from storage or S3
func (v *objectsListView) loadObjects() {
	slog.Info("Loading objects list", slog.String("bucket", v.bucketName))

	v.app.fyneApp.Driver().DoFromGoroutine(func() {
		v.statusBar.SetText("Loading objects...")
		v.refreshButton.Disable()
	}, true)

	// Get bucket ID - first try from storage
	bucket, err := v.app.storage.GetBucket(v.app.ctx, v.app.sessionID, v.bucketName)
	if err != nil {
		slog.Error("Failed to get bucket from storage", slogx.Error(err), slog.String("bucket", v.bucketName))
		v.app.fyneApp.Driver().DoFromGoroutine(func() {
			v.statusBar.SetText(fmt.Sprintf("Error: %v", err))
			v.refreshButton.Enable()
		}, true)
		return
	}

	// If bucket not found in storage, try to find it in the app's in-memory data and store it
	if bucket == nil {
		slog.Warn("Bucket not found in storage, attempting to store it", slog.String("bucket", v.bucketName))

		// Find bucket in app's in-memory data
		var foundBucket *models.Bucket
		for _, b := range v.app.treeData.buckets {
			if b.Name == v.bucketName {
				foundBucket = &b
				break
			}
		}

		if foundBucket == nil {
			slog.Error("Bucket not found in app data", slog.String("bucket", v.bucketName))
			v.app.fyneApp.Driver().DoFromGoroutine(func() {
				v.statusBar.SetText("Bucket not found")
				v.refreshButton.Enable()
			}, true)
			return
		}

		// Store the bucket with metadata if available
		metadata := v.app.treeData.bucketMetadata[v.bucketName]
		details := models.NewBucketDetails(*foundBucket, metadata)
		if err := v.app.storage.UpsertBucket(v.app.ctx, v.app.sessionID, foundBucket.Name, foundBucket.CreationDate, details); err != nil {
			slog.Error("Failed to store bucket", slogx.Error(err), slog.String("bucket", v.bucketName))
			v.app.fyneApp.Driver().DoFromGoroutine(func() {
				v.statusBar.SetText(fmt.Sprintf("Error storing bucket: %v", err))
				v.refreshButton.Enable()
			}, true)
			return
		}

		// Now try to get it again
		bucket, err = v.app.storage.GetBucket(v.app.ctx, v.app.sessionID, v.bucketName)
		if err != nil || bucket == nil {
			slog.Error("Failed to get bucket after storing", slogx.Error(err), slog.String("bucket", v.bucketName))
			v.app.fyneApp.Driver().DoFromGoroutine(func() {
				v.statusBar.SetText("Error: Could not retrieve bucket")
				v.refreshButton.Enable()
			}, true)
			return
		}
	}

	v.bucketID = bucket.ID

	// Build list options
	opts := storage.ObjectListOptions{
		Limit: objectsListLimit,
	}

	// Apply sorting
	switch v.sortSelect.Selected {
	case "Size ↓":
		opts.OrderBy = storage.OrderBySize
		opts.OrderDesc = true
	case "Size ↑":
		opts.OrderBy = storage.OrderBySize
		opts.OrderDesc = false
	case "Date ↓":
		opts.OrderBy = storage.OrderByLastModified
		opts.OrderDesc = true
	case "Date ↑":
		opts.OrderBy = storage.OrderByLastModified
		opts.OrderDesc = false
	}

	// Apply filtering
	switch v.filterSelect.Selected {
	case "Files only":
		deleteMarker := false
		opts.FilterDeleteMarker = &deleteMarker
	case "Delete markers only":
		deleteMarker := true
		opts.FilterDeleteMarker = &deleteMarker
	}

	// Try to load from storage first
	storageObjects, err := v.app.storage.ListObjectsByBucket(v.app.ctx, v.bucketID, opts)
	if err != nil {
		slog.Error("Failed to list objects from storage", slogx.Error(err), slog.String("bucket", v.bucketName))
	}

	// If we have objects in storage, use them
	if len(storageObjects) > 0 {
		slog.Info("Loading objects from storage", slog.String("bucket", v.bucketName), slog.Int("count", len(storageObjects)))

		// Deserialize objects
		v.objects = make([]models.ObjectVersion, 0, len(storageObjects))
		for _, obj := range storageObjects {
			var version models.ObjectVersion
			if err := json.Unmarshal(obj.Properties, &version); err != nil {
				slog.Warn("Failed to unmarshal object version", slogx.Error(err))
				continue
			}
			v.objects = append(v.objects, version)
		}

		v.app.fyneApp.Driver().DoFromGoroutine(func() {
			v.table.Refresh()
			v.statusBar.SetText(fmt.Sprintf("Loaded %d object(s) from cache", len(v.objects)))
			v.refreshButton.Enable()
		}, false)
		return
	}

	// No objects in storage, fetch from S3
	slog.Info("Fetching objects from S3", slog.String("bucket", v.bucketName))
	v.app.fyneApp.Driver().DoFromGoroutine(func() {
		v.statusBar.SetText("Fetching objects from S3...")
	}, true)

	// Fetch from S3
	if err := v.fetchFromS3(); err != nil {
		slog.Error("Failed to fetch objects from S3", slogx.Error(err), slog.String("bucket", v.bucketName))
		v.app.fyneApp.Driver().DoFromGoroutine(func() {
			v.statusBar.SetText(fmt.Sprintf("Error: %v", err))
			v.refreshButton.Enable()
		}, true)
		return
	}

	// Reload from storage with filters applied
	v.loadObjects()
}

// fetchFromS3 fetches all objects from S3 and stores them
func (v *objectsListView) fetchFromS3() error {
	// Use app context to ensure we're using the same storage instance
	ctx := v.app.ctx

	// List all object versions with pagination
	var allVersions []models.ObjectVersion
	pagination := &models.Pagination{}

	for {
		versions, nextPagination, err := v.app.s3Client.ListObjectVersions(ctx, v.bucketName, pagination)
		if err != nil {
			return fmt.Errorf("failed to list object versions: %w", err)
		}

		allVersions = append(allVersions, versions...)

		// Check if there are more pages
		if nextPagination == nil || !nextPagination.IsTruncated {
			break
		}

		pagination = nextPagination
	}

	slog.Info("Fetched objects from S3", slog.String("bucket", v.bucketName), slog.Int("count", len(allVersions)))

	// Store in database
	if err := v.app.storage.BulkInsertObjectVersions(ctx, v.bucketID, allVersions); err != nil {
		return fmt.Errorf("failed to store objects: %w", err)
	}

	return nil
}

// refreshObjects invalidates cache and reloads objects
func (v *objectsListView) refreshObjects() {
	slog.Info("Refreshing objects list", slog.String("bucket", v.bucketName))

	v.app.fyneApp.Driver().DoFromGoroutine(func() {
		v.statusBar.SetText("Refreshing objects...")
		v.refreshButton.Disable()
	}, true)

	// Delete objects from storage
	if err := v.app.storage.DeleteObjectsByBucket(v.app.ctx, v.bucketID); err != nil {
		slog.Warn("Failed to delete objects from storage", slogx.Error(err), slog.String("bucket", v.bucketName))
	}

	// Reload
	v.loadObjects()
}

// initializeSelections sets the initial selections for dropdowns without triggering callbacks
func (v *objectsListView) initializeSelections() {
	// Initial values are already set in buildUI, now attach the callbacks
	v.sortSelect.OnChanged = func(selected string) {
		go v.loadObjects()
	}
	v.filterSelect.OnChanged = func(selected string) {
		go v.loadObjects()
	}
}
