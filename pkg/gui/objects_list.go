package gui

import (
	"fmt"
	"log/slog"
	"time"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/driver/desktop"
	"fyne.io/fyne/v2/theme"
	"fyne.io/fyne/v2/widget"
	"github.com/cappuccinotm/slogx"
	"github.com/dustin/go-humanize"

	"github.com/vgarvardt/stree/pkg/models"
	"github.com/vgarvardt/stree/pkg/service"
)

const objectsListLimit = 1000

// showObjectsList opens a modal window displaying objects in the bucket
func (a *App) showObjectsList(bucketName string) {
	// Create and show the window on the Fyne UI thread
	a.doUIAsync(func() {
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

		// Add Cmd+W / Ctrl+W shortcut to close the window
		modalWindow.Canvas().AddShortcut(
			&desktop.CustomShortcut{KeyName: fyne.KeyW, Modifier: fyne.KeyModifierShortcutDefault},
			func(_ fyne.Shortcut) { modalWindow.Close() },
		)

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
	})
}

// objectsListView manages the objects list UI state
type objectsListView struct {
	app        *App
	window     fyne.Window
	bucketName string

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
			return newTappableLabel("Template")
		},
		// Update
		func(cell widget.TableCellID, obj fyne.CanvasObject) {
			tl := obj.(*tappableLabel)

			// Header row
			if cell.Row == 0 {
				headers := []string{"Key", "Version ID", "Latest", "Size", "Last Modified", "Delete Marker", "Storage Class"}
				if cell.Col < len(headers) {
					tl.SetText(headers[cell.Col])
					tl.label.TextStyle = fyne.TextStyle{Bold: true}
				}
				tl.onSecondaryTap = nil
				return
			}

			// Data rows
			rowIndex := cell.Row - 1
			if rowIndex >= len(v.objects) {
				tl.SetText("")
				tl.onSecondaryTap = nil
				return
			}

			objectVersion := v.objects[rowIndex]
			tl.label.TextStyle = fyne.TextStyle{Bold: false}

			switch cell.Col {
			case 0: // Key
				tl.SetText(objectVersion.Key)
			case 1: // Version ID
				versionID := objectVersion.VersionID
				if versionID == "" {
					versionID = "null"
				}
				tl.SetText(versionID)
			case 2: // Latest
				if objectVersion.IsLatest {
					tl.SetText("Yes")
				} else {
					tl.SetText("No")
				}
			case 3: // Size
				tl.SetText(humanize.Bytes(uint64(objectVersion.Size)))
			case 4: // Last Modified
				tl.SetText(objectVersion.LastModified.Format(time.RFC3339))
			case 5: // Delete Marker
				if objectVersion.IsDeleteMarker {
					tl.SetText("Yes")
				} else {
					tl.SetText("No")
				}
			case 6: // Storage Class
				storageClass := objectVersion.StorageClass
				if storageClass == "" {
					storageClass = "-"
				}
				tl.SetText(storageClass)
			default:
				tl.SetText("")
			}

			// Set right-click handler for data rows — capture objectVersion by value
			ov := objectVersion
			tl.onSecondaryTap = func(pe *fyne.PointEvent) {
				v.showObjectContextMenu(ov, pe.AbsolutePosition)
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

// getSortMode returns the service sort mode from the UI dropdown selection
func (v *objectsListView) getSortMode() service.ObjectSort {
	switch v.sortSelect.Selected {
	case "Size ↓":
		return service.ObjectSortSizeDesc
	case "Size ↑":
		return service.ObjectSortSizeAsc
	case "Date ↓":
		return service.ObjectSortDateDesc
	case "Date ↑":
		return service.ObjectSortDateAsc
	default:
		return service.ObjectSortDateDesc
	}
}

// getFilter returns the service filter from the UI dropdown selection
func (v *objectsListView) getFilter() service.ObjectFilter {
	switch v.filterSelect.Selected {
	case "Files only":
		return service.ObjectFilterFilesOnly
	case "Delete markers only":
		return service.ObjectFilterDeleteMarkersOnly
	default:
		return service.ObjectFilterAll
	}
}

// loadObjects loads objects from storage via the service
func (v *objectsListView) loadObjects() {
	slog.Info("Loading objects list", slog.String("bucket", v.bucketName))

	v.app.doUI(func() {
		v.statusBar.SetText("Loading objects...")
		v.refreshButton.Disable()
	})

	var foundBucket *models.Bucket
	if b, ok := v.app.treeData.bucketIndex.Get(v.bucketName); ok {
		bucketCopy := *b
		foundBucket = &bucketCopy
	}
	metadata, _ := v.app.treeData.bucketMetadata.Get(v.bucketName)

	// Ensure bucket exists in storage (handles the case where bucket was loaded but not yet stored)
	if foundBucket != nil {
		if err := v.app.svc.EnsureBucketInStorage(v.app.svc.OpCtx(), *foundBucket, metadata); err != nil {
			slog.Error("Failed to ensure bucket in storage", slogx.Error(err), slog.String("bucket", v.bucketName))
			v.app.doUI(func() {
				v.statusBar.SetText(fmt.Sprintf("Error: %v", err))
				v.refreshButton.Enable()
			})
			return
		}
	}

	objects, err := v.app.svc.ListObjects(v.app.svc.OpCtx(), v.bucketName, v.getSortMode(), v.getFilter(), objectsListLimit)
	if err != nil {
		slog.Error("Failed to list objects", slogx.Error(err), slog.String("bucket", v.bucketName))
		v.app.doUI(func() {
			v.statusBar.SetText(fmt.Sprintf("Error: %v", err))
			v.refreshButton.Enable()
		})
		return
	}

	if len(objects) == 0 {
		v.objects = []models.ObjectVersion{}
		v.app.doUIAsync(func() {
			v.table.Refresh()
			v.statusBar.SetText("No objects in cache. Use 'Refresh' on the Objects metadata in the tree to load objects.")
			v.refreshButton.Enable()
		})
		return
	}

	v.objects = objects

	v.app.doUIAsync(func() {
		v.table.Refresh()
		v.statusBar.SetText(fmt.Sprintf("Loaded %d object(s) from cache", len(v.objects)))
		v.refreshButton.Enable()
	})
}

// refreshObjects clears the current view and instructs user to refresh from the main tree
func (v *objectsListView) refreshObjects() {
	slog.Info("Refresh requested for objects list", slog.String("bucket", v.bucketName))

	v.app.doUI(func() {
		v.statusBar.SetText("To refresh objects, use 'Refresh' on the Objects metadata in the main tree view.")
		v.refreshButton.Enable()
	})
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

// showObjectContextMenu displays a context menu for an object row
func (v *objectsListView) showObjectContextMenu(obj models.ObjectVersion, position fyne.Position) {
	copyKeyItem := fyne.NewMenuItem("Copy key", func() {
		v.window.Clipboard().SetContent(obj.Key)
		slog.Info("Copied object key to clipboard", slog.String("key", obj.Key))
		v.statusBar.SetText(fmt.Sprintf("Copied key %q to clipboard", obj.Key))
	})
	copyKeyItem.Icon = theme.ContentCopyIcon()

	copyVersionIDItem := fyne.NewMenuItem("Copy version ID", func() {
		versionID := obj.VersionID
		if versionID == "" {
			versionID = "null"
		}
		v.window.Clipboard().SetContent(versionID)
		slog.Info("Copied version ID to clipboard", slog.String("versionID", versionID))
		v.statusBar.SetText(fmt.Sprintf("Copied version ID %q to clipboard", versionID))
	})
	copyVersionIDItem.Icon = theme.ContentCopyIcon()

	menu := fyne.NewMenu("", copyKeyItem, copyVersionIDItem)
	popUpMenu := widget.NewPopUpMenu(menu, v.window.Canvas())
	popUpMenu.ShowAtPosition(position)
}

// tappableLabel is a label widget that supports right-click (secondary tap) for context menus.
type tappableLabel struct {
	widget.BaseWidget
	label          *widget.Label
	onSecondaryTap func(*fyne.PointEvent)
}

func newTappableLabel(text string) *tappableLabel {
	tl := &tappableLabel{
		label: widget.NewLabel(text),
	}
	tl.label.Truncation = fyne.TextTruncateEllipsis
	tl.ExtendBaseWidget(tl)
	return tl
}

func (tl *tappableLabel) SetText(text string) {
	tl.label.SetText(text)
}

func (tl *tappableLabel) CreateRenderer() fyne.WidgetRenderer {
	return widget.NewSimpleRenderer(tl.label)
}

func (tl *tappableLabel) TappedSecondary(pe *fyne.PointEvent) {
	if tl.onSecondaryTap != nil {
		tl.onSecondaryTap(pe)
	}
}
