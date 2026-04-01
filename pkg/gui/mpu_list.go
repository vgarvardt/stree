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
)

const mpuListLimit = 1000

// showMPUList opens a modal window displaying multipart uploads in the bucket
func (a *App) showMPUList(bucketName string) {
	// Create and show the window on the Fyne UI thread
	a.doUIAsync(func() {
		// If an MPU window is already open, close it first
		if a.mpuWindow != nil {
			a.mpuWindow.Close()
			a.mpuWindow = nil
		}

		// Create the modal window
		modalWindow := a.fyneApp.NewWindow(fmt.Sprintf("Multipart Uploads @ %s", bucketName))
		modalWindow.Resize(fyne.NewSize(1200, 700))

		// Store reference to the MPU window
		a.mpuWindow = modalWindow

		// Set up close handler to clean up reference
		modalWindow.SetOnClosed(func() {
			a.mpuWindow = nil
		})

		// Add Cmd+W / Ctrl+W shortcut to close the window
		modalWindow.Canvas().AddShortcut(
			&desktop.CustomShortcut{KeyName: fyne.KeyW, Modifier: fyne.KeyModifierShortcutDefault},
			func(_ fyne.Shortcut) { modalWindow.Close() },
		)

		// Create the MPU list view
		mpuView := newMPUListView(a, modalWindow, bucketName)

		// Set window content
		modalWindow.SetContent(mpuView.buildUI())

		// Show window
		modalWindow.Show()

		// Initialize dropdown selections after window is shown to avoid triggering callbacks during setup
		mpuView.initializeSelections()

		// Load MPUs asynchronously after the window is shown
		go mpuView.loadMPUs()
	})
}

// mpuListView manages the MPU list UI state
type mpuListView struct {
	app        *App
	window     fyne.Window
	bucketName string

	// UI components
	table         *widget.Table
	statusBar     *widget.Label
	sortSelect    *widget.Select
	refreshButton *widget.Button

	// Data
	uploads []models.MultipartUploadWithParts
}

// newMPUListView creates a new MPU list view
func newMPUListView(app *App, window fyne.Window, bucketName string) *mpuListView {
	return &mpuListView{
		app:        app,
		window:     window,
		bucketName: bucketName,
		uploads:    []models.MultipartUploadWithParts{},
	}
}

// buildUI constructs the UI layout
func (v *mpuListView) buildUI() fyne.CanvasObject {
	// Create status bar
	v.statusBar = widget.NewLabel("Loading...")
	statusContainer := container.NewBorder(nil, nil, widget.NewIcon(theme.InfoIcon()), nil, v.statusBar)

	// Create sort dropdown with initial selection but NO callback yet
	sortOptions := []string{
		"Date ↓",
		"Date ↑",
	}
	v.sortSelect = widget.NewSelect(sortOptions, nil) // No callback during construction
	v.sortSelect.SetSelected("Date ↓")                // Set initial value before attaching callback

	// Create refresh button
	v.refreshButton = widget.NewButtonWithIcon("", theme.ViewRefreshIcon(), func() {
		go v.refreshMPUs()
	})

	// Create toolbar with proper layout
	sortLabel := widget.NewLabel("Sort:")

	toolbar := container.NewHBox(
		v.refreshButton,
		sortLabel,
		v.sortSelect,
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

// createTable creates the MPU table widget
func (v *mpuListView) createTable() *widget.Table {
	table := widget.NewTable(
		// Length
		func() (int, int) {
			return len(v.uploads) + 1, 6 // +1 for header row, 6 columns
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
				headers := []string{"Key", "Upload ID", "Parts", "Total Size", "Initiated", "Storage Class"}
				if cell.Col < len(headers) {
					label.SetText(headers[cell.Col])
					label.TextStyle = fyne.TextStyle{Bold: true}
				}
				return
			}

			// Data rows
			rowIndex := cell.Row - 1
			if rowIndex >= len(v.uploads) {
				label.SetText("")
				return
			}

			upload := v.uploads[rowIndex]
			label.TextStyle = fyne.TextStyle{Bold: false}

			switch cell.Col {
			case 0: // Key
				label.SetText(upload.Key)
			case 1: // Upload ID
				uploadID := upload.UploadID
				if len(uploadID) > 20 {
					uploadID = uploadID[:17] + "..."
				}
				label.SetText(uploadID)
			case 2: // Parts
				label.SetText(fmt.Sprintf("%d", upload.PartsCount))
			case 3: // Total Size
				label.SetText(humanize.Bytes(uint64(upload.TotalSize)))
			case 4: // Initiated
				label.SetText(upload.Initiated.Format(time.RFC3339))
			case 5: // Storage Class
				storageClass := upload.StorageClass
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
	table.SetColumnWidth(1, 180) // Upload ID
	table.SetColumnWidth(2, 80)  // Parts
	table.SetColumnWidth(3, 100) // Total Size
	table.SetColumnWidth(4, 200) // Initiated
	table.SetColumnWidth(5, 120) // Storage Class

	return table
}

// loadMPUs loads multipart uploads from storage via the service
func (v *mpuListView) loadMPUs() {
	slog.Info("Loading MPU list", slog.String("bucket", v.bucketName))

	v.app.doUI(func() {
		v.statusBar.SetText("Loading multipart uploads...")
		v.refreshButton.Disable()
	})

	sortDesc := v.sortSelect.Selected == "Date ↓"

	uploads, err := v.app.svc.ListMPUs(v.app.svc.OpCtx(), v.bucketName, sortDesc, mpuListLimit)
	if err != nil {
		slog.Error("Failed to list MPUs", slogx.Error(err), slog.String("bucket", v.bucketName))
		v.app.doUI(func() {
			v.statusBar.SetText(fmt.Sprintf("Error: %v", err))
			v.refreshButton.Enable()
		})
		return
	}

	if len(uploads) == 0 {
		v.uploads = []models.MultipartUploadWithParts{}
		v.app.doUIAsync(func() {
			v.table.Refresh()
			v.statusBar.SetText("No multipart uploads in cache. Use 'Refresh' on the MPUs metadata in the tree to load.")
			v.refreshButton.Enable()
		})
		return
	}

	slog.Info("Loading MPUs from storage", slog.String("bucket", v.bucketName), slog.Int("count", len(uploads)))

	v.uploads = uploads

	v.app.doUIAsync(func() {
		v.table.Refresh()
		v.statusBar.SetText(fmt.Sprintf("Loaded %d multipart upload(s) from cache", len(v.uploads)))
		v.refreshButton.Enable()
	})
}

// refreshMPUs clears the current view and instructs user to refresh from the main tree
func (v *mpuListView) refreshMPUs() {
	slog.Info("Refresh requested for MPU list", slog.String("bucket", v.bucketName))

	v.app.doUI(func() {
		v.statusBar.SetText("To refresh MPUs, use 'Refresh' on the MPUs metadata in the main tree view.")
		v.refreshButton.Enable()
	})
}

// initializeSelections sets the initial selections for dropdowns without triggering callbacks
func (v *mpuListView) initializeSelections() {
	// Initial values are already set in buildUI, now attach the callbacks
	v.sortSelect.OnChanged = func(selected string) {
		go v.loadMPUs()
	}
}
