package gui

import (
	"context"
	"fmt"
	"strings"
	"time"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/app"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/theme"
	"fyne.io/fyne/v2/widget"
	"github.com/dustin/go-humanize"

	"github.com/vgarvardt/stree/assets"
	"github.com/vgarvardt/stree/pkg/models"
	"github.com/vgarvardt/stree/pkg/service"
)

const (
	uidPrefixBucket = "bucket:"
	uidPrefixMeta   = "meta:"
)

// App represents the GUI application
type App struct {
	fyneApp   fyne.App
	window    fyne.Window
	tree      *widget.Tree
	statusBar *widget.Label
	treeData  *TreeData
	version   string

	svc *service.Service

	// Bookmark management
	bookmarkSelect *widget.Select

	// Track the objects list window to ensure modality
	objectsWindow fyne.Window
	// Track the MPU list window to ensure modality
	mpuWindow   fyne.Window
	mainContent fyne.CanvasObject
}

// TreeData holds the hierarchical data for the tree widget
type TreeData struct {
	buckets        []models.Bucket
	bucketIndex    map[string]*models.Bucket         // O(1) lookup by name
	bucketMetadata map[string]*models.BucketMetadata // bucketName -> metadata
	searchFilter   string                            // search filter for bucket names
	sortMode       service.SortMode                  // current sorting mode
}

// setBuckets replaces the bucket list and rebuilds the name index.
func (td *TreeData) setBuckets(buckets []models.Bucket) {
	td.buckets = buckets
	td.rebuildBucketIndex()
}

// rebuildBucketIndex rebuilds the name→bucket pointer map from the current slice.
// Must be called after sorting or modifying individual bucket entries.
func (td *TreeData) rebuildBucketIndex() {
	td.bucketIndex = make(map[string]*models.Bucket, len(td.buckets))
	for i := range td.buckets {
		td.bucketIndex[td.buckets[i].Name] = &td.buckets[i]
	}
}

// NewApp creates a new GUI application
func NewApp(svc *service.Service, version string) *App {
	fyneApp := app.New()
	fyneApp.SetIcon(assets.AppIcon)

	return &App{
		fyneApp: fyneApp,
		version: version,
		svc:     svc,
		treeData: &TreeData{
			buckets:        []models.Bucket{},
			bucketIndex:    make(map[string]*models.Bucket),
			bucketMetadata: make(map[string]*models.BucketMetadata),
			searchFilter:   "",
			sortMode:       service.SortNameAsc,
		},
	}
}

// doUI runs a function on the Fyne UI thread (blocking until complete).
func (a *App) doUI(fn func()) {
	a.fyneApp.Driver().DoFromGoroutine(fn, true)
}

// doUIAsync runs a function on the Fyne UI thread (non-blocking).
func (a *App) doUIAsync(fn func()) {
	a.fyneApp.Driver().DoFromGoroutine(fn, false)
}

// Run starts the GUI application
func (a *App) Run(ctx context.Context) error {
	a.svc.SetContext(ctx)

	a.window = a.fyneApp.NewWindow("STree Browser")
	a.window.Resize(fyne.NewSize(1000, 700))

	// Initialize bookmark selector
	bookmarkSelector := a.initBookmarkSelector()

	// Load bookmarks list
	a.refreshBookmarksList()

	// Create top toolbar with bookmark selector on the left
	refreshButton := widget.NewButtonWithIcon("", theme.ViewRefreshIcon(), func() {
		if !a.svc.IsConnected() {
			a.statusBar.SetText("Not connected. Select a bookmark to connect.")
			return
		}
		go a.refreshBuckets()
	})

	// Create sort dropdown
	sortOptions := widget.NewSelect(
		[]string{service.SortNameAsc.String(), service.SortNameDesc.String(), service.SortDateAsc.String(), service.SortDateDesc.String()},
		func(selected string) {
			var mode service.SortMode
			switch selected {
			case service.LabelSortByNameAsc:
				mode = service.SortNameAsc
			case service.LabelSortByNameDesc:
				mode = service.SortNameDesc
			case service.LabelSortByDateAsc:
				mode = service.SortDateAsc
			case service.LabelSortByDateDesc:
				mode = service.SortDateDesc
			default:
				mode = service.SortNameAsc
			}
			a.treeData.sortMode = mode
			a.sortBuckets()
			a.tree.Refresh()
		},
	)

	// Create search input
	searchEntry := widget.NewEntry()
	searchEntry.SetPlaceHolder("Filter by name...")
	searchEntry.OnChanged = func(query string) {
		a.treeData.searchFilter = query
		a.tree.Refresh()
	}

	// Left side: bookmark selector
	leftToolbar := container.NewHBox(bookmarkSelector, refreshButton, sortOptions)

	// Right side: search
	toolbar := container.NewAdaptiveGrid(2, leftToolbar, searchEntry)

	// Create status bar
	a.statusBar = widget.NewLabel("Not connected. Select a bookmark to connect.")
	statusContainer := container.NewBorder(nil, nil, widget.NewIcon(theme.InfoIcon()), nil, a.statusBar)

	// Create tree widget
	a.tree = a.createTree()
	// Set initial sort option only after creating a tree to avoid nil pointer dereference
	sortOptions.SetSelected(a.treeData.sortMode.String())

	// Create main content with scrolling
	content := container.NewBorder(
		toolbar,                     // top
		statusContainer,             // bottom
		nil,                         // left
		nil,                         // right
		container.NewScroll(a.tree), // center
	)

	a.window.SetContent(content)

	// Don't load buckets automatically - wait for bookmark selection

	a.window.ShowAndRun()

	return nil
}

// createTree initializes the tree widget
func (a *App) createTree() *widget.Tree {
	tree := widget.NewTree(
		// ChildUIDs function
		func(uid string) []string {
			if uid == "" {
				// Root level - return filtered bucket names
				filteredBuckets := a.getFilteredBuckets()
				uids := make([]string, len(filteredBuckets))
				for i, bucket := range filteredBuckets {
					uids[i] = uidPrefixBucket + bucket.Name
				}
				return uids
			}

			// Check if this is a bucket node
			if strings.HasPrefix(uid, uidPrefixBucket) {
				bucketName := uid[len(uidPrefixBucket):]
				metadata, exists := a.treeData.bucketMetadata[bucketName]
				if !exists {
					return []string{}
				}

				// Return metadata items as child nodes
				items := []string{
					uidPrefixMeta + bucketName + ":created",
					uidPrefixMeta + bucketName + ":versioning",
					uidPrefixMeta + bucketName + ":lock",
					uidPrefixMeta + bucketName + ":retention",
				}

				// Add encryption item if bucket has encryption configured
				if b := a.treeData.bucketIndex[bucketName]; b != nil && b.Encryption != nil {
					items = append(items, uidPrefixMeta+bucketName+":encryption")
				}

				items = append(items,
					uidPrefixMeta+bucketName+":objects",
					uidPrefixMeta+bucketName+":mpus",
				)

				_ = metadata // Avoid unused variable
				return items
			}

			return []string{}
		},
		// IsBranch function
		func(uid string) bool {
			if uid == "" {
				return true
			}
			// Buckets are always branches (can be expanded)
			if strings.HasPrefix(uid, uidPrefixBucket) {
				return true
			}
			// Metadata items are not branches
			return false
		},
		// Create function
		func(branch bool) fyne.CanvasObject {
			if branch {
				icon := widget.NewIcon(theme.FolderIcon())
				label := widget.NewLabel("Template")
				box := container.NewHBox(icon, label)
				tappable := NewTappableContainer(box, nil)
				return tappable
			} else {
				icon := widget.NewIcon(theme.DocumentIcon())
				label := widget.NewLabel("Template")
				box := container.NewHBox(icon, label)
				tappable := NewTappableContainer(box, nil)
				return tappable
			}
		},
		// Update function
		func(uid string, branch bool, obj fyne.CanvasObject) {
			if uid == "" {
				// Root node
				if tappable, ok := obj.(*TappableContainer); ok {
					c := tappable.container
					icon := c.Objects[0].(*widget.Icon)
					label := c.Objects[1].(*widget.Label)
					label.SetText("Root")
					icon.SetResource(theme.FolderIcon())
					tappable.onSecondaryTap = nil
				}
				return
			}

			// Handle bucket nodes (branches)
			if strings.HasPrefix(uid, uidPrefixBucket) {
				tappable := obj.(*TappableContainer)
				c := tappable.container
				icon := c.Objects[0].(*widget.Icon)
				label := c.Objects[1].(*widget.Label)

				bucketName := uid[len(uidPrefixBucket):]
				var hasEncryption bool
				if bucket := a.treeData.bucketIndex[bucketName]; bucket != nil {
					label.SetText(bucketName + " @ " + bucket.CreationDate.Format(time.RFC3339))
					hasEncryption = bucket.Encryption != nil
				}

				// Update folder icon based on branch open/closed state and encryption
				bucketUID := uidPrefixBucket + bucketName
				if a.tree.IsBranchOpen(bucketUID) {
					icon.SetResource(theme.FolderOpenIcon())
				} else if hasEncryption {
					icon.SetResource(theme.FolderNewIcon())
				} else {
					icon.SetResource(theme.FolderIcon())
				}

				// Set right-click handler for bucket nodes
				tappable.onSecondaryTap = func(position fyne.Position) {
					a.showBucketContextMenu(bucketName, position)
				}
				return
			}

			// Handle metadata nodes (leaves) - these have icon + label
			if strings.HasPrefix(uid, uidPrefixMeta) {
				tappable := obj.(*TappableContainer)
				c := tappable.container
				icon := c.Objects[0].(*widget.Icon)
				label := c.Objects[1].(*widget.Label)

				parts := uid[len(uidPrefixMeta):] // Remove "meta:" prefix
				// Parse: bucketName:fieldName
				lastColon := -1
				for i := len(parts) - 1; i >= 0; i-- {
					if parts[i] == ':' {
						lastColon = i
						break
					}
				}
				if lastColon == -1 {
					label.SetText("Unknown")
					icon.SetResource(theme.QuestionIcon())
					tappable.onSecondaryTap = nil
					return
				}

				bucketName := parts[:lastColon]
				fieldName := parts[lastColon+1:]

				metadata, exists := a.treeData.bucketMetadata[bucketName]
				if !exists {
					label.SetText("Loading...")
					icon.SetResource(theme.InfoIcon())
					tappable.onSecondaryTap = nil
					return
				}

				switch fieldName {
				case "created":
					if bucket := a.treeData.bucketIndex[bucketName]; bucket != nil {
						label.SetText("Created: " + bucket.CreationDate.Format(time.RFC3339))
					} else {
						label.SetText("Created: Unknown")
					}
					icon.SetResource(theme.HistoryIcon())
					tappable.onSecondaryTap = nil
				case "versioning":
					status := "Disabled"
					if metadata.VersioningEnabled {
						status = "Enabled"
					} else if metadata.VersioningStatus != "" {
						status = metadata.VersioningStatus
					}
					label.SetText("Versioning: " + status)
					if metadata.VersioningEnabled {
						icon.SetResource(theme.CheckButtonCheckedIcon())
					} else {
						icon.SetResource(theme.CheckButtonIcon())
					}
					tappable.onSecondaryTap = nil
				case "lock":
					status := "Disabled"
					if metadata.ObjectLockEnabled {
						status = "Enabled"
					}
					label.SetText("Object Lock: " + status)
					if metadata.ObjectLockEnabled {
						icon.SetResource(theme.ConfirmIcon())
					} else {
						icon.SetResource(theme.CancelIcon())
					}
					tappable.onSecondaryTap = nil
				case "retention":
					if metadata.RetentionEnabled {
						if metadata.RetentionYears > 0 {
							period := "year"
							if metadata.RetentionYears > 1 {
								period = "years"
							}

							label.SetText(fmt.Sprintf("Retention: %d %s (%s)", metadata.RetentionYears, period, metadata.RetentionMode))
						} else if metadata.RetentionDays > 0 {
							period := "day"
							if metadata.RetentionDays > 1 {
								period = "days"
							}

							label.SetText(fmt.Sprintf("Retention: %d %s (%s)", metadata.RetentionDays, period, metadata.RetentionMode))
						} else {
							label.SetText(fmt.Sprintf("Retention: Enabled (%s)", metadata.RetentionMode))
						}
						icon.SetResource(theme.ContentAddIcon())
					} else {
						label.SetText("Retention: Not configured")
						icon.SetResource(theme.ContentRemoveIcon())
					}
					tappable.onSecondaryTap = nil
				case "encryption":
					label.SetText("Encryption: Enabled")
					icon.SetResource(theme.FolderNewIcon())
					tappable.onSecondaryTap = nil
					tappable.onDoubleTap = func() {
						a.showEncryptionDetails(bucketName)
					}
				case "objects":
					refreshedAt := "???"
					if metadata.ObjectsRefreshedAt != nil {
						refreshedAt = metadata.ObjectsRefreshedAt.Format(time.RFC3339)
					}
					label.SetText(fmt.Sprintf("Objects: %s / %s (dm %s) @ %s",
						humanize.Comma(metadata.ObjectsCount),
						humanize.Bytes(uint64(metadata.ObjectsSize)),
						humanize.Comma(metadata.DeleteMarkersCount),
						refreshedAt,
					))
					icon.SetResource(theme.StorageIcon())
					// Set right-click handler for objects metadata
					tappable.onSecondaryTap = func(position fyne.Position) {
						a.showObjectsContextMenu(bucketName, metadata, position)
					}
					// Set double-click handler to open objects list
					tappable.onDoubleTap = func() {
						go a.showObjectsList(bucketName)
					}
				case "mpus":
					refreshedAt := "???"
					if metadata.MPUsRefreshedAt != nil {
						refreshedAt = metadata.MPUsRefreshedAt.Format(time.RFC3339)
					}
					label.SetText(fmt.Sprintf("MPUs: %s / %s parts / %s @ %s",
						humanize.Comma(metadata.MPUsCount),
						humanize.Comma(metadata.MPUsTotalParts),
						humanize.Bytes(uint64(metadata.MPUsTotalSize)),
						refreshedAt,
					))
					icon.SetResource(theme.UploadIcon())
					// Set right-click handler for MPUs metadata
					tappable.onSecondaryTap = func(position fyne.Position) {
						a.showMPUsContextMenu(bucketName, metadata, position)
					}
					// Set double-click handler to open MPU list
					tappable.onDoubleTap = func() {
						go a.showMPUList(bucketName)
					}
				default:
					label.SetText("Unknown field")
					icon.SetResource(theme.QuestionIcon())
					tappable.onSecondaryTap = nil
				}
			}
		},
	)

	// Visual settings
	tree.HideSeparators = true

	// Handle node opening (expansion)
	tree.OnBranchOpened = func(uid string) {
		// Check if this is a bucket that hasn't been loaded yet
		if strings.HasPrefix(uid, uidPrefixBucket) {
			bucketName := uid[len(uidPrefixBucket):]
			if _, exists := a.treeData.bucketMetadata[bucketName]; !exists {
				go a.loadBucketMetadata(bucketName)
			}
		}
	}

	return tree
}

// closeObjectsWindow closes the objects list window if it's open
func (a *App) closeObjectsWindow() {
	if a.objectsWindow != nil {
		a.doUIAsync(func() {
			a.objectsWindow.Close()
			a.objectsWindow = nil
		})
	}
}

// closeMPUWindow closes the MPU list window if it's open
func (a *App) closeMPUWindow() {
	if a.mpuWindow != nil {
		a.doUIAsync(func() {
			a.mpuWindow.Close()
			a.mpuWindow = nil
		})
	}
}
