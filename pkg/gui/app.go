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

	"github.com/vgarvardt/stree/pkg/models"
	"github.com/vgarvardt/stree/pkg/s3client"
	"github.com/vgarvardt/stree/pkg/storage"
)

// TODO: These will be configurable later
var (
	s3Endpoint     = "http://localhost:9000"
	s3AccessKeyID  = "YOUR_ACCESS_KEY_ID"
	s3SecretKey    = "YOUR_SECRET_ACCESS_KEY"
	s3SessionToken = ""
	s3Region       = "eu-west-1"
)

const (
	uidPrefixBucket = "bucket:"
	uidPrefixMeta   = "meta:"
)

// SortMode represents the bucket sorting mode
type SortMode int

const (
	sortNameAsc SortMode = iota
	sortNameDesc
	sortDateAsc
	sortDateDesc
)

const (
	labelSortByNameAsc  = "Name ↓"
	labelSortByNameDesc = "Name ↑"
	labelSortByDateAsc  = "Date ↓"
	labelSortByDateDesc = "Date ↑"
)

// String returns the display name for the sort mode
func (s SortMode) String() string {
	switch s {
	case sortNameAsc:
		return labelSortByNameAsc
	case sortNameDesc:
		return labelSortByNameDesc
	case sortDateAsc:
		return labelSortByDateAsc
	case sortDateDesc:
		return labelSortByDateDesc
	default:
		return labelSortByNameAsc
	}
}

// App represents the GUI application
type App struct {
	fyneApp   fyne.App
	window    fyne.Window
	tree      *widget.Tree
	statusBar *widget.Label
	treeData  *TreeData

	ctx     context.Context
	storage *storage.Storage
	version string
	verbose bool

	s3Client  *s3client.Client
	sessionID int64

	// Bookmark management
	credStore      *storage.CredentialStore
	activeBookmark *models.Bookmark
	bookmarkSelect *widget.Select

	// Track the objects list window to ensure modality
	objectsWindow fyne.Window
	mainContent   fyne.CanvasObject
}

// TreeData holds the hierarchical data for the tree widget
type TreeData struct {
	buckets        []models.Bucket
	bucketMetadata map[string]*models.BucketMetadata // bucketName -> metadata
	searchFilter   string                            // search filter for bucket names
	sortMode       SortMode                          // current sorting mode
}

// NewApp creates a new GUI application
func NewApp(stor *storage.Storage, credStore *storage.CredentialStore, verbose bool, version string) *App {
	return &App{
		fyneApp:   app.New(),
		version:   version,
		verbose:   verbose,
		storage:   stor,
		credStore: credStore,
		treeData: &TreeData{
			buckets:        []models.Bucket{},
			bucketMetadata: make(map[string]*models.BucketMetadata),
			searchFilter:   "",
			sortMode:       sortNameAsc, // Default sorting
		},
	}
}

// Run starts the GUI application
func (a *App) Run(ctx context.Context) error {
	a.ctx = ctx

	a.window = a.fyneApp.NewWindow("STree Browser")
	a.window.Resize(fyne.NewSize(1000, 700))

	// Initialize bookmark selector
	bookmarkSelector := a.initBookmarkSelector()

	// Load bookmarks list
	a.refreshBookmarksList()

	// Create top toolbar with bookmark selector on the left
	refreshButton := widget.NewButtonWithIcon("", theme.ViewRefreshIcon(), func() {
		if a.s3Client == nil {
			a.statusBar.SetText("Not connected. Select a bookmark to connect.")
			return
		}
		go a.refreshBuckets()
	})

	// Create sort dropdown
	sortOptions := widget.NewSelect(
		[]string{sortNameAsc.String(), sortNameDesc.String(), sortDateAsc.String(), sortDateDesc.String()},
		func(selected string) {
			var mode SortMode
			switch selected {
			case labelSortByNameAsc:
				mode = sortNameAsc
			case labelSortByNameDesc:
				mode = sortNameDesc
			case labelSortByDateAsc:
				mode = sortDateAsc
			case labelSortByDateDesc:
				mode = sortDateDesc
			default:
				mode = sortNameAsc
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
					uidPrefixMeta + bucketName + ":objects",
				}
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
				// For branches (buckets), create icon + label
				// The icon will be updated to show open/closed folder state
				icon := widget.NewIcon(theme.FolderIcon())
				label := widget.NewLabel("Template")
				box := container.NewHBox(icon, label)
				// Wrap in TappableContainer to handle right-clicks
				tappable := NewTappableContainer(box, nil)
				return tappable
			} else {
				// For leaves (metadata items), create icon + label
				icon := widget.NewIcon(theme.DocumentIcon())
				label := widget.NewLabel("Template")
				box := container.NewHBox(icon, label)
				// Wrap in TappableContainer to handle right-clicks on metadata items
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
				for _, bucket := range a.treeData.buckets {
					if bucket.Name == bucketName {
						label.SetText(bucketName + " @ " + bucket.CreationDate.Format(time.RFC3339))
					}
				}

				// Update folder icon based on branch open/closed state
				bucketUID := uidPrefixBucket + bucketName
				if a.tree.IsBranchOpen(bucketUID) {
					icon.SetResource(theme.FolderOpenIcon())
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
					for _, bucket := range a.treeData.buckets {
						if bucket.Name == bucketName {
							label.SetText("Created: " + bucket.CreationDate.Format(time.RFC3339))
							icon.SetResource(theme.HistoryIcon())
							tappable.onSecondaryTap = nil
							return
						}
					}
					label.SetText("Created: Unknown")
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
		a.fyneApp.Driver().DoFromGoroutine(func() {
			a.objectsWindow.Close()
			a.objectsWindow = nil
		}, true)
	}
}
