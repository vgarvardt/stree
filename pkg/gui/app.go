package gui

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/app"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/theme"
	"fyne.io/fyne/v2/widget"
	"github.com/cappuccinotm/slogx"
	"github.com/dustin/go-humanize"
	"github.com/goccy/go-json"

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

	s3Client  *s3client.Client
	sessionID int64
}

// TreeData holds the hierarchical data for the tree widget
type TreeData struct {
	buckets        []models.Bucket
	bucketMetadata map[string]*models.BucketMetadata // bucketName -> metadata
	searchFilter   string                            // search filter for bucket names
	sortMode       SortMode                          // current sorting mode
}

// NewApp creates a new GUI application
func NewApp(stor *storage.Storage, version string) *App {
	return &App{
		fyneApp: app.New(),
		version: version,
		storage: stor,
		treeData: &TreeData{
			buckets:        []models.Bucket{},
			bucketMetadata: make(map[string]*models.BucketMetadata),
			searchFilter:   "",
			sortMode:       sortNameAsc, // Default sorting
		},
	}
}

// Run starts the GUI application
func (a *App) Run(ctx context.Context, verbose bool) error {
	s3Cfg := s3client.Config{
		Endpoint:     s3Endpoint,
		AccessKey:    s3AccessKeyID,
		SecretKey:    s3SecretKey,
		SessionToken: s3SessionToken,
		Region:       s3Region,
		Debug:        verbose,
	}
	s3Client, err := s3client.NewClient(ctx, s3Cfg, a.version)
	if err != nil {
		return fmt.Errorf("failed to create S3 client: %w", err)
	}

	if a.sessionID, err = a.storage.UpsertSession(ctx, s3Cfg.String()); err != nil {
		return fmt.Errorf("failed to store session to storage: %w", err)
	}
	slog.Info("Initialised storage session", slog.Int64("session-id", a.sessionID))

	a.s3Client = s3Client
	a.ctx = ctx

	a.window = a.fyneApp.NewWindow("S3 Tree Browser")
	a.window.Resize(fyne.NewSize(800, 600))

	// Create top toolbar with refresh button (icon-only)
	refreshButton := widget.NewButtonWithIcon("", theme.ViewRefreshIcon(), func() {
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

	buttonsContainer := container.NewHBox(refreshButton, sortOptions)

	// Simple toolbar with everything aligned to the left
	toolbar := container.NewAdaptiveGrid(2,
		buttonsContainer,
		searchEntry,
	)

	// Create status bar
	a.statusBar = widget.NewLabel("Ready")
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

	// Load buckets asynchronously
	go a.loadBuckets()

	a.window.ShowAndRun()

	return nil
}

// getFilteredBuckets returns buckets filtered by the search query
func (a *App) getFilteredBuckets() []models.Bucket {
	if a.treeData.searchFilter == "" {
		return a.treeData.buckets
	}

	filtered := make([]models.Bucket, 0)
	for _, bucket := range a.treeData.buckets {
		// Case-sensitive substring matching
		if strings.Contains(bucket.Name, a.treeData.searchFilter) {
			filtered = append(filtered, bucket)
		}
	}
	return filtered
}

// sortBuckets sorts the buckets based on the current sort mode
func (a *App) sortBuckets() {
	switch a.treeData.sortMode {
	case sortNameAsc:
		sort.Slice(a.treeData.buckets, func(i, j int) bool {
			return a.treeData.buckets[i].Name < a.treeData.buckets[j].Name
		})
	case sortNameDesc:
		sort.Slice(a.treeData.buckets, func(i, j int) bool {
			return a.treeData.buckets[i].Name > a.treeData.buckets[j].Name
		})
	case sortDateAsc:
		sort.Slice(a.treeData.buckets, func(i, j int) bool {
			return a.treeData.buckets[i].CreationDate.Before(a.treeData.buckets[j].CreationDate)
		})
	case sortDateDesc:
		sort.Slice(a.treeData.buckets, func(i, j int) bool {
			return a.treeData.buckets[i].CreationDate.After(a.treeData.buckets[j].CreationDate)
		})
	}
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
					label.SetText(fmt.Sprintf("Objects: %s / %s @ %s", humanize.Comma(metadata.ObjectsCount), humanize.Bytes(uint64(metadata.ObjectsSize)), refreshedAt))
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

// refreshBuckets clears cached data and reloads the buckets list
func (a *App) refreshBuckets() {
	slog.Info("Refreshing S3 buckets")

	// Close all open branches to reset the tree state
	a.fyneApp.Driver().DoFromGoroutine(func() {
		a.tree.CloseAllBranches()
	}, true)

	// Clear cached bucket metadata
	a.treeData.bucketMetadata = make(map[string]*models.BucketMetadata)

	// Invalidate storage cache by deleting the current session
	newSessionID, err := a.storage.InvalidateSession(a.ctx, a.sessionID)
	if err != nil {
		slog.Warn("Failed to invalidate storage cache", slogx.Error(err))
	}
	a.sessionID = newSessionID
	slog.Info("Invalidated storage session", slog.Int64("session-id", a.sessionID))

	// Reload buckets
	a.loadBuckets()
}

// showBucketContextMenu displays a context menu for a bucket
func (a *App) showBucketContextMenu(bucketName string, position fyne.Position) {
	// Create menu items
	copyNameItem := fyne.NewMenuItem("Copy name", func() {
		a.window.Clipboard().SetContent(bucketName)
		slog.Info("Copied bucket name to clipboard", slog.String("bucket", bucketName))
		a.fyneApp.Driver().DoFromGoroutine(func() {
			a.statusBar.SetText(fmt.Sprintf("Copied %q to clipboard", bucketName))
		}, true)
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
	)
	popUpMenu := widget.NewPopUpMenu(menu, a.window.Canvas())
	popUpMenu.ShowAtPosition(position)
}

// refreshSingleBucket invalidates and reloads metadata for a specific bucket
func (a *App) refreshSingleBucket(bucketName string) {
	slog.Info("Refreshing bucket", slog.String("bucket", bucketName))

	a.fyneApp.Driver().DoFromGoroutine(func() {
		a.statusBar.SetText(fmt.Sprintf("Refreshing %s...", bucketName))
	}, true)

	// Remove cached metadata
	delete(a.treeData.bucketMetadata, bucketName)

	// Delete bucket from storage to force refresh
	if err := a.storage.DeleteBucket(a.ctx, a.sessionID, bucketName); err != nil {
		slog.Warn("Failed to delete bucket from storage", slogx.Error(err), slog.String("bucket", bucketName))
	}

	// Reload bucket metadata from S3
	a.loadBucketMetadata(bucketName)
}

// loadBuckets loads the list of S3 buckets
func (a *App) loadBuckets() {
	slog.Info("Loading S3 buckets")
	a.fyneApp.Driver().DoFromGoroutine(func() {
		a.statusBar.SetText("Loading buckets...")
	}, true)

	buckets, err := a.s3Client.ListBuckets(a.ctx)
	if err != nil {
		slog.Error("Failed to load buckets", slogx.Error(err))
		a.fyneApp.Driver().DoFromGoroutine(func() {
			a.statusBar.SetText(fmt.Sprintf("Error: %v", err))
		}, true)
		return
	}

	a.treeData.buckets = buckets

	// Sort buckets according to current sort mode
	a.sortBuckets()

	// Store all buckets to storage using BucketDetails
	for _, bucket := range buckets {
		details := models.NewBucketDetails(bucket, nil)
		if err := a.storage.UpsertBucket(context.TODO(), a.sessionID, bucket.Name, bucket.CreationDate, details); err != nil {
			slog.Warn("Failed to store bucket to storage", slogx.Error(err), slog.String("bucket", bucket.Name))
		}
	}

	// Refresh tree on UI thread
	a.fyneApp.Driver().DoFromGoroutine(func() {
		a.tree.Refresh()
		a.statusBar.SetText(fmt.Sprintf("Loaded %d bucket(s)", len(buckets)))
	}, false)

	slog.Info("Loaded buckets", slog.Int("count", len(buckets)))
}

// loadBucketMetadata loads metadata for a specific bucket
func (a *App) loadBucketMetadata(bucketName string) {
	slog.Info("Loading metadata for bucket", slog.String("bucket", bucketName))
	a.fyneApp.Driver().DoFromGoroutine(func() {
		a.statusBar.SetText(fmt.Sprintf("Loading metadata for %s...", bucketName))
	}, true)

	// First, try to load from storage
	storedBucket, err := a.storage.GetBucket(a.ctx, a.sessionID, bucketName)
	if err != nil {
		slog.Warn("Failed to get bucket from storage", slogx.Error(err), slog.String("bucket", bucketName))
	} else if storedBucket != nil {
		// Deserialize stored details into BucketDetails
		var details models.BucketDetails
		if err := json.Unmarshal(storedBucket.Details, &details); err == nil {
			// Check if the details contain metadata (not just basic bucket info)
			if details.HasMetadata() {
				slog.Info("Loading bucket metadata from storage", slog.String("bucket", bucketName))

				// Convert to BucketMetadata
				metadata := details.ToMetadata()
				a.treeData.bucketMetadata[bucketName] = metadata

				// Refresh tree on UI thread
				a.fyneApp.Driver().DoFromGoroutine(func() {
					a.tree.Refresh()
					a.statusBar.SetText(fmt.Sprintf("Loaded metadata for %s (from cache)", bucketName))
				}, false)
				return
			}
		}
	}

	// Not in storage or no metadata, fetch from S3
	slog.Info("Fetching bucket metadata from S3", slog.String("bucket", bucketName))
	metadata, err := a.s3Client.GetBucketMetadata(a.ctx, bucketName)
	if err != nil {
		slog.Error("Failed to load bucket metadata", slogx.Error(err), slog.String("bucket", bucketName))
		a.fyneApp.Driver().DoFromGoroutine(func() {
			a.statusBar.SetText(fmt.Sprintf("Error loading %s: %v", bucketName, err))
		}, true)
		return
	}

	a.treeData.bucketMetadata[bucketName] = metadata

	// Find the bucket to get its creation date
	var bucket models.Bucket
	for _, b := range a.treeData.buckets {
		if b.Name == bucketName {
			bucket = b
			break
		}
	}

	// Store the metadata in storage using BucketDetails
	details := models.NewBucketDetails(bucket, metadata)
	if err := a.storage.UpsertBucket(a.ctx, a.sessionID, bucketName, bucket.CreationDate, details); err != nil {
		slog.Warn("Failed to store bucket metadata to storage", slogx.Error(err), slog.String("bucket", bucketName))
	}

	// Refresh tree on UI thread
	a.fyneApp.Driver().DoFromGoroutine(func() {
		a.tree.Refresh()
		a.statusBar.SetText(fmt.Sprintf("Loaded metadata for %s", bucketName))
	}, false)

	slog.Info("Loaded bucket metadata", slog.String("bucket", bucketName))
}
