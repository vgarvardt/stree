package gui

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/app"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/theme"
	"fyne.io/fyne/v2/widget"
	"github.com/cappuccinotm/slogx"
	"github.com/goccy/go-json"

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
	buckets        []s3client.Bucket
	bucketMetadata map[string]*s3client.BucketMetadata // bucketName -> metadata
	searchFilter   string                              // search filter for bucket names
}

// NewApp creates a new GUI application
func NewApp(stor *storage.Storage, version string) *App {
	return &App{
		fyneApp: app.New(),
		version: version,
		storage: stor,
		treeData: &TreeData{
			buckets:        []s3client.Bucket{},
			bucketMetadata: make(map[string]*s3client.BucketMetadata),
			searchFilter:   "",
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

	a.s3Client = s3Client
	a.ctx = ctx

	a.window = a.fyneApp.NewWindow("S3 Tree Browser")
	a.window.Resize(fyne.NewSize(800, 600))

	// Create top toolbar with refresh button (icon-only)
	refreshButton := widget.NewButtonWithIcon("", theme.ViewRefreshIcon(), func() {
		go a.refreshBuckets()
	})

	// Create search input
	searchEntry := widget.NewEntry()
	searchEntry.SetPlaceHolder("Filter by name...")
	searchEntry.OnChanged = func(query string) {
		a.treeData.searchFilter = query
		a.tree.Refresh()
	}

	buttonsContainer := container.NewHBox(refreshButton)

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
func (a *App) getFilteredBuckets() []s3client.Bucket {
	if a.treeData.searchFilter == "" {
		return a.treeData.buckets
	}

	filtered := make([]s3client.Bucket, 0)
	for _, bucket := range a.treeData.buckets {
		// Case-sensitive substring matching
		if strings.Contains(bucket.Name, a.treeData.searchFilter) {
			filtered = append(filtered, bucket)
		}
	}
	return filtered
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
					uids[i] = "bucket:" + bucket.Name
				}
				return uids
			}

			// Check if this is a bucket node
			if len(uid) > 7 && uid[:7] == "bucket:" {
				bucketName := uid[7:]
				metadata, exists := a.treeData.bucketMetadata[bucketName]
				if !exists {
					return []string{}
				}

				// Return metadata items as child nodes
				items := []string{
					"meta:" + bucketName + ":created",
					"meta:" + bucketName + ":versioning",
					"meta:" + bucketName + ":lock",
					"meta:" + bucketName + ":retention",
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
			if len(uid) > 7 && uid[:7] == "bucket:" {
				return true
			}
			// Metadata items are not branches
			return false
		},
		// Create function
		func(branch bool) fyne.CanvasObject {
			icon := widget.NewIcon(theme.DocumentIcon())
			label := widget.NewLabel("Template")
			return container.NewHBox(icon, label)
		},
		// Update function
		func(uid string, branch bool, obj fyne.CanvasObject) {
			c := obj.(*fyne.Container)
			icon := c.Objects[0].(*widget.Icon)
			label := c.Objects[1].(*widget.Label)

			if uid == "" {
				label.SetText("Root")
				icon.SetResource(theme.FolderIcon())
				return
			}

			// Handle bucket nodes
			if len(uid) > 7 && uid[:7] == "bucket:" {
				bucketName := uid[7:]
				label.SetText(bucketName)
				icon.SetResource(theme.FolderIcon())
				return
			}

			// Handle metadata nodes
			if len(uid) > 5 && uid[:5] == "meta:" {
				parts := uid[5:] // Remove "meta:" prefix
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
					return
				}

				bucketName := parts[:lastColon]
				fieldName := parts[lastColon+1:]

				metadata, exists := a.treeData.bucketMetadata[bucketName]
				if !exists {
					label.SetText("Loading...")
					icon.SetResource(theme.InfoIcon())
					return
				}

				switch fieldName {
				case "created":
					for _, bucket := range a.treeData.buckets {
						if bucket.Name == bucketName {
							label.SetText("Created: " + bucket.CreationDate.Format(time.RFC3339))
							icon.SetResource(theme.HistoryIcon())
							return
						}
					}
					label.SetText("Created: Unknown")
					icon.SetResource(theme.HistoryIcon())
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
				default:
					label.SetText("Unknown field")
					icon.SetResource(theme.QuestionIcon())
				}
			}
		},
	)

	// Handle node opening (expansion)
	tree.OnBranchOpened = func(uid string) {
		// Check if this is a bucket that hasn't been loaded yet
		if len(uid) > 7 && uid[:7] == "bucket:" {
			bucketName := uid[7:]
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

	// Clear cached bucket metadata
	a.treeData.bucketMetadata = make(map[string]*s3client.BucketMetadata)

	// Reload buckets
	a.loadBuckets()
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

	// Store all buckets to storage
	for _, bucket := range buckets {
		bucketData := map[string]any{
			"Name":         bucket.Name,
			"CreationDate": bucket.CreationDate,
		}
		if err := a.storage.UpsertBucket(context.TODO(), a.sessionID, bucket.Name, bucket.CreationDate, bucketData); err != nil {
			slog.Warn("Failed to store bucket to storage", slogx.Error(err), slog.String("bucket", bucket.Name))
		}
	}

	a.fyneApp.Driver().DoFromGoroutine(func() {
		a.tree.Refresh()
	}, true)

	slog.Info("Loaded buckets", slog.Int("count", len(buckets)))
	a.fyneApp.Driver().DoFromGoroutine(func() {
		a.statusBar.SetText(fmt.Sprintf("Loaded %d bucket(s)", len(buckets)))
	}, true)
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
		// Check if we have metadata in the stored details
		var storedDetails map[string]any
		if err := json.Unmarshal(storedBucket.Details, &storedDetails); err == nil {
			// Check if the details contain metadata fields (not just basic bucket info)
			if _, hasMetadata := storedDetails["VersioningEnabled"]; hasMetadata {
				slog.Info("Loading bucket metadata from storage", slog.String("bucket", bucketName))

				// Convert stored details to BucketMetadata
				metadata := &s3client.BucketMetadata{}
				if v, ok := storedDetails["VersioningEnabled"].(bool); ok {
					metadata.VersioningEnabled = v
				}
				if v, ok := storedDetails["VersioningStatus"].(string); ok {
					metadata.VersioningStatus = v
				}
				if v, ok := storedDetails["ObjectLockEnabled"].(bool); ok {
					metadata.ObjectLockEnabled = v
				}
				if v, ok := storedDetails["ObjectLockMode"].(string); ok {
					metadata.ObjectLockMode = v
				}
				if v, ok := storedDetails["RetentionEnabled"].(bool); ok {
					metadata.RetentionEnabled = v
				}
				if v, ok := storedDetails["RetentionDays"].(float64); ok {
					metadata.RetentionDays = int32(v)
				}
				if v, ok := storedDetails["RetentionYears"].(float64); ok {
					metadata.RetentionYears = int32(v)
				}
				if v, ok := storedDetails["RetentionMode"].(string); ok {
					metadata.RetentionMode = v
				}

				a.treeData.bucketMetadata[bucketName] = metadata
				a.fyneApp.Driver().DoFromGoroutine(func() {
					a.tree.Refresh()
					a.statusBar.SetText(fmt.Sprintf("Loaded metadata for %s (from cache)", bucketName))
				}, true)
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

	// Store the metadata in storage
	var creationDate time.Time
	for _, bucket := range a.treeData.buckets {
		if bucket.Name == bucketName {
			creationDate = bucket.CreationDate
			break
		}
	}

	bucketData := map[string]any{
		"Name":              bucketName,
		"VersioningEnabled": metadata.VersioningEnabled,
		"VersioningStatus":  metadata.VersioningStatus,
		"ObjectLockEnabled": metadata.ObjectLockEnabled,
		"ObjectLockMode":    metadata.ObjectLockMode,
		"RetentionEnabled":  metadata.RetentionEnabled,
		"RetentionDays":     metadata.RetentionDays,
		"RetentionYears":    metadata.RetentionYears,
		"RetentionMode":     metadata.RetentionMode,
	}
	if err := a.storage.UpsertBucket(a.ctx, a.sessionID, bucketName, creationDate, bucketData); err != nil {
		slog.Warn("Failed to store bucket metadata to storage", slogx.Error(err), slog.String("bucket", bucketName))
	}

	a.fyneApp.Driver().DoFromGoroutine(func() {
		a.tree.Refresh()
	}, true)

	slog.Info("Loaded bucket metadata", slog.String("bucket", bucketName))
	a.fyneApp.Driver().DoFromGoroutine(func() {
		a.statusBar.SetText(fmt.Sprintf("Loaded metadata for %s", bucketName))
	}, true)
}
