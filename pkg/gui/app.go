package gui

import (
	"context"
	"fmt"
	"log/slog"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/app"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/theme"
	"fyne.io/fyne/v2/widget"
	"github.com/cappuccinotm/slogx"

	"github.com/vgarvardt/stree/pkg/s3client"
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

	s3Client *s3client.Client
	version  string
}

// TreeData holds the hierarchical data for the tree widget
type TreeData struct {
	buckets        []s3client.Bucket
	bucketMetadata map[string]*s3client.BucketMetadata // bucketName -> metadata
}

// NewApp creates a new GUI application
func NewApp(version string) *App {
	return &App{
		fyneApp: app.New(),
		version: version,
		treeData: &TreeData{
			buckets:        []s3client.Bucket{},
			bucketMetadata: make(map[string]*s3client.BucketMetadata),
		},
	}
}

// Run starts the GUI application
func (a *App) Run(ctx context.Context, verbose bool) error {
	s3Client, err := s3client.NewClient(ctx, s3client.Config{
		Endpoint:     s3Endpoint,
		AccessKey:    s3AccessKeyID,
		SecretKey:    s3SecretKey,
		SessionToken: s3SessionToken,
		Region:       s3Region,
		Debug:        verbose,
	}, a.version)
	if err != nil {
		return fmt.Errorf("failed to create S3 client: %w", err)
	}

	a.s3Client = s3Client

	a.window = a.fyneApp.NewWindow("S3 Tree Browser")
	a.window.Resize(fyne.NewSize(800, 600))

	// Create top toolbar with refresh button (icon-only)
	refreshButton := widget.NewButtonWithIcon("", theme.ViewRefreshIcon(), func() {
		go a.refreshBuckets()
	})
	toolbar := container.NewHBox(refreshButton)

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

// createTree initializes the tree widget
func (a *App) createTree() *widget.Tree {
	tree := widget.NewTree(
		// ChildUIDs function
		func(uid string) []string {
			if uid == "" {
				// Root level - return bucket names
				uids := make([]string, len(a.treeData.buckets))
				for i, bucket := range a.treeData.buckets {
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
				// Find the bucket to show creation date
				var creationDate string
				for _, bucket := range a.treeData.buckets {
					if bucket.Name == bucketName {
						if bucket.CreationDate != nil {
							creationDate = " (" + *bucket.CreationDate + ")"
						}
						break
					}
				}
				label.SetText(bucketName + creationDate)
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
						if bucket.Name == bucketName && bucket.CreationDate != nil {
							label.SetText("Created: " + *bucket.CreationDate)
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

	buckets, err := a.s3Client.ListBuckets(context.TODO())
	if err != nil {
		slog.Error("Failed to load buckets", slogx.Error(err))
		a.fyneApp.Driver().DoFromGoroutine(func() {
			a.statusBar.SetText(fmt.Sprintf("Error: %v", err))
		}, true)
		return
	}

	a.treeData.buckets = buckets
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

	metadata, err := a.s3Client.GetBucketMetadata(context.TODO(), bucketName)
	if err != nil {
		slog.Error("Failed to load bucket metadata", slogx.Error(err), slog.String("bucket", bucketName))
		a.fyneApp.Driver().DoFromGoroutine(func() {
			a.statusBar.SetText(fmt.Sprintf("Error loading %s: %v", bucketName, err))
		}, true)
		return
	}

	a.treeData.bucketMetadata[bucketName] = metadata
	a.fyneApp.Driver().DoFromGoroutine(func() {
		a.tree.Refresh()
	}, true)

	slog.Info("Loaded bucket metadata", slog.String("bucket", bucketName))
	a.fyneApp.Driver().DoFromGoroutine(func() {
		a.statusBar.SetText(fmt.Sprintf("Loaded metadata for %s", bucketName))
	}, true)
}
