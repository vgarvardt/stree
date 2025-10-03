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
	buckets    []s3client.Bucket
	bucketData map[string][]s3client.Object // bucketName -> objects
}

// NewApp creates a new GUI application
func NewApp(version string) *App {
	return &App{
		fyneApp: app.New(),
		version: version,
		treeData: &TreeData{
			buckets:    []s3client.Bucket{},
			bucketData: make(map[string][]s3client.Object),
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
				objects, exists := a.treeData.bucketData[bucketName]
				if !exists {
					return []string{}
				}

				uids := make([]string, len(objects))
				for i, obj := range objects {
					uids[i] = "object:" + bucketName + ":" + obj.Key
				}
				return uids
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
			// Objects with IsPrefix are branches (folders)
			if len(uid) > 7 && uid[:7] == "object:" {
				parts := uid[7:]
				// Parse: bucketName:objectKey
				for bucketName, objects := range a.treeData.bucketData {
					for _, obj := range objects {
						key := bucketName + ":" + obj.Key
						if parts == key && obj.IsPrefix {
							return true
						}
					}
				}
			}
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

			// Handle object nodes
			if len(uid) > 7 && uid[:7] == "object:" {
				parts := uid[7:]
				// Find the object
				for bucketName, objects := range a.treeData.bucketData {
					for _, obj := range objects {
						key := bucketName + ":" + obj.Key
						if parts == key {
							label.SetText(obj.Key)
							if obj.IsPrefix {
								icon.SetResource(theme.FolderIcon())
							} else {
								icon.SetResource(theme.DocumentIcon())
							}
							return
						}
					}
				}
				label.SetText(parts)
				icon.SetResource(theme.DocumentIcon())
			}
		},
	)

	// Handle node opening (expansion)
	tree.OnBranchOpened = func(uid string) {
		// Check if this is a bucket that hasn't been loaded yet
		if len(uid) > 7 && uid[:7] == "bucket:" {
			bucketName := uid[7:]
			if _, exists := a.treeData.bucketData[bucketName]; !exists {
				go a.loadBucketObjects(bucketName)
			}
		}
	}

	return tree
}

// refreshBuckets clears cached data and reloads the buckets list
func (a *App) refreshBuckets() {
	slog.Info("Refreshing S3 buckets")

	// Clear cached bucket data
	a.treeData.bucketData = make(map[string][]s3client.Object)

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

// loadBucketObjects loads objects for a specific bucket
func (a *App) loadBucketObjects(bucketName string) {
	slog.Info("Loading objects for bucket", slog.String("bucket", bucketName))
	a.fyneApp.Driver().DoFromGoroutine(func() {
		a.statusBar.SetText(fmt.Sprintf("Loading objects from %s...", bucketName))
	}, true)

	objects, err := a.s3Client.ListObjects(context.TODO(), bucketName)
	if err != nil {
		slog.Error("Failed to load objects", slogx.Error(err), slog.String("bucket", bucketName))
		a.fyneApp.Driver().DoFromGoroutine(func() {
			a.statusBar.SetText(fmt.Sprintf("Error loading %s: %v", bucketName, err))
		}, true)
		return
	}

	a.treeData.bucketData[bucketName] = objects
	a.fyneApp.Driver().DoFromGoroutine(func() {
		a.tree.Refresh()
	}, true)

	slog.Info("Loaded objects", slog.String("bucket", bucketName), slog.Int("count", len(objects)))
	a.fyneApp.Driver().DoFromGoroutine(func() {
		a.statusBar.SetText(fmt.Sprintf("Loaded %d object(s) from %s", len(objects), bucketName))
	}, true)
}
