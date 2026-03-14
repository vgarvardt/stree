package assets

import (
	_ "embed"

	"fyne.io/fyne/v2"
)

//go:embed stree.png
var iconData []byte

// AppIcon is the application icon resource.
var AppIcon = &fyne.StaticResource{
	StaticName:    "stree.png",
	StaticContent: iconData,
}
