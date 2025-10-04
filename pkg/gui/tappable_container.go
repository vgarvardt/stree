package gui

import (
	"time"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/widget"
)

// TappableContainer is a custom container that implements secondary tap (right-click) functionality
type TappableContainer struct {
	widget.BaseWidget
	container      *fyne.Container
	onSecondaryTap func(fyne.Position)
	onDoubleTap    func()
	lastTapTime    time.Time
}

// NewTappableContainer creates a new tappable container
func NewTappableContainer(container *fyne.Container, onSecondaryTap func(fyne.Position)) *TappableContainer {
	tc := &TappableContainer{
		container:      container,
		onSecondaryTap: onSecondaryTap,
	}
	tc.ExtendBaseWidget(tc)
	return tc
}

// CreateRenderer returns the renderer for the tappable container
func (t *TappableContainer) CreateRenderer() fyne.WidgetRenderer {
	return widget.NewSimpleRenderer(t.container)
}

// Tapped handles primary tap events (left-click)
func (t *TappableContainer) Tapped(*fyne.PointEvent) {
	now := time.Now()
	// Detect double-tap: tapped within 500ms
	if now.Sub(t.lastTapTime) < 500*time.Millisecond {
		// Double-tap detected
		if t.onDoubleTap != nil {
			t.onDoubleTap()
		}
		// Reset to prevent triple-tap from triggering another double-tap
		t.lastTapTime = time.Time{}
	} else {
		// Single tap - record the time
		t.lastTapTime = now
	}
}

// TappedSecondary handles secondary tap events (right-click)
func (t *TappableContainer) TappedSecondary(pe *fyne.PointEvent) {
	if t.onSecondaryTap != nil {
		t.onSecondaryTap(pe.AbsolutePosition)
	}
}
