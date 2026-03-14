APP_NAME   := STree
BUNDLE_ID  := com.github.vgarvardt.stree
VERSION    := 0.0.1
ICON_PNG   := pkg/gui/assets/stree.png
BUILD_TIME := $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS    := -trimpath -ldflags "-s -w -X main.version=$(VERSION) -X main.built=$(BUILD_TIME)"

.PHONY: build run app clean

build:
	go build $(LDFLAGS) -o stree .

run: build
	./stree

# macOS .app bundle with proper Dock icon
app: build
	@rm -rf $(APP_NAME).app
	@mkdir -p $(APP_NAME).app/Contents/MacOS
	@mkdir -p $(APP_NAME).app/Contents/Resources
	@# Generate .icns from PNG
	@mkdir -p /tmp/stree-icon.iconset
	@sips -z 16 16     $(ICON_PNG) --out /tmp/stree-icon.iconset/icon_16x16.png      > /dev/null
	@sips -z 32 32     $(ICON_PNG) --out /tmp/stree-icon.iconset/icon_16x16@2x.png   > /dev/null
	@sips -z 32 32     $(ICON_PNG) --out /tmp/stree-icon.iconset/icon_32x32.png      > /dev/null
	@sips -z 64 64     $(ICON_PNG) --out /tmp/stree-icon.iconset/icon_32x32@2x.png   > /dev/null
	@sips -z 128 128   $(ICON_PNG) --out /tmp/stree-icon.iconset/icon_128x128.png    > /dev/null
	@sips -z 256 256   $(ICON_PNG) --out /tmp/stree-icon.iconset/icon_128x128@2x.png > /dev/null
	@sips -z 256 256   $(ICON_PNG) --out /tmp/stree-icon.iconset/icon_256x256.png    > /dev/null
	@sips -z 512 512   $(ICON_PNG) --out /tmp/stree-icon.iconset/icon_256x256@2x.png > /dev/null
	@sips -z 512 512   $(ICON_PNG) --out /tmp/stree-icon.iconset/icon_512x512.png    > /dev/null
	@sips -z 1024 1024 $(ICON_PNG) --out /tmp/stree-icon.iconset/icon_512x512@2x.png > /dev/null
	@iconutil -c icns /tmp/stree-icon.iconset -o $(APP_NAME).app/Contents/Resources/icon.icns
	@rm -rf /tmp/stree-icon.iconset
	@# Copy binary
	@cp stree $(APP_NAME).app/Contents/MacOS/$(APP_NAME)
	@# Generate Info.plist
	@printf '<?xml version="1.0" encoding="UTF-8"?>\n\
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">\n\
<plist version="1.0">\n\
<dict>\n\
	<key>CFBundleExecutable</key>\n\
	<string>$(APP_NAME)</string>\n\
	<key>CFBundleIconFile</key>\n\
	<string>icon</string>\n\
	<key>CFBundleIdentifier</key>\n\
	<string>$(BUNDLE_ID)</string>\n\
	<key>CFBundleName</key>\n\
	<string>$(APP_NAME)</string>\n\
	<key>CFBundleShortVersionString</key>\n\
	<string>$(VERSION)</string>\n\
	<key>CFBundlePackageType</key>\n\
	<string>APPL</string>\n\
	<key>NSHighResolutionCapable</key>\n\
	<true/>\n\
	<key>NSSupportsAutomaticGraphicsSwitching</key>\n\
	<true/>\n\
</dict>\n\
</plist>' > $(APP_NAME).app/Contents/Info.plist
	@echo "Built $(APP_NAME).app"

clean:
	rm -rf stree $(APP_NAME).app
