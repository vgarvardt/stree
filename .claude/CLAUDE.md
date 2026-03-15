# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What is STree

STree is a desktop S3-compatible browser built with Go, using Fyne for the GUI and AWS SDK v2 for S3 operations. It supports bookmarked connections to any S3-compatible endpoint, caches bucket/object data in SQLite, and stores secret keys securely in the OS keychain.

## Go Version

The project uses **Go 1.26** and leverages modern language features (range-over-int, range-over-func iterators, etc.). Use current Go idioms — do not write backwards-compatible code for older Go versions.

## Build & Test Commands

```bash
go build -o stree              # Build the binary
go test ./...                  # Run all tests
go test ./pkg/storage -v       # Run storage tests (only package with tests currently)
go test -run TestName ./pkg/storage  # Run a single test
go fmt ./...                   # Format code
```

Dependencies are vendored. Use `go mod tidy && go mod vendor` after changing dependencies.

## Architecture

**Entry point**: `main.go` — Cobra CLI that initializes storage, credentials, logging, and launches the GUI.

**Key packages** (all under `pkg/`):

- **gui/** — Fyne-based desktop UI. `app.go` is the main window with a tree widget for browsing buckets. Separate files handle bucket listing, object listing, multipart uploads, bookmarks dialogs, and custom widgets.
- **s3client/** — Thin wrapper around AWS SDK v2 S3 client. Handles `ListBuckets`, `ListObjectVersions`, `ListMultipartUploads`, `ListParts`, and bucket metadata/encryption queries. Supports custom S3-compatible endpoints.
- **storage/** — SQLite persistence layer (CGO-free via modernc.org/sqlite). Manages sessions, buckets, objects, multipart uploads, and bookmarks. Uses golang-migrate for schema migrations (in `storage/migrations/`). `credentials.go` uses go-keyring to store secret keys in the OS keychain separately from the DB.
- **models/** — Domain types: `Bucket`, `ObjectVersion`, `MultipartUpload`, `Bookmark`, `Pagination`. Models use JSON serialization for flexible metadata columns in SQLite.
- **logging/** — slog-based structured logger with verbose mode toggle.

**Data flow**: User selects a bookmark → secret key retrieved from OS keychain → S3 client initialized → buckets fetched and cached in SQLite → tree view populated → expanding/double-clicking loads metadata, objects, or MPUs on demand.

**Credential security**: Non-sensitive bookmark data (endpoint, region, access key ID) lives in SQLite. Secret keys are stored in the OS keychain (macOS Keychain / Windows Credential Manager / Linux Secret Service) under service name `stree-s3-browser`. See `docs/CREDENTIALS.md` for details.

## Key Dependencies

- **fyne.io/fyne/v2** — Cross-platform GUI framework
- **github.com/aws/aws-sdk-go-v2** — AWS S3 client
- **modernc.org/sqlite** — CGO-free SQLite driver
- **github.com/golang-migrate/migrate/v4** — DB migrations
- **github.com/zalando/go-keyring** — OS keychain access
- **github.com/spf13/cobra** — CLI framework

## Build Verification

When working on Go code changes (especially multi-step tasks), verify the code compiles by building the binary to `/tmp/stree`:

```bash
go build -trimpath -o /tmp/stree .
