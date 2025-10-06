package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/cappuccinotm/slogx"
	"github.com/spf13/cobra"

	"github.com/vgarvardt/stree/pkg/gui"
	"github.com/vgarvardt/stree/pkg/logging"
	"github.com/vgarvardt/stree/pkg/storage"
)

var (
	version = "0.0.0-dev"
	built   = "1970-01-01T00:00:00Z"
)

func main() {
	var verboseLogs bool
	var storageDSN string
	var storagePurge bool

	rootCmd := &cobra.Command{
		Use:     "stree [command]",
		Version: fmt.Sprintf("%s (built %s)", version, built),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()

			logger := logging.BuildLogger(verboseLogs)
			slog.SetDefault(logger)

			storageConfig := storage.Config{
				DSN:   storageDSN,
				Purge: storagePurge,
			}

			stor, err := storage.New(ctx, storageConfig)
			if err != nil {
				return fmt.Errorf("could not initialize storage: %w", err)
			}
			defer func() {
				if err := stor.Close(); err != nil {
					slog.Error("Could not properly close storage", slogx.Error(err))
				}
			}()

			credStore := storage.NewCredentialStore()
			// Test keychain availability (optional, logs warnings if unavailable)
			if err := credStore.TestKeychain(ctx); err != nil {
				return fmt.Errorf("could not ensure credentials store: %w", err)
			}

			logger.Info("Starting main GUI app",
				slog.String("version", version),
				slog.String("built", built),
				slog.String("storage-dsn", storageDSN),
				slog.Bool("storage-purge", storagePurge))

			// Launch the GUI application
			app := gui.NewApp(stor, credStore, version)

			return app.Run(ctx)
		},
	}

	pf := rootCmd.PersistentFlags()
	pf.BoolVarP(&verboseLogs, "verbose", "v", false, "verbose logging")
	pf.StringVar(&storageDSN, "storage-dsn", "./storage.db", "SQLite database file path (use ':memory:' for in-memory database)")
	pf.BoolVar(&storagePurge, "storage-purge", false, "remove storage file before initialization (start from scratch)")

	ctx := context.Background()
	if err := rootCmd.ExecuteContext(ctx); err != nil {
		slog.Error("command execution failed", slogx.Error(err))
		os.Exit(1)
	}
}
