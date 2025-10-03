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

	rootCmd := &cobra.Command{
		Use:     "stree [command]",
		Version: fmt.Sprintf("%s (built %s)", version, built),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()

			logger := logging.BuildLogger(verboseLogs)
			slog.SetDefault(logger)

			stor, err := storage.New(ctx)
			if err != nil {
				return fmt.Errorf("could not initialize storage: %w", err)
			}
			defer func() {
				if err := stor.Close(); err != nil {
					slog.Error("Could not properly close storage", slogx.Error(err))
				}
			}()

			logger.Info("Starting main GUI app", slog.String("version", version), slog.String("built", built))

			// Launch the GUI application
			app := gui.NewApp(stor, version)

			return app.Run(ctx, verboseLogs)
		},
	}

	pf := rootCmd.PersistentFlags()
	pf.BoolVarP(&verboseLogs, "verbose", "v", false, "verbose logging")

	ctx := context.Background()
	if err := rootCmd.ExecuteContext(ctx); err != nil {
		slog.Error("command execution failed", slogx.Error(err))
		os.Exit(1)
	}
}
