package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/cappuccinotm/slogx"
	"github.com/cappuccinotm/slogx/slogm"
	"github.com/spf13/cobra"
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
			logger := buildLogger(verboseLogs)
			logger.Info("Starting main GUI app", slog.String("version", version), slog.String("built", built))
			return nil
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

func buildLogger(verbose bool) *slog.Logger {
	so := &slog.HandlerOptions{
		AddSource: true,
		Level:     map[bool]slog.Level{true: slog.LevelDebug, false: slog.LevelInfo}[verbose],
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if groups != nil {
				return a
			}

			// TODO: turn source into relative path instead of full path
			// TODO: remove "time=", "level=", "source=", "msg=" prefixes from the printed log lines
			return a
		},
	}

	logger := slog.New(slogx.NewChain(slog.NewTextHandler(os.Stderr, so), slogm.StacktraceOnError()))

	return logger
}
