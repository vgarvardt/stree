package logging

import (
	"log/slog"
	"os"

	"github.com/cappuccinotm/slogx"
	"github.com/cappuccinotm/slogx/slogm"
)

// BuildLogger builds a structured logger
func BuildLogger(verbose bool) *slog.Logger {
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
