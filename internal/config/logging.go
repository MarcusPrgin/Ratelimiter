package config

import (
	"fmt"
	"io"
	"log/slog"
	"strings"
)

// ParseLogLevel maps a config value onto an slog level.
func ParseLogLevel(s string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("unknown level %q (want debug, info, warn or error)", s)
	}
}

// NewLogger builds the application logger. JSON is the default because these logs
// are meant to be shipped and queried; text exists for local development.
func (c *Config) NewLogger(w io.Writer) *slog.Logger {
	level, err := ParseLogLevel(c.Log.Level)
	if err != nil {
		// Validate has already rejected a bad level, so this is unreachable from
		// Load. Default rather than panic in case a Config is built by hand.
		level = slog.LevelInfo
	}
	opts := &slog.HandlerOptions{Level: level}

	var h slog.Handler
	if c.Log.Format == "text" {
		h = slog.NewTextHandler(w, opts)
	} else {
		h = slog.NewJSONHandler(w, opts)
	}
	return slog.New(h)
}
