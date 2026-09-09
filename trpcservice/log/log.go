// Package log configures log levels and redaction for secrets.
package log

import (
	"fmt"
	"log/slog"
	"os"
)

// Init installs the process-wide structured logger described by the config
// log section: level filtering plus optional JSON encoding for container
// deployments. Everything else in the platform logs through slog.Default.
func Init(level string, json bool) error {
	var lv slog.Level
	switch level {
	case "", "info":
		lv = slog.LevelInfo
	case "debug":
		lv = slog.LevelDebug
	case "warn":
		lv = slog.LevelWarn
	case "error":
		lv = slog.LevelError
	default:
		return fmt.Errorf("unknown log level %q (want debug, info, warn, or error)", level)
	}
	opts := &slog.HandlerOptions{Level: lv}
	var h slog.Handler
	if json {
		h = slog.NewJSONHandler(os.Stderr, opts)
	} else {
		h = slog.NewTextHandler(os.Stderr, opts)
	}
	slog.SetDefault(slog.New(h))
	return nil
}

// Redact masks a secret so it can never leak through logs, traces, or error
// reports: short values are fully masked, longer ones keep a fingerprint of
// the first three and last four characters. Same rule as the Admin API DTO
// masking, so a value seen in both places looks identical.
func Redact(secret string) string {
	if secret == "" {
		return ""
	}
	if len(secret) <= 8 {
		return "****"
	}
	return secret[:3] + "****" + secret[len(secret)-4:]
}
