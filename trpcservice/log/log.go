// Package log configures log levels and redaction for secrets.
package log

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
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

// redactAttrsHandler wraps an slog.Handler and replaces every occurrence of
// any known secret pattern in the log message and its attributes with the
// literal string "<redacted>". It is the platform's only run-time output
// filter: secrets that reached the log (through error propagation, audit
// detail, or trace attributes) are masked before they reach stderr.
type redactAttrsHandler struct {
	inner   slog.Handler
	secrets []string
}

// WithLogRedaction wraps a handler with secret-text redaction. secrets is a
// list of plaintext values that must never appear in logs; each is replaced
// globally with "<redacted>".
func WithLogRedaction(inner slog.Handler, secrets []string) slog.Handler {
	if len(secrets) == 0 {
		return inner // no secrets configured → pass through
	}
	dedup := make([]string, 0, len(secrets))
	seen := make(map[string]struct{}, len(secrets))
	for _, s := range secrets {
		if s != "" {
			if _, ok := seen[s]; !ok {
				seen[s] = struct{}{}
				dedup = append(dedup, s)
			}
		}
	}
	return &redactAttrsHandler{inner: inner, secrets: dedup}
}

func (h *redactAttrsHandler) Enabled(ctx context.Context, l slog.Level) bool {
	return h.inner.Enabled(ctx, l)
}

func (h *redactAttrsHandler) Handle(ctx context.Context, r slog.Record) error {
	// Redact the message text.
	msg := r.Message
	for _, s := range h.secrets {
		if strings.Contains(msg, s) {
			msg = strings.ReplaceAll(msg, s, "<redacted>")
		}
	}
	r2 := slog.NewRecord(r.Time, r.Level, msg, r.PC)
	r.Attrs(func(a slog.Attr) bool {
		key := a.Key
		val := a.Value.String()
		for _, s := range h.secrets {
			if strings.Contains(val, s) {
				if a.Value.Kind() == slog.KindString {
					val = strings.ReplaceAll(val, s, "<redacted>")
					a = slog.String(key, val)
				}
			}
		}
		r2.AddAttrs(a)
		return true
	})
	return h.inner.Handle(ctx, r2)
}

func (h *redactAttrsHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &redactAttrsHandler{inner: h.inner.WithAttrs(attrs), secrets: h.secrets}
}

func (h *redactAttrsHandler) WithGroup(name string) slog.Handler {
	return &redactAttrsHandler{inner: h.inner.WithGroup(name), secrets: h.secrets}
}
