// Package log configures log levels and redaction for secrets.
package log

import (
	"context"
	"log/slog"
	"os"
	"strings"
)

// sensitiveKeySuffixes match slog attribute keys whose values must never be
// logged verbatim (IM tokens, model API keys, DB passwords/DSNs, master keys).
// Matching is case-insensitive substring on the lower-cased key.
var sensitiveKeySuffixes = []string{
	"password", "passwd", "secret", "credential", "authorization",
	"token", "apikey", "api_key", "dsn", "masterkey", "master_key", "accesskey",
}

// Mask replaces s with a short masked form so secrets never reach logs or
// error reports in full. Short values are fully masked.
func Mask(s string) string {
	if s == "" {
		return ""
	}
	if len(s) <= 4 {
		return "***"
	}
	if len(s) <= 8 {
		return s[:2] + "***" + s[len(s)-2:]
	}
	return s[:3] + "***" + s[len(s)-4:]
}

// isSensitiveKey reports whether an attribute key carries a secret.
func isSensitiveKey(key string) bool {
	k := strings.ToLower(key)
	for _, sub := range sensitiveKeySuffixes {
		if strings.Contains(k, sub) {
			return true
		}
	}
	return false
}

// redactHandler wraps another handler and masks the values of sensitive
// attributes (and groups) before they reach the sink.
type redactHandler struct {
	next slog.Handler
}

func (h *redactHandler) Enabled(ctx context.Context, l slog.Level) bool {
	return h.next.Enabled(ctx, l)
}

func (h *redactHandler) Handle(ctx context.Context, r slog.Record) error {
	attrs := make([]slog.Attr, 0, r.NumAttrs())
	r.Attrs(func(a slog.Attr) bool {
		attrs = append(attrs, redactAttr(a))
		return true
	})
	r2 := slog.NewRecord(r.Time, r.Level, r.Message, r.PC)
	r2.AddAttrs(attrs...)
	return h.next.Handle(ctx, r2)
}

func (h *redactHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	redacted := make([]slog.Attr, 0, len(attrs))
	for _, a := range attrs {
		redacted = append(redacted, redactAttr(a))
	}
	return &redactHandler{next: h.next.WithAttrs(redacted)}
}

func (h *redactHandler) WithGroup(name string) slog.Handler {
	return &redactHandler{next: h.next.WithGroup(name)}
}

func redactAttr(a slog.Attr) slog.Attr {
	if a.Value.Kind() == slog.KindGroup {
		child := make([]slog.Attr, 0, len(a.Value.Group()))
		for _, c := range a.Value.Group() {
			child = append(child, redactAttr(c))
		}
		return slog.Attr{Key: a.Key, Value: slog.GroupValue(child...)}
	}
	if isSensitiveKey(a.Key) && a.Value.Kind() == slog.KindString {
		return slog.String(a.Key, Mask(a.Value.String()))
	}
	return a
}

// parseLevel maps a level string to a slog.Level, defaulting to info.
func parseLevel(level string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// New builds a structured JSON logger at the given level. Sensitive attribute
// values (secrets/tokens/passwords/DSNs) are masked before logging.
func New(level string) *slog.Logger {
	base := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: parseLevel(level)})
	return slog.New(&redactHandler{next: base})
}
