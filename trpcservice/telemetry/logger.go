package telemetry

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"go.opentelemetry.io/otel/trace"
)

func traceSpanFromContext(ctx context.Context) trace.SpanContext {
	return trace.SpanContextFromContext(ctx)
}

func toAttrValues(args []any) []slog.Attr {
	attrs := make([]slog.Attr, 0, len(args))
	for i := 0; i < len(args); i++ {
		switch value := args[i].(type) {
		case slog.Attr:
			attrs = append(attrs, value)
		default:
			continue
		}
	}
	return attrs
}

func toAttrs(args []any) []slog.Attr { return toAttrValues(args) }

func normalizeLevel(level slog.Level) slog.Level {
	switch level {
	case slog.LevelDebug, slog.LevelInfo, slog.LevelWarn, slog.LevelError:
		return level
	default:
		return slog.LevelInfo
	}
}

// Logger is the structured application-owned logger. Production-owned logs
// are single-line parseable JSON with a stable, bounded field set; values are
// truncated and secrets are never passed to it (callers pass categories, not
// raw errors).
type Logger struct {
	logger *slog.Logger
	mu     sync.Mutex
	rate   *rateState
}

// NewJSONLogger builds a JSON slog.Logger writing single-line records to the
// process log sink.
func NewJSONLogger(w *os.File) *Logger {
	handler := slog.NewJSONHandler(w, &slog.HandlerOptions{Level: slog.LevelInfo})
	return &Logger{logger: slog.New(handler)}
}

// jsonLineWriter is any single-record sink (test seam).
type jsonLineWriter interface{ Write([]byte) (int, error) }

// NewJSONLoggerForTest builds the production JSON logger over a test sink.
func NewJSONLoggerForTest(w jsonLineWriter) *Logger {
	handler := slog.NewJSONHandler(w, &slog.HandlerOptions{Level: slog.LevelInfo})
	return &Logger{logger: slog.New(handler)}
}

// NewNopLogger returns a logger that discards everything.
func NewNopLogger() *Logger {
	return &Logger{logger: slog.New(slog.NewJSONHandler(discardWriter{}, &slog.HandlerOptions{Level: slog.LevelError + 10}))}
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

// With returns a child logger with bounded attributes attached.
func (l *Logger) With(attributes ...string) *Logger {
	if l == nil {
		return nil
	}
	args := make([]any, 0, len(attributes))
	for i := 0; i+1 < len(attributes); i += 2 {
		args = append(args, attributes[i], Truncate(attributes[i+1], 128))
	}
	return &Logger{logger: l.logger.With(args...)}
}

// Event emits one structured record. Only bounded, pre-classified values are
// accepted: key/value strings are truncated, and callers must never pass raw
// errors, bodies, queries, vectors or credentials.
func (l *Logger) Event(ctx context.Context, level slog.Level, event, component, operation, outcome string, fields ...string) {
	if l == nil || l.logger == nil {
		return
	}
	if !utf8.ValidString(event) {
		event = "invalid_event"
	}
	args := make([]any, 0, 10+len(fields)/2*2)
	args = append(args,
		slog.String("event", Truncate(event, 64)),
		slog.String("component", Truncate(component, 32)),
		slog.String("operation", Truncate(operation, 64)),
	)
	if outcome != "" {
		args = append(args, slog.String("outcome", Truncate(outcome, 64)))
	}
	if len(fields)%2 == 1 {
		fields = fields[:len(fields)-1]
	}
	for i := 0; i+1 < len(fields); i += 2 {
		key := Truncate(fields[i], 32)
		if !allowedLogKeys[key] {
			// Unknown keys are dropped wholesale: stable fields are the only
			// contract, and unknown keys often carry raw identifiers.
			continue
		}
		args = append(args, slog.String(key, Truncate(fields[i+1], 256)))
	}
	if ctx != nil {
		if span := traceSpanFromContext(ctx); span.IsValid() {
			args = append(args,
				slog.String("trace_id", span.TraceID().String()),
				slog.String("span_id", span.SpanID().String()),
			)
		}
	}
	args = append(args, slog.Time("timestamp", time.Now().UTC()))
	level = normalizeLevel(level)
	switch {
	case level >= slog.LevelError:
		l.logger.LogAttrs(ctx, slog.LevelError, event, toAttrValues(args)...)
	case level >= slog.LevelWarn:
		l.logger.LogAttrs(ctx, slog.LevelWarn, event, toAttrValues(args)...)
	default:
		l.logger.LogAttrs(ctx, slog.LevelInfo, event, toAttrValues(args)...)
	}
}

// EventRateLimited drops repeated identical events within the window so
// readiness polling and export failures cannot flood the log.
func (l *Logger) EventRateLimited(ctx context.Context, level slog.Level, event, component, operation, outcome string, window time.Duration, fields ...string) {
	if l == nil {
		return
	}
	key := event + "|" + component + "|" + operation
	if !l.rateLimit(key, window) {
		return
	}
	l.Event(ctx, level, event, component, operation, outcome, fields...)
}

var allowedLogKeys = map[string]bool{
	"error_category": true, "context_state": true, "outcome": true,
	"attempt": true, "candidates": true, "hydrated": true,
	"scanned": true, "enqueued": true, "filtered_reason": true,
	"backend_kind": true, "status_class": true, "route": true,
	"method": true, "component": true, "operation": true,
	"worker_kind": true, "reason": true,
}

type rateState struct {
	mu      sync.Mutex
	lastHit map[string]time.Time
}

// SafeError maps known sentinel categories to a bounded string. Raw error
// text never passes through; unknown errors collapse to "unknown".
func SafeError(err error) string {
	if err == nil {
		return ""
	}
	text := err.Error()
	// Category-only sentinels in this codebase have the shape
	// "<domain>: <category>" with no dynamic content. Any error text that
	// contains characters outside a conservative allowlist collapses.
	if !safeCategoryText(text) {
		return "unknown"
	}
	return Truncate(text, 128)
}

// safeCategoryText accepts only category-shaped sentinels:
// "<domain>: <snake_words>" with at most one colon and no digits.
func safeCategoryText(text string) bool {
	if len(text) > 64 || !utf8.ValidString(text) {
		return false
	}
	if strings.ContainsAny(text, "\x00\r\n") {
		return false
	}
	domain, category, ok := strings.Cut(text, ": ")
	if !ok || strings.Contains(category, ":") {
		return false
	}
	if domain == "" || category == "" {
		return false
	}
	for _, r := range domain + category {
		switch {
		case r >= 'a' && r <= 'z', r == '_', r == '-':
		default:
			return false
		}
	}
	return true
}

// Fingerprint returns a bounded, salted fingerprint of a correlation value.
// Raw business identifiers never become metrics labels or log fields.
func Fingerprint(value string) string {
	if value == "" {
		return ""
	}
	sum := sha256.Sum256([]byte("trpc-agent/telemetry-fingerprint/v1\x00" + value))
	return hex.EncodeToString(sum[:])[:16]
}

// Truncate bounds a string and strips control characters and newlines so a
// record stays single-line and parseable.
func Truncate(value string, max int) string {
	if max <= 0 {
		return ""
	}
	var builder strings.Builder
	count := 0
	for _, r := range value {
		if r == '\n' || r == '\r' || r == '\x00' {
			builder.WriteRune(' ')
			continue
		}
		if r < 0x20 || r == 0x7f {
			continue
		}
		if count >= max {
			break
		}
		builder.WriteRune(r)
		count++
	}
	if !utf8.ValidString(builder.String()) {
		return ""
	}
	return builder.String()
}

func (l *Logger) rateLimit(key string, window time.Duration) bool {
	if l.rate == nil {
		l.rate = &rateState{lastHit: make(map[string]time.Time)}
	}
	l.rate.mu.Lock()
	defer l.rate.mu.Unlock()
	now := time.Now()
	if last, ok := l.rate.lastHit[key]; ok && now.Sub(last) < window {
		return false
	}
	if l.rate.lastHit == nil {
		l.rate.lastHit = make(map[string]time.Time)
	}
	l.rate.lastHit[key] = now
	if len(l.rate.lastHit) > 512 {
		l.rate.lastHit = make(map[string]time.Time)
	}
	return true
}
