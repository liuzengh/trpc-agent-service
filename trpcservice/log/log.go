// Package log configures log levels and redaction for secrets and common PII.
package log

import (
	"context"
	"log/slog"
	"regexp"
	"strings"
)

var redactionPatterns = []*regexp.Regexp{
	// Match quoted JSON before the more permissive assignment form so the
	// closing quote and surrounding payload remain intact.
	regexp.MustCompile(`(?i)("(?:authorization|proxy[_-]?authorization|api[_-]?key|(?:(?:access|refresh|id)[_-]?)?token|client[_-]?secret|secret|password|passwd)"\s*:\s*")(?:\\.|[^"\\])*(")`),
	regexp.MustCompile(`(?i)('(?:authorization|proxy[_-]?authorization|api[_-]?key|(?:(?:access|refresh|id)[_-]?)?token|client[_-]?secret|secret|password|passwd)'\s*:\s*')(?:\\.|[^'\\])*(')`),
	regexp.MustCompile(`(?i)((?:authorization|proxy[_-]?authorization|api[_-]?key|(?:(?:access|refresh|id)[_-]?)?token|client[_-]?secret|secret|password|passwd)\s*[:=]\s*)(?:bearer\s+)?[^\s,;]+`),
	// URI userinfo has the same password shape across PostgreSQL, Redis,
	// AMQP, MongoDB and other registered schemes.
	regexp.MustCompile(`(?i)([a-z][a-z0-9+.-]*://[^:/@\s"']*:)[^/@\s"']+(@)`),
}

var piiRedactionPatterns = []*regexp.Regexp{
	// Mainland China resident identity number. Keep this before the generic
	// payment-card pattern because both may contain 18 digits.
	regexp.MustCompile(`(?i)[1-9][0-9]{5}(?:18|19|20|21)[0-9]{2}(?:0[1-9]|1[0-2])(?:0[1-9]|[12][0-9]|3[01])[0-9]{3}[0-9x]`),
	// Mainland China mobile number.
	regexp.MustCompile(`1[3-9][0-9]{9}`),
	// Common payment-card/PAN lengths. Log redaction deliberately favors
	// over-redaction over copying a plausible financial identifier.
	regexp.MustCompile(`[1-9][0-9]{15,18}`),
	// Email addresses are treated as PII even when the attribute key itself is
	// not sensitive (for example error messages copied from an upstream API).
	regexp.MustCompile(`(?i)[a-z0-9.!#$%&'*+/=?^_` + "`" + `{|}~-]+@[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+`),
}

// Redact replaces credential-like values so logs, traces and errors can be
// written without copying secrets.
func Redact(detail string) string {
	redacted := detail
	for _, pattern := range redactionPatterns {
		redacted = pattern.ReplaceAllString(redacted, "$1[REDACTED]$2")
	}
	for _, pattern := range piiRedactionPatterns {
		redacted = pattern.ReplaceAllString(redacted, "[REDACTED_PII]")
	}
	return redacted
}

// NewRedactingHandler wraps a slog handler and redacts credential forms in
// messages and attributes.
func NewRedactingHandler(inner slog.Handler) slog.Handler {
	if inner == nil {
		return slog.Default().Handler()
	}
	return &redactingHandler{inner: inner}
}

type redactingHandler struct {
	inner slog.Handler
}

func (h *redactingHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h *redactingHandler) Handle(ctx context.Context, record slog.Record) error {
	record.Message = Redact(record.Message)
	attrs := make([]slog.Attr, 0, record.NumAttrs())
	record.Attrs(func(attr slog.Attr) bool {
		attrs = append(attrs, redactAttr(attr))
		return true
	})
	clone := slog.NewRecord(record.Time, record.Level, record.Message, record.PC)
	clone.AddAttrs(attrs...)
	return h.inner.Handle(ctx, clone)
}

func (h *redactingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	redacted := make([]slog.Attr, len(attrs))
	for i, attr := range attrs {
		redacted[i] = redactAttr(attr)
	}
	return &redactingHandler{inner: h.inner.WithAttrs(redacted)}
}

func (h *redactingHandler) WithGroup(name string) slog.Handler {
	return &redactingHandler{inner: h.inner.WithGroup(name)}
}

func secretAttrKey(key string) bool {
	k := strings.ToLower(strings.TrimSpace(key))
	return strings.Contains(k, "token") || strings.Contains(k, "secret") || strings.Contains(k, "password") ||
		strings.Contains(k, "authorization") || strings.Contains(k, "api_key") || strings.Contains(k, "api-key")
}

func redactAttr(attr slog.Attr) slog.Attr {
	attr.Value = attr.Value.Resolve()
	if secretAttrKey(attr.Key) && attr.Value.Kind() != slog.KindGroup {
		return slog.String(attr.Key, "[REDACTED]")
	}
	switch attr.Value.Kind() {
	case slog.KindString:
		return slog.String(attr.Key, Redact(attr.Value.String()))
	case slog.KindGroup:
		group := attr.Value.Group()
		redacted := make([]slog.Attr, len(group))
		for i, nested := range group {
			redacted[i] = redactAttr(nested)
		}
		return slog.Attr{Key: attr.Key, Value: slog.GroupValue(redacted...)}
	default:
		if formatted := attr.Value.String(); formatted != "" {
			redacted := Redact(formatted)
			if redacted != formatted {
				return slog.String(attr.Key, redacted)
			}
		}
		return attr
	}
}
