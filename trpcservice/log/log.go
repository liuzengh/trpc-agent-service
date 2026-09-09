// Package log provides structural and textual secret redaction.
package log

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"sync"

	trpclog "trpc.group/trpc-go/trpc-agent-go/log"
)

var credentialPattern = regexp.MustCompile(`(?i)((?:"?(?:authorization|api[_-]?key|token|secret|password)"?)\s*[:=]\s*(?:"?bearer\s+)?["']?)[^\s,;}"']+`)

type Redactor struct {
	fields  map[string]struct{}
	secrets []string
	custom  *regexp.Regexp
}

func (redactor *Redactor) RedactField(field, value string) string {
	if redactor == nil {
		return NewRedactor(nil, nil).RedactString(value)
	}
	if _, sensitive := redactor.fields[strings.ToLower(field)]; sensitive {
		return "[REDACTED]"
	}
	return redactor.RedactString(value)
}

func NewRedactor(fields, secrets []string) *Redactor {
	defaults := []string{"authorization", "api_key", "apikey", "token", "secret", "password", "dsn"}
	result := &Redactor{fields: make(map[string]struct{})}
	for _, field := range append(defaults, fields...) {
		result.fields[strings.ToLower(field)] = struct{}{}
	}
	for _, secret := range secrets {
		if secret != "" {
			result.secrets = append(result.secrets, secret)
		}
	}
	customFields := make([]string, 0, len(fields))
	for _, field := range fields {
		if trimmed := strings.TrimSpace(field); trimmed != "" {
			customFields = append(customFields, regexp.QuoteMeta(trimmed))
		}
	}
	if len(customFields) > 0 {
		result.custom = regexp.MustCompile(`(?i)((?:"?(?:` + strings.Join(customFields, "|") + `)"?)\s*[:=]\s*(?:"?bearer\s+)?["']?)[^\s,;}"']+`)
	}
	return result
}
func (redactor *Redactor) RedactString(value string) string {
	if redactor == nil {
		return value
	}
	for _, secret := range redactor.secrets {
		value = strings.ReplaceAll(value, secret, "[REDACTED]")
	}
	value = credentialPattern.ReplaceAllStringFunc(value, redactAssignment)
	if redactor.custom != nil {
		value = redactor.custom.ReplaceAllStringFunc(value, redactAssignment)
	}
	return value
}

func redactAssignment(match string) string {
	if index := strings.IndexAny(match, "=:"); index >= 0 {
		return match[:index+1] + " [REDACTED]"
	}
	return "[REDACTED]"
}
func (redactor *Redactor) RedactMap(value map[string]any) map[string]any {
	if redactor == nil {
		redactor = NewRedactor(nil, nil)
	}
	result := make(map[string]any, len(value))
	for key, item := range value {
		if _, sensitive := redactor.fields[strings.ToLower(key)]; sensitive {
			result[key] = "[REDACTED]"
			continue
		}
		result[key] = redactor.redactValue(item)
	}
	return result
}

// RedactValue recursively sanitizes strings and secret-bearing map fields
// while preserving the original structured value shape.
func (redactor *Redactor) RedactValue(value any) any {
	if redactor == nil {
		redactor = NewRedactor(nil, nil)
	}
	return redactor.redactValue(value)
}

func (redactor *Redactor) redactValue(value any) any {
	switch typed := value.(type) {
	case string:
		return redactor.RedactString(typed)
	case map[string]any:
		return redactor.RedactMap(typed)
	case []any:
		result := make([]any, len(typed))
		for i, item := range typed {
			result[i] = redactor.redactValue(item)
		}
		return result
	default:
		return value
	}
}
func (redactor *Redactor) JSON(value any) []byte {
	payload, _ := json.Marshal(value)
	return []byte(redactor.RedactString(string(payload)))
}

type redactorContextKey struct{}

func WithRedactor(ctx context.Context, redactor *Redactor) context.Context {
	return context.WithValue(ctx, redactorContextKey{}, redactor)
}
func fromContext(ctx context.Context) *Redactor {
	if redactor, ok := ctx.Value(redactorContextKey{}).(*Redactor); ok && redactor != nil {
		return redactor
	}
	return NewRedactor(nil, nil)
}

type wrappedLogger struct {
	delegate trpclog.Logger
	redactor *Redactor
}

func (logger wrappedLogger) text(args ...any) string {
	return logger.redactor.RedactString(fmt.Sprint(args...))
}
func (logger wrappedLogger) format(format string, args ...any) string {
	return logger.redactor.RedactString(fmt.Sprintf(format, args...))
}
func (logger wrappedLogger) Debug(args ...any) { logger.delegate.Debug(logger.text(args...)) }
func (logger wrappedLogger) Debugf(format string, args ...any) {
	logger.delegate.Debug(logger.format(format, args...))
}
func (logger wrappedLogger) Info(args ...any) { logger.delegate.Info(logger.text(args...)) }
func (logger wrappedLogger) Infof(format string, args ...any) {
	logger.delegate.Info(logger.format(format, args...))
}
func (logger wrappedLogger) Warn(args ...any) { logger.delegate.Warn(logger.text(args...)) }
func (logger wrappedLogger) Warnf(format string, args ...any) {
	logger.delegate.Warn(logger.format(format, args...))
}
func (logger wrappedLogger) Error(args ...any) { logger.delegate.Error(logger.text(args...)) }
func (logger wrappedLogger) Errorf(format string, args ...any) {
	logger.delegate.Error(logger.format(format, args...))
}
func (logger wrappedLogger) Fatal(args ...any) { logger.delegate.Fatal(logger.text(args...)) }
func (logger wrappedLogger) Fatalf(format string, args ...any) {
	logger.delegate.Fatal(logger.format(format, args...))
}

var installOnce sync.Once

// InstallUpstreamRedaction wraps the global tRPC-Agent-Go logger once and makes
// its context-aware log functions honor the request's tenant redactor.
func InstallUpstreamRedaction() {
	installOnce.Do(func() {
		generic := NewRedactor(nil, nil)
		trpclog.Default = wrappedLogger{delegate: trpclog.Default, redactor: generic}
		trpclog.ContextDefault = wrappedLogger{delegate: trpclog.ContextDefault, redactor: generic}
		trpclog.DebugContext = func(ctx context.Context, args ...any) {
			trpclog.ContextDefault.Debug(fromContext(ctx).RedactString(fmt.Sprint(args...)))
		}
		trpclog.DebugfContext = func(ctx context.Context, format string, args ...any) {
			trpclog.ContextDefault.Debug(fromContext(ctx).RedactString(fmt.Sprintf(format, args...)))
		}
		trpclog.InfoContext = func(ctx context.Context, args ...any) {
			trpclog.ContextDefault.Info(fromContext(ctx).RedactString(fmt.Sprint(args...)))
		}
		trpclog.InfofContext = func(ctx context.Context, format string, args ...any) {
			trpclog.ContextDefault.Info(fromContext(ctx).RedactString(fmt.Sprintf(format, args...)))
		}
		trpclog.WarnContext = func(ctx context.Context, args ...any) {
			trpclog.ContextDefault.Warn(fromContext(ctx).RedactString(fmt.Sprint(args...)))
		}
		trpclog.WarnfContext = func(ctx context.Context, format string, args ...any) {
			trpclog.ContextDefault.Warn(fromContext(ctx).RedactString(fmt.Sprintf(format, args...)))
		}
		trpclog.ErrorContext = func(ctx context.Context, args ...any) {
			trpclog.ContextDefault.Error(fromContext(ctx).RedactString(fmt.Sprint(args...)))
		}
		trpclog.ErrorfContext = func(ctx context.Context, format string, args ...any) {
			trpclog.ContextDefault.Error(fromContext(ctx).RedactString(fmt.Sprintf(format, args...)))
		}
	})
}
