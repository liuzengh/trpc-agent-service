// Package log provides the service's structured, redacting log boundary.
package log

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/redaction"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"go.opentelemetry.io/otel/trace"
)

type Level string

const (
	LevelDebug Level = "debug"
	LevelInfo  Level = "info"
	LevelWarn  Level = "warn"
	LevelError Level = "error"
)

type MaskingLevel = redaction.Level

const (
	MaskNone   = redaction.LevelNone
	MaskBasic  = redaction.LevelBasic
	MaskStrict = redaction.LevelStrict
)

// Attribute is a structured field. Values are normalized before reaching the
// output handler, so a custom Stringer or error cannot bypass redaction.
type Attribute struct {
	Key   string
	Value any
}

func String(key, value string) Attribute    { return Attribute{Key: key, Value: value} }
func Int(key string, value int) Attribute   { return Attribute{Key: key, Value: value} }
func Bool(key string, value bool) Attribute { return Attribute{Key: key, Value: value} }
func Error(err error) Attribute             { return Attribute{Key: "error", Value: err} }

type Config struct {
	Writer         io.Writer
	Level          Level
	MaskingLevel   MaskingLevel
	RedactionRules []redaction.Rule
	Role           string
}

// Logger retains Print and Printf for the existing role call sites while
// exposing context-aware structured methods for new code.
type Logger struct {
	logger   *slog.Logger
	redactor *redaction.Program
}

func New(config Config) (*Logger, error) {
	if config.MaskingLevel == "" {
		config.MaskingLevel = MaskBasic
	}
	redactor, err := redaction.Compile(redaction.Config{Level: config.MaskingLevel, Rules: config.RedactionRules})
	if err != nil {
		return nil, fmt.Errorf("invalid log redaction policy: %w", err)
	}
	return newLogger(config, redactor)
}

// NewForTenant binds a Logger to an immutable tenant policy version. The
// caller is expected to resolve the tenant through its versioned repository.
func NewForTenant(config Config, policy tenant.Tenant) (*Logger, error) {
	redactor, err := policy.RedactionProgram()
	if err != nil {
		return nil, fmt.Errorf("invalid tenant redaction policy: %w", err)
	}
	config.MaskingLevel = MaskingLevel(policy.LogMaskingLevel)
	return newLogger(config, redactor)
}

func newLogger(config Config, redactor *redaction.Program) (*Logger, error) {
	if config.Writer == nil || !validRole(config.Role) {
		return nil, errors.New("invalid log configuration")
	}
	level, ok := parseLevel(config.Level)
	if !ok {
		return nil, errors.New("invalid log level")
	}
	handler := slog.NewJSONHandler(config.Writer, &slog.HandlerOptions{Level: level})
	return &Logger{logger: slog.New(handler).With(slog.String("role", config.Role)), redactor: redactor}, nil
}

func NewFromEnv(getenv func(string) string, writer io.Writer, role string) (*Logger, error) {
	if getenv == nil {
		return nil, errors.New("invalid log environment")
	}
	level := Level(strings.ToLower(strings.TrimSpace(valueOr(getenv("TRPC_LOG_LEVEL"), string(LevelInfo)))))
	masking := MaskingLevel(strings.ToLower(strings.TrimSpace(valueOr(getenv("TRPC_LOG_MASKING_LEVEL"), string(MaskBasic)))))
	return New(Config{Writer: writer, Level: level, MaskingLevel: masking, Role: role})
}

func (l *Logger) Debug(ctx context.Context, message string, attributes ...Attribute) {
	l.log(ctx, slog.LevelDebug, message, attributes)
}

func (l *Logger) Info(ctx context.Context, message string, attributes ...Attribute) {
	l.log(ctx, slog.LevelInfo, message, attributes)
}

func (l *Logger) Warn(ctx context.Context, message string, attributes ...Attribute) {
	l.log(ctx, slog.LevelWarn, message, attributes)
}

func (l *Logger) Error(ctx context.Context, message string, attributes ...Attribute) {
	l.log(ctx, slog.LevelError, message, attributes)
}

func (l *Logger) Print(values ...any) {
	if l != nil {
		l.Info(context.Background(), fmt.Sprint(values...))
	}
}

func (l *Logger) Printf(format string, values ...any) {
	if l != nil {
		l.Info(context.Background(), fmt.Sprintf(format, values...))
	}
}

func (l *Logger) log(ctx context.Context, level slog.Level, message string, attributes []Attribute) {
	if l == nil || l.logger == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	redactor := l.redactor
	if scoped, ok := redaction.ProgramFromContext(ctx); ok {
		redactor = scoped
	}
	fields := make([]slog.Attr, 0, len(attributes)+2)
	if span := trace.SpanContextFromContext(ctx); span.IsValid() {
		fields = append(fields, slog.String("trace_id", span.TraceID().String()), slog.String("span_id", span.SpanID().String()))
	}
	for _, attribute := range attributes {
		if key := strings.TrimSpace(attribute.Key); validKey(key) {
			fields = append(fields, slog.Any(key, normalizeValue(redactor, key, attribute.Value)))
		}
	}
	l.logger.LogAttrs(ctx, level, redactor.RedactText(message), fields...)
}

func parseLevel(value Level) (slog.Level, bool) {
	switch value {
	case LevelDebug:
		return slog.LevelDebug, true
	case LevelInfo:
		return slog.LevelInfo, true
	case LevelWarn:
		return slog.LevelWarn, true
	case LevelError:
		return slog.LevelError, true
	default:
		return 0, false
	}
}

func validRole(value string) bool {
	value = strings.TrimSpace(value)
	return value != "" && len(value) <= 64 && !strings.ContainsAny(value, "\x00\r\n")
}

func validKey(value string) bool {
	if value == "" || len(value) > 96 {
		return false
	}
	for _, runeValue := range value {
		if !(runeValue >= 'a' && runeValue <= 'z' || runeValue >= 'A' && runeValue <= 'Z' || runeValue >= '0' && runeValue <= '9' || runeValue == '_' || runeValue == '.' || runeValue == '-') {
			return false
		}
	}
	return true
}

func normalizeValue(redactor *redaction.Program, key string, value any) any {
	if redactor == nil || redactor.RedactKey(key) {
		return redaction.Replacement
	}
	switch typed := value.(type) {
	case nil:
		return nil
	case string:
		return redactor.RedactText(typed)
	case error:
		return redactor.RedactText(typed.Error())
	case bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return typed
	default:
		return redactor.RedactText(fmt.Sprint(typed))
	}
}

func valueOr(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}
