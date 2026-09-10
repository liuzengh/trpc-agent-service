// Package log configures structured logging, log levels, and field redaction.
package log

import (
	"fmt"
	"io"
	"os"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Encoding selects the wire format used by a logger.
type Encoding string

const (
	// EncodingJSON is suitable for production log collection.
	EncodingJSON Encoding = "json"
	// EncodingConsole is suitable for local development.
	EncodingConsole Encoding = "console"

	// RedactedValue is written in place of a sensitive field value.
	RedactedValue = "[REDACTED]"
)

// Config controls the logger created by New.
//
// The zero value uses INFO level, JSON encoding, stdout, and the built-in
// sensitive-key list. RedactKeys adds key suffixes to that built-in list.
type Config struct {
	Level       zapcore.Level
	Encoding    Encoding
	Output      io.Writer
	ErrorOutput io.Writer
	Development bool
	RedactKeys  []string
}

// New creates a structured logger with caller information and key-based
// redaction. The caller owns the returned logger and should call Sync when the
// process is shutting down if the configured output requires it.
func New(config Config) (*zap.Logger, error) {
	if config.Level < zapcore.DebugLevel || config.Level > zapcore.FatalLevel {
		return nil, fmt.Errorf("invalid log level %q", config.Level.String())
	}

	encoding := config.Encoding
	if encoding == "" {
		encoding = EncodingJSON
	}

	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.TimeKey = "ts"
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	encoderConfig.EncodeLevel = zapcore.LowercaseLevelEncoder
	encoderConfig.EncodeDuration = zapcore.StringDurationEncoder

	var encoder zapcore.Encoder
	switch encoding {
	case EncodingJSON:
		encoder = zapcore.NewJSONEncoder(encoderConfig)
	case EncodingConsole:
		encoder = zapcore.NewConsoleEncoder(encoderConfig)
	default:
		return nil, fmt.Errorf("invalid log encoding %q", encoding)
	}

	if config.Output == nil {
		config.Output = os.Stdout
	}
	if config.ErrorOutput == nil {
		config.ErrorOutput = os.Stderr
	}

	core := zapcore.NewCore(encoder, zapcore.AddSync(config.Output), config.Level)
	core = &redactingCore{
		Core:          core,
		sensitiveKeys: sensitiveKeys(config.RedactKeys),
	}

	options := []zap.Option{
		zap.AddCaller(),
		zap.ErrorOutput(zapcore.AddSync(config.ErrorOutput)),
	}
	if config.Development {
		options = append(options, zap.Development(), zap.AddStacktrace(zapcore.ErrorLevel))
	}

	return zap.New(core, options...), nil
}

// ParseLevel parses a zap log level from configuration text.
func ParseLevel(value string) (zapcore.Level, error) {
	level := zapcore.InfoLevel
	if err := level.UnmarshalText([]byte(strings.ToLower(strings.TrimSpace(value)))); err != nil {
		return zapcore.InfoLevel, fmt.Errorf("parse log level %q: %w", value, err)
	}
	return level, nil
}

// L returns the process-wide structured logger.
func L() *zap.Logger {
	return zap.L()
}

// S returns the process-wide sugared logger.
func S() *zap.SugaredLogger {
	return zap.S()
}

// SetDefault replaces the process-wide logger and returns a restore function.
// A nil logger installs a no-op logger.
func SetDefault(logger *zap.Logger) func() {
	if logger == nil {
		logger = zap.NewNop()
	}
	return zap.ReplaceGlobals(logger)
}

// PrefixedLogger is a lightweight view that adds a component prefix to each
// message while using the current process-wide logger.
type PrefixedLogger struct {
	prefix string
}

// NewPrefixedLogger creates a logger view with the supplied message prefix.
// The prefix is resolved against the process-wide logger for every entry, so
// replacing the default logger remains effective for existing views.
func NewPrefixedLogger(prefix string) PrefixedLogger {
	return PrefixedLogger{prefix: strings.TrimSpace(prefix)}
}

// Debug logs a debug-level message with the configured prefix.
func (logger PrefixedLogger) Debug(message string, fields ...zap.Field) {
	packageLogger().Debug(logger.prefixedMessage(message), fields...)
}

// Info logs an info-level message with the configured prefix.
func (logger PrefixedLogger) Info(message string, fields ...zap.Field) {
	packageLogger().Info(logger.prefixedMessage(message), fields...)
}

// Warn logs a warning-level message with the configured prefix.
func (logger PrefixedLogger) Warn(message string, fields ...zap.Field) {
	packageLogger().Warn(logger.prefixedMessage(message), fields...)
}

// Error logs an error-level message with the configured prefix.
func (logger PrefixedLogger) Error(message string, fields ...zap.Field) {
	packageLogger().Error(logger.prefixedMessage(message), fields...)
}

func (logger PrefixedLogger) prefixedMessage(message string) string {
	if logger.prefix == "" {
		return message
	}
	return logger.prefix + " " + message
}

func packageLogger() *zap.Logger {
	return L().WithOptions(zap.AddCallerSkip(1))
}

// Debug logs a debug-level message using the process-wide logger.
func Debug(message string, fields ...zap.Field) {
	packageLogger().Debug(message, fields...)
}

// Info logs an info-level message using the process-wide logger.
func Info(message string, fields ...zap.Field) {
	packageLogger().Info(message, fields...)
}

// Warn logs a warning-level message using the process-wide logger.
func Warn(message string, fields ...zap.Field) {
	packageLogger().Warn(message, fields...)
}

// Error logs an error-level message using the process-wide logger.
func Error(message string, fields ...zap.Field) {
	packageLogger().Error(message, fields...)
}

// DPanic logs a panic-level message in development and an error-level message
// otherwise.
func DPanic(message string, fields ...zap.Field) {
	packageLogger().DPanic(message, fields...)
}

// Panic logs a message and then panics.
func Panic(message string, fields ...zap.Field) {
	packageLogger().Panic(message, fields...)
}

// Fatal logs a message and then calls os.Exit(1). Prefer returning errors in
// libraries and reserve this for process boundaries.
func Fatal(message string, fields ...zap.Field) {
	packageLogger().Fatal(message, fields...)
}

var defaultSensitiveKeys = []string{
	"access_key",
	"access_token",
	"api_key",
	"apikey",
	"authorization",
	"client_secret",
	"cookie",
	"password",
	"passwd",
	"private_key",
	"refresh_token",
	"secret",
	"secret_ref",
	"token",
}

type redactingCore struct {
	zapcore.Core
	sensitiveKeys map[string]struct{}
}

func (c *redactingCore) Check(entry zapcore.Entry, checked *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if c.Enabled(entry.Level) {
		return checked.AddCore(entry, c)
	}
	return checked
}

func (c *redactingCore) With(fields []zapcore.Field) zapcore.Core {
	return &redactingCore{
		Core:          c.Core.With(redactFields(fields, c.sensitiveKeys)),
		sensitiveKeys: c.sensitiveKeys,
	}
}

func (c *redactingCore) Write(entry zapcore.Entry, fields []zapcore.Field) error {
	return c.Core.Write(entry, redactFields(fields, c.sensitiveKeys))
}

func sensitiveKeys(additional []string) map[string]struct{} {
	keys := make(map[string]struct{}, len(defaultSensitiveKeys)+len(additional))
	for _, key := range defaultSensitiveKeys {
		if normalized := normalizeKey(key); normalized != "" {
			keys[normalized] = struct{}{}
		}
	}
	for _, key := range additional {
		if normalized := normalizeKey(key); normalized != "" {
			keys[normalized] = struct{}{}
		}
	}
	return keys
}

func redactFields(fields []zapcore.Field, keys map[string]struct{}) []zapcore.Field {
	redacted := make([]zapcore.Field, len(fields))
	copy(redacted, fields)
	for index, field := range redacted {
		if isSensitiveKey(field.Key, keys) {
			redacted[index] = zap.String(field.Key, RedactedValue)
		}
	}
	return redacted
}

func isSensitiveKey(key string, sensitive map[string]struct{}) bool {
	normalized := normalizeKey(key)
	if normalized == "" {
		return false
	}
	if _, ok := sensitive[normalized]; ok {
		return true
	}
	for key := range sensitive {
		if strings.HasSuffix(normalized, "_"+key) {
			return true
		}
	}
	return false
}

func normalizeKey(key string) string {
	key = strings.ToLower(strings.TrimSpace(key))
	return strings.NewReplacer("-", "_", ".", "_", " ", "_", "/", "_").Replace(key)
}
