// Package log provides platform-wide structured logging (based on zap) with
// secret redaction: fields whose key matches a sensitive-word list are
// replaced with "***" at the Core layer, no matter which package logs them.
//
//	log.Init("info", false) // call once at process startup
//	zap.L().Info("msg", zap.String("session_key", sk))
//
// Two blind spots redaction cannot catch, guarded by review instead:
//  1. Never log whole structs/request bodies via zap.Any — nested secrets
//     won't match the word list.
//  2. Never splice secrets into the message string — redaction only sees
//     field keys, not message content.
package log

import (
	"os"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// sensitiveKeys lists sensitive field names (lowercase, exact match).
// Exact rather than substring match avoids false positives like session_key.
var sensitiveKeys = map[string]struct{}{
	"token":         {},
	"access_token":  {},
	"secret":        {},
	"password":      {},
	"aeskey":        {},
	"aes_key":       {},
	"api_key":       {},
	"apikey":        {},
	"authorization": {},
	"cookie":        {},
}

// level is the global dynamic level, changeable at runtime via SetLevel.
var level = zap.NewAtomicLevelAt(zapcore.InfoLevel)

// init pre-installs a dev logger so logs emitted before Init (e.g. from
// other packages' init) are not silently dropped.
func init() {
	install(true)
}

// Init initializes the global logger (zap.L()); call once at process startup.
// levelStr is one of debug/info/warn/error; invalid values fall back to info.
// development selects human-readable console output, otherwise JSON.
func Init(levelStr string, development bool) {
	if err := level.UnmarshalText([]byte(levelStr)); err != nil {
		level.SetLevel(zapcore.InfoLevel)
	}
	// Flush the old logger before replacing it, so a repeated Init loses nothing.
	_ = zap.L().Sync()
	install(development)
}

// install builds a logger according to development and replaces the global one.
func install(development bool) {
	encCfg := zap.NewProductionEncoderConfig()
	encCfg.EncodeTime = zapcore.ISO8601TimeEncoder

	var enc zapcore.Encoder
	if development {
		encCfg.EncodeLevel = zapcore.CapitalColorLevelEncoder
		enc = zapcore.NewConsoleEncoder(encCfg)
	} else {
		enc = zapcore.NewJSONEncoder(encCfg)
	}

	core := redactCore{zapcore.NewCore(enc, zapcore.Lock(os.Stderr), level)}
	// Stacktraces at Error and above: error paths are rare and exactly where
	// the stack pays off; lower levels stay lean.
	opts := []zap.Option{zap.AddCaller(), zap.AddStacktrace(zapcore.ErrorLevel)}
	zap.ReplaceGlobals(zap.New(core, opts...))
	// Package-level functions need AddCallerSkip(1) so the caller points at
	// the real call site, not inside this package.
	sugarOpts := append([]zap.Option{zap.AddCallerSkip(1)}, opts...)
	sugar = zap.New(core, sugarOpts...).Sugar()
}

// sugar backs the package-level convenience functions.
var sugar = zap.NewNop().Sugar()

// Common cross-link field names. Call sites must use these constants rather
// than hand-written literals so log queries can rely on stable spellings.
const (
	FieldTraceID    = "trace_id"
	FieldTenantID   = "tenant_id"
	FieldChannel    = "channel"
	FieldUserID     = "user_id"
	FieldSessionKey = "session_key"
	FieldMsgID      = "msg_id"
)

// With returns a logger pre-bound with the given fields, typically built once
// at the message-handling entry point so downstream logs carry them.
// Pre-bound sensitive fields are redacted like inline ones.
func With(fields ...zap.Field) *zap.Logger {
	return zap.L().With(fields...)
}

// The package-level functions route through the same redacting Core as
// zap.L(), so framework logs share the redaction path.

// Debug logs at DebugLevel.
func Debug(args ...any) { sugar.Debug(args...) }

// Debugf logs at DebugLevel with printf-style formatting.
func Debugf(format string, args ...any) { sugar.Debugf(format, args...) }

// Info logs at InfoLevel.
func Info(args ...any) { sugar.Info(args...) }

// Infof logs at InfoLevel with printf-style formatting.
func Infof(format string, args ...any) { sugar.Infof(format, args...) }

// Warn logs at WarnLevel.
func Warn(args ...any) { sugar.Warn(args...) }

// Warnf logs at WarnLevel with printf-style formatting.
func Warnf(format string, args ...any) { sugar.Warnf(format, args...) }

// Error logs at ErrorLevel.
func Error(args ...any) { sugar.Error(args...) }

// Errorf logs at ErrorLevel with printf-style formatting.
func Errorf(format string, args ...any) { sugar.Errorf(format, args...) }

// SetLevel adjusts the log level at runtime; an invalid value returns an
// error and leaves the level unchanged.
func SetLevel(levelStr string) error {
	return level.UnmarshalText([]byte(levelStr))
}

// Sync flushes buffered logs; call before process exit.
func Sync() { _ = zap.L().Sync() }

// redactCore wraps a zapcore.Core and redacts sensitive fields.
type redactCore struct {
	zapcore.Core
}

// Check must be overridden: the promoted default would register the inner
// Core directly, bypassing this layer's Write and skipping redaction.
func (c redactCore) Check(ent zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if !c.Enabled(ent.Level) {
		return ce
	}
	return ce.AddCore(ent, c)
}

// Write redacts sensitive fields, then delegates to the inner Core.
func (c redactCore) Write(ent zapcore.Entry, fields []zapcore.Field) error {
	redact(fields)
	return c.Core.Write(ent, fields)
}

// With redacts pre-bound fields, which bypass the Write path.
func (c redactCore) With(fields []zapcore.Field) zapcore.Core {
	redact(fields)
	return redactCore{c.Core.With(fields)}
}

// redact rewrites the values of sensitive fields in place.
func redact(fields []zapcore.Field) {
	for i := range fields {
		if _, ok := sensitiveKeys[strings.ToLower(fields[i].Key)]; ok {
			fields[i] = zap.String(fields[i].Key, "***")
		}
	}
}
