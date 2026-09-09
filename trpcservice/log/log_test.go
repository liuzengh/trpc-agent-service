package log

import (
	"strings"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// observedFields logs one entry and returns its fields for assertion.
func observedFields(t *testing.T, fields ...zapcore.Field) []zapcore.Field {
	t.Helper()
	core, logs := observer.New(zapcore.InfoLevel)
	l := zap.New(redactCore{core})
	l.Info("test", fields...)
	entries := logs.All()
	if len(entries) != 1 {
		t.Fatalf("expected 1 log entry, got %d", len(entries))
	}
	return entries[0].Context
}

func TestRedactSensitiveKeys(t *testing.T) {
	fields := observedFields(t,
		zap.String("token", "real-token-value"),
		zap.String("AESKey", "real-aes-key"), // case-insensitive
		zap.String("api_key", "real-api-key"),
	)
	for _, f := range fields {
		if f.String != "***" {
			t.Errorf("field %q should be redacted, got %q", f.Key, f.String)
		}
	}
}

func TestNormalFieldsNotRedacted(t *testing.T) {
	fields := observedFields(t,
		zap.String("session_key", "dm:mock:alice"), // contains "key" substring but must not be a false positive
		zap.String("msg_id", "m1"),
		zap.String("trace_id", "abc"),
	)
	for _, f := range fields {
		if f.String == "***" {
			t.Errorf("field %q should NOT be redacted", f.Key)
		}
	}
}

// Fields pre-bound via With must also be redacted (they bypass the Write path).
func TestRedactWithFields(t *testing.T) {
	core, logs := observer.New(zapcore.InfoLevel)
	l := zap.New(redactCore{core}).With(zap.String("password", "p@ssw0rd"))
	l.Info("test")
	entries := logs.All()
	if len(entries) != 1 {
		t.Fatalf("expected 1 log entry, got %d", len(entries))
	}
	if got := entries[0].Context[0].String; got != "***" {
		t.Errorf("With-bound field should be redacted, got %q", got)
	}
}

func TestInitFallbackLevel(t *testing.T) {
	// Invalid level falls back to info without panicking.
	Init("not-a-level", false)
	if !zap.L().Core().Enabled(zapcore.InfoLevel) {
		t.Error("info should be enabled")
	}
	if zap.L().Core().Enabled(zapcore.DebugLevel) {
		t.Error("debug should be disabled at info level")
	}
}

func TestSetLevelAtRuntime(t *testing.T) {
	Init("info", false)
	if zap.L().Core().Enabled(zapcore.DebugLevel) {
		t.Fatal("debug should be disabled initially")
	}

	if err := SetLevel("debug"); err != nil {
		t.Fatal(err)
	}
	if !zap.L().Core().Enabled(zapcore.DebugLevel) {
		t.Error("debug should be enabled after SetLevel")
	}

	if err := SetLevel("bogus"); err == nil {
		t.Error("invalid level should return error")
	}
	if !zap.L().Core().Enabled(zapcore.DebugLevel) {
		t.Error("level should stay unchanged after invalid SetLevel")
	}
}

// The package-level functions' caller must point at the call site, not inside
// the log package.
func TestPackageFuncsCallerSkip(t *testing.T) {
	core, logs := observer.New(zapcore.DebugLevel)
	sugar = zap.New(redactCore{core}, zap.AddCaller(), zap.AddCallerSkip(1)).Sugar()

	Infof("hello %s", "world")

	entries := logs.All()
	if len(entries) != 1 {
		t.Fatalf("expected 1 log entry, got %d", len(entries))
	}
	if entries[0].Message != "hello world" {
		t.Errorf("unexpected message: %q", entries[0].Message)
	}
	if !strings.HasSuffix(entries[0].Caller.File, "log_test.go") {
		t.Errorf("caller should be log_test.go (caller skip works), got %s", entries[0].Caller.File)
	}
}

// With-bound sensitive fields must be redacted; common fields pass through.
func TestWithAttachesAndRedacts(t *testing.T) {
	core, logs := observer.New(zapcore.InfoLevel)
	zap.ReplaceGlobals(zap.New(redactCore{core}))

	With(
		zap.String(FieldTraceID, "t-1"),
		zap.String("token", "should-be-masked"),
	).Info("hello")

	entries := logs.All()
	if len(entries) != 1 {
		t.Fatalf("expected 1 log entry, got %d", len(entries))
	}
	fields := entries[0].Context
	if len(fields) != 2 {
		t.Fatalf("expected 2 fields, got %d", len(fields))
	}
	if fields[0].Key != FieldTraceID || fields[0].String != "t-1" {
		t.Errorf("trace_id field wrong: %+v", fields[0])
	}
	if fields[1].String != "***" {
		t.Errorf("sensitive With-field should be redacted, got %q", fields[1].String)
	}
}

// Error-and-above logs carry a stacktrace; Warn does not.
func TestStacktraceOnlyAtError(t *testing.T) {
	core, logs := observer.New(zapcore.DebugLevel)
	l := zap.New(redactCore{core}, zap.AddStacktrace(zapcore.ErrorLevel))

	l.Warn("warn")
	l.Error("boom")

	entries := logs.All()
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
	if entries[0].Stack != "" {
		t.Error("warn should not carry a stacktrace")
	}
	if !strings.Contains(entries[1].Stack, "TestStacktraceOnlyAtError") {
		t.Error("error should carry a stacktrace containing the test frame")
	}
}
