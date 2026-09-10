package log

import (
	"bytes"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestNewDefaultsToInfoJSONAndRedactsSensitiveFields(t *testing.T) {
	var output bytes.Buffer
	logger, err := New(Config{Output: &output})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	logger.Debug("hidden", zap.String("debug", "not emitted"))
	logger.With(
		zap.String("client_secret", "child-secret"),
	).Info("created", zap.String("provider_token", "request-secret"), zap.String("tenant_id", "tenant-1"))

	logged := output.String()
	if strings.Contains(logged, "child-secret") || strings.Contains(logged, "request-secret") {
		t.Fatalf("sensitive values leaked: %s", logged)
	}
	if !strings.Contains(logged, RedactedValue) {
		t.Fatalf("redacted marker missing: %s", logged)
	}
	if !strings.Contains(logged, `"tenant_id":"tenant-1"`) {
		t.Fatalf("structured field missing: %s", logged)
	}
	if strings.Contains(logged, "hidden") {
		t.Fatalf("debug entry was emitted at the default level: %s", logged)
	}
	if !strings.Contains(logged, `"level":"info"`) {
		t.Fatalf("info level missing: %s", logged)
	}
}

func TestNewFiltersEntriesBelowConfiguredLevel(t *testing.T) {
	var output bytes.Buffer
	logger, err := New(Config{Level: zapcore.WarnLevel, Output: &output})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	logger.Info("not emitted")
	logger.Warn("emitted")

	logged := output.String()
	if strings.Contains(logged, "not emitted") {
		t.Fatalf("info entry was emitted at warn level: %s", logged)
	}
	if !strings.Contains(logged, "emitted") {
		t.Fatalf("warn entry was not emitted: %s", logged)
	}
}

func TestNewSupportsConsoleAndDevelopment(t *testing.T) {
	var output bytes.Buffer
	var errorOutput bytes.Buffer
	logger, err := New(Config{
		Encoding:    EncodingConsole,
		Output:      &output,
		ErrorOutput: &errorOutput,
		Development: true,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	logger.Info("console output")

	if !strings.Contains(output.String(), "console output") {
		t.Fatalf("console entry missing: %s", output.String())
	}
}

func TestNewUsesDefaultOutputsWhenUnset(t *testing.T) {
	if _, err := New(Config{}); err != nil {
		t.Fatalf("New() error = %v", err)
	}
}

func TestNewRedactsConfiguredKeySuffix(t *testing.T) {
	var output bytes.Buffer
	logger, err := New(Config{Output: &output, RedactKeys: []string{"session_key"}})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	logger.Info("configured", zap.String("oauth-session-key", "session-secret"))

	logged := output.String()
	if strings.Contains(logged, "session-secret") || !strings.Contains(logged, RedactedValue) {
		t.Fatalf("configured sensitive field was not redacted: %s", logged)
	}
}

func TestNewRejectsInvalidConfiguration(t *testing.T) {
	if _, err := New(Config{Level: zapcore.Level(99)}); err == nil {
		t.Fatal("New() accepted an invalid log level")
	}
	if _, err := New(Config{Encoding: Encoding("xml")}); err == nil {
		t.Fatal("New() accepted an invalid log encoding")
	}
}

func TestPackageHelpersForwardToDefaultLogger(t *testing.T) {
	var output bytes.Buffer
	logger, err := New(Config{Level: zapcore.DebugLevel, Output: &output})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	restore := SetDefault(logger)
	t.Cleanup(restore)

	Debug("debug helper")
	Warn("warn helper")
	Error("error helper")
	DPanic("dpanic helper")

	logged := output.String()
	for _, message := range []string{"debug helper", "warn helper", "error helper", "dpanic helper"} {
		if !strings.Contains(logged, message) {
			t.Errorf("package helper did not log %q: %s", message, logged)
		}
	}
}

func TestPrefixedLoggerAddsComponentPrefix(t *testing.T) {
	var output bytes.Buffer
	logger, err := New(Config{Level: zapcore.DebugLevel, Output: &output})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	restore := SetDefault(logger)
	t.Cleanup(restore)

	prefixed := NewPrefixedLogger("[component]")
	prefixed.Debug("debug message")
	prefixed.Info("info message")
	prefixed.Warn("warn message")
	prefixed.Error("error message")

	logged := output.String()
	for _, message := range []string{
		"[component] debug message",
		"[component] info message",
		"[component] warn message",
		"[component] error message",
	} {
		if !strings.Contains(logged, message) {
			t.Errorf("prefixed logger did not log %q: %s", message, logged)
		}
	}
	if strings.Contains(logged, "log/log.go") {
		t.Fatalf("prefixed logger reported its implementation as the caller: %s", logged)
	}
}

func TestPrefixedLoggerOmitsBlankPrefix(t *testing.T) {
	var output bytes.Buffer
	logger, err := New(Config{Output: &output})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	restore := SetDefault(logger)
	t.Cleanup(restore)

	NewPrefixedLogger(" \t").Info("unprefixed message")
	if !strings.Contains(output.String(), "unprefixed message") || strings.Contains(output.String(), "[ ]") {
		t.Fatalf("blank prefix was not omitted: %s", output.String())
	}
}

func TestPackagePanicLogsAndPanics(t *testing.T) {
	var output bytes.Buffer
	logger, err := New(Config{Output: &output})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	restore := SetDefault(logger)
	t.Cleanup(restore)

	defer func() {
		if recover() == nil {
			t.Fatal("Panic() did not panic")
		}
		if !strings.Contains(output.String(), "panic helper") {
			t.Errorf("panic entry missing: %s", output.String())
		}
	}()
	Panic("panic helper")
}

func TestPackageFatalExits(t *testing.T) {
	if os.Getenv("TRPC_LOG_FATAL_TEST") == "1" {
		logger, err := New(Config{Output: io.Discard, ErrorOutput: io.Discard})
		if err != nil {
			os.Exit(2)
		}
		SetDefault(logger)
		Fatal("fatal helper")
		return
	}

	command := exec.Command("go", "test", ".", "-run=^TestPackageFatalExits$", "-count=1")
	command.Env = append(os.Environ(), "TRPC_LOG_FATAL_TEST=1")
	if err := command.Run(); err == nil {
		t.Fatal("Fatal() did not exit")
	} else if exitError, ok := err.(*exec.ExitError); !ok || exitError.ExitCode() != 1 {
		t.Fatalf("Fatal() exit = %v, want exit code 1", err)
	}
}

func TestParseLevel(t *testing.T) {
	tests := []struct {
		input string
		want  zapcore.Level
	}{
		{input: "debug", want: zapcore.DebugLevel},
		{input: " INFO ", want: zapcore.InfoLevel},
		{input: "warn", want: zapcore.WarnLevel},
		{input: "error", want: zapcore.ErrorLevel},
		{input: "dpanic", want: zapcore.DPanicLevel},
		{input: "panic", want: zapcore.PanicLevel},
		{input: "fatal", want: zapcore.FatalLevel},
	}

	for _, test := range tests {
		got, err := ParseLevel(test.input)
		if err != nil {
			t.Errorf("ParseLevel(%q) error = %v", test.input, err)
			continue
		}
		if got != test.want {
			t.Errorf("ParseLevel(%q) = %v, want %v", test.input, got, test.want)
		}
	}

	if _, err := ParseLevel("verbose"); err == nil {
		t.Fatal("ParseLevel accepted an unknown level")
	}
}

func TestSetDefaultUsesPackageHelpers(t *testing.T) {
	var output bytes.Buffer
	logger, err := New(Config{Output: &output})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	restore := SetDefault(logger)
	t.Cleanup(restore)

	Info("default logger", zap.String("request_id", "request-1"))

	logged := output.String()
	if !strings.Contains(logged, `"request_id":"request-1"`) {
		t.Fatalf("package helper did not use the configured default: %s", logged)
	}
	if strings.Contains(logged, "log/log.go") {
		t.Fatalf("package helper reported its implementation as the caller: %s", logged)
	}
}

func TestSugaredLoggerUsesDefault(t *testing.T) {
	var output bytes.Buffer
	logger, err := New(Config{Output: &output})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	restore := SetDefault(logger)
	t.Cleanup(restore)

	S().Infow("sugared logger", "request_id", "request-2")

	if !strings.Contains(output.String(), `"request_id":"request-2"`) {
		t.Fatalf("sugared logger entry missing: %s", output.String())
	}
}

func TestSetDefaultNilUsesNoopLogger(t *testing.T) {
	restore := SetDefault(nil)
	t.Cleanup(restore)

	if L().Core().Enabled(zapcore.InfoLevel) {
		t.Fatal("nil default logger is not a no-op")
	}
}

func TestIsSensitiveKeyRejectsEmptyKey(t *testing.T) {
	if isSensitiveKey(" ", sensitiveKeys(nil)) {
		t.Fatal("empty key was considered sensitive")
	}
}

func TestRedactingCoreCheckSkipsDisabledEntries(t *testing.T) {
	core := &redactingCore{
		Core: zapcore.NewCore(
			zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
			zapcore.AddSync(io.Discard),
			zapcore.ErrorLevel,
		),
		sensitiveKeys: sensitiveKeys(nil),
	}

	if checked := core.Check(zapcore.Entry{Level: zapcore.InfoLevel}, nil); checked != nil {
		t.Fatal("disabled entry was added to the checked entry")
	}
}
