package log

import (
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// The package-level convenience wrappers must each emit one entry at their
// level, through the sugar logger with redaction, and Sync must not panic.
func TestPackageLevelFuncs(t *testing.T) {
	core, logs := observer.New(zapcore.DebugLevel)
	old := sugar
	sugar = zap.New(redactCore{core}).Sugar()
	t.Cleanup(func() { sugar = old })

	Debug("d", 1)
	Debugf("d-%s", "f")
	Info("i", 1)
	Infof("i-%s", "f")
	Warn("w", 1)
	Warnf("w-%s", "f")
	Error("e", 1)
	Errorf("e-%s", "f")
	Sync()

	entries := logs.All()
	if len(entries) != 8 {
		t.Fatalf("expected 8 entries, got %d", len(entries))
	}
	wantMsg := []string{"d1", "d-f", "i1", "i-f", "w1", "w-f", "e1", "e-f"}
	wantLvl := []zapcore.Level{
		zapcore.DebugLevel, zapcore.DebugLevel,
		zapcore.InfoLevel, zapcore.InfoLevel,
		zapcore.WarnLevel, zapcore.WarnLevel,
		zapcore.ErrorLevel, zapcore.ErrorLevel,
	}
	for i, e := range entries {
		if e.Message != wantMsg[i] {
			t.Errorf("entry %d message = %q, want %q", i, e.Message, wantMsg[i])
		}
		if e.Level != wantLvl[i] {
			t.Errorf("entry %d level = %v, want %v", i, e.Level, wantLvl[i])
		}
	}
}

// Check must drop entries below the enabled level (the promoted default Core
// would bypass this wrapper's Write and skip redaction). zap.Logger
// short-circuits before Check when the level is disabled, so the contract is
// exercised directly on the Core: only the enabled path registers the
// redacting core, observable through redaction on the subsequent write.
func TestRedactCoreCheckDropsDisabledLevels(t *testing.T) {
	core, logs := observer.New(zapcore.InfoLevel)
	c := redactCore{core}

	// Disabled level: nothing registered — Check returns nil and the write
	// is a silent no-op.
	ent := zapcore.Entry{Level: zapcore.DebugLevel, Message: "hidden"}
	if ce := c.Check(ent, nil); ce != nil {
		t.Fatal("disabled entry must not produce a checked entry")
	}

	// Enabled level: the redacting core is registered, so the write is both
	// delivered and redacted.
	ent.Level = zapcore.InfoLevel
	ce := c.Check(ent, nil)
	if ce == nil {
		t.Fatal("enabled entry must produce a checked entry")
	}
	ce.Write(zap.String("token", "leak-me"))
	entries := logs.All()
	if len(entries) != 1 {
		t.Fatalf("enabled entry must be written exactly once, got %d", len(entries))
	}
	if len(entries[0].Context) != 1 || entries[0].Context[0].String != "***" {
		t.Fatalf("write must pass through the redacting core, got %+v", entries[0].Context)
	}
}
