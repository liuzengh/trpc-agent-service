package main

import (
	"context"
	"testing"
)

func TestConfigPubEnabledIsDisabledByDefault(t *testing.T) {
	t.Setenv("CONFIGPUB_ENABLED", "")
	enabled, err := configPubEnabled()
	if err != nil || enabled {
		t.Fatalf("default enabled=%v err=%v", enabled, err)
	}
	composition, err := newConfigComposition(nil, nil)
	if err != nil || composition != nil {
		t.Fatalf("disabled composition=%v err=%v", composition, err)
	}
}

func TestConfigPubEnabledRejectsInvalidBoolean(t *testing.T) {
	t.Setenv("CONFIGPUB_ENABLED", "maybe")
	if enabled, err := configPubEnabled(); err == nil || enabled {
		t.Fatalf("invalid value enabled=%v err=%v", enabled, err)
	}
}

func TestConfigPubEnabledRequiresPoolWhenEnabled(t *testing.T) {
	t.Setenv("CONFIGPUB_ENABLED", "true")
	if composition, err := newConfigComposition(nil, nil); err == nil || composition != nil {
		t.Fatalf("nil pool composition=%v err=%v", composition, err)
	}
}

func TestConfigPubTelemetryNilRuntimeIsNoop(t *testing.T) {
	adapter := configpubTelemetry{}
	ctx, end := adapter.StartConfigSpan(context.Background(), "read")
	if ctx == nil {
		t.Fatalf("nil runtime changed context to nil")
	}
	end("committed")
	adapter.ConfigOperation("read", "hit")
	adapter.Event(context.Background(), 4, "event", "component", "read", "hit")
}
