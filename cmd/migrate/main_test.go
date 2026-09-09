package main

import (
	"context"
	"testing"
	"time"
)

func TestParseMigrationTimeout(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		want    time.Duration
		wantErr bool
	}{
		{name: "default", want: defaultMigrationTimeout},
		{name: "explicit", value: "90m", want: 90 * time.Minute},
		{name: "disabled", value: "0", want: 0},
		{name: "trimmed", value: " 45m ", want: 45 * time.Minute},
		{name: "negative", value: "-1s", wantErr: true},
		{name: "invalid", value: "tomorrow", wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := parseMigrationTimeout(test.value)
			if (err != nil) != test.wantErr {
				t.Fatalf("parseMigrationTimeout(%q) error = %v", test.value, err)
			}
			if got != test.want {
				t.Fatalf("parseMigrationTimeout(%q) = %s, want %s", test.value, got, test.want)
			}
		})
	}
}

func TestOperationContextSupportsOperatorManagedDeadline(t *testing.T) {
	ctx, cancel := operationContext(context.Background(), 0)
	if _, ok := ctx.Deadline(); ok {
		cancel()
		t.Fatal("zero migration timeout unexpectedly installed a deadline")
	}
	cancel()
	select {
	case <-ctx.Done():
	default:
		t.Fatal("cancel did not stop deadline-free migration context")
	}

	ctx, cancel = operationContext(context.Background(), time.Minute)
	defer cancel()
	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("positive migration timeout did not install a deadline")
	}
	remaining := time.Until(deadline)
	if remaining <= 59*time.Second || remaining > time.Minute {
		t.Fatalf("migration deadline remaining = %s, want approximately 1m", remaining)
	}
}
