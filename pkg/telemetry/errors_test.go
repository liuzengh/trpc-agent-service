package telemetry

import (
	"context"
	"errors"
	"testing"
)

func TestStableErrorCodeNeverReturnsErrorText(t *testing.T) {
	secret := "postgres://user:password@example.invalid/db?token=live"
	tests := []struct {
		name string
		err  error
		want string
	}{
		{name: "nil", want: "none"},
		{name: "canceled", err: context.Canceled, want: "canceled"},
		{name: "deadline", err: context.DeadlineExceeded, want: "deadline_exceeded"},
		{name: "provider", err: errors.New(secret), want: "internal_error"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := StableErrorCode(test.err); got != test.want {
				t.Fatalf("StableErrorCode() = %q, want %q", got, test.want)
			}
			if got := StableErrorCode(test.err); got == secret {
				t.Fatalf("error text leaked into stable code: %q", got)
			}
		})
	}
}
