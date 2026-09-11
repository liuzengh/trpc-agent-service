package main

import (
	"errors"
	"fmt"
	"syscall"
	"testing"
)

func TestStartupFailureCategory(t *testing.T) {
	cases := map[error]string{
		fmt.Errorf("listen: %w", syscall.EADDRINUSE):   "address_in_use",
		fmt.Errorf("listen: %w", syscall.EACCES):       "permission_denied",
		errors.New("provider detail must stay hidden"): "initialization_failed",
	}
	for err, want := range cases {
		if got := startupFailureCategory(err); got != want {
			t.Fatalf("startupFailureCategory(%v)=%q, want %q", err, got, want)
		}
	}
}
