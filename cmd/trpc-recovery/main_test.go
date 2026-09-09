package main

import (
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/recovery"
)

func TestExitCodeMappingIsStable(t *testing.T) {
	cases := map[string]int{
		"invalid_config":             2,
		"dependency_unavailable":     3,
		"integrity_mismatch":         4,
		"forbidden_state":            5,
		"replay_integrity_violation": 6,
		"restore_failed":             7,
		"backup_failed":              8,
		"timeout_or_cancelled":       9,
	}
	for category, want := range cases {
		if got := exitCodeOf(category); got != want {
			t.Fatalf("exit code for %s: got %d want %d", category, got, want)
		}
	}
	if got := exitCodeOf("anything_else"); got != 2 {
		t.Fatalf("unknown category must map to invalid config: %d", got)
	}
}

func TestCategoryOfMapsSentinels(t *testing.T) {
	cases := []struct {
		err      error
		category string
	}{
		{nil, "ok"},
		{recovery.ErrInvalidConfig, "invalid_config"},
		{recovery.ErrDependencyUnavailable, "dependency_unavailable"},
		{recovery.ErrIntegrityMismatch, "integrity_mismatch"},
		{recovery.ErrForbiddenState, "forbidden_state"},
		{recovery.ErrReplayViolation, "replay_integrity_violation"},
		{recovery.ErrRestoreFailed, "restore_failed"},
		{recovery.ErrBackupFailed, "backup_failed"},
		{recovery.ErrTimeoutOrCancelled, "timeout_or_cancelled"},
		{errors.Join(recovery.ErrForbiddenState, errors.New("context")), "forbidden_state"},
	}
	for _, c := range cases {
		if got := categoryOf(c.err); got != c.category {
			t.Fatalf("category of %v: got %s want %s", c.err, got, c.category)
		}
	}
}

func TestParseTimeoutBounds(t *testing.T) {
	if _, err := parseTimeout(""); err != nil {
		t.Fatalf("default timeout rejected: %v", err)
	}
	if got, _ := parseTimeout("45"); got != 45*time.Second {
		t.Fatalf("seconds parse: %v", got)
	}
	if got, _ := parseTimeout("90s"); got != 90*time.Second {
		t.Fatalf("duration parse: %v", got)
	}
	for _, bad := range []string{"0", "7201", "abc", "7201s"} {
		if _, err := parseTimeout(bad); err == nil {
			t.Fatalf("timeout %q accepted", bad)
		}
	}
}
