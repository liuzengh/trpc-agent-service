package controlplane

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestPostgresResolverRejectsMissingDatabaseWithoutPanic(t *testing.T) {
	tests := []struct {
		name string
		call func(*PostgresResolver) error
	}{
		{
			name: "resolve active",
			call: func(resolver *PostgresResolver) error {
				_, err := resolver.Resolve(context.Background(), "tenant-a", "support", "session-a")
				return err
			},
		},
		{
			name: "resolve pinned",
			call: func(resolver *PostgresResolver) error {
				_, err := resolver.ResolvePinned(context.Background(), "tenant-a", "support", "session-a", "inbox:1")
				return err
			},
		},
		{
			name: "resolve pinned with payload",
			call: func(resolver *PostgresResolver) error {
				_, err := resolver.ResolvePinnedWithPayload(
					context.Background(), "tenant-a", "support", "session-a", "inbox:1",
					strings.Repeat("a", 64),
				)
				return err
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for _, resolver := range []*PostgresResolver{nil, NewPostgresResolver(nil)} {
				if err := test.call(resolver); err == nil || !strings.Contains(err.Error(), "database is not configured") {
					t.Fatalf("resolver error = %v, want missing database", err)
				}
			}
		})
	}
}

func TestExecutionHeartbeatRejectsUnconfiguredRecorder(t *testing.T) {
	recorder := NewExecutionRecorder(nil)
	err := recorder.RunHeartbeat(context.Background(), ExecutionHandle{ID: 1, Token: "token"}, time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "recorder is not configured") {
		t.Fatalf("heartbeat error = %v, want unconfigured recorder", err)
	}
}
