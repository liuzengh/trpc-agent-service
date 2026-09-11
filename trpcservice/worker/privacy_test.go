package worker

import (
	"context"
	"errors"
	"github.com/cyl6/trpc-agent-service/trpcservice/config"
	"github.com/cyl6/trpc-agent-service/trpcservice/domain"
	"github.com/cyl6/trpc-agent-service/trpcservice/privacy"
	"strings"
	"testing"
	"time"
	"trpc.group/trpc-go/trpc-agent-go/event"
)

func TestTenantInputPrivacyAndLegacyIsolation(t *testing.T) {
	service, cleanup := newTestService(nil)
	defer cleanup()
	tenant := testTenant("private-tenant")
	tenant.Privacy = config.PrivacyPolicy{Input: "redact", Output: "redact"}
	task := taskFor(tenant, "first", "user", "conversation", domain.ScopeDirect)
	task.Message.Text = "alice@example.com 13800138000"
	result, err := service.Process(context.Background(), task)
	if err != nil || strings.Contains(result.Text, "alice@example.com") || strings.Contains(result.Text, "13800138000") || !strings.Contains(result.Text, "[REDACTED]") {
		t.Fatalf("private reply=%q err=%v", result.Text, err)
	}
	task.Tenant = testTenant("legacy-tenant")
	task.Message.TenantID = task.Tenant.TenantID
	result, err = service.Process(context.Background(), task)
	if err != nil || !strings.Contains(result.Text, "alice@example.com") {
		t.Fatalf("privacy crossed tenant boundary: %v", err)
	}
}
func TestOutputDLPBeforeCanonicalCommitAndReplay(t *testing.T) {
	for _, mode := range []string{"redact", "block"} {
		t.Run(mode, func(t *testing.T) {
			runner := &strictTestRunner{run: func(context.Context) (<-chan *event.Event, error) {
				return strictCompletionEvents("email alice@example.com", 1, 1), nil
			}}
			turn := &strictTestTurn{}
			service, _, _ := newStrictTestService(t, runner, turn, nil, nil, nil, time.Second)
			task := strictTask(false)
			task.Tenant.Privacy.Output = mode
			result, err := service.Process(context.Background(), task)
			if mode == "block" {
				if !errors.Is(err, privacy.ErrBlocked) {
					t.Fatalf("block=%v", err)
				}
				if commits, _ := turn.counts(); commits != 0 {
					t.Fatal("blocked output committed")
				}
				var failure *ProcessFailure
				if !errors.As(err, &failure) || failure.disposition != ProcessTerminalIgnored {
					t.Fatal("blocked output retries model")
				}
			} else {
				if err != nil || strings.Contains(result.Text, "alice@example.com") {
					t.Fatalf("reply=%q err=%v", result.Text, err)
				}
				if strings.Contains(string(turn.lastCommit), "alice@example.com") {
					t.Fatal("unredacted canonical outbox payload")
				}
				replay, err := service.Process(context.Background(), task)
				if err != nil || replay.Text != result.Text {
					t.Fatalf("unsafe replay: %v", err)
				}
				if calls, _ := runner.snapshot(); calls != 1 {
					t.Fatal("replay regenerated model output")
				}
			}
		})
	}
}
