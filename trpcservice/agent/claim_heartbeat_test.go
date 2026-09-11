package agent

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

type renewFailingClaimStore struct {
	renewed chan struct{}
}

func (s *renewFailingClaimStore) Begin(context.Context, string, string, string, string, string, string, time.Duration) (storage.BeginResult, error) {
	return storage.ExecutionFresh, nil
}
func (s *renewFailingClaimStore) Abort(context.Context, string, string, string, string, string) error {
	return nil
}
func (s *renewFailingClaimStore) Fail(context.Context, string, string, string, string, string) error {
	return nil
}
func (s *renewFailingClaimStore) Renew(context.Context, string, string, string, string, string) error {
	select {
	case <-s.renewed:
	default:
		close(s.renewed)
	}
	return storage.ErrLeaseLost
}

func TestClaimHeartbeatCancelsExecutionWhenRenewFenceIsLost(t *testing.T) {
	store := &renewFailingClaimStore{renewed: make(chan struct{})}
	heartbeat := startClaimHeartbeat(context.Background(), store, "tenant-a", "web", "web-console", "message-1", "trace-1", time.Second)
	defer heartbeat.Stop()
	select {
	case <-heartbeat.Context().Done():
	case <-time.After(2 * time.Second):
		t.Fatal("heartbeat did not cancel execution after renewal failure")
	}
	if !errors.Is(heartbeat.Err(), storage.ErrLeaseLost) {
		t.Fatalf("heartbeat error = %v, want ErrLeaseLost", heartbeat.Err())
	}
}
