package agent

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

type recordingSessionLeaseStore struct {
	*storage.MemoryStateStore
	renewed chan storage.SessionExecutionLease
	err     error
}

func (s *recordingSessionLeaseStore) RenewSessionExecutionLease(_ context.Context, lease storage.SessionExecutionLease, ttl time.Duration) (storage.SessionExecutionLease, error) {
	select {
	case s.renewed <- lease:
	default:
	}
	if s.err != nil {
		return storage.SessionExecutionLease{}, s.err
	}
	lease.LeaseUntil = time.Now().Add(ttl)
	return lease, nil
}

func TestSessionLeaseHeartbeatRenewsAndStops(t *testing.T) {
	store := &recordingSessionLeaseStore{MemoryStateStore: storage.NewMemoryStateStore(), renewed: make(chan storage.SessionExecutionLease, 1)}
	lease := storage.SessionExecutionLease{TenantID: "tenant-a", SessionKey: "tenant-a/support/session/1", OwnerID: "worker-1", FencingToken: 7}
	heartbeat := startSessionLeaseHeartbeat(context.Background(), store, lease, 3*time.Second)

	select {
	case renewed := <-store.renewed:
		if renewed.FencingToken != lease.FencingToken {
			t.Fatalf("renewed fencing token = %d, want %d", renewed.FencingToken, lease.FencingToken)
		}
	case <-time.After(1500 * time.Millisecond):
		t.Fatal("session lease was not renewed")
	}

	deadline := time.Now().Add(time.Second)
	for {
		got := heartbeat.Value()
		if got.FencingToken == lease.FencingToken && !got.LeaseUntil.IsZero() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("heartbeat lease = %+v", got)
		}
		time.Sleep(time.Millisecond)
	}
	if err := heartbeat.Err(); err != nil {
		t.Fatalf("heartbeat error = %v", err)
	}
	heartbeat.Stop()
}

func TestSessionLeaseHeartbeatCancelsOnRenewFailure(t *testing.T) {
	renewErr := errors.New("lease backend unavailable")
	store := &recordingSessionLeaseStore{MemoryStateStore: storage.NewMemoryStateStore(), renewed: make(chan storage.SessionExecutionLease, 1), err: renewErr}
	heartbeat := startSessionLeaseHeartbeat(context.Background(), store, storage.SessionExecutionLease{
		TenantID: "tenant-a", SessionKey: "tenant-a/support/session/2", OwnerID: "worker-2", FencingToken: 8,
	}, 3*time.Second)

	select {
	case <-heartbeat.Context().Done():
	case <-time.After(1500 * time.Millisecond):
		t.Fatal("heartbeat context was not cancelled after renewal failure")
	}
	if !errors.Is(heartbeat.Err(), renewErr) {
		t.Fatalf("heartbeat error = %v, want %v", heartbeat.Err(), renewErr)
	}
	heartbeat.Stop()
}

func TestNilSessionLeaseHeartbeatAccessorsAreSafe(t *testing.T) {
	var heartbeat *sessionLeaseHeartbeat
	if heartbeat.Err() != nil {
		t.Fatal("nil heartbeat Err() must be nil")
	}
	if got := heartbeat.Value(); got != (storage.SessionExecutionLease{}) {
		t.Fatalf("nil heartbeat Value() = %+v", got)
	}
	heartbeat.Stop()
}
