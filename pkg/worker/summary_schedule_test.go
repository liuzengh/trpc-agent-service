package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/fence"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/storage"
	summarycoord "trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/summary"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

type summarySessionService struct {
	session.Service
	value *session.Session
	err   error
}

func (s summarySessionService) GetSession(context.Context, session.Key, ...session.Option) (*session.Session, error) {
	if s.err != nil {
		return nil, s.err
	}
	if s.value == nil {
		return nil, nil
	}
	return s.value.Clone(), nil
}

func TestWorkerBuildsExactVersionedSummarySchedule(t *testing.T) {
	value := session.NewSession("physical-app", "owner-1", "session-1")
	incarnation := "00000000-0000-4000-8000-000000000001"
	value.SetState(storage.SessionIncarnationStateKey, []byte(incarnation))
	value.Events = []event.Event{{ID: "event-1"}, {ID: "event-2"}, {ID: "event-3"}}
	w := &Worker{
		tenant:         testTenantForSummary("tenant-a"),
		summaryEnabled: true,
		sessionService: summarySessionService{value: value},
		appName:        "physical-app",
		agentAppID:     "app-1",
		versionID:      "version-1",
	}
	request := &Request{TenantID: "tenant-a", SessionOwnerID: "owner-1", SessionID: "session-1"}
	schedule := w.buildSummarySchedule(context.Background(), request)
	if schedule == nil {
		t.Fatal("summary schedule is nil")
	}
	if err := schedule.Validate(); err != nil {
		t.Fatalf("schedule validation: %v", err)
	}
	if schedule.Key != (summarycoord.Key{TenantID: "tenant-a", AgentAppID: "app-1", SessionOwnerID: "owner-1", SessionID: "session-1", SessionIncarnationID: incarnation}) ||
		schedule.AgentVersionID != "version-1" || schedule.TargetEventSequence != 3 {
		t.Fatalf("summary schedule=%#v", schedule)
	}
}

func TestWorkerSummaryScheduleFallsBackToDeferredTarget(t *testing.T) {
	w := &Worker{
		tenant:         testTenantForSummary("tenant-a"),
		summaryEnabled: true,
		sessionService: summarySessionService{err: errors.New("session backend unavailable")},
		appName:        "physical-app",
		agentAppID:     "app-1",
		versionID:      "version-1",
	}
	schedule := w.buildSummarySchedule(context.Background(), &Request{
		TenantID: "tenant-a", SessionOwnerID: "owner-1", SessionID: "session-1",
	})
	if schedule == nil || schedule.TargetEventSequence != 0 {
		t.Fatalf("deferred schedule=%#v", schedule)
	}
	if err := schedule.Validate(); err != nil {
		t.Fatalf("deferred schedule validation: %v", err)
	}
}

func testTenantForSummary(id string) *tenant.Tenant {
	return &tenant.Tenant{ID: id}
}

type summaryScheduleFence struct{}

func (summaryScheduleFence) Acquire(context.Context, fence.Token) (func() error, error) {
	return func() error { return nil }, nil
}

func TestWorkerDeferredReceiptRetainsInvocationIncarnationAfterReadFailure(t *testing.T) {
	mini := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mini.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	key := session.Key{AppName: "tsa1:8:tenant-a:support", UserID: "owner-1", SessionID: "session-1"}
	lease, err := storage.NewSessionLockManager(client).AcquireLease(context.Background(), storage.SessionInvocationLeaseKey("tenant-a", key), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lease.Release(context.Background()) })
	ctx := fence.WithToken(context.Background(), fence.Token{TenantID: "tenant-a", AgentAppID: "app-1", AgentAppName: "support",
		ScopedAppName: key.AppName, UserID: key.UserID, SessionID: key.SessionID, ExecutionID: 1, Generation: 1, Value: "test-fence"})
	ctx = storage.ContextWithSessionLease(ctx, key, lease)
	service, err := storage.NewStrictFencedSessionService(inmemory.NewSessionService(), summaryScheduleFence{}, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	created, err := service.CreateSession(ctx, key, nil)
	if err != nil {
		t.Fatal(err)
	}
	incarnation, err := storage.SessionIncarnationID(created)
	if err != nil || incarnation == "" {
		t.Fatalf("incarnation=%q err=%v", incarnation, err)
	}
	w := &Worker{tenant: testTenantForSummary("tenant-a"), strictScope: true, summaryEnabled: true,
		sessionService: summarySessionService{err: errors.New("bounded receipt read unavailable")},
		appName:        key.AppName, agentAppID: "app-1", versionID: "version-1"}
	request := &Request{TenantID: "tenant-a", SessionOwnerID: key.UserID, SessionID: key.SessionID}
	schedule := w.buildSummarySchedule(ctx, request)
	if schedule == nil || schedule.TargetEventSequence != 0 || schedule.SessionIncarnationID != incarnation {
		t.Fatalf("deferred receipt lost incarnation: %#v", schedule)
	}
	if schedule := w.buildSummarySchedule(context.Background(), request); schedule != nil {
		t.Fatalf("strict worker emitted an unbound summary receipt: %#v", schedule)
	}
}
