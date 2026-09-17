package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	profilememory "github.com/liuzengh/trpc-agent-service/trpcservice/profile/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	sessionmemory "github.com/liuzengh/trpc-agent-service/trpcservice/storage/session/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker/mockmodel"
)

type taskStub struct{ envelope runtime.ExecutionEnvelope }

func (s taskStub) PrepareDispatch(context.Context, gateway.PrepareDispatchRequest) (gateway.PreparedDispatch, error) {
	panic("unexpected call")
}

func TestWorkerRedeliveryAfterCommitDoesNotRepeatModelEffect(t *testing.T) {
	envelope := runtime.ExecutionEnvelope{
		SchemaVersion: 1, TenantID: "tenant-a", TenantVersion: 1, AgentAppID: "app", AgentAppVersion: 1,
		AgentAppRevision: 1, AgentContentDigest: "digest", ConfigVersion: 1, PolicyVersion: 1,
		RequestID: "request", SessionID: "session", UserID: "user", Channel: "fake", InputSeq: 1,
		PayloadRef: "payload://request", CreatedAt: time.Now().UTC(),
	}
	key := profile.ExecutionProfileKey{TenantID: envelope.TenantID, TenantVersion: envelope.TenantVersion, AgentAppID: envelope.AgentAppID, AgentAppVersion: envelope.AgentAppVersion, AgentAppRevision: envelope.AgentAppRevision, ContentDigest: envelope.AgentContentDigest, ConfigVersion: envelope.ConfigVersion, PolicyVersion: envelope.PolicyVersion}
	snapshot := profile.ExecutionProfileSnapshot{Key: key, TenantVersion: envelope.TenantVersion, AgentAppVersion: envelope.AgentAppVersion, ContentDigest: envelope.AgentContentDigest}
	model := mockmodel.New()
	executor := DeterministicTestExecutor{Tasks: taskStub{envelope: envelope}, Profiles: profilememory.NewResolver(snapshot), Sessions: sessionmemory.New(), Model: model}
	for attempt := 0; attempt < 2; attempt++ {
		if err := executor.ExecuteWithLease(context.Background(), envelope, 1, func(context.Context) error { return nil }); err != nil {
			t.Fatalf("attempt %d: %v", attempt, err)
		}
	}
	if calls := model.Calls(envelope.TenantID, envelope.RequestID); calls != 1 {
		t.Fatalf("model calls=%d", calls)
	}
}
func (s taskStub) GetExecution(context.Context, gateway.ExecutionKey) (gateway.ExecutionStatus, error) {
	return gateway.ExecutionStatus{Envelope: s.envelope}, nil
}
func (s taskStub) RequestCancel(context.Context, gateway.CancelRequest) (gateway.CancelResult, error) {
	panic("unexpected call")
}
func (s taskStub) ParkInput(context.Context, gateway.ParkRequest) (gateway.ParkResult, error) {
	panic("unexpected call")
}

func TestWorkerRejectsEnvelopeVersionForgery(t *testing.T) {
	authoritative := runtime.ExecutionEnvelope{SchemaVersion: 1, TenantID: "tenant-a", TenantVersion: 1, AgentAppID: "app", AgentAppVersion: 1, AgentAppRevision: 1, AgentContentDigest: "digest", ConfigVersion: 1, PolicyVersion: 1, RequestID: "request", SessionID: "session", UserID: "user", Channel: "fake", InputSeq: 1, PayloadRef: "payload://request", CreatedAt: time.Now().UTC()}
	forged := authoritative
	forged.ConfigVersion = 2
	err := (DeterministicTestExecutor{Tasks: taskStub{envelope: authoritative}}).Execute(context.Background(), forged)
	if !errors.Is(err, runtime.ErrVersionMismatch) {
		t.Fatalf("got %v", err)
	}
}
