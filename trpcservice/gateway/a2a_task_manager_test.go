package gateway_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"trpc.group/trpc-go/trpc-a2a-go/protocol"
	"trpc.group/trpc-go/trpc-a2a-go/taskmanager"
)

type a2aTaskStoreStub struct{ status gateway.ExecutionStatus }

func (s *a2aTaskStoreStub) GetExecution(_ context.Context, key gateway.ExecutionKey) (gateway.ExecutionStatus, error) {
	if key.TenantID != s.status.Envelope.TenantID || key.RequestID != s.status.Envelope.RequestID {
		return gateway.ExecutionStatus{}, runtime.ErrNotFound
	}
	return s.status, nil
}

func (s *a2aTaskStoreStub) RequestCancel(context.Context, gateway.CancelRequest) (gateway.CancelResult, error) {
	return gateway.CancelResult{}, runtime.ErrCommitConflict
}

func TestDurableA2ATaskManagerReplaysTerminalArtifactAndStatus(t *testing.T) {
	fixture := newRunFixture()
	status := gateway.ExecutionStatus{Envelope: runtime.ExecutionEnvelope{TenantID: "tenant-a", RequestID: "request-1",
		AgentAppID: fixture.route.Tenant.AgentAppID, UserID: fixture.route.UserID, SessionID: fixture.route.SessionID,
		CreatedAt: time.Unix(1_800_000_000, 0).UTC()}, Outcome: runtime.OutcomeSucceeded, Version: 3}
	manager := &gateway.DurableA2ATaskManager{Submitter: fixture.submitter, Tasks: &a2aTaskStoreStub{status: status},
		Events: &terminalBridgeStore{}, Readiness: readinessStub{ready: true}, PollInterval: time.Millisecond}
	trusted := gateway.ServerInvocationContext{Tenant: fixture.route.Tenant, PrincipalID: "principal-a", UserID: fixture.route.UserID,
		SessionID: fixture.route.SessionID, Protocol: "a2a", IdempotencyKey: "key", CanRead: true}
	ctx := gateway.WithServerInvocationContext(context.Background(), trusted)
	events, err := manager.OnResubscribe(ctx, protocol.TaskIDParams{ID: "request-1"})
	if err != nil {
		t.Fatal(err)
	}
	var initial, artifact, final bool
	for value := range events {
		switch result := value.Result.(type) {
		case *protocol.Task:
			initial = result.Status.State == protocol.TaskStateCompleted && result.ContextID == fixture.route.SessionID && len(result.Artifacts) == 1
		case *protocol.TaskArtifactUpdateEvent:
			artifact = result.ContextID == fixture.route.SessionID && result.Artifact.Parts[0].(*protocol.TextPart).Text == "done"
		case *protocol.TaskStatusUpdateEvent:
			final = result.Final && result.Status.State == protocol.TaskStateCompleted
		}
	}
	if !initial || !artifact || !final {
		t.Fatalf("initial=%v artifact=%v final=%v", initial, artifact, final)
	}
	otherApp := trusted
	otherApp.Tenant.AgentAppID = "other-app"
	if _, err := manager.OnGetTask(gateway.WithServerInvocationContext(context.Background(), otherApp), protocol.TaskQueryParams{ID: "request-1"}); !errors.Is(err, gateway.ErrForbidden) {
		t.Fatalf("cross-app task read err=%v", err)
	}
}

func TestDurableA2ATaskManagerEnforcesPerOperationPermission(t *testing.T) {
	fixture := newRunFixture()
	manager := &gateway.DurableA2ATaskManager{Submitter: fixture.submitter, Tasks: fixture.tasks, Events: &terminalBridgeStore{},
		Readiness: readinessStub{ready: true}}
	trusted := gateway.ServerInvocationContext{Tenant: fixture.route.Tenant, PrincipalID: "principal-a", UserID: fixture.route.UserID,
		SessionID: fixture.route.SessionID, Protocol: "a2a", IdempotencyKey: "key", CanRun: true}
	ctx := gateway.WithServerInvocationContext(context.Background(), trusted)
	if _, err := manager.OnGetTask(ctx, protocol.TaskQueryParams{ID: "missing"}); !errors.Is(err, gateway.ErrForbidden) {
		t.Fatalf("read without permission err=%v", err)
	}
	if _, err := manager.OnPushNotificationSet(ctx, protocol.TaskPushNotificationConfig{}); !errors.Is(err, taskmanager.ErrPushNotificationNotSupportedSentinel) {
		t.Fatalf("push notification err=%v", err)
	}
}
