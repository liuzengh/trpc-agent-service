package configpub

import (
	"context"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func testTenantContext(tenantID string, version int64) tenant.TenantContext {
	return tenant.TenantContext{
		TenantID: tenantID, AgentAppID: "agent-1", BindingID: "binding-1", Channel: tenant.ChannelLark,
		RequestID: "request-1", MessageID: "message-1", TraceID: "trace-1",
		ConfigVersion: version, BackendPolicy: tenant.BackendPolicy{Session: "postgres", Memory: "postgres", Vector: "none", Object: "none"},
	}
}

type fakeTelemetry struct {
	spans   []string
	metrics []string
	events  []string
}

func (f *fakeTelemetry) StartConfigSpan(ctx context.Context, operation string) (context.Context, func(string)) {
	f.spans = append(f.spans, operation)
	return ctx, func(outcome string) {}
}

func (f *fakeTelemetry) ConfigOperation(operation, outcome string) {
	f.metrics = append(f.metrics, operation+"/"+outcome)
}

func (f *fakeTelemetry) Event(ctx context.Context, level int, event, component, operation, outcome string, fields ...string) {
	f.events = append(f.events, event+"/"+outcome)
}

func newTestCoordinator() (*Coordinator, *fakeRepository, *fakeTelemetry) {
	repo := newFakeRepository()
	tel := &fakeTelemetry{}
	coordinator, _ := NewCoordinator(repo, repo, tel)
	return coordinator, repo, tel
}

// happyPath validates the full lifecycle: create -> validate -> publish ->
// assignment -> rollback.
func TestCoordinatorHappyPath(t *testing.T) {
	co, _, tel := newTestCoordinator()
	ctx := context.Background()
	tc := testTenantContext("tenant-a", 1)

	if _, err := co.CreateRevision(ctx, tc, CreateRevisionInput{Version: 5, Document: validDocument(), ActorCategory: "operator"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := co.ValidateRevision(ctx, tc, 5, "operator"); err != nil {
		t.Fatalf("validate: %v", err)
	}
	record, err := co.Publish(ctx, tc, PublishInput{Version: 5, ExpectedActiveVersion: 0, Percentage: 100, OperationID: "op-publish-1", ActorCategory: "operator"})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if record.Outcome != OutcomeCommitted || record.ResultActiveVersion != 5 {
		t.Fatalf("unexpected publish record: %+v", record)
	}
	state, managed, err := co.RolloutState(ctx, "tenant-a")
	if err != nil || !managed || state.ActiveVersion != 5 || state.Percentage != 100 {
		t.Fatalf("unexpected rollout state: %+v managed=%v err=%v", state, managed, err)
	}
	bucket, _ := AssignmentBucket("tenant-a")
	version, ok := state.Assign(bucket)
	if !ok || version != 5 {
		t.Fatalf("assignment must serve the active revision: %d %v", version, ok)
	}

	rb, err := co.Rollback(ctx, tc, RollbackInput{TargetVersion: 5, ExpectedActiveVersion: 5, OperationID: "operation-rb-0", ActorCategory: "operator"})
	_ = rb
	if err == nil {
		t.Fatalf("rollback to the active revision itself must fail")
	}

	// Publish v6, then roll back to v5.
	v6doc := validDocument()
	v6doc.Agent.ToolPolicyRef = "v6-policy"
	if _, err := co.CreateRevision(ctx, tc, CreateRevisionInput{Version: 6, Document: v6doc, ActorCategory: "operator"}); err != nil {
		t.Fatalf("create v6: %v", err)
	}
	if _, err := co.ValidateRevision(ctx, tc, 6, "operator"); err != nil {
		t.Fatalf("validate v6: %v", err)
	}
	if _, err := co.Publish(ctx, tc, PublishInput{Version: 6, ExpectedActiveVersion: 5, Percentage: 100, OperationID: "op-publish-2", ActorCategory: "operator"}); err != nil {
		t.Fatalf("publish v6: %v", err)
	}
	if _, err := co.Rollback(ctx, tc, RollbackInput{TargetVersion: 5, ExpectedActiveVersion: 6, OperationID: "operation-rb-1", ActorCategory: "operator"}); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	state, _, _ = co.RolloutState(ctx, "tenant-a")
	if state.ActiveVersion != 5 || state.Percentage != 100 || state.BaselineVersion != 5 {
		t.Fatalf("post-rollback rollout state wrong: %+v", state)
	}
	snapshot, err := co.Snapshot(ctx, "tenant-a", 6)
	if err != nil || !snapshot.Status.RuntimeReadable() {
		t.Fatalf("recalled revision must remain readable: %+v err=%v", snapshot, err)
	}
	if _, err := co.CreateRevision(ctx, tc, CreateRevisionInput{Version: 7, Document: v6doc, ActorCategory: "operator"}); !errors.Is(err, ErrDuplicateContent) {
		t.Fatalf("duplicate content must be rejected, got %v", err)
	}
	if len(tel.metrics) == 0 || len(tel.spans) == 0 {
		t.Fatalf("telemetry seam must observe operations")
	}
}

func TestCoordinatorIdempotentReplay(t *testing.T) {
	co, _, _ := newTestCoordinator()
	ctx := context.Background()
	tc := testTenantContext("tenant-a", 1)
	_, _ = co.CreateRevision(ctx, tc, CreateRevisionInput{Version: 5, Document: validDocument(), ActorCategory: "operator"})
	_, _ = co.ValidateRevision(ctx, tc, 5, "operator")
	first, err := co.Publish(ctx, tc, PublishInput{Version: 5, ExpectedActiveVersion: 0, Percentage: 100, OperationID: "operation-1", ActorCategory: "operator"})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	replay, err := co.Publish(ctx, tc, PublishInput{Version: 5, ExpectedActiveVersion: 0, Percentage: 100, OperationID: "operation-1", ActorCategory: "operator"})
	if err != nil {
		t.Fatalf("replay must converge: %v", err)
	}
	if replay.OperationID != first.OperationID || replay.ResultActiveVersion != first.ResultActiveVersion {
		t.Fatalf("replay diverged: %+v vs %+v", replay, first)
	}
	// Same key with a different payload must conflict.
	if _, err := co.Publish(ctx, tc, PublishInput{Version: 5, ExpectedActiveVersion: 0, Percentage: 50, OperationID: "operation-1", ActorCategory: "operator"}); !errors.Is(err, ErrOperationMismatch) {
		t.Fatalf("key reuse with different payload must fail, got %v", err)
	}
}

func TestCoordinatorStaleExpectedVersion(t *testing.T) {
	co, _, _ := newTestCoordinator()
	ctx := context.Background()
	tc := testTenantContext("tenant-a", 1)
	_, _ = co.CreateRevision(ctx, tc, CreateRevisionInput{Version: 5, Document: validDocument(), ActorCategory: "operator"})
	_, _ = co.CreateRevision(ctx, tc, CreateRevisionInput{Version: 6, Document: validDocument(), ActorCategory: "operator"})
	_ = validDocument()
	different := validDocument()
	different.Agent.ToolPolicyRef = "strict"
	_, _ = co.CreateRevision(ctx, tc, CreateRevisionInput{Version: 6, Document: different, ActorCategory: "operator"})
	_ = tc
	ctx2 := context.Background()
	tcA := testTenantContext("tenant-a", 1)
	_, _ = co.CreateRevision(ctx2, tcA, CreateRevisionInput{Version: 5, Document: validDocument(), ActorCategory: "operator"})
	_, _ = co.ValidateRevision(ctx2, tcA, 5, "operator")
	other := validDocument()
	other.Agent.ToolPolicyRef = "strict"
	_, _ = co.CreateRevision(ctx2, tcA, CreateRevisionInput{Version: 6, Document: other, ActorCategory: "operator"})
	_, _ = co.ValidateRevision(ctx2, tcA, 6, "operator")
	if _, err := co.Publish(ctx2, tcA, PublishInput{Version: 5, ExpectedActiveVersion: 0, Percentage: 100, OperationID: "operation-a", ActorCategory: "operator"}); err != nil {
		t.Fatalf("publish v5: %v", err)
	}
	// Publisher B expected no active revision; the CAS must reject it.
	if _, err := co.Publish(ctx2, tcA, PublishInput{Version: 6, ExpectedActiveVersion: 0, Percentage: 100, OperationID: "operation-b", ActorCategory: "operator"}); !errors.Is(err, ErrStaleExpectedVersion) {
		t.Fatalf("stale CAS must be rejected, got %v", err)
	}
	// With the correct expected version it succeeds.
	if _, err := co.Publish(ctx2, tcA, PublishInput{Version: 6, ExpectedActiveVersion: 5, Percentage: 100, OperationID: "operation-c", ActorCategory: "operator"}); err != nil {
		t.Fatalf("publish v6 with fresh CAS: %v", err)
	}
	// Rolling back with a stale expectation is rejected.
	if _, err := co.Rollback(ctx2, tcA, RollbackInput{TargetVersion: 5, ExpectedActiveVersion: 5, OperationID: "operation-d", ActorCategory: "operator"}); !errors.Is(err, ErrStaleExpectedVersion) {
		t.Fatalf("stale rollback must be rejected, got %v", err)
	}
}

func TestCoordinatorStateTransitions(t *testing.T) {
	co, _, _ := newTestCoordinator()
	ctx := context.Background()
	tc := testTenantContext("tenant-a", 1)

	// Publishing a draft must fail (never validated).
	_, _ = co.CreateRevision(ctx, tc, CreateRevisionInput{Version: 5, Document: validDocument(), ActorCategory: "operator"})
	if _, err := co.Publish(ctx, tc, PublishInput{Version: 5, ExpectedActiveVersion: 0, Percentage: 100, OperationID: "operation-draft", ActorCategory: "operator"}); !errors.Is(err, ErrRevisionState) {
		t.Fatalf("publishing a draft must fail, got %v", err)
	}
	// Partial rollout without an active revision must fail.
	_, _ = co.ValidateRevision(ctx, tc, 5, "operator")
	if _, err := co.Publish(ctx, tc, PublishInput{Version: 5, ExpectedActiveVersion: 0, Percentage: 50, OperationID: "operation-partial", ActorCategory: "operator"}); !errors.Is(err, ErrInvalidRollout) {
		t.Fatalf("partial first publish must fail, got %v", err)
	}
	if _, err := co.Publish(ctx, tc, PublishInput{Version: 5, ExpectedActiveVersion: 0, Percentage: 100, OperationID: "operation-full", ActorCategory: "operator"}); err != nil {
		t.Fatalf("full publish: %v", err)
	}
	// Re-publishing an already published revision must fail (no-op transition).
	if _, err := co.Publish(ctx, tc, PublishInput{Version: 5, ExpectedActiveVersion: 5, Percentage: 100, OperationID: "operation-again", ActorCategory: "operator"}); !errors.Is(err, ErrRevisionState) {
		t.Fatalf("re-publish must fail, got %v", err)
	}
	// Percentage progression on the active revision works.
	if _, err := co.SetRollout(ctx, tc, RolloutInput{Version: 5, Percentage: 30, ExpectedActiveVersion: 5, OperationID: "operation-rollout", ActorCategory: "operator"}); err != nil {
		t.Fatalf("set rollout: %v", err)
	}
	state, _, _ := co.RolloutState(ctx, "tenant-a")
	if state.Percentage != 30 {
		t.Fatalf("percentage not applied: %+v", state)
	}
	// Rollback to a never-published (draft) revision must fail.
	draftDoc := validDocument()
	draftDoc.Agent.ToolPolicyRef = "draft-policy"
	_, _ = co.CreateRevision(ctx, tc, CreateRevisionInput{Version: 8, Document: draftDoc, ActorCategory: "operator"})
	if _, err := co.Rollback(ctx, tc, RollbackInput{TargetVersion: 8, ExpectedActiveVersion: 5, OperationID: "operation-rb-draft", ActorCategory: "operator"}); !errors.Is(err, ErrRevisionState) {
		t.Fatalf("rollback to draft must fail, got %v", err)
	}
}

func TestCoordinatorValidationRejection(t *testing.T) {
	co, repo, _ := newTestCoordinator()
	ctx := context.Background()
	tc := testTenantContext("tenant-a", 1)
	bad := validDocument()
	bad.BackendPolicy.Vector = "milvus" // forbidden production dependency
	if _, err := co.CreateRevision(ctx, tc, CreateRevisionInput{Version: 5, Document: bad, ActorCategory: "operator"}); err == nil {
		// syntactic validation at create rejects it before it becomes a draft
		t.Fatalf("vector-enabled document must be rejected at creation")
	}
	good := validDocument()
	if _, err := co.CreateRevision(ctx, tc, CreateRevisionInput{Version: 5, Document: good, ActorCategory: "operator"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	// Cross-tenant binding reference fails validation and records rejection.
	repo.ownedHooks["lark|binding-1"] = BindingForeignTenant
	revision, err := co.ValidateRevision(ctx, tc, 5, "operator")
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	if revision.Status != StatusRejected || revision.ReasonCategory != CategoryCrossTenantRef {
		t.Fatalf("expected rejected with cross-tenant category, got %+v", revision)
	}
	// Unknown binding reference is distinguished.
	repo.ownedHooks["lark|binding-1"] = BindingMissing
	second := good
	second.Agent.ToolPolicyRef = "unknown-ref-test"
	if _, err := co.CreateRevision(ctx, tc, CreateRevisionInput{Version: 6, Document: second, ActorCategory: "operator"}); err != nil {
		t.Fatalf("create v6: %v", err)
	}
	revision, err = co.ValidateRevision(ctx, tc, 6, "operator")
	if err != nil {
		t.Fatalf("validate v6: %v", err)
	}
	if revision.Status != StatusRejected || revision.ReasonCategory != CategoryUnknownRef {
		t.Fatalf("expected unknown-reference rejection, got %+v", revision)
	}
	// A rejected revision can never serve runtime traffic.
	if _, err := co.Snapshot(ctx, "tenant-a", 5); !errors.Is(err, ErrRevisionState) {
		t.Fatalf("rejected revision must not serve, got %v", err)
	}
}

func TestCoordinatorTenantIsolation(t *testing.T) {
	co, _, _ := newTestCoordinator()
	ctx := context.Background()
	tcA := testTenantContext("tenant-a", 1)
	tcB := testTenantContext("tenant-b", 1)
	_, _ = co.CreateRevision(ctx, tcA, CreateRevisionInput{Version: 5, Document: validDocument(), ActorCategory: "operator"})
	_, _ = co.ValidateRevision(ctx, tcA, 5, "operator")
	if _, err := co.Publish(ctx, tcA, PublishInput{Version: 5, ExpectedActiveVersion: 0, Percentage: 100, OperationID: "operation-a", ActorCategory: "operator"}); err != nil {
		t.Fatalf("publish tenant-a: %v", err)
	}
	// Tenant B sees no rollout state and no revisions.
	if _, managed, err := co.RolloutState(ctx, "tenant-b"); err != nil || managed {
		t.Fatalf("tenant-b must be unmanaged: %v %v", managed, err)
	}
	if _, err := co.Snapshot(ctx, "tenant-b", 5); !errors.Is(err, ErrRevisionNotFound) {
		t.Fatalf("cross-tenant snapshot read must fail, got %v", err)
	}
	// Publishing tenant B's own revision must not observe tenant A's active.
	if _, err := co.CreateRevision(ctx, tcB, CreateRevisionInput{Version: 5, Document: validDocument(), ActorCategory: "operator"}); err != nil {
		t.Fatalf("create tenant-b: %v", err)
	}
	_, _ = co.ValidateRevision(ctx, tcB, 5, "operator")
	if _, err := co.Publish(ctx, tcB, PublishInput{Version: 5, ExpectedActiveVersion: 0, Percentage: 100, OperationID: "operation-b", ActorCategory: "operator"}); err != nil {
		t.Fatalf("tenant-b independent publish must succeed: %v", err)
	}
}

func TestCoordinatorInvalidInputs(t *testing.T) {
	co, _, _ := newTestCoordinator()
	ctx := context.Background()
	tc := testTenantContext("tenant-a", 1)
	if _, err := co.CreateRevision(ctx, tc, CreateRevisionInput{Version: 0, Document: validDocument(), ActorCategory: "operator"}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("version 0 must fail, got %v", err)
	}
	if _, err := co.CreateRevision(ctx, tc, CreateRevisionInput{Version: 5, Document: validDocument(), ActorCategory: "hacker"}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("unknown actor must fail, got %v", err)
	}
	if _, err := co.Publish(ctx, tc, PublishInput{Version: 5, Percentage: 101, OperationID: "op-x-long-enough", ActorCategory: "operator"}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("percentage > 100 must fail, got %v", err)
	}
	if _, err := co.Publish(ctx, tc, PublishInput{Version: 5, Percentage: 100, OperationID: "short", ActorCategory: "operator"}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("short operation id must fail, got %v", err)
	}
	badTc := testTenantContext("tenant-a", 1)
	badTc.TenantID = ""
	if _, err := co.CreateRevision(ctx, badTc, CreateRevisionInput{Version: 5, Document: validDocument(), ActorCategory: "operator"}); !errors.Is(err, ErrTenantMismatch) {
		t.Fatalf("invalid tenant context must fail, got %v", err)
	}
}

func TestCoordinatorUnavailableFailsClosed(t *testing.T) {
	co, repo, _ := newTestCoordinator()
	ctx := context.Background()
	tc := testTenantContext("tenant-a", 1)
	repo.failNext = ErrUnavailable
	if _, err := co.CreateRevision(ctx, tc, CreateRevisionInput{Version: 5, Document: validDocument(), ActorCategory: "operator"}); !isUnavailable(err) {
		t.Fatalf("unavailable must propagate fail-closed, got %v", err)
	}
}

func TestSnapshotRequiresReadableStatus(t *testing.T) {
	co, _, _ := newTestCoordinator()
	ctx := context.Background()
	tc := testTenantContext("tenant-a", 1)
	if _, err := co.Snapshot(ctx, "tenant-a", 9); !errors.Is(err, ErrRevisionNotFound) {
		t.Fatalf("missing snapshot must be not-found, got %v", err)
	}
	_, _ = co.CreateRevision(ctx, tc, CreateRevisionInput{Version: 5, Document: validDocument(), ActorCategory: "operator"})
	if _, err := co.Snapshot(ctx, "tenant-a", 5); !errors.Is(err, ErrRevisionState) {
		t.Fatalf("draft snapshot must fail closed, got %v", err)
	}
}
