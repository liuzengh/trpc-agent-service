package governance

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/control"
	"github.com/liuzengh/trpc-agent-service/trpcservice/message"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type fakeControlRepository struct {
	policy control.TenantPolicy
	err    error
}

func TestExecutionErrorCaptureKeepsFirstError(t *testing.T) {
	ctx, captured := WithExecutionErrorCapture(context.Background())
	captureExecutionError(ctx, ErrToolForbidden)
	captureExecutionError(ctx, ErrPolicyUnavailable)
	if !errors.Is(captured(), ErrToolForbidden) {
		t.Fatalf("captured=%v", captured())
	}
}

type recordingAuditSink struct {
	records []control.AuditRecord
}

func (s *recordingAuditSink) Emit(_ context.Context, record control.AuditRecord) {
	s.records = append(s.records, record)
}

type confirmationRepository struct {
	*fakeControlRepository
	consumeErr error
}

type refreshControlRepository struct {
	*fakeControlRepository
	mu       sync.Mutex
	policies map[string]control.TenantPolicy
	errors   map[string]error
	calls    map[string]int
	started  chan struct{}
	gate     <-chan struct{}
}

func newRefreshControlRepository(policies ...control.TenantPolicy) *refreshControlRepository {
	result := &refreshControlRepository{
		fakeControlRepository: &fakeControlRepository{}, policies: make(map[string]control.TenantPolicy),
		errors: make(map[string]error), calls: make(map[string]int),
	}
	for _, policy := range policies {
		result.policies[policy.TenantID] = policy
	}
	return result
}

func (r *refreshControlRepository) GetPolicy(ctx context.Context, tenantID string) (control.TenantPolicy, error) {
	r.mu.Lock()
	r.calls[tenantID]++
	policy := r.policies[tenantID]
	err := r.errors[tenantID]
	started := r.started
	gate := r.gate
	r.mu.Unlock()
	if started != nil {
		select {
		case started <- struct{}{}:
		default:
		}
	}
	if gate != nil {
		select {
		case <-ctx.Done():
			return control.TenantPolicy{}, ctx.Err()
		case <-gate:
		}
	}
	return policy, err
}

func (r *refreshControlRepository) setPolicy(policy control.TenantPolicy) {
	r.mu.Lock()
	r.policies[policy.TenantID] = policy
	delete(r.errors, policy.TenantID)
	r.mu.Unlock()
}

func (r *refreshControlRepository) setError(tenantID string, err error) {
	r.mu.Lock()
	r.errors[tenantID] = err
	r.mu.Unlock()
}

func (r *refreshControlRepository) callCount(tenantID string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls[tenantID]
}

func (r *confirmationRepository) ConsumeConfirmation(context.Context, control.Confirmation) error {
	return r.consumeErr
}

func (f *fakeControlRepository) Ready(context.Context) error { return nil }
func (f *fakeControlRepository) InitializeTenants(context.Context, []tenant.Tenant, []string) error {
	return nil
}
func (f *fakeControlRepository) GetPolicy(context.Context, string) (control.TenantPolicy, error) {
	return f.policy, f.err
}
func (f *fakeControlRepository) PutPolicy(context.Context, control.TenantPolicy, int64) (control.TenantPolicy, error) {
	return control.TenantPolicy{}, nil
}
func (f *fakeControlRepository) GetPlacement(context.Context, string) (control.TenantPlacement, error) {
	return control.TenantPlacement{}, nil
}
func (f *fakeControlRepository) PutPlacement(context.Context, control.TenantPlacement, int64) (control.TenantPlacement, error) {
	return control.TenantPlacement{}, nil
}
func (f *fakeControlRepository) RegisterNode(context.Context, control.NodeRecord) error { return nil }
func (f *fakeControlRepository) HeartbeatNode(context.Context, string, string, control.NodeState, int, time.Time) error {
	return nil
}
func (f *fakeControlRepository) GetNode(context.Context, string) (control.NodeRecord, error) {
	return control.NodeRecord{}, nil
}
func (f *fakeControlRepository) ListNodes(context.Context) ([]control.NodeRecord, error) {
	return nil, nil
}
func (f *fakeControlRepository) CreateAssignment(context.Context, control.NodeAssignment) (control.NodeAssignment, bool, error) {
	return control.NodeAssignment{}, false, nil
}
func (f *fakeControlRepository) GetAssignment(context.Context, string) (control.NodeAssignment, error) {
	return control.NodeAssignment{}, nil
}
func (f *fakeControlRepository) ListAssignments(context.Context) ([]control.NodeAssignment, error) {
	return nil, nil
}
func (f *fakeControlRepository) PutAssignment(context.Context, control.NodeAssignment, int64) (control.NodeAssignment, error) {
	return control.NodeAssignment{}, nil
}
func (f *fakeControlRepository) AppendAudit(context.Context, control.AuditRecord) error { return nil }
func (f *fakeControlRepository) QueryAudit(context.Context, control.AuditQuery) ([]control.AuditRecord, string, error) {
	return nil, "", nil
}
func (f *fakeControlRepository) AppendMetric(context.Context, control.MetricEvent) error { return nil }
func (f *fakeControlRepository) QueryMetrics(context.Context, control.MetricQuery) ([]control.MetricEvent, string, error) {
	return nil, "", nil
}
func (f *fakeControlRepository) SetTenantDegraded(context.Context, string, string) error { return nil }
func (f *fakeControlRepository) ClearTenantDegraded(context.Context, string) error       { return nil }
func (f *fakeControlRepository) CreateConfirmation(context.Context, control.Confirmation, time.Duration) error {
	return nil
}
func (f *fakeControlRepository) ApproveConfirmation(context.Context, string, string, string, string) (control.Confirmation, error) {
	return control.Confirmation{}, nil
}
func (f *fakeControlRepository) ConsumeConfirmation(context.Context, control.Confirmation) error {
	return nil
}
func (f *fakeControlRepository) AcquireLease(context.Context, string, string, time.Duration) (bool, error) {
	return true, nil
}
func (f *fakeControlRepository) RenewLease(context.Context, string, string, time.Duration) (bool, error) {
	return true, nil
}
func (f *fakeControlRepository) ReleaseLease(context.Context, string, string) error { return nil }
func (f *fakeControlRepository) Close() error                                       { return nil }

func TestAuthorizeActorUsesHMACAndEmptyAllowlist(t *testing.T) {
	policy := control.TenantPolicy{TenantID: "tenant-a", Revision: 1, CreatedAt: time.Now(), UpdatedAt: time.Now(), ActorAllowlistHashes: []string{ActorHash([]byte("secret"), "actor-a")}}
	if err := AuthorizeActor(policy, []byte("secret"), "actor-a"); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(AuthorizeActor(policy, []byte("secret"), "actor-b"), ErrActorForbidden) {
		t.Fatal("unauthorized actor was allowed")
	}
	policy.ActorAllowlistHashes = nil
	if err := AuthorizeActor(policy, []byte("secret"), "actor-b"); err != nil {
		t.Fatal(err)
	}
}

func TestPolicyCacheUsesUnexpiredSnapshotOnControlPlaneFailure(t *testing.T) {
	now := time.Now().UTC()
	repo := newRefreshControlRepository(control.DefaultTenantPolicy("tenant-a", nil, now))
	cache, err := NewPolicyCache(repo, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	cache.clock = func() time.Time { return now }
	if _, err := cache.Snapshot(context.Background(), "tenant-a"); err != nil {
		t.Fatal(err)
	}
	repo.setError("tenant-a", control.ErrUnavailable)
	cache.clock = func() time.Time { return now.Add(30 * time.Second) }
	if _, err := cache.Snapshot(context.Background(), "tenant-a"); err != nil {
		t.Fatal(err)
	}
	waitForPolicyRefresh(t, cache, "tenant-a")
	cache.clock = func() time.Time { return now.Add(2 * time.Minute) }
	if !errors.Is(func() error { _, e := cache.Snapshot(context.Background(), "tenant-a"); return e }(), ErrPolicyUnavailable) {
		t.Fatal("expired cache did not fail closed")
	}
}

func TestPolicyCacheReturnsCurrentSnapshotAndRefreshesOnceInBackground(t *testing.T) {
	now := time.Now().UTC()
	initial := control.DefaultTenantPolicy("tenant-a", nil, now)
	repo := newRefreshControlRepository(initial)
	cache, err := NewPolicyCache(repo, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	cache.clock = func() time.Time { return now }
	if snapshot, err := cache.Snapshot(context.Background(), "tenant-a"); err != nil || snapshot.Policy.Revision != 1 {
		t.Fatalf("initial Snapshot = (%#v, %v)", snapshot, err)
	}
	updated := initial
	updated.Revision = 2
	updated.UpdatedAt = now.Add(time.Second)
	repo.setPolicy(updated)
	started := make(chan struct{}, 1)
	gate := make(chan struct{})
	repo.mu.Lock()
	repo.started = started
	repo.gate = gate
	repo.mu.Unlock()
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	snapshot, err := cache.Snapshot(cancelled, "tenant-a")
	if err != nil || snapshot.Policy.Revision != 1 {
		t.Fatalf("fresh Snapshot = (%#v, %v)", snapshot, err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("background refresh did not start")
	}
	for index := 0; index < 16; index++ {
		if _, err := cache.Snapshot(context.Background(), "tenant-a"); err != nil {
			t.Fatal(err)
		}
	}
	if calls := repo.callCount("tenant-a"); calls != 2 {
		t.Fatalf("GetPolicy calls during refresh = %d, want 2", calls)
	}
	close(gate)
	waitForPolicyRefresh(t, cache, "tenant-a")
	cache.mu.RLock()
	refreshed := cache.entries["tenant-a"]
	cache.mu.RUnlock()
	if refreshed.Policy.Revision != 2 {
		t.Fatalf("refreshed revision = %d", refreshed.Policy.Revision)
	}
}

func TestPolicyCacheMissingPolicyInvalidatesOnlyTargetTenant(t *testing.T) {
	now := time.Now().UTC()
	repo := newRefreshControlRepository(
		control.DefaultTenantPolicy("tenant-a", nil, now),
		control.DefaultTenantPolicy("tenant-b", nil, now),
	)
	cache, _ := NewPolicyCache(repo, time.Minute)
	cache.clock = func() time.Time { return now }
	if err := cache.Warm(context.Background(), []string{"tenant-a", "tenant-b"}); err != nil {
		t.Fatal(err)
	}
	repo.setError("tenant-a", control.ErrPolicyMissing)
	if _, err := cache.Snapshot(context.Background(), "tenant-a"); err != nil {
		t.Fatal(err)
	}
	waitForPolicyRefresh(t, cache, "tenant-a")
	if _, err := cache.Snapshot(context.Background(), "tenant-a"); !errors.Is(err, ErrPolicyMissing) {
		t.Fatalf("missing tenant Snapshot = %v", err)
	}
	if snapshot, err := cache.Snapshot(context.Background(), "tenant-b"); err != nil || snapshot.Policy.TenantID != "tenant-b" {
		t.Fatalf("unaffected tenant Snapshot = (%#v, %v)", snapshot, err)
	}
}

func waitForPolicyRefresh(t *testing.T, cache *PolicyCache, tenantID string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		cache.mu.RLock()
		_, refreshing := cache.inflight[tenantID]
		cache.mu.RUnlock()
		if !refreshing {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("policy refresh did not finish")
}

func TestPolicyEnforcer(t *testing.T) {
	now := time.Now().UTC()
	repo := &fakeControlRepository{policy: control.DefaultTenantPolicy("tenant-a", nil, now)}
	cache, _ := NewPolicyCache(repo, time.Minute)
	enforcer := NewPolicyEnforcer(cache, []byte("secret"), repo)
	task := message.ExecutionTask{TenantID: "tenant-a", ActorUserID: "actor-a"}
	if err := enforcer.AuthorizeTask(context.Background(), task); err != nil {
		t.Fatal(err)
	}
}

func TestPolicyEnforcerAuditsActorAndPolicyRejections(t *testing.T) {
	now := time.Now().UTC()
	policy := control.DefaultTenantPolicy("tenant-a", nil, now)
	policy.ActorAllowlistHashes = []string{ActorHash([]byte("secret"), "allowed")}
	repo := &fakeControlRepository{policy: policy}
	cache, _ := NewPolicyCache(repo, time.Minute)
	enforcer := NewPolicyEnforcer(cache, []byte("secret"), repo)
	sink := &recordingAuditSink{}
	enforcer.SetAuditSink(sink)
	task := message.ExecutionTask{TaskID: "task-a", TenantID: "tenant-a", AgentAppID: "app-a", ActorUserID: "denied", SessionID: "session-a", TraceID: "trace-a", RequestID: "request-a"}
	if err := enforcer.AuthorizeTask(context.Background(), task); !errors.Is(err, ErrActorForbidden) {
		t.Fatalf("actor authorization = %v", err)
	}
	if len(sink.records) != 1 || sink.records[0].EventType != "actor_authorization" || sink.records[0].Decision != "denied" || sink.records[0].ErrorType != "actor_forbidden" {
		t.Fatalf("actor audit = %#v", sink.records)
	}

	cache.Invalidate("tenant-a")
	repo.err = control.ErrPolicyMissing
	if err := enforcer.AuthorizeTask(context.Background(), task); !errors.Is(err, ErrPolicyMissing) {
		t.Fatalf("policy authorization = %v", err)
	}
	if len(sink.records) != 2 || sink.records[1].EventType != "policy_authorization" || sink.records[1].Decision != "denied" || sink.records[1].ErrorType != "tenant_policy_missing" {
		t.Fatalf("policy audit = %#v", sink.records)
	}
}

func TestPolicyEnforcerWorkerRecheckAuditsOnlyFailures(t *testing.T) {
	now := time.Now().UTC()
	policy := control.DefaultTenantPolicy("tenant-a", nil, now)
	repo := &fakeControlRepository{policy: policy}
	cache, _ := NewPolicyCache(repo, time.Minute)
	enforcer := NewPolicyEnforcer(cache, []byte("secret"), repo)
	sink := &recordingAuditSink{}
	enforcer.SetAuditSink(sink)
	task := message.ExecutionTask{TaskID: "task-a", TenantID: "tenant-a", AgentAppID: "app-a", ActorUserID: "allowed", SessionID: "session-a", TraceID: "trace-a", RequestID: "request-a"}

	if err := enforcer.ReauthorizeTask(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	if len(sink.records) != 0 {
		t.Fatalf("successful worker recheck emitted audit: %#v", sink.records)
	}

	policy.ActorAllowlistHashes = []string{ActorHash([]byte("secret"), "other")}
	policy.Revision++
	policy.UpdatedAt = now.Add(time.Second)
	repo.policy = policy
	cache.Invalidate("tenant-a")
	if err := enforcer.ReauthorizeTask(context.Background(), task); !errors.Is(err, ErrActorForbidden) {
		t.Fatalf("worker actor recheck = %v", err)
	}
	if len(sink.records) != 1 || sink.records[0].EventType != "worker_actor_authorization" || sink.records[0].Decision != "denied" || sink.records[0].ErrorType != "actor_forbidden" {
		t.Fatalf("worker actor audit = %#v", sink.records)
	}

	cache.Invalidate("tenant-a")
	repo.err = control.ErrUnavailable
	if err := enforcer.ReauthorizeTask(context.Background(), task); !errors.Is(err, ErrPolicyUnavailable) {
		t.Fatalf("worker policy recheck = %v", err)
	}
	if len(sink.records) != 2 || sink.records[1].EventType != "worker_policy_authorization" || sink.records[1].Decision != "denied" || sink.records[1].ErrorType != "tenant_policy_unavailable" {
		t.Fatalf("worker policy audit = %#v", sink.records)
	}
}

func TestAuthorizeToolCallAuditsConfirmationLifecycle(t *testing.T) {
	now := time.Now().UTC()
	policy := control.DefaultTenantPolicy("tenant-a", nil, now)
	policy.ToolAllowlist = []string{"phase6.dangerous_echo"}
	policy.DangerousTools = []string{"phase6.dangerous_echo"}
	repo := &confirmationRepository{fakeControlRepository: &fakeControlRepository{}}
	manager, err := NewConfirmationManager(repo, time.Minute, []byte("secret"))
	if err != nil {
		t.Fatal(err)
	}
	sink := &recordingAuditSink{}
	task := message.ExecutionTask{TaskID: "task-a", TenantID: "tenant-a", AgentAppID: "app-a", ActorUserID: "actor-a", SessionID: "session-a", TraceID: "trace-a", RequestID: "request-a"}
	ctx := WithPolicy(context.Background(), PolicySnapshot{Policy: policy, LoadedAt: now, ExpiresAt: now.Add(time.Minute)})
	ctx = WithConfirmationManager(ctx, manager, ActorHash([]byte("secret"), task.ActorUserID), task.SessionID)
	ctx = WithTaskAudit(ctx, sink, task, []byte("secret"))

	nonce, err := AuthorizeToolCall(ctx, "phase6.dangerous_echo", []byte(`{"value":"hello"}`))
	if err != nil || nonce == "" || len(sink.records) != 1 || sink.records[0].EventType != "confirmation_required" {
		t.Fatalf("required = (%q, %v, %#v)", nonce, err, sink.records)
	}
	actorHash := ActorHash([]byte("secret"), task.ActorUserID)
	argsDigest := sha256.Sum256([]byte(`{"value":"hello"}`))
	key := confirmationKey(task.TenantID, actorHash, task.SessionID, "phase6.dangerous_echo", hex.EncodeToString(argsDigest[:]))
	manager.mu.Lock()
	approved := manager.pending[key]
	approved.State = "approved"
	manager.pending[key] = approved
	manager.mu.Unlock()
	if nonce, err = AuthorizeToolCall(ctx, "phase6.dangerous_echo", []byte(`{"value":"hello"}`)); err != nil || nonce != "" || sink.records[len(sink.records)-1].EventType != "allowed" {
		t.Fatalf("allowed = (%q, %v, %#v)", nonce, err, sink.records)
	}

	manager.mu.Lock()
	manager.pending[key] = control.Confirmation{TenantID: task.TenantID, ActorUserIDHash: actorHash, SessionID: task.SessionID, ToolName: "phase6.dangerous_echo", ArgsDigest: hex.EncodeToString(argsDigest[:]), ExpiresAt: now.Add(time.Minute)}
	manager.mu.Unlock()
	repo.consumeErr = control.ErrConfirmationMismatch
	if _, err = AuthorizeToolCall(ctx, "phase6.dangerous_echo", []byte(`{"value":"hello"}`)); !errors.Is(err, control.ErrConfirmationMismatch) || sink.records[len(sink.records)-1].EventType != "denied" {
		t.Fatalf("denied = (%v, %#v)", err, sink.records)
	}

	manager.mu.Lock()
	manager.pending[key] = control.Confirmation{TenantID: task.TenantID, ActorUserIDHash: actorHash, SessionID: task.SessionID, ToolName: "phase6.dangerous_echo", ArgsDigest: hex.EncodeToString(argsDigest[:]), ExpiresAt: now.Add(-time.Second)}
	manager.mu.Unlock()
	repo.consumeErr = nil
	if nonce, err = AuthorizeToolCall(ctx, "phase6.dangerous_echo", []byte(`{"value":"hello"}`)); err != nil || nonce == "" {
		t.Fatalf("expired replacement = (%q, %v)", nonce, err)
	}
	last := sink.records[len(sink.records)-2:]
	if last[0].EventType != "expired" || last[1].EventType != "confirmation_required" {
		t.Fatalf("expired audit = %#v", last)
	}
}
