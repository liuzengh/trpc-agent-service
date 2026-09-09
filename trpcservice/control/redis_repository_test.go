package control

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type countingRepository struct {
	Repository
	calls     atomic.Int32
	failFirst bool
}

func (r *countingRepository) InitializeTenants(ctx context.Context, tenants []tenant.Tenant, tools []string) error {
	call := r.calls.Add(1)
	if r.failFirst && call == 1 {
		return ErrUnavailable
	}
	return r.Repository.InitializeTenants(ctx, tenants, tools)
}

func TestInitializingRepositoryRunsInitializationOnceAndRetriesFailure(t *testing.T) {
	raw, _ := newTestRepository(t)
	counting := &countingRepository{Repository: raw, failFirst: true}
	tenantList := []tenant.Tenant{{ID: "tenant-a", Enabled: true}}
	repository := NewInitializingRepository(counting, tenantList, []string{"safe.tool"})
	if err := repository.Ready(context.Background()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("first Ready = %v", err)
	}
	if err := repository.Ready(context.Background()); err != nil {
		t.Fatalf("retry Ready = %v", err)
	}
	var group sync.WaitGroup
	for index := 0; index < 16; index++ {
		group.Add(1)
		go func() {
			defer group.Done()
			if err := repository.Ready(context.Background()); err != nil {
				t.Errorf("concurrent Ready = %v", err)
			}
		}()
	}
	group.Wait()
	if calls := counting.calls.Load(); calls != 2 {
		t.Fatalf("InitializeTenants calls = %d, want 2", calls)
	}
}

func TestInitializingRepositoryPolicyLossIsTenantLocalAfterStartup(t *testing.T) {
	raw, _ := newTestRepository(t)
	repository := NewInitializingRepository(raw, []tenant.Tenant{{ID: "tenant-a", Enabled: true}, {ID: "tenant-b", Enabled: true}}, []string{"safe.tool"})
	ctx := context.Background()
	if err := repository.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	if err := raw.client.Del(ctx, raw.policyKey("tenant-a")).Err(); err != nil {
		t.Fatal(err)
	}
	if err := repository.Ready(ctx); err != nil {
		t.Fatalf("global Ready after tenant policy loss = %v", err)
	}
	if _, err := repository.GetPolicy(ctx, "tenant-a"); !errors.Is(err, ErrPolicyMissing) {
		t.Fatalf("lost tenant policy = %v", err)
	}
	if _, err := repository.GetPolicy(ctx, "tenant-b"); err != nil {
		t.Fatalf("unaffected tenant policy = %v", err)
	}
}

func TestRedisRepositoryInitializesPolicyAndPlacementWithoutTTL(t *testing.T) {
	repository, redisServer := newTestRepository(t)
	ctx := context.Background()
	tenants := []tenant.Tenant{{ID: "tenant-a", Enabled: true}, {ID: "tenant-off", Enabled: false}}
	if err := repository.InitializeTenants(ctx, tenants, []string{"tool.beta", "tool.alpha", "tool.alpha"}); err != nil {
		t.Fatal(err)
	}
	if err := repository.InitializeTenants(ctx, tenants, []string{"changed-default"}); err != nil {
		t.Fatal(err)
	}
	policy, err := repository.GetPolicy(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(policy.ToolAllowlist) != "[tool.alpha tool.beta]" || policy.Revision != 1 {
		t.Fatalf("unexpected default policy: %#v", policy)
	}
	placement, err := repository.GetPlacement(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if placement.Mode != PlacementShared || placement.NodeID != "" || placement.Revision != 1 {
		t.Fatalf("unexpected default placement: %#v", placement)
	}
	for _, key := range []string{repository.policyKey("tenant-a"), repository.placementKey("tenant-a")} {
		if ttl := redisServer.TTL(key); ttl != 0 {
			t.Fatalf("%s TTL = %s, want no expiry", key, ttl)
		}
	}
	if redisServer.Exists(repository.policyKey("tenant-off")) {
		t.Fatal("disabled tenant control state was initialized")
	}
	if err := repository.client.Del(ctx, repository.policyKey("tenant-a")).Err(); err != nil {
		t.Fatal(err)
	}
	if err := repository.InitializeTenants(ctx, tenants, nil); !errors.Is(err, ErrPolicyMissing) {
		t.Fatalf("InitializeTenants after policy loss = %v", err)
	}
	if _, err := repository.GetPolicy(ctx, "tenant-a"); !errors.Is(err, ErrPolicyMissing) {
		t.Fatalf("GetPolicy after loss = %v", err)
	}
}

func TestRedisRepositoryPolicyAndPlacementCAS(t *testing.T) {
	repository, _ := newTestRepository(t)
	ctx := context.Background()
	if err := repository.InitializeTenants(ctx, []tenant.Tenant{{ID: "tenant-a", Enabled: true}}, []string{"safe.tool"}); err != nil {
		t.Fatal(err)
	}
	policy, _ := repository.GetPolicy(ctx, "tenant-a")
	policy.RedactionPatterns = []string{"secret-[0-9]+"}
	updated, err := repository.PutPolicy(ctx, policy, 1)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Revision != 2 {
		t.Fatalf("policy revision = %d", updated.Revision)
	}
	if _, err := repository.PutPolicy(ctx, policy, 1); !errors.Is(err, ErrRevisionConflict) {
		t.Fatalf("stale policy update = %v", err)
	}
	updated.RedactionPatterns = nil
	if _, err := repository.PutPolicy(ctx, updated, 2); !errors.Is(err, ErrRevisionConflict) {
		t.Fatalf("system accepted redaction pattern removal: %v", err)
	}
	placement, _ := repository.GetPlacement(ctx, "tenant-a")
	placement.Mode = PlacementDedicated
	placement.NodeID = "worker-a"
	placement, err = repository.PutPlacement(ctx, placement, 1)
	if err != nil || placement.Revision != 2 {
		t.Fatalf("dedicated placement update = (%#v, %v)", placement, err)
	}
	placement.Mode = PlacementShared
	if _, err := repository.PutPlacement(ctx, placement, 2); err == nil {
		t.Fatal("shared placement retained node_id")
	}
}

func TestRedisRepositoryNodeLeaseAndOfflineState(t *testing.T) {
	repository, redisServer := newTestRepository(t)
	ctx := context.Background()
	now := time.Now().UTC()
	node := NodeRecord{
		NodeID: "worker-a", Role: "worker", State: NodeReady, Capacity: 1,
		BuildVersion: "v6", ProtocolCapabilities: []string{"governance.v1"}, BootID: "boot-a",
		LeaseUntil: now.Add(time.Minute), StartedAt: now, LastHeartbeat: now,
	}
	if err := repository.RegisterNode(ctx, node); err != nil {
		t.Fatal(err)
	}
	conflict := node
	conflict.BootID = "boot-b"
	if err := repository.RegisterNode(ctx, conflict); !errors.Is(err, ErrNodeConflict) {
		t.Fatalf("duplicate live node = %v", err)
	}
	if err := repository.HeartbeatNode(ctx, node.NodeID, node.BootID, NodeDraining, 0, time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	got, err := repository.GetNode(ctx, node.NodeID)
	if err != nil || got.State != NodeDraining {
		t.Fatalf("heartbeat node = (%#v, %v)", got, err)
	}
	redisServer.FastForward(2 * time.Minute)
	got, err = repository.GetNode(ctx, node.NodeID)
	if err != nil || got.State != NodeOffline {
		t.Fatalf("expired node = (%#v, %v)", got, err)
	}
}

func TestRedisRepositoryAssignmentIdempotencyAndCAS(t *testing.T) {
	repository, _ := newTestRepository(t)
	ctx := context.Background()
	now := time.Now().UTC()
	assignment := NodeAssignment{
		InboxID: "inbox-a", TenantID: "tenant-a", AgentAppID: "agent-a",
		PayloadDigest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", NodeID: "worker-a", Mode: PlacementShared,
		State: AssignmentPlanned, Revision: 1, CreatedAt: now, UpdatedAt: now,
	}
	created, wasCreated, err := repository.CreateAssignment(ctx, assignment)
	if err != nil || !wasCreated || created.Revision != 1 {
		t.Fatalf("create assignment = (%#v, %t, %v)", created, wasCreated, err)
	}
	existing, wasCreated, err := repository.CreateAssignment(ctx, assignment)
	if err != nil || wasCreated || existing.NodeID != "worker-a" {
		t.Fatalf("repeat assignment = (%#v, %t, %v)", existing, wasCreated, err)
	}
	conflict := assignment
	conflict.PayloadDigest = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	if _, _, err := repository.CreateAssignment(ctx, conflict); !errors.Is(err, ErrAssignmentConflict) {
		t.Fatalf("digest conflict = %v", err)
	}
	assignment.State = AssignmentAdmitted
	updated, err := repository.PutAssignment(ctx, assignment, 1)
	if err != nil || updated.Revision != 2 {
		t.Fatalf("update assignment = (%#v, %v)", updated, err)
	}
	if _, err := repository.PutAssignment(ctx, assignment, 1); !errors.Is(err, ErrRevisionConflict) {
		t.Fatalf("stale assignment update = %v", err)
	}
}

func TestRedisRepositoryAuditMetricsConfirmationAndLease(t *testing.T) {
	repository, redisServer := newTestRepository(t)
	ctx := context.Background()
	now := time.Now().UTC()
	for index := 0; index < 3; index++ {
		record := AuditRecord{ID: fmt.Sprintf("audit-%d", index), TenantID: "tenant-a", EventType: "tool", Decision: "allowed", Sequence: index, OccurredAt: now.Add(time.Duration(index) * time.Millisecond)}
		if err := repository.AppendAudit(ctx, record); err != nil {
			t.Fatal(err)
		}
		event := MetricEvent{ID: fmt.Sprintf("metric-%d", index), TenantID: "tenant-a", Name: "requests", Value: 1, Count: 1, OccurredAt: now.Add(time.Duration(index) * time.Millisecond), Labels: map[string]string{"channel": "demo"}}
		if err := repository.AppendMetric(ctx, event); err != nil {
			t.Fatal(err)
		}
	}
	first, cursor, err := repository.QueryAudit(ctx, AuditQuery{TenantID: "tenant-a", To: now.Add(time.Second), Limit: 2})
	if err != nil || len(first) != 2 || cursor == "" {
		t.Fatalf("first audit page = (%d, %q, %v)", len(first), cursor, err)
	}
	second, next, err := repository.QueryAudit(ctx, AuditQuery{TenantID: "tenant-a", To: now.Add(time.Second), Limit: 2, Cursor: cursor})
	if err != nil || len(second) != 1 || next != "" {
		t.Fatalf("second audit page = (%d, %q, %v)", len(second), next, err)
	}
	metrics, _, err := repository.QueryMetrics(ctx, MetricQuery{TenantID: "tenant-a", To: now.Add(time.Second), Limit: 10})
	if err != nil || len(metrics) != 3 {
		t.Fatalf("metric query = (%d, %v)", len(metrics), err)
	}
	confirmation := Confirmation{
		TenantID: "tenant-a", ActorUserIDHash: "actor-hash", SessionID: "session-a",
		ToolName: "dangerous.echo", ArgsDigest: "args-digest", Nonce: "nonce-a", State: "requested", ExpiresAt: now.Add(5 * time.Minute),
	}
	if err := repository.CreateConfirmation(ctx, confirmation, 5*time.Minute); err != nil {
		t.Fatal(err)
	}
	approved, err := repository.ApproveConfirmation(ctx, "tenant-a", "actor-hash", "session-a", "nonce-a")
	if err != nil || approved.State != "approved" {
		t.Fatalf("approve confirmation = (%#v, %v)", approved, err)
	}
	if err := repository.ConsumeConfirmation(ctx, approved); err != nil {
		t.Fatal(err)
	}
	if err := repository.ConsumeConfirmation(ctx, approved); !errors.Is(err, ErrConfirmationNotFound) {
		t.Fatalf("replayed confirmation = %v", err)
	}
	acquired, err := repository.AcquireLease(ctx, "reconciler", "gateway-a", time.Minute)
	if err != nil || !acquired {
		t.Fatalf("acquire lease = (%t, %v)", acquired, err)
	}
	acquired, err = repository.AcquireLease(ctx, "reconciler", "gateway-b", time.Minute)
	if err != nil || acquired {
		t.Fatalf("contended lease = (%t, %v)", acquired, err)
	}
	redisServer.FastForward(2 * time.Minute)
	acquired, err = repository.AcquireLease(ctx, "reconciler", "gateway-b", time.Minute)
	if err != nil || !acquired {
		t.Fatalf("expired lease takeover = (%t, %v)", acquired, err)
	}
}

func TestRedisRepositoryAppendAuditIsIdempotentByID(t *testing.T) {
	repository, _ := newTestRepository(t)
	ctx := context.Background()
	now := time.Now().UTC()
	const writers = 16
	var group sync.WaitGroup
	errs := make(chan error, writers)
	for index := 0; index < writers; index++ {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			errs <- repository.AppendAudit(ctx, AuditRecord{
				ID: "audit-replayed", TenantID: "tenant-a", EventType: "actor_authorization",
				Decision: "allowed", Sequence: 0, OccurredAt: now.Add(time.Duration(index) * time.Millisecond),
			})
		}(index)
	}
	group.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	records, next, err := repository.QueryAudit(ctx, AuditQuery{TenantID: "tenant-a", To: now.Add(time.Second), Limit: 10})
	if err != nil || next != "" || len(records) != 1 || records[0].ID != "audit-replayed" {
		t.Fatalf("replayed audit query = (%#v, %q, %v)", records, next, err)
	}
	retained := records[0]
	conflict := retained
	conflict.Decision = "denied"
	conflict.OccurredAt = now.Add(2 * time.Second)
	if err := repository.AppendAudit(ctx, conflict); err != nil {
		t.Fatal(err)
	}
	records, _, err = repository.QueryAudit(ctx, AuditQuery{TenantID: "tenant-a", To: now.Add(3 * time.Second), Limit: 10})
	if err != nil || len(records) != 1 || records[0].Decision != retained.Decision || !records[0].OccurredAt.Equal(retained.OccurredAt) {
		t.Fatalf("duplicate audit overwrote first record = (%#v, %v)", records, err)
	}
}

func TestAdminAuthenticator(t *testing.T) {
	disabled := NewAdminAuthenticator("")
	if err := disabled.Authenticate("Bearer anything"); !errors.Is(err, ErrAdminAPIDisabled) {
		t.Fatalf("disabled auth = %v", err)
	}
	auth := NewAdminAuthenticator("secret-token")
	if !auth.Enabled() || auth.Authenticate("Bearer secret-token") != nil {
		t.Fatal("valid administrator token was rejected")
	}
	for _, value := range []string{"secret-token", "Bearer wrong", "bearer secret-token", ""} {
		if err := auth.Authenticate(value); !errors.Is(err, ErrAdminUnauthorized) {
			t.Fatalf("invalid authorization %q = %v", value, err)
		}
	}
}

func newTestRepository(t *testing.T) (*RedisRepository, *miniredis.Miniredis) {
	t.Helper()
	redisServer := miniredis.RunT(t)
	redisURL := "redis://" + redisServer.Addr() + "/4"
	repository, err := NewRedisRepository(config.ControlPlaneConfig{
		RedisEndpoint: "redis://" + redisServer.Addr(), RedisURL: redisURL, LogicalDB: 4,
		KeyPrefix: "phase6-test", PolicyCacheTTL: 5 * time.Minute, AuditRetention: 30 * 24 * time.Hour,
		MetricRetention: 7 * 24 * time.Hour, NodeHeartbeatInterval: 10 * time.Second,
		NodeOfflineAfter: 30 * time.Second, NodeAssignmentWaitBackoff: 250 * time.Millisecond,
		DevelopmentAllowSharedRedis: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = repository.Close() })
	return repository, redisServer
}
