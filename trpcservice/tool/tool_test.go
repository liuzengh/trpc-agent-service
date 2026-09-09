package tool

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func testTenantContext() tenant.TenantContext {
	return tenant.TenantContext{TenantID: "tenant-a", AgentAppID: "agent-a", BindingID: "binding-a", Channel: "web", ExternalUser: "user-a", SessionID: "session-a", RequestID: "request-a", MessageID: "message-a", TraceID: "trace-a", ConfigVersion: 1, BackendPolicy: tenant.BackendPolicy{Session: "memory", Memory: "memory", Vector: "none", Object: "none"}}
}

func testToolSpec() agent.AgentSpec {
	return agent.AgentSpec{TenantID: "tenant-a", AgentAppID: "agent-a", Version: 1, Name: "agent", ModelProvider: "fake", ToolPolicyRef: "policy-a", Tools: []agent.ToolSpec{{Name: "lookup", Version: 1, Capability: "read", InputSchema: map[string]any{"type": "object", "properties": map[string]any{"key": map[string]any{"type": "string"}}, "required": []any{"key"}, "additionalProperties": false}}}}
}

func testDefinition(called *atomic.Int32) Definition {
	return Definition{Name: "lookup", Version: 1, Capability: "read", Enabled: true, MaxInputByte: 1024, MaxOutputByte: 1024, InputSchema: testToolSpec().Tools[0].InputSchema, Invoker: agent.ToolInvokerFunc(func(context.Context, agent.ToolRequest) (agent.ToolResult, error) {
		called.Add(1)
		return agent.ToolResult{Content: "lookup result"}, nil
	})}
}

func testSecureInvoker(t *testing.T, definition Definition, policy PolicyResolver, budget *BudgetManager, sink AuditSink) *SecureInvoker {
	t.Helper()
	registry, err := NewRegistry([]Definition{definition})
	if err != nil {
		t.Fatal(err)
	}
	guardrail, err := NewGuardrailPipeline(GuardrailConfig{MaxInputBytes: 1024, MaxOutputBytes: 1024, Redactor: audit.NewRedactor(1024)})
	if err != nil {
		t.Fatal(err)
	}
	invoker, err := NewSecureInvoker(SecureInvokerConfig{Registry: registry, Policy: policy, Guardrail: guardrail, Budget: budget, Audit: sink})
	if err != nil {
		t.Fatal(err)
	}
	return invoker
}

func testPolicy(t *testing.T, category DecisionCategory, expires time.Time) *StaticPolicyResolver {
	t.Helper()
	policy := Policy{TenantID: "tenant-a", AgentAppID: "agent-a", AgentVersion: 1, PolicyRef: "policy-a", Version: 7, ExpiresAt: expires, Rules: map[string]PolicyRule{"lookup": {ToolVersion: 1, Capability: "read", Enabled: true, Allow: category == CategoryAllow, ApprovalRequired: category == CategoryApprovalRequired}}}
	resolver, err := NewStaticPolicyResolver([]Policy{policy})
	if err != nil {
		t.Fatal(err)
	}
	return resolver
}

func testBudget(t *testing.T) *BudgetManager {
	t.Helper()
	budget, err := NewBudgetManager(BudgetLimits{Requests: 2, InputBytes: 4096, OutputBytes: 4096, Duration: time.Second, Concurrent: 1, ReservationTTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	return budget
}

func testAuditSink(records *[]AuditRecord) AuditSink {
	var mu sync.Mutex
	return AuditSinkFunc(func(_ context.Context, record AuditRecord) error {
		mu.Lock()
		defer mu.Unlock()
		*records = append(*records, record)
		return nil
	})
}

func TestSecureInvokerAppliesPolicyGuardrailBudgetAndAudit(t *testing.T) {
	var called atomic.Int32
	definition := testDefinition(&called)
	resolver := testPolicy(t, CategoryAllow, time.Now().Add(time.Minute))
	budget := testBudget(t)
	var records []AuditRecord
	invoker := testSecureInvoker(t, definition, resolver, budget, testAuditSink(&records))
	request := agent.ToolRequest{TenantContext: testTenantContext(), Agent: testToolSpec(), ToolName: "lookup", Arguments: map[string]any{"key": "value"}}
	result, err := invoker.Invoke(context.Background(), request)
	if err != nil || result.Content != "lookup result" {
		t.Fatalf("unexpected result: result=%+v err=%v", result, err)
	}
	if called.Load() != 1 || len(records) != 2 {
		t.Fatalf("pipeline was not applied: calls=%d records=%d", called.Load(), len(records))
	}
	for _, record := range records {
		if record.TenantID != request.TenantContext.TenantID || record.ToolName != "lookup" || strings.Contains(record.InputFingerprint, "value") {
			t.Fatalf("unsafe audit record: %+v", record)
		}
	}
	snapshot, err := budget.Snapshot(context.Background(), request.TenantContext)
	if err != nil || snapshot.Requests != 1 || snapshot.Concurrent != 0 {
		t.Fatalf("unexpected budget snapshot: %+v err=%v", snapshot, err)
	}
}

func TestSecureInvokerFailsClosedForUnknownDisabledAndUndeclaredTools(t *testing.T) {
	var called atomic.Int32
	definition := testDefinition(&called)
	resolver := testPolicy(t, CategoryAllow, time.Now().Add(time.Minute))
	invoker := testSecureInvoker(t, definition, resolver, testBudget(t), testAuditSink(&[]AuditRecord{}))
	request := agent.ToolRequest{TenantContext: testTenantContext(), Agent: testToolSpec(), ToolName: "unknown", Arguments: map[string]any{}}
	_, err := invoker.Invoke(context.Background(), request)
	if CategoryOf(err) != CategoryDeny || called.Load() != 0 {
		t.Fatalf("unknown tool did not fail closed: category=%s err=%v calls=%d", CategoryOf(err), err, called.Load())
	}
	request.ToolName = "lookup"
	request.Agent.Tools = nil
	_, err = invoker.Invoke(context.Background(), request)
	if CategoryOf(err) != CategoryDeny || called.Load() != 0 {
		t.Fatalf("undeclared tool did not fail closed: category=%s err=%v", CategoryOf(err), err)
	}
	definition.Enabled = false
	invoker = testSecureInvoker(t, definition, resolver, testBudget(t), testAuditSink(&[]AuditRecord{}))
	request.Agent = testToolSpec()
	_, err = invoker.Invoke(context.Background(), request)
	if CategoryOf(err) != CategoryDeny || called.Load() != 0 {
		t.Fatalf("disabled tool did not fail closed: category=%s err=%v", CategoryOf(err), err)
	}
}

func TestSecureInvokerRejectsTenantAgentVersionAndPolicyFailures(t *testing.T) {
	var called atomic.Int32
	definition := testDefinition(&called)
	request := agent.ToolRequest{TenantContext: testTenantContext(), Agent: testToolSpec(), ToolName: "lookup", Arguments: map[string]any{"key": "value"}}
	resolver := testPolicy(t, CategoryAllow, time.Now().Add(time.Minute))
	invoker := testSecureInvoker(t, definition, resolver, testBudget(t), testAuditSink(&[]AuditRecord{}))
	request.TenantContext.TenantID = "tenant-b"
	_, err := invoker.Invoke(context.Background(), request)
	if CategoryOf(err) != CategoryInvalidContext || called.Load() != 0 {
		t.Fatalf("tenant mismatch was not rejected: category=%s err=%v", CategoryOf(err), err)
	}
	request.TenantContext = testTenantContext()
	request.Agent.Version = 2
	_, err = invoker.Invoke(context.Background(), request)
	if CategoryOf(err) != CategoryInvalidContext || called.Load() != 0 {
		t.Fatalf("agent version mismatch was not rejected: category=%s err=%v", CategoryOf(err), err)
	}
	unavailable := PolicyResolverFunc(func(context.Context, tenant.TenantContext, agent.AgentSpec, Definition) (PolicyDecision, error) {
		return PolicyDecision{}, errors.New("raw postgres password=do-not-leak")
	})
	request.Agent = testToolSpec()
	invoker = testSecureInvoker(t, definition, unavailable, testBudget(t), testAuditSink(&[]AuditRecord{}))
	_, err = invoker.Invoke(context.Background(), request)
	if CategoryOf(err) != CategoryPolicyUnavailable || strings.Contains(err.Error(), "do-not-leak") || called.Load() != 0 {
		t.Fatalf("policy backend error leaked or allowed: category=%s err=%v", CategoryOf(err), err)
	}
}

func TestSecureInvokerRejectsExpiredVersionAndApprovalPolicy(t *testing.T) {
	var called atomic.Int32
	definition := testDefinition(&called)
	request := agent.ToolRequest{TenantContext: testTenantContext(), Agent: testToolSpec(), ToolName: "lookup", Arguments: map[string]any{"key": "value"}}
	for _, category := range []DecisionCategory{CategoryApprovalRequired, CategoryVersionMismatch, CategoryExpiredPolicy, CategoryDeny} {
		var resolver PolicyResolver
		if category == CategoryExpiredPolicy {
			resolver = testPolicy(t, CategoryAllow, time.Now().Add(-time.Minute))
		} else if category == CategoryVersionMismatch {
			resolver = testPolicy(t, CategoryAllow, time.Now().Add(time.Minute))
			resolver = PolicyResolverFunc(func(context.Context, tenant.TenantContext, agent.AgentSpec, Definition) (PolicyDecision, error) {
				return PolicyDecision{Category: CategoryVersionMismatch, PolicyVersion: 1, ExpiresAt: time.Now().Add(time.Minute)}, nil
			})
		} else {
			resolver = testPolicy(t, category, time.Now().Add(time.Minute))
		}
		invoker := testSecureInvoker(t, definition, resolver, testBudget(t), testAuditSink(&[]AuditRecord{}))
		_, err := invoker.Invoke(context.Background(), request)
		if CategoryOf(err) != category || called.Load() != 0 {
			t.Fatalf("policy category %s did not fail closed: category=%s err=%v calls=%d", category, CategoryOf(err), err, called.Load())
		}
	}
}

func TestGuardrailRejectsSensitiveOversizedAndUnsafeArguments(t *testing.T) {
	var called atomic.Int32
	definition := testDefinition(&called)
	resolver := testPolicy(t, CategoryAllow, time.Now().Add(time.Minute))
	sink := testAuditSink(&[]AuditRecord{})
	invoker := testSecureInvoker(t, definition, resolver, testBudget(t), sink)
	request := agent.ToolRequest{TenantContext: testTenantContext(), Agent: testToolSpec(), ToolName: "lookup"}
	for _, arguments := range []map[string]any{{"authorization": "Bearer secret-token"}, {"key": strings.Repeat("x", 2048)}, {"other": "value"}} {
		request.Arguments = arguments
		_, err := invoker.Invoke(context.Background(), request)
		if CategoryOf(err) != CategoryInvalidInput && CategoryOf(err) != CategoryOversized {
			t.Fatalf("unsafe arguments were allowed: category=%s err=%v", CategoryOf(err), err)
		}
	}
	definition.Capability = "shell"
	definition.Invoker = agent.ToolInvokerFunc(func(context.Context, agent.ToolRequest) (agent.ToolResult, error) {
		called.Add(1)
		return agent.ToolResult{Content: "executed"}, nil
	})
	request.Agent = testToolSpec()
	request.Arguments = map[string]any{"key": "value"}
	invoker = testSecureInvoker(t, definition, resolver, testBudget(t), testAuditSink(&[]AuditRecord{}))
	_, err := invoker.Invoke(context.Background(), request)
	if CategoryOf(err) != CategoryDeny || called.Load() != 0 {
		t.Fatalf("shell capability was allowed: category=%s err=%v", CategoryOf(err), err)
	}
}

func TestGuardrailRedactsResultAndHidesBackendError(t *testing.T) {
	called := atomic.Int32{}
	definition := testDefinition(&called)
	definition.Invoker = agent.ToolInvokerFunc(func(context.Context, agent.ToolRequest) (agent.ToolResult, error) {
		called.Add(1)
		return agent.ToolResult{Content: "Authorization: Bearer secret-token"}, nil
	})
	resolver := testPolicy(t, CategoryAllow, time.Now().Add(time.Minute))
	invoker := testSecureInvoker(t, definition, resolver, testBudget(t), testAuditSink(&[]AuditRecord{}))
	request := agent.ToolRequest{TenantContext: testTenantContext(), Agent: testToolSpec(), ToolName: "lookup", Arguments: map[string]any{"key": "value"}}
	result, err := invoker.Invoke(context.Background(), request)
	if err != nil || result.Content == "" || strings.Contains(result.Content, "secret-token") || !strings.Contains(result.Content, RedactionMarker()) {
		t.Fatalf("result was not safely redacted: result=%+v err=%v", result, err)
	}
	definition.Invoker = agent.ToolInvokerFunc(func(context.Context, agent.ToolRequest) (agent.ToolResult, error) {
		return agent.ToolResult{Content: "raw database error password=secret"}, errors.New("raw database error password=secret")
	})
	invoker = testSecureInvoker(t, definition, resolver, testBudget(t), testAuditSink(&[]AuditRecord{}))
	_, err = invoker.Invoke(context.Background(), request)
	if CategoryOf(err) != CategoryToolFailure || strings.Contains(err.Error(), "password=secret") {
		t.Fatalf("backend error leaked: category=%s err=%v", CategoryOf(err), err)
	}
}

func RedactionMarker() string { return audit.RedactedValue }

func TestSecureInvokerAuditFailureDoesNotExecute(t *testing.T) {
	var called atomic.Int32
	definition := testDefinition(&called)
	resolver := testPolicy(t, CategoryAllow, time.Now().Add(time.Minute))
	sink := AuditSinkFunc(func(context.Context, AuditRecord) error {
		return errors.New("raw audit backend DSN=postgres://user:secret@host/db")
	})
	invoker := testSecureInvoker(t, definition, resolver, testBudget(t), sink)
	request := agent.ToolRequest{TenantContext: testTenantContext(), Agent: testToolSpec(), ToolName: "lookup", Arguments: map[string]any{"key": "value"}}
	_, err := invoker.Invoke(context.Background(), request)
	if CategoryOf(err) != CategoryPolicyUnavailable || called.Load() != 0 || strings.Contains(err.Error(), "postgres://") {
		t.Fatalf("audit failure was not fail closed: category=%s err=%v calls=%d", CategoryOf(err), err, called.Load())
	}
}

func TestSecureInvokerBudgetAndContextFailures(t *testing.T) {
	var called atomic.Int32
	definition := testDefinition(&called)
	definition.Invoker = agent.ToolInvokerFunc(func(ctx context.Context, _ agent.ToolRequest) (agent.ToolResult, error) {
		<-ctx.Done()
		return agent.ToolResult{}, ctx.Err()
	})
	resolver := testPolicy(t, CategoryAllow, time.Now().Add(time.Minute))
	budget := testBudget(t)
	invoker := testSecureInvoker(t, definition, resolver, budget, testAuditSink(&[]AuditRecord{}))
	request := agent.ToolRequest{TenantContext: testTenantContext(), Agent: testToolSpec(), ToolName: "lookup", Arguments: map[string]any{"key": "value"}}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := invoker.Invoke(ctx, request)
	if !errors.Is(err, context.DeadlineExceeded) && CategoryOf(err) != CategoryTimeout {
		t.Fatalf("deadline was not classified safely: category=%s err=%v", CategoryOf(err), err)
	}
	budget = testBudget(t)
	first, err := budget.Reserve(context.Background(), request.TenantContext, 10)
	if err != nil {
		t.Fatal(err)
	}
	second, err := budget.Reserve(context.Background(), request.TenantContext, 10)
	if err == nil || second != nil || CategoryOf(err) != CategoryBudgetExceeded {
		t.Fatalf("concurrent budget oversubscribed: reservation=%v category=%s err=%v", second, CategoryOf(err), err)
	}
	if err := first.Release(); err != nil {
		t.Fatal(err)
	}
	if _, err := budget.Reserve(context.Background(), request.TenantContext, 10); err != nil {
		t.Fatalf("released reservation did not free capacity: %v", err)
	}
}

func TestBudgetReclaimsStaleReservationsAndScopesByServerContext(t *testing.T) {
	budget, err := NewBudgetManager(BudgetLimits{Requests: 1, InputBytes: 100, OutputBytes: 100, Duration: time.Second, Concurrent: 1, ReservationTTL: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	var now time.Time
	budget.now = func() time.Time { return now }
	now = time.Unix(100, 0).UTC()
	tc := testTenantContext()
	reservation, err := budget.Reserve(context.Background(), tc, 10)
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Second)
	if got := budget.Reclaim(now); got != 1 {
		t.Fatalf("expected one stale reservation, got %d", got)
	}
	if err := reservation.Release(); err != nil {
		t.Fatal(err)
	}
	other := tc
	other.TenantID = "tenant-b"
	if _, err := budget.Reserve(context.Background(), other, 10); err != nil {
		t.Fatalf("independent tenant scope was rejected: %v", err)
	}
}

func TestGuardrailAndFingerprintAreBoundedAndDeterministic(t *testing.T) {
	redactor := audit.NewRedactor(32)
	value := redactor.RedactString(strings.Repeat("x", 100))
	if len(value) > 32 || !strings.Contains(value, "TRUNCATED") {
		t.Fatalf("redactor exceeded bound: %q", value)
	}
	if audit.Fingerprint("same") != audit.Fingerprint("same") || audit.Fingerprint("same") == audit.Fingerprint("different") {
		t.Fatal("fingerprint is not deterministic")
	}
}
