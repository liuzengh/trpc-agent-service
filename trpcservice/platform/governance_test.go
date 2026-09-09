package platform

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type governancePolicyStoreFixture struct {
	policies map[string]TenantPolicy
	loadErr  error
}

func (s *governancePolicyStoreFixture) loadGovernancePolicies(ctx context.Context) (map[string]TenantPolicy, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.loadErr != nil {
		return nil, s.loadErr
	}
	result := make(map[string]TenantPolicy, len(s.policies))
	for key, policy := range s.policies {
		result[key] = clonePolicy(policy)
	}
	return result, nil
}

func (s *governancePolicyStoreFixture) saveGovernancePolicy(ctx context.Context, policy TenantPolicy) (TenantPolicy, error) {
	if err := ctx.Err(); err != nil {
		return TenantPolicy{}, err
	}
	if s.policies == nil {
		s.policies = make(map[string]TenantPolicy)
	}
	key := governanceKey(policy.TenantID, policy.AgentAppID)
	policy.Revision = s.policies[key].Revision + 1
	policy.UpdatedAt = time.Now().UTC()
	s.policies[key] = clonePolicy(policy)
	return clonePolicy(policy), nil
}

func TestGovernancePolicyStoreImportsLegacyLocalPolicies(t *testing.T) {
	center := NewGovernanceCenter()
	center.policies[governanceKey("tenant-a", "app-a")] = TenantPolicy{
		TenantID: "tenant-a", AgentAppID: "app-a", AllowedTools: []string{"search"}, Revision: 7,
	}
	store := &governancePolicyStoreFixture{}

	center.configurePolicyStore(store)

	policy, found := store.policies[governanceKey("tenant-a", "app-a")]
	if !found || policy.Revision != 1 || len(policy.AllowedTools) != 1 {
		t.Fatalf("imported policy = %#v, found = %v", policy, found)
	}
	if active, found, err := center.Policy(context.Background(), "tenant-a", "app-a"); err != nil || !found || active.Revision != 1 {
		t.Fatalf("active policy = %#v, found = %v, error = %v", active, found, err)
	}
}

func TestGovernanceAuthorizeToolFailsClosedWhenPolicyStoreIsUnavailable(t *testing.T) {
	store := &governancePolicyStoreFixture{policies: map[string]TenantPolicy{
		governanceKey("tenant-a", "app-a"): {TenantID: "tenant-a", AgentAppID: "app-a", AllowedTools: []string{"search"}, Revision: 1},
	}}
	center := NewGovernanceCenter()
	center.configurePolicyStore(store)
	store.loadErr = errors.New("control plane unavailable")

	err := center.AuthorizeTool(context.Background(), GovernanceRequest{
		TenantID: "tenant-a", AgentAppID: "app-a", RequestID: "request-a", PolicyRevision: 1,
	}, "trace-a", "search", nil)
	if !IsGovernanceError(err, "control_plane_unavailable") {
		t.Fatalf("AuthorizeTool error = %v", err)
	}
}

func TestGovernancePersistenceFailureRollsBackPolicyAndExecution(t *testing.T) {
	blocker := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(blocker, []byte("block"), 0600); err != nil {
		t.Fatal(err)
	}
	center := NewGovernanceCenter()
	center.SetPersistencePath(filepath.Join(blocker, "governance.json"))
	if _, err := center.PutPolicy(context.Background(), TenantPolicy{TenantID: "tenant-a", AgentAppID: "app-a"}); err == nil {
		t.Fatal("policy update succeeded without durable audit")
	}
	if _, found, err := center.Policy(context.Background(), "tenant-a", "app-a"); err != nil || found {
		t.Fatalf("failed policy update remained active in memory: found = %v, error = %v", found, err)
	}
	center.policies[governanceKey("tenant-a", "app-a")] = TenantPolicy{TenantID: "tenant-a", AgentAppID: "app-a", Revision: 1}
	_, err := center.Evaluate(context.Background(), GovernanceRequest{TenantID: "tenant-a", AgentAppID: "app-a", RequestID: "request-a", Input: "hello"})
	if !IsGovernanceError(err, "audit_unavailable") {
		t.Fatalf("evaluate error = %v", err)
	}
	if metrics := center.Metrics("tenant-a"); metrics.Requests != 0 || metrics.Active != 0 {
		t.Fatalf("rolled back metrics = %#v", metrics)
	}
}

func TestGovernanceRecordSpanReportsPersistenceFailure(t *testing.T) {
	blocker := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(blocker, []byte("block"), 0600); err != nil {
		t.Fatal(err)
	}
	center := NewGovernanceCenter()
	center.SetPersistencePath(filepath.Join(blocker, "governance.json"))

	err := center.RecordSpan(
		GovernanceRequest{TenantID: "tenant-a", AgentAppID: "app-a", RequestID: "request-a"},
		"trace-a", "gateway.receive", "ok",
	)
	if err == nil {
		t.Fatal("trace span succeeded without durable persistence")
	}
	if trace, found := center.Trace("tenant-a", "trace-a", ""); found || len(trace.Spans) != 0 {
		t.Fatalf("failed trace write remained active in memory: %#v", trace)
	}
}

func TestGovernanceCenterPersistsPoliciesAndAuditEvents(t *testing.T) {
	path := filepath.Join(t.TempDir(), "governance.json")
	center, err := NewPersistentGovernanceCenter(path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = center.PutPolicy(context.Background(), TenantPolicy{TenantID: "tenant-a", AgentAppID: "app-a", AllowedTools: []string{"search"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := center.Record(context.Background(), AuditEvent{TenantID: "tenant-a", Decision: "authorization.denied", TraceID: "trace-a"}); err != nil {
		t.Fatal(err)
	}
	center.RecordSpan(GovernanceRequest{TenantID: "tenant-a", AgentAppID: "app-a", RequestID: "request-a"}, "trace-a", "gateway.receive", "ok")

	reloaded, err := NewPersistentGovernanceCenter(path)
	if err != nil {
		t.Fatal(err)
	}
	policy, found, err := reloaded.Policy(context.Background(), "tenant-a", "app-a")
	if err != nil || !found || policy.Revision != 1 || len(policy.AllowedTools) != 1 {
		t.Fatalf("policy = %#v, found = %v, error = %v", policy, found, err)
	}
	audits := reloaded.AuditEvents(AuditQuery{TenantID: "tenant-a", Limit: 10})
	if len(audits) != 2 || audits[0].Decision != "authorization.denied" {
		t.Fatalf("audits = %#v", audits)
	}
	if trace, found := reloaded.Trace("tenant-a", "trace-a", ""); !found || len(trace.Spans) != 1 {
		t.Fatalf("trace = %#v, found = %v", trace, found)
	}
}

func TestGovernancePersistenceRestoresActiveExecutionAndToolStart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "governance.json")
	center, err := NewPersistentGovernanceCenter(path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = center.PutPolicy(context.Background(), TenantPolicy{
		TenantID: "tenant-a", AgentAppID: "app-a", AllowedTools: []string{"deploy"}, DangerousTools: []string{"deploy"},
		TokenBudget: 10, EstimatedTokensPerRun: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	request := GovernanceRequest{TenantID: "tenant-a", AgentAppID: "app-a", SessionID: "session-a", RequestID: "request-a", RequiredTools: []string{"deploy"}}
	result, err := center.Evaluate(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if err := center.AuthorizeTool(context.Background(), request, result.TraceID, "deploy", []byte(`{"target":"stage5"}`)); !IsGovernanceError(err, "confirmation_required") {
		t.Fatalf("initial tool authorization = %v", err)
	}
	confirmation := center.Confirmations("tenant-a")[0]
	if _, err := center.DecideConfirmation(context.Background(), "tenant-a", confirmation.ID, "operator", true); err != nil {
		t.Fatal(err)
	}
	if err := center.AuthorizeTool(context.Background(), request, result.TraceID, "deploy", []byte(`{"target":"stage5"}`)); err != nil {
		t.Fatal(err)
	}
	reloaded, err := NewPersistentGovernanceCenter(path)
	if err != nil {
		t.Fatal(err)
	}
	if metrics := reloaded.Metrics("tenant-a"); metrics.Active != 1 {
		t.Fatalf("active reservation was not restored: %#v", metrics)
	}
	if err := reloaded.CompleteTool(context.Background(), request, result.TraceID, "deploy", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := reloaded.Complete(context.Background(), GovernanceCompletion{TenantID: "tenant-a", AgentAppID: "app-a", RequestID: "request-a", Tokens: 1}); err != nil {
		t.Fatal(err)
	}
	if status := reloaded.Confirmations("tenant-a")[0].Status; status != ConfirmationOutcomeUnknown {
		t.Fatalf("restored Tool confirmation status = %q", status)
	}
}

func TestGovernanceCenterEncryptsRedactionPatternsAtRest(t *testing.T) {
	path := filepath.Join(t.TempDir(), "governance.json")
	center, err := NewPersistentGovernanceCenter(path)
	if err != nil {
		t.Fatal(err)
	}
	const canary = "stage5-redaction-canary"
	if _, err := center.PutPolicy(context.Background(), TenantPolicy{TenantID: "tenant-a", AgentAppID: "app-a", RedactedPatterns: []string{canary}}); err != nil {
		t.Fatal(err)
	}
	for _, persistedPath := range []string{path, path + ".key"} {
		data, err := os.ReadFile(persistedPath)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(data), canary) {
			t.Fatalf("redaction secret persisted in plaintext at %s", persistedPath)
		}
	}
	reloaded, err := NewPersistentGovernanceCenter(path)
	if err != nil {
		t.Fatal(err)
	}
	if output, _ := reloaded.FilterOutput("tenant-a", "app-a", "value="+canary); output != "value=[REDACTED]" {
		t.Fatalf("reloaded redaction output = %q", output)
	}
}

func TestGovernanceCenterEnforcesPolicyBeforeExecutionAndRecordsAudit(t *testing.T) {
	center := NewGovernanceCenter()
	policy, err := center.PutPolicy(context.Background(), TenantPolicy{
		TenantID: "tenant-a", AgentAppID: "app-a", AllowedTools: []string{"search"}, AllowedMCP: []string{"calendar"},
		DeniedInputPatterns: []string{"blocked"}, RedactedPatterns: []string{"canary-secret"},
		AllowedIMUsers: []string{"user-a"}, AllowedIMSubjects: []string{"chat-a"},
		TokenBudget: 100, CostBudget: 1, CostPerToken: 0.01, ToolCosts: map[string]float64{"search": 0}, EstimatedTokensPerRun: 10, RateLimit: 2, RateWindowSeconds: 60,
	})
	if err != nil || policy.Revision != 1 {
		t.Fatalf("policy = %#v, err = %v", policy, err)
	}

	denied, err := center.Evaluate(context.Background(), GovernanceRequest{
		TenantID: "tenant-a", AgentAppID: "app-a", UserID: "user-a", SessionID: "session-a", RequestID: "request-denied",
		Input: "this is blocked", RequiredTools: []string{"search"},
	})
	var governanceErr *GovernanceError
	if !errors.As(err, &governanceErr) || governanceErr.Code != "policy_denied" || denied.TraceID == "" {
		t.Fatalf("result = %#v, err = %v", denied, err)
	}

	allowed, err := center.Evaluate(context.Background(), GovernanceRequest{
		TenantID: "tenant-a", AgentAppID: "app-a", UserID: "user-a", SessionID: "session-a", RequestID: "request-allowed",
		Channel: ChannelTelegram, ExternalSubject: "chat-a", Input: "hello canary-secret", RequiredTools: []string{"search"}, RequiredMCP: []string{"calendar"},
	})
	if err != nil || allowed.Input != "hello [REDACTED]" || allowed.TraceID == "" {
		t.Fatalf("result = %#v, err = %v", allowed, err)
	}
	_, _ = center.Complete(context.Background(), GovernanceCompletion{TenantID: "tenant-a", AgentAppID: "app-a", RequestID: "request-allowed", Output: "done", Tokens: 4})

	audits := center.AuditEvents(AuditQuery{TenantID: "tenant-a", Limit: 20})
	if len(audits) < 3 {
		t.Fatalf("audit events = %#v", audits)
	}
	for _, event := range audits {
		if event.TenantID != "tenant-a" || event.TraceID == "" {
			t.Fatalf("audit event = %#v", event)
		}
	}
	metrics := center.Metrics("tenant-a")
	if metrics.Requests != 2 || metrics.Denied != 1 || metrics.Completed != 1 || metrics.Tokens != 4 || metrics.Cost != 0.04 {
		t.Fatalf("metrics = %#v", metrics)
	}
}

func TestGovernanceCenterFailsClosedWithoutPolicy(t *testing.T) {
	center := NewGovernanceCenter()
	request := GovernanceRequest{TenantID: "tenant-a", AgentAppID: "app-a", RequestID: "request-a", Input: "hello", RequiredTools: []string{"search"}}

	if _, err := center.Evaluate(context.Background(), request); !IsGovernanceError(err, "policy_unavailable") {
		t.Fatalf("missing-policy evaluation error = %v", err)
	}
	if err := center.AuthorizeTool(context.Background(), request, "trace-a", "search", nil); !IsGovernanceError(err, "policy_unavailable") {
		t.Fatalf("missing-policy Tool error = %v", err)
	}
}

func TestGovernanceCenterEnforcesProviderAccountAndConversationType(t *testing.T) {
	center := NewGovernanceCenter()
	_, _ = center.PutPolicy(context.Background(), TenantPolicy{
		TenantID: "tenant-a", AgentAppID: "app-a", AllowedProviderAccounts: []string{"bot-a"}, AllowedConversationTypes: []string{ConversationSingle},
	})
	request := GovernanceRequest{TenantID: "tenant-a", AgentAppID: "app-a", Channel: ChannelTelegram, ProviderAccount: "bot-b", ConversationType: ConversationSingle, RequestID: "request-account", Input: "hello"}
	if _, err := center.Evaluate(context.Background(), request); !IsGovernanceError(err, "provider_account_denied") {
		t.Fatalf("provider account error = %v", err)
	}
	request.RequestID, request.ProviderAccount, request.ConversationType = "request-conversation", "bot-a", ConversationGroup
	if _, err := center.Evaluate(context.Background(), request); !IsGovernanceError(err, "conversation_type_denied") {
		t.Fatalf("conversation type error = %v", err)
	}
	request.RequestID, request.ConversationType = "request-allowed", ConversationSingle
	if _, err := center.Evaluate(context.Background(), request); err != nil {
		t.Fatalf("allowed request error = %v", err)
	}
}

func TestGovernanceCenterBlocksOutputBeforeCompletionAudit(t *testing.T) {
	center := NewGovernanceCenter()
	_, err := center.PutPolicy(context.Background(), TenantPolicy{
		TenantID: "tenant-a", AgentAppID: "app-a", DeniedOutputPatterns: []string{"forbidden-output"},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = center.Evaluate(context.Background(), GovernanceRequest{
		TenantID: "tenant-a", AgentAppID: "app-a", SessionID: "session-a", RequestID: "request-a", Input: "hello",
	})
	if err != nil {
		t.Fatal(err)
	}
	output, err := center.Complete(context.Background(), GovernanceCompletion{
		TenantID: "tenant-a", AgentAppID: "app-a", RequestID: "request-a", Output: "contains forbidden-output",
	})
	if err != nil {
		t.Fatal(err)
	}
	if output != "[REDACTED]" {
		t.Fatalf("output = %q", output)
	}
	audits := center.AuditEvents(AuditQuery{TenantID: "tenant-a", RequestID: "request-a", ErrorType: "output_guardrail"})
	if len(audits) != 1 || audits[0].Decision != "run.failed" {
		t.Fatalf("audits = %#v", audits)
	}
	metrics := center.Metrics("tenant-a")
	if metrics.Failed != 1 || metrics.Active != 0 {
		t.Fatalf("metrics = %#v", metrics)
	}
}

func TestGovernanceAuditSearchFiltersAndPaginates(t *testing.T) {
	center := NewGovernanceCenter()
	base := time.Date(2026, 9, 3, 10, 0, 0, 0, time.UTC)
	for index, event := range []AuditEvent{
		{TenantID: "tenant-a", Channel: ChannelTelegram, UserID: "user-a", SessionID: "session-a", AgentName: "app-a", Decision: "policy.allowed", RequestID: "request-a", TraceID: "trace-a"},
		{TenantID: "tenant-a", Channel: ChannelEnterpriseWeChat, UserID: "user-b", SessionID: "session-b", AgentName: "app-b", Decision: "run.failed", ErrorType: "runner_failed", RequestID: "request-b", TraceID: "trace-b"},
		{TenantID: "tenant-b", Channel: ChannelTelegram, UserID: "user-a", SessionID: "session-a", AgentName: "app-a", Decision: "policy.allowed", RequestID: "request-a", TraceID: "trace-a"},
	} {
		event.OccurredAt = base.Add(time.Duration(index) * time.Minute)
		if err := center.Record(context.Background(), event); err != nil {
			t.Fatal(err)
		}
	}

	audits := center.AuditEvents(AuditQuery{
		TenantID: "tenant-a", Channel: ChannelEnterpriseWeChat, AgentName: "app-b", ErrorType: "runner_failed",
		From: base.Add(30 * time.Second), To: base.Add(90 * time.Second), Limit: 1,
	})
	if len(audits) != 1 || audits[0].RequestID != "request-b" {
		t.Fatalf("filtered audits = %#v", audits)
	}
	if audits := center.AuditEvents(AuditQuery{TenantID: "tenant-a", Offset: 1, Limit: 1}); len(audits) != 1 || audits[0].RequestID != "request-a" {
		t.Fatalf("paginated audits = %#v", audits)
	}
}

func TestGovernanceCenterRequiresDangerousToolConfirmationExactlyOnce(t *testing.T) {
	center := NewGovernanceCenter()
	_, err := center.PutPolicy(context.Background(), TenantPolicy{
		TenantID: "tenant-a", AgentAppID: "app-a", AllowedTools: []string{"deploy"}, DangerousTools: []string{"deploy"},
		TokenBudget: 100, EstimatedTokensPerRun: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	request := GovernanceRequest{TenantID: "tenant-a", AgentAppID: "app-a", UserID: "user-a", SessionID: "session-a", RequestID: "request-a", RequiredTools: []string{"deploy"}, Input: "ship"}
	result, err := center.Evaluate(context.Background(), request)
	if err != nil || result.TraceID == "" {
		t.Fatalf("result = %#v, err = %v", result, err)
	}
	arguments := []byte(`{"target":"stage5-secret-target"}`)
	err = center.AuthorizeTool(context.Background(), request, result.TraceID, "deploy", arguments)
	var governanceErr *GovernanceError
	if !errors.As(err, &governanceErr) || governanceErr.Code != "confirmation_required" || governanceErr.ConfirmationID == "" {
		t.Fatalf("Tool authorization error = %#v", governanceErr)
	}
	pending := center.Confirmations("tenant-a")[0]
	if pending.ArgumentSummary == "" || strings.Contains(pending.ArgumentSummary, "stage5-secret-target") {
		t.Fatalf("unsafe pending argument summary = %q", pending.ArgumentSummary)
	}
	confirmation, err := center.DecideConfirmation(context.Background(), "tenant-a", governanceErr.ConfirmationID, "admin-a", true)
	if err != nil || confirmation.Status != ConfirmationApproved {
		t.Fatalf("confirmation = %#v, err = %v", confirmation, err)
	}
	if err := center.AuthorizeTool(context.Background(), request, result.TraceID, "deploy", arguments); err != nil {
		t.Fatalf("approved Tool was not authorized: %v", err)
	}
	if err := center.AuthorizeTool(context.Background(), request, result.TraceID, "deploy", arguments); !IsGovernanceError(err, "confirmation_consumed") {
		t.Fatalf("second Tool invocation error = %v", err)
	}
	second, err := center.DecideConfirmation(context.Background(), "tenant-a", confirmation.ID, "admin-a", true)
	if err != nil || second.DecidedAt != confirmation.DecidedAt {
		t.Fatalf("idempotent decision = %#v, err = %v", second, err)
	}
	if err := center.CompleteTool(context.Background(), request, result.TraceID, "deploy", nil); err != nil {
		t.Fatal(err)
	}
	completed := center.Confirmations("tenant-a")[0]
	if completed.Status != ConfirmationCompleted || completed.CompletedAt.IsZero() {
		t.Fatalf("completed confirmation = %#v", completed)
	}
	audits := center.AuditEvents(AuditQuery{TenantID: "tenant-a", RequestID: "request-a", Limit: 20})
	if !containsAuditDecision(audits, "tool.completed") {
		t.Fatalf("Tool completion audit missing: %#v", audits)
	}
}

func TestGovernanceToolCompletionFailureKeepsTerminalStateForRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "governance.json")
	center, err := NewPersistentGovernanceCenter(path)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = center.PutPolicy(context.Background(), TenantPolicy{TenantID: "tenant-a", AgentAppID: "app-a", AllowedTools: []string{"deploy"}, DangerousTools: []string{"deploy"}, TokenBudget: 10, EstimatedTokensPerRun: 1})
	request := GovernanceRequest{TenantID: "tenant-a", AgentAppID: "app-a", SessionID: "session-a", RequestID: "request-a"}
	result, err := center.Evaluate(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	arguments := []byte(`{"target":"stage5"}`)
	if err := center.AuthorizeTool(context.Background(), request, result.TraceID, "deploy", arguments); !IsGovernanceError(err, "confirmation_required") {
		t.Fatalf("initial tool authorization = %v", err)
	}
	confirmation := center.Confirmations("tenant-a")[0]
	if _, err := center.DecideConfirmation(context.Background(), "tenant-a", confirmation.ID, "operator", true); err != nil {
		t.Fatal(err)
	}
	if err := center.AuthorizeTool(context.Background(), request, result.TraceID, "deploy", arguments); err != nil {
		t.Fatal(err)
	}
	blocker := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(blocker, []byte("block"), 0600); err != nil {
		t.Fatal(err)
	}
	center.SetPersistencePath(filepath.Join(blocker, "governance.json"))
	if err := center.CompleteTool(context.Background(), request, result.TraceID, "deploy", nil); !IsGovernanceError(err, "audit_unavailable") {
		t.Fatalf("Tool completion persistence error = %v", err)
	}
	if status := center.Confirmations("tenant-a")[0].Status; status != ConfirmationCompleted {
		t.Fatalf("Tool completion was rolled back after side effect: %q", status)
	}
	center.SetPersistencePath(path)
	if err := center.AuthorizeTool(context.Background(), request, result.TraceID, "deploy", arguments); !IsGovernanceError(err, "confirmation_consumed") {
		t.Fatalf("recovery retry authorized a second side effect: %v", err)
	}
	if _, err := center.Complete(context.Background(), GovernanceCompletion{TenantID: "tenant-a", AgentAppID: "app-a", RequestID: "request-a", Tokens: 1}); err != nil {
		t.Fatal(err)
	}
	reloaded, err := NewPersistentGovernanceCenter(path)
	if err != nil {
		t.Fatal(err)
	}
	if status := reloaded.Confirmations("tenant-a")[0].Status; status != ConfirmationCompleted {
		t.Fatalf("recovered Tool completion status = %q", status)
	}
}

func TestGovernanceCenterAuthorizesActualToolCallAndHashesArguments(t *testing.T) {
	center := NewGovernanceCenter()
	_, _ = center.PutPolicy(context.Background(), TenantPolicy{TenantID: "tenant-a", AgentAppID: "app-a", AllowedTools: []string{"deploy"}, DangerousTools: []string{"deploy"}})
	request := GovernanceRequest{TenantID: "tenant-a", AgentAppID: "app-a", UserID: "user-a", SessionID: "session-a", RequestID: "request-a", RequiredTools: []string{"deploy"}}
	result, _ := center.Evaluate(context.Background(), request)
	arguments := []byte(`{"target":"stage5-secret-target"}`)
	_ = center.AuthorizeTool(context.Background(), request, result.TraceID, "deploy", arguments)
	confirmation := center.Confirmations("tenant-a")[0]
	if confirmation.ArgumentSummary == "" || strings.Contains(confirmation.ArgumentSummary, "stage5-secret-target") {
		t.Fatalf("argument summary = %q", confirmation.ArgumentSummary)
	}
	if len(confirmation.ArgumentSummary) != len("sha256:")+64 {
		t.Fatalf("argument summary is not a full SHA-256 digest: %q", confirmation.ArgumentSummary)
	}
	if err := center.AuthorizeTool(context.Background(), request, result.TraceID, "unknown", nil); !IsGovernanceError(err, "tool_not_allowed") {
		t.Fatalf("unknown Tool error = %v", err)
	}
}

func containsAuditDecision(events []AuditEvent, decision string) bool {
	for _, event := range events {
		if event.Decision == decision {
			return true
		}
	}
	return false
}

func TestGovernanceToolAuthorizationRejectsStaleAdmissionPolicyRevision(t *testing.T) {
	center := NewGovernanceCenter()
	policy := TenantPolicy{TenantID: "tenant-a", AgentAppID: "app-a", AllowedTools: []string{"deploy"}, DangerousTools: []string{"deploy"}}
	if _, err := center.PutPolicy(context.Background(), policy); err != nil {
		t.Fatal(err)
	}
	request := GovernanceRequest{TenantID: "tenant-a", AgentAppID: "app-a", SessionID: "session-a", RequestID: "request-a"}
	result, err := center.Evaluate(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := center.PutPolicy(context.Background(), policy); err != nil {
		t.Fatal(err)
	}
	request.PolicyRevision = result.PolicyRevision
	if err := center.AuthorizeTool(context.Background(), request, result.TraceID, "deploy", []byte(`{"target":"stage5"}`)); !IsGovernanceError(err, "policy_revision_stale") {
		t.Fatalf("stale policy authorization = %v", err)
	}
}

func TestGovernanceCenterEnforcesBudgetRateAndIMAuthorization(t *testing.T) {
	now := time.Date(2026, 9, 3, 10, 0, 0, 0, time.UTC)
	center := NewGovernanceCenter()
	center.now = func() time.Time { return now }
	_, _ = center.PutPolicy(context.Background(), TenantPolicy{
		TenantID: "tenant-a", AgentAppID: "app-a", AllowedIMUsers: []string{"allowed"}, TokenBudget: 2,
		EstimatedTokensPerRun: 1, RateLimit: 1, RateWindowSeconds: 60,
	})
	_, err := center.Evaluate(context.Background(), GovernanceRequest{TenantID: "tenant-a", AgentAppID: "app-a", Channel: ChannelTelegram, UserID: "denied", RequestID: "denied", Input: "hello"})
	if !IsGovernanceError(err, "im_user_denied") {
		t.Fatalf("IM error = %v", err)
	}
	_, err = center.Evaluate(context.Background(), GovernanceRequest{TenantID: "tenant-a", AgentAppID: "app-a", Channel: ChannelTelegram, UserID: "allowed", RequestID: "one", Input: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = center.Evaluate(context.Background(), GovernanceRequest{TenantID: "tenant-a", AgentAppID: "app-a", Channel: ChannelTelegram, UserID: "allowed", RequestID: "two", Input: "hello"})
	if !IsGovernanceError(err, "tenant_rate_limited") {
		t.Fatalf("rate error = %v", err)
	}
	now = now.Add(time.Minute)
	_, err = center.Evaluate(context.Background(), GovernanceRequest{TenantID: "tenant-a", AgentAppID: "app-a", Channel: ChannelTelegram, UserID: "allowed", RequestID: "three", Input: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Minute)
	_, err = center.Evaluate(context.Background(), GovernanceRequest{TenantID: "tenant-a", AgentAppID: "app-a", Channel: ChannelTelegram, UserID: "allowed", RequestID: "four", Input: "hello"})
	if !IsGovernanceError(err, "budget_exceeded") {
		t.Fatalf("budget error = %v", err)
	}
}

func TestGovernanceCenterReconcilesExpiredReservationExactlyOnce(t *testing.T) {
	now := time.Date(2026, 9, 3, 10, 0, 0, 0, time.UTC)
	center := NewGovernanceCenter()
	center.now = func() time.Time { return now }
	_, _ = center.PutPolicy(context.Background(), TenantPolicy{
		TenantID: "tenant-a", AgentAppID: "app-a", TokenBudget: 2, EstimatedTokensPerRun: 1,
	})
	if _, err := center.Evaluate(context.Background(), GovernanceRequest{TenantID: "tenant-a", AgentAppID: "app-a", RequestID: "request-one", Input: "one"}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(16 * time.Minute)
	if _, err := center.Evaluate(context.Background(), GovernanceRequest{TenantID: "tenant-a", AgentAppID: "app-a", RequestID: "request-two", Input: "two"}); err != nil {
		t.Fatalf("expired reservation still consumed budget: %v", err)
	}
	metrics := center.Metrics("tenant-a")
	if metrics.Active != 1 || metrics.Failed != 1 {
		t.Fatalf("reconciled metrics = %#v", metrics)
	}
	if audits := center.AuditEvents(AuditQuery{TenantID: "tenant-a", Decision: "reservation.expired", Limit: 10}); len(audits) != 1 {
		t.Fatalf("reservation expiry audits = %#v", audits)
	}
	result, err := center.Evaluate(context.Background(), GovernanceRequest{TenantID: "tenant-a", AgentAppID: "app-a", RequestID: "request-one", Input: "one"})
	if err != nil || !result.NewExecution {
		t.Fatalf("same request did not receive a fresh reservation after expiry: result=%#v err=%v", result, err)
	}
	if _, err := center.Complete(context.Background(), GovernanceCompletion{TenantID: "tenant-a", AgentAppID: "app-a", RequestID: "request-one", Tokens: 1}); err != nil {
		t.Fatal(err)
	}
	if audits := center.AuditEvents(AuditQuery{TenantID: "tenant-a", Decision: "reservation.expired", Limit: 10}); len(audits) != 1 {
		t.Fatalf("same request expiry was reconciled more than once: %#v", audits)
	}
}

func TestGovernancePolicyUpdatePreservesTenantUsage(t *testing.T) {
	center := NewGovernanceCenter()
	_, _ = center.PutPolicy(context.Background(), TenantPolicy{TenantID: "tenant-a", AgentAppID: "app-a", TokenBudget: 100, CostPerToken: 0.5})
	_, _ = center.Evaluate(context.Background(), GovernanceRequest{TenantID: "tenant-a", AgentAppID: "app-a", RequestID: "request-one", Input: "hello"})
	_, _ = center.Complete(context.Background(), GovernanceCompletion{TenantID: "tenant-a", AgentAppID: "app-a", RequestID: "request-one", Tokens: 10})
	_, _ = center.PutPolicy(context.Background(), TenantPolicy{TenantID: "tenant-a", AgentAppID: "app-b", TokenBudget: 100, CostPerToken: 0.5})
	if metrics := center.Metrics("tenant-a"); metrics.Tokens != 10 || metrics.Cost != 5 {
		t.Fatalf("policy update reset tenant usage: %#v", metrics)
	}
}

func TestGovernanceMetricsQueryUsesBoundedAppProviderAndTimeDimensions(t *testing.T) {
	now := time.Date(2026, 9, 3, 10, 0, 0, 0, time.UTC)
	center := NewGovernanceCenter()
	center.now = func() time.Time { return now }
	for _, appID := range []string{"app-a", "app-b"} {
		_, _ = center.PutPolicy(context.Background(), TenantPolicy{TenantID: "tenant-a", AgentAppID: appID})
	}
	for _, request := range []GovernanceRequest{
		{TenantID: "tenant-a", AgentAppID: "app-a", Channel: ChannelTelegram, RequestID: "request-a", Input: "hello"},
		{TenantID: "tenant-a", AgentAppID: "app-b", Channel: ChannelEnterpriseWeChat, RequestID: "request-b", Input: "hello"},
	} {
		if _, err := center.Evaluate(context.Background(), request); err != nil {
			t.Fatal(err)
		}
		if _, err := center.Complete(context.Background(), GovernanceCompletion{TenantID: request.TenantID, AgentAppID: request.AgentAppID, RequestID: request.RequestID, Tokens: 4}); err != nil {
			t.Fatal(err)
		}
	}
	metrics, err := center.QueryMetrics(MetricsQuery{
		TenantID: "tenant-a", AgentAppID: "app-a", Provider: ChannelTelegram,
		From: now.Add(-time.Hour), To: now.Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	if metrics.Requests != 1 || metrics.Completed != 1 || metrics.Tokens != 4 {
		t.Fatalf("filtered metrics = %#v", metrics)
	}
	if _, err := center.QueryMetrics(MetricsQuery{TenantID: "tenant-a", From: now.Add(-32 * 24 * time.Hour), To: now}); err == nil {
		t.Fatal("unbounded metrics range was accepted")
	}
}

func TestGovernanceToolLatencyKeepsProviderDimension(t *testing.T) {
	now := time.Date(2026, 9, 3, 10, 0, 0, 0, time.UTC)
	center := NewGovernanceCenter()
	center.now = func() time.Time { return now }
	_, _ = center.PutPolicy(context.Background(), TenantPolicy{TenantID: "tenant-a", AgentAppID: "app-a", AllowedTools: []string{"deploy"}, DangerousTools: []string{"deploy"}})
	request := GovernanceRequest{TenantID: "tenant-a", AgentAppID: "app-a", Channel: ChannelTelegram, RequestID: "request-a"}
	if _, err := center.Evaluate(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if err := center.AuthorizeTool(context.Background(), request, "trace-a", "deploy", []byte(`{"target":"stage5"}`)); !IsGovernanceError(err, "confirmation_required") {
		t.Fatalf("initial tool authorization = %v", err)
	}
	confirmation := center.Confirmations("tenant-a")[0]
	if _, err := center.DecideConfirmation(context.Background(), "tenant-a", confirmation.ID, "operator", true); err != nil {
		t.Fatal(err)
	}
	if err := center.AuthorizeTool(context.Background(), request, "trace-a", "deploy", []byte(`{"target":"stage5"}`)); err != nil {
		t.Fatal(err)
	}
	now = now.Add(25 * time.Millisecond)
	if err := center.CompleteTool(context.Background(), request, "trace-a", "deploy", nil); err != nil {
		t.Fatal(err)
	}
	metrics, err := center.QueryMetrics(MetricsQuery{TenantID: "tenant-a", AgentAppID: "app-a", Provider: ChannelTelegram, From: now.Add(-time.Hour), To: now.Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	if metrics.ToolLatencyMS != 25 {
		t.Fatalf("provider-filtered tool latency = %d, want 25", metrics.ToolLatencyMS)
	}
}

func TestGovernanceCenterReservesAndAttributesConfiguredToolCost(t *testing.T) {
	center := NewGovernanceCenter()
	_, _ = center.PutPolicy(context.Background(), TenantPolicy{
		TenantID: "tenant-a", AgentAppID: "app-a", AllowedTools: []string{"search", "unknown"},
		TokenBudget: 100, CostBudget: 5, CostPerToken: 1, EstimatedTokensPerRun: 1, ToolCosts: map[string]float64{"search": 2},
	})
	request := GovernanceRequest{TenantID: "tenant-a", AgentAppID: "app-a", SessionID: "session-a", RequestID: "request-a", RequiredTools: []string{"search"}, Input: "hello"}
	result, err := center.Evaluate(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if err := center.AuthorizeTool(context.Background(), request, result.TraceID, "search", []byte(`{"query":"safe"}`)); err != nil {
		t.Fatal(err)
	}
	if err := center.CompleteTool(context.Background(), request, result.TraceID, "search", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := center.Complete(context.Background(), GovernanceCompletion{TenantID: "tenant-a", AgentAppID: "app-a", RequestID: "request-a", Tokens: 1}); err != nil {
		t.Fatal(err)
	}
	if metrics := center.Metrics("tenant-a"); metrics.Cost != 3 {
		t.Fatalf("model and Tool cost = %v, want 3", metrics.Cost)
	}
	_, err = center.Evaluate(context.Background(), GovernanceRequest{TenantID: "tenant-a", AgentAppID: "app-a", RequestID: "request-b", RequiredTools: []string{"unknown"}, Input: "hello"})
	if !IsGovernanceError(err, "tool_pricing_unknown") {
		t.Fatalf("unknown Tool pricing error = %v", err)
	}
}

func TestGovernanceMarksExecutingToolOutcomeUnknownAfterWorkerLoss(t *testing.T) {
	center := NewGovernanceCenter()
	_, _ = center.PutPolicy(context.Background(), TenantPolicy{
		TenantID: "tenant-a", AgentAppID: "app-a", AllowedTools: []string{"deploy"}, DangerousTools: []string{"deploy"},
	})
	request := GovernanceRequest{TenantID: "tenant-a", AgentAppID: "app-a", SessionID: "session-a", RequestID: "request-worker-loss"}
	result, _ := center.Evaluate(context.Background(), request)
	arguments := []byte(`{"target":"stage7"}`)
	if err := center.AuthorizeTool(context.Background(), request, result.TraceID, "deploy", arguments); !IsGovernanceError(err, "confirmation_required") {
		t.Fatalf("initial authorization = %v", err)
	}
	confirmation := center.Confirmations("tenant-a")[0]
	if _, err := center.DecideConfirmation(context.Background(), "tenant-a", confirmation.ID, "operator", true); err != nil {
		t.Fatal(err)
	}
	if err := center.AuthorizeTool(context.Background(), request, result.TraceID, "deploy", arguments); err != nil {
		t.Fatal(err)
	}
	if err := center.MarkExecutingToolsOutcomeUnknown(context.Background(), request, result.TraceID); err != nil {
		t.Fatal(err)
	}
	confirmation = center.Confirmations("tenant-a")[0]
	if confirmation.Status != ConfirmationOutcomeUnknown {
		t.Fatalf("confirmation status = %q", confirmation.Status)
	}
	if err := center.AuthorizeTool(context.Background(), request, result.TraceID, "deploy", arguments); !IsGovernanceError(err, "confirmation_consumed") {
		t.Fatalf("outcome-unknown retry = %v", err)
	}
}
