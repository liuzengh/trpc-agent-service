//go:build integration && e2e

package postgres_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/liuzengh/trpc-agent-service/trpcservice/admin"
	platformapproval "github.com/liuzengh/trpc-agent-service/trpcservice/approval"
	platformaudit "github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/auth"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/ingress"
	platformpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/postgres"
	platformredis "github.com/liuzengh/trpc-agent-service/trpcservice/redis"
	"github.com/liuzengh/trpc-agent-service/trpcservice/relay"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

const governanceReportPathEnv = "TRPC_GOVERNANCE_E2E_REPORT"

func TestGovernanceApprovalAcrossRealWorkerProcesses(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	pool, postgresDSN := openRuntimePool(t)
	store, err := platformpostgres.New(pool)
	if err != nil {
		t.Fatalf("new postgres store: %v", err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate postgres: %v", err)
	}

	runID := strings.ReplaceAll(uuid.NewString(), "-", "")
	scopeA := seedGovernanceScope(t, ctx, store, "governance-tenant-a-"+runID, "support")
	scopeB := seedGovernanceScope(t, ctx, store, "governance-tenant-b-"+runID, "support")
	operatorToken := "governance-operator-" + runID
	adminAPI := admin.API{Repository: store}
	adminHandler, err := admin.NewHTTPHandlerWithAuth(adminAPI, admin.AdminAuthConfig{
		SystemAdminToken:  "governance-system-" + runID,
		OperatorToken:     operatorToken,
		OperatorTenantIDs: []string{scopeA.TenantID},
	})
	if err != nil {
		t.Fatalf("new governance admin handler: %v", err)
	}
	adminServer := httptest.NewServer(adminHandler)
	defer adminServer.Close()

	redisURL := runtimeRedisURL()
	redisClient, err := platformredis.NewClient(ctx, redisURL)
	if err != nil {
		t.Fatalf("new redis client: %v", err)
	}
	defer redisClient.Close()
	streamName := "trpc-agent-service:governance-e2e:" + uuid.NewString()
	group := "governance-e2e-workers"
	stream, err := platformredis.NewStream(redisClient, streamName, group, 6*time.Second)
	if err != nil {
		t.Fatalf("new governance stream: %v", err)
	}
	if err := stream.Init(ctx); err != nil {
		t.Fatalf("initialize governance stream: %v", err)
	}
	dispatcher, err := relay.New(store, stream, "governance-e2e-relay")
	if err != nil {
		t.Fatalf("new governance relay: %v", err)
	}
	relayCtx, stopRelay := context.WithCancel(ctx)
	relayDone := make(chan error, 1)
	go func() { relayDone <- dispatcher.Run(relayCtx) }()
	t.Cleanup(func() {
		stopRelay()
		if err := <-relayDone; err != nil && !errors.Is(err, context.Canceled) {
			t.Logf("governance relay stopped with error: %v", err)
		}
	})

	events, err := platformpostgres.NewExecutionEventJournal(store)
	if err != nil {
		t.Fatalf("new execution event journal: %v", err)
	}
	queued, err := gateway.NewQueuedRunner(gateway.New(store), events)
	if err != nil {
		t.Fatalf("new queued runner: %v", err)
	}
	dataPlane, err := ingress.NewOpenAIHandler(auth.HTTPAPIKeyResolver{
		Credentials: store,
		Directory:   store,
	}, queued)
	if err != nil {
		t.Fatalf("new OpenAI handler: %v", err)
	}
	dataPlaneServer := httptest.NewServer(dataPlane)
	defer dataPlaneServer.Close()

	evidence := governanceEvidence{
		Stream: streamName,
		Tenants: []governanceTenantEvidence{
			{TenantID: scopeA.TenantID, AppID: scopeA.AppID},
			{TenantID: scopeB.TenantID, AppID: scopeB.AppID},
		},
	}
	defer func() { writeGovernanceEvidence(t, evidence) }()

	workerBinary := buildRuntimeWorker(t)
	workerEnv := runtimeWorkerEnv(postgresDSN, redisURL, streamName, group)
	worker := startRuntimeWorker(t, ctx, workerBinary, workerEnv, "governance-worker-a", "approval")

	approveRequest := launchRuntimeRequest(ctx, dataPlaneServer.URL, scopeA.APIKey,
		"governance-approve", "governance-idem-approve", "governance-approve-session", "approve")
	if _, err := waitRuntimeExecution(ctx, pool, "governance-approve", func(value runtimeExecution) bool {
		return value.Status == "WAITING_APPROVAL" && value.TenantID == scopeA.TenantID
	}); err != nil {
		t.Fatalf("wait ASK execution: %v", err)
	}
	approval := waitGovernanceApproval(t, ctx, adminAPI, scopeA, "governance-approve")
	if approval.Status != platformapproval.StatusPending || approval.ToolName != "todo_write" {
		t.Fatalf("ASK approval = %#v", approval)
	}
	evidence.Ask = governanceApprovalEvidence{
		RequestID: approval.RequestID, ApprovalID: approval.ApprovalID,
		TenantID: approval.TenantID, ToolName: approval.ToolName, Status: string(approval.Status),
	}

	status, body, err := decideGovernanceApproval(ctx, adminServer.URL, operatorToken, scopeB, approval.ApprovalID, "approve")
	if err != nil {
		t.Fatalf("cross-tenant approval decision request: %v", err)
	}
	if status != http.StatusForbidden {
		t.Fatalf("cross-tenant approval status=%d body=%q, want %d", status, body, http.StatusForbidden)
	}
	evidence.CrossTenantDenied = true

	status, body, err = decideGovernanceApproval(ctx, adminServer.URL, operatorToken, scopeA, approval.ApprovalID, "approve")
	if err != nil {
		t.Fatalf("approve request: %v", err)
	}
	if status != http.StatusOK {
		t.Fatalf("approve status=%d body=%q", status, body)
	}
	approved, err := waitRuntimeExecution(ctx, pool, "governance-approve", func(value runtimeExecution) bool {
		return value.Status == "SUCCEEDED"
	})
	if err != nil {
		t.Fatalf("wait approved execution: %v", err)
	}
	if err := requireRuntimeHTTPSuccess(<-approveRequest); err != nil {
		t.Fatalf("approved request: %v", err)
	}
	if repeatedStatus, _, err := decideGovernanceApproval(ctx, adminServer.URL, operatorToken, scopeA, approval.ApprovalID, "approve"); err != nil || repeatedStatus != http.StatusOK {
		t.Fatalf("repeated approve status=%d err=%v", repeatedStatus, err)
	}
	evidence.Approve = governanceExecutionEvidence{
		RequestID: approved.RequestID, Worker: approved.Owner, Status: approved.Status,
		Attempt: approved.Attempt,
	}

	denyRequest := launchRuntimeRequest(ctx, dataPlaneServer.URL, scopeA.APIKey,
		"governance-deny", "governance-idem-deny", "governance-deny-session", "deny")
	if _, err := waitRuntimeExecution(ctx, pool, "governance-deny", func(value runtimeExecution) bool {
		return value.Status == "WAITING_APPROVAL"
	}); err != nil {
		t.Fatalf("wait DENY ASK execution: %v", err)
	}
	denial := waitGovernanceApproval(t, ctx, adminAPI, scopeA, "governance-deny")
	status, body, err = decideGovernanceApproval(ctx, adminServer.URL, operatorToken, scopeA, denial.ApprovalID, "deny")
	if err != nil {
		t.Fatalf("deny request: %v", err)
	}
	if status != http.StatusOK {
		t.Fatalf("deny status=%d body=%q", status, body)
	}
	denied, err := waitRuntimeTerminal(ctx, pool, "governance-deny")
	if err != nil {
		t.Fatalf("wait denied execution: %v", err)
	}
	if result := <-denyRequest; result.Err != nil || result.Status != http.StatusOK {
		t.Fatalf("denied request did not reach a terminal platform response: status=%d err=%v body=%s", result.Status, result.Err, result.Body)
	}
	deniedAudit, err := waitGovernanceAudit(ctx, adminAPI, scopeA, "governance-deny", "", 1)
	if err != nil {
		t.Fatalf("read denied governance audit: %v", err)
	}
	for _, event := range deniedAudit {
		if event.EventType == platformaudit.ToolCompleted {
			t.Fatalf("denied tool produced completion audit: %#v", event)
		}
	}
	evidence.Deny = governanceExecutionEvidence{
		RequestID: denied.RequestID, Worker: denied.Owner, Status: denied.Status, Attempt: denied.Attempt,
	}

	allowConfig := runtimeConfig(scopeA.TenantID, scopeA.AppID, "v2", "governance-allow-model")
	allowConfig.Tools = tenant.ToolPolicy{
		VisibleTools:    []string{"todo_write"},
		ExecutableTools: []string{"todo_write"},
	}
	allowConfig.Audit = tenant.AuditPolicy{
		Enabled:             true,
		RecordToolDecisions: true,
		RecordExecutions:    true,
		RedactPII:           true,
	}
	if err := adminAPI.PublishAppConfig(ctx, allowConfig); err != nil {
		t.Fatalf("publish allow config: %v", err)
	}
	if err := adminAPI.ActivateAppConfig(ctx, tenant.Scope{TenantID: scopeA.TenantID, AppID: scopeA.AppID}, allowConfig.Version); err != nil {
		t.Fatalf("activate allow config: %v", err)
	}
	allowRequest := launchRuntimeRequest(ctx, dataPlaneServer.URL, scopeA.APIKey,
		"governance-allow", "governance-idem-allow", "governance-allow-session", "allow")
	allowed, err := waitRuntimeExecution(ctx, pool, "governance-allow", func(value runtimeExecution) bool {
		return value.Status == "SUCCEEDED"
	})
	if err != nil {
		t.Fatalf("wait ALLOW execution: %v", err)
	}
	if err := requireRuntimeHTTPSuccess(<-allowRequest); err != nil {
		t.Fatalf("allowed request: %v", err)
	}
	allowAudit, err := waitGovernanceAudit(ctx, adminAPI, scopeA, "governance-allow", "", 1)
	if err != nil {
		t.Fatalf("read ALLOW audit: %v", err)
	}
	allowTypes := make(map[string]bool, len(allowAudit))
	for _, event := range allowAudit {
		allowTypes[event.EventType] = true
	}
	if !allowTypes[platformaudit.ToolAllowed] || !allowTypes[platformaudit.ToolCompleted] {
		t.Fatalf("ALLOW audit events = %#v", allowTypes)
	}
	evidence.Allow = governanceExecutionEvidence{
		RequestID: allowed.RequestID, Worker: allowed.Owner, Status: allowed.Status, Attempt: allowed.Attempt,
	}
	if err := adminAPI.ActivateAppConfig(ctx, tenant.Scope{TenantID: scopeA.TenantID, AppID: scopeA.AppID}, "v1"); err != nil {
		t.Fatalf("restore review-required config: %v", err)
	}

	// Seed an already-expired approval through the public repository boundary.
	// The worker still resolves it through the normal approval path; no database
	// state is changed directly by the test. This keeps expiry deterministic
	// without waiting for the production 15-minute default TTL.
	expiredArguments := []byte(`{"todos":[{"content":"approval e2e","activeForm":"Running approval e2e","status":"in_progress"}]}`)
	expiredApproval, err := store.ResolveOrCreate(ctx, platformapproval.Request{
		TenantID:       scopeA.TenantID,
		AppID:          scopeA.AppID,
		ConfigVersion:  "v1",
		RequestID:      "governance-expired",
		SessionID:      "governance-expired-session",
		ToolName:       "todo_write",
		ToolCallID:     "e2e-approval-call",
		ArgumentDigest: platformapproval.DigestArguments(expiredArguments),
		ExpiresAt:      time.Now().UTC().Add(-time.Second),
	})
	if err != nil {
		t.Fatalf("seed expired approval: %v", err)
	}
	if expiredApproval.Status != platformapproval.StatusPending {
		t.Fatalf("seed expired approval status = %q, want PENDING", expiredApproval.Status)
	}
	expiredRequest := launchRuntimeRequest(ctx, dataPlaneServer.URL, scopeA.APIKey,
		"governance-expired", "governance-idem-expired", "governance-expired-session", "expired")
	expiredExecution, err := waitRuntimeTerminal(ctx, pool, "governance-expired")
	if err != nil {
		t.Fatalf("wait expired execution: %v", err)
	}
	if expiredExecution.Status == "WAITING_APPROVAL" {
		t.Fatal("expired approval left execution waiting for approval")
	}
	if err := requireRuntimeHTTPSuccess(<-expiredRequest); err != nil {
		t.Fatalf("expired approval request: %v", err)
	}
	resolvedExpired := waitGovernanceApprovalStatus(t, ctx, adminAPI, scopeA,
		"governance-expired", platformapproval.StatusExpired)
	if _, err := waitGovernanceAudit(ctx, adminAPI, scopeA,
		"governance-expired", platformaudit.ApprovalExpired, 1); err != nil {
		t.Fatalf("wait expired approval audit: %v", err)
	}
	allExpiredAudit, err := adminAPI.ListAuditEvents(ctx, tenant.Scope{
		TenantID: scopeA.TenantID,
		AppID:    scopeA.AppID,
	}, 1000)
	if err != nil {
		t.Fatalf("list expired approval audit: %v", err)
	}
	expiredToolCompleted := 0
	for _, event := range allExpiredAudit {
		if event.RequestID == "governance-expired" && event.EventType == platformaudit.ToolCompleted {
			expiredToolCompleted++
		}
	}
	if expiredToolCompleted != 0 {
		t.Fatalf("expired approval produced %d tool completion audits", expiredToolCompleted)
	}
	evidence.Expire = governanceApprovalEvidence{
		RequestID: resolvedExpired.RequestID, ApprovalID: resolvedExpired.ApprovalID,
		TenantID: resolvedExpired.TenantID, ToolName: resolvedExpired.ToolName,
		Status: string(resolvedExpired.Status),
	}
	evidence.ExpireToolCompleted = expiredToolCompleted > 0

	stopRuntimeWorker(worker, true)
	crashing := startRuntimeWorker(t, ctx, workerBinary, workerEnv, "governance-worker-crash", "approval-block-final")
	crashRequest := launchRuntimeRequest(ctx, dataPlaneServer.URL, scopeA.APIKey,
		"governance-crash", "governance-idem-crash", "governance-crash-session", "crash")
	if _, err := waitRuntimeExecution(ctx, pool, "governance-crash", func(value runtimeExecution) bool {
		return value.Status == "WAITING_APPROVAL"
	}); err != nil {
		t.Fatalf("wait crash ASK execution: %v", err)
	}
	crashApproval := waitGovernanceApproval(t, ctx, adminAPI, scopeA, "governance-crash")
	if status, body, err := decideGovernanceApproval(ctx, adminServer.URL, operatorToken, scopeA, crashApproval.ApprovalID, "approve"); err != nil || status != http.StatusOK {
		t.Fatalf("approve crash continuation status=%d body=%q err=%v", status, body, err)
	}
	if _, err := waitRuntimeExecution(ctx, pool, "governance-crash", func(value runtimeExecution) bool {
		return value.Status == "RUNNING" && value.Owner == crashing.owner
	}); err != nil {
		t.Fatalf("wait approved continuation ownership: %v", err)
	}
	if _, err := waitGovernanceAudit(ctx, adminAPI, scopeA, "governance-crash", platformaudit.ToolCompleted, 1); err != nil {
		t.Fatalf("wait tool completion before crash: %v", err)
	}
	stopRuntimeWorker(crashing, true)
	reclaimer := startRuntimeWorker(t, ctx, workerBinary, workerEnv, "governance-worker-recovery", "approval")
	reclaimed, err := waitRuntimeExecution(ctx, pool, "governance-crash", func(value runtimeExecution) bool {
		return value.Status == "RUNNING" && value.Owner == reclaimer.owner && value.Attempt >= 2
	})
	if err != nil {
		t.Fatalf("wait crash continuation reclaim: %v", err)
	}
	if err := requireRuntimeHTTPSuccess(<-crashRequest); err != nil {
		t.Fatalf("recovered approval continuation request: %v", err)
	}
	crashTerminal, err := waitRuntimeExecution(ctx, pool, "governance-crash", func(value runtimeExecution) bool {
		return value.Status == "SUCCEEDED"
	})
	if err != nil {
		t.Fatalf("wait crash continuation terminal: %v", err)
	}
	if crashTerminal.Attempt < reclaimed.Attempt || crashTerminal.Owner != "" {
		t.Fatalf("crash continuation terminal = %#v", crashTerminal)
	}
	toolCompleted, err := waitGovernanceAudit(ctx, adminAPI, scopeA, "governance-crash", platformaudit.ToolCompleted, 1)
	if err != nil {
		t.Fatalf("read crash tool audit: %v", err)
	}
	if len(toolCompleted) != 1 {
		t.Fatalf("crash continuation tool completion count=%d, want 1", len(toolCompleted))
	}
	evidence.CrashContinuation = governanceExecutionEvidence{
		RequestID: crashTerminal.RequestID, Worker: reclaimed.Owner, Status: crashTerminal.Status,
		Attempt: crashTerminal.Attempt,
	}

	auditEvents, err := waitGovernanceAudit(ctx, adminAPI, scopeA, "governance-approve", "", 6)
	if err != nil {
		t.Fatalf("read governance audit: %v", err)
	}
	counts := make(map[string]int)
	for _, event := range auditEvents {
		counts[event.EventType]++
	}
	for _, required := range []string{
		platformaudit.ApprovalCreated,
		platformaudit.ApprovalApproved,
		platformaudit.ToolReviewRequired,
		platformaudit.ToolAllowed,
		platformaudit.ToolCompleted,
		platformaudit.ExecutionStarted,
		platformaudit.ExecutionCompleted,
	} {
		if counts[required] == 0 {
			t.Fatalf("missing governance audit event %q in %#v", required, counts)
		}
	}
	encoded, err := json.Marshal(auditEvents)
	if err != nil {
		t.Fatalf("encode governance audit: %v", err)
	}
	if bytes.Contains(encoded, []byte("approval e2e")) {
		t.Fatal("governance audit contains raw tool arguments")
	}
	evidence.AuditEventCounts = counts
}

func seedGovernanceScope(t *testing.T, ctx context.Context, store *platformpostgres.Store, tenantID, appID string) runtimeScope {
	t.Helper()
	api := admin.API{Repository: store}
	if err := api.CreateTenant(ctx, tenant.Tenant{
		ID: tenantID, Name: "Governance Tenant", Status: tenant.StatusActive,
		Audit: tenant.AuditPolicy{
			Enabled:             true,
			RecordToolDecisions: true,
			RecordExecutions:    true,
			RedactPII:           true,
		},
	}); err != nil {
		t.Fatalf("create governance tenant %s: %v", tenantID, err)
	}
	cfg := runtimeConfig(tenantID, appID, "v1", "governance-model")
	cfg.Tools = tenant.ToolPolicy{
		VisibleTools:        []string{"todo_write"},
		ExecutableTools:     []string{"todo_write"},
		ReviewRequiredTools: []string{"todo_write"},
	}
	cfg.Audit = tenant.AuditPolicy{
		Enabled:             true,
		RecordToolDecisions: true,
		RecordExecutions:    true,
		RedactPII:           true,
	}
	if err := api.CreateAgentApp(ctx, tenant.AgentApp{
		TenantID: tenantID, AppID: appID, Name: "Governance", ActiveConfigVersion: cfg.Version, Status: tenant.StatusActive,
	}, cfg); err != nil {
		t.Fatalf("create governance app %s/%s: %v", tenantID, appID, err)
	}
	issued, err := api.IssueCredential(ctx, tenant.Scope{TenantID: tenantID, AppID: appID}, time.Time{})
	if err != nil {
		t.Fatalf("issue governance credential %s/%s: %v", tenantID, appID, err)
	}
	return runtimeScope{TenantID: tenantID, AppID: appID, APIKey: issued.APIKey, CredentialID: issued.Credential.ID}
}

func waitGovernanceApproval(t *testing.T, ctx context.Context, api admin.API, scope runtimeScope, requestID string) platformapproval.Record {
	return waitGovernanceApprovalStatus(t, ctx, api, scope, requestID, "")
}

func waitGovernanceApprovalStatus(t *testing.T, ctx context.Context, api admin.API, scope runtimeScope, requestID string, status platformapproval.Status) platformapproval.Record {
	t.Helper()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		approvals, err := api.ListApprovalsForPrincipal(ctx, admin.AdminPrincipal{
			Role: admin.RoleSystemAdmin, ActorID: "governance-e2e",
		}, platformapproval.Query{TenantID: scope.TenantID, AppID: scope.AppID, Limit: 100})
		if err != nil {
			t.Fatalf("list governance approvals: %v", err)
		}
		for _, approval := range approvals {
			if approval.RequestID == requestID && (status == "" || approval.Status == status) {
				return approval
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait governance approval %s: %v", requestID, ctx.Err())
		case <-ticker.C:
		}
	}
}

func decideGovernanceApproval(ctx context.Context, endpoint, token string, scope runtimeScope, approvalID, action string) (int, []byte, error) {
	query := url.Values{}
	query.Set("tenant_id", scope.TenantID)
	query.Set("app_id", scope.AppID)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost,
		endpoint+"/admin/v1/approvals/"+url.PathEscape(approvalID)+"/"+action+"?"+query.Encode(), nil)
	if err != nil {
		return 0, nil, err
	}
	request.Header.Set("Authorization", "Bearer "+token)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	return response.StatusCode, body, err
}

func waitGovernanceAudit(ctx context.Context, api admin.API, scope runtimeScope, requestID, eventType string, minimum int) ([]platformaudit.Event, error) {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		allEvents, err := api.ListAuditEvents(ctx, tenant.Scope{
			TenantID: scope.TenantID,
			AppID:    scope.AppID,
		}, 1000)
		if err != nil {
			return nil, err
		}
		events := make([]platformaudit.Event, 0, len(allEvents))
		for _, event := range allEvents {
			if (requestID == "" || event.RequestID == requestID) &&
				(eventType == "" || event.EventType == eventType) {
				events = append(events, event)
			}
		}
		if len(events) >= minimum {
			return events, nil
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("wait governance audit %s/%s: %w", requestID, eventType, ctx.Err())
		case <-ticker.C:
		}
	}
}

type governanceEvidence struct {
	Stream              string                      `json:"stream"`
	Tenants             []governanceTenantEvidence  `json:"tenants"`
	Ask                 governanceApprovalEvidence  `json:"ask"`
	Expire              governanceApprovalEvidence  `json:"expire"`
	ExpireToolCompleted bool                        `json:"expire_tool_completed"`
	Approve             governanceExecutionEvidence `json:"approve"`
	Allow               governanceExecutionEvidence `json:"allow"`
	Deny                governanceExecutionEvidence `json:"deny"`
	CrashContinuation   governanceExecutionEvidence `json:"crash_continuation"`
	CrossTenantDenied   bool                        `json:"cross_tenant_denied"`
	AuditEventCounts    map[string]int              `json:"audit_event_counts"`
}

type governanceTenantEvidence struct {
	TenantID string `json:"tenant_id"`
	AppID    string `json:"app_id"`
}

type governanceApprovalEvidence struct {
	RequestID  string `json:"request_id"`
	ApprovalID string `json:"approval_id"`
	TenantID   string `json:"tenant_id"`
	ToolName   string `json:"tool_name"`
	Status     string `json:"status"`
}

type governanceExecutionEvidence struct {
	RequestID string `json:"request_id"`
	Worker    string `json:"worker"`
	Status    string `json:"status"`
	Attempt   int    `json:"attempt"`
}

func writeGovernanceEvidence(t *testing.T, evidence governanceEvidence) {
	t.Helper()
	path := strings.TrimSpace(os.Getenv(governanceReportPathEnv))
	if path == "" {
		t.Logf("governance e2e evidence: %+v", evidence)
		return
	}
	encoded, err := json.MarshalIndent(evidence, "", "  ")
	if err != nil {
		t.Logf("encode governance evidence: %v", err)
		return
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Logf("create governance evidence directory: %v", err)
		return
	}
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Logf("write governance evidence: %v", err)
	}
}
