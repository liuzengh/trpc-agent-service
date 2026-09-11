package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	platformskill "github.com/liuzengh/trpc-agent-service/trpcservice/skill"
	platformtool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
)

func validationFixture(t *testing.T) (*Service, *controlplane.MemoryRepository, controlplane.AgentRevision) {
	t.Helper()
	repo := controlplane.NewMemoryRepository(controlplane.DefaultBootstrapData())
	t.Cleanup(func() { _ = repo.Close() })
	skillRoot := t.TempDir()
	if err := os.Mkdir(filepath.Join(skillRoot, "preflight-check"), 0700); err != nil {
		t.Fatal(err)
	}
	for name, content := range map[string]string{
		"catalog.json":             `[{"name":"preflight-check","version":"1","directory":"preflight-check"}]`,
		"preflight-check/SKILL.md": "---\nname: preflight-check\ndescription: Revision validation fixture\n---\nCheck revision validation rules.\n",
		"preflight-check/run.sh":   "#!/bin/sh\nexit 0\n",
	} {
		if err := os.WriteFile(filepath.Join(skillRoot, name), []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
	}
	registry, err := platformskill.Load(skillRoot, `[{"tenant_id":"tutorial-tenant","name":"preflight-check","version":"1"}]`)
	if err != nil {
		t.Fatal(err)
	}
	toolService := &platformskill.Service{Registry: registry, Repository: repo}
	s, err := New(repo, platformtool.DefaultCatalog(toolService.RunTool()))
	if err != nil {
		t.Fatal(err)
	}
	s.WithSkills(registry)
	revision := controlplane.DefaultBootstrapData().Revisions[0]
	revision.ID, revision.RevisionNo = "preflight-revision", 2
	revision.CreatedBy = "fixture"
	revision.AgentConfig, _ = json.Marshal(map[string]any{"name": "fixture", "instruction": "test", "skills": []platformskill.Ref{registry.List("tutorial-tenant")[0].Ref}})
	revision.ToolPolicy = json.RawMessage(`{"allowed_tools":["skill_load","skill_run"],"max_tool_calls":1}`)
	return s, repo, revision
}

func hasIssue(report ValidationReport, code string) bool {
	for _, issue := range report.Issues {
		if issue.Code == code {
			return true
		}
	}
	return false
}

func TestRevisionPreflightBudgetSharedByAllMutations(t *testing.T) {
	s, repo, revision := validationFixture(t)
	ctx := context.Background()
	original := string(revision.ToolPolicy)
	report, err := s.ValidateRevision(ctx, revision)
	if err != nil || report.Valid || !hasIssue(report, "skill_tool_budget_too_low") {
		t.Fatalf("report=%+v err=%v", report, err)
	}
	if report.RuntimeStatus != "unknown" || !hasIssue(report, "runtime_not_verified") {
		t.Fatal("static check falsely claimed runtime readiness")
	}
	if _, err := repo.GetRevision(ctx, revision.TenantID, revision.ID); !errors.Is(err, controlplane.ErrNotFound) {
		t.Fatal("preflight persisted a revision")
	}
	if _, err := s.CreateRevision(ctx, revision); !errors.Is(err, ErrInvalid) {
		t.Fatal("create bypassed validation", err)
	}
	// Simulate an already-published legacy configuration without mutating it.
	if err := repo.CreateRevision(ctx, revision); err != nil {
		t.Fatal(err)
	}
	app, _ := repo.GetAgentApp(ctx, revision.TenantID, revision.AppID)
	if _, err := s.PublishRevision(ctx, revision.TenantID, revision.AppID, revision.ID, app.Version); !errors.Is(err, ErrInvalid) {
		t.Fatal("publish bypassed validation", err)
	}
	if _, err := s.UpdateRolloutPolicy(ctx, revision.TenantID, revision.AppID, json.RawMessage(`{"canary_revision_id":"preflight-revision","canary_percent":10}`), app.Version); !errors.Is(err, ErrInvalid) {
		t.Fatal("canary bypassed validation", err)
	}
	current, _ := repo.GetAgentApp(ctx, revision.TenantID, revision.AppID)
	stored, _ := repo.GetRevision(ctx, revision.TenantID, revision.ID)
	if current.Version != app.Version || current.StableRevisionID != app.StableRevisionID || string(stored.ToolPolicy) != original || string(revision.ToolPolicy) != original {
		t.Fatal("invalid preflight changed existing state")
	}
	for _, limit := range []string{"2", "4", "0"} {
		revision.ToolPolicy = json.RawMessage(`{"allowed_tools":["skill_load","skill_run"],"max_tool_calls":` + limit + `}`)
		report, err = s.ValidateRevision(ctx, revision)
		if err != nil || !report.Valid || (limit == "0" && !hasIssue(report, "tool_budget_unlimited")) {
			t.Fatalf("limit=%s report=%+v err=%v", limit, report, err)
		}
	}
}

func TestPreflightReadinessMustBeFreshAndFromWorker(t *testing.T) {
	s, _, revision := validationFixture(t)
	revision.ToolPolicy = json.RawMessage(`{"allowed_tools":["skill_load","skill_run"],"max_tool_calls":4}`)
	for _, tc := range []struct {
		name, source    string
		age             time.Duration
		wantUnavailable bool
	}{
		{"fresh", "local_worker", time.Second, true}, {"stale", "local_worker", time.Minute, false}, {"future", "local_worker", -time.Minute, false}, {"admin-not-worker", "admin", time.Second, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s.WithDependencyObservations(func() []DependencyCheck {
				return []DependencyCheck{{Component: "sandbox", State: "unavailable", Source: tc.source, ObservedAt: time.Now().Add(-tc.age)}}
			})
			r, err := s.ValidateRevision(context.Background(), revision)
			if err != nil || hasIssue(r, "sandbox_unavailable") != tc.wantUnavailable || r.Valid == tc.wantUnavailable {
				t.Fatalf("%+v %v", r, err)
			}
		})
	}
	s.WithDependencyObservations(func() []DependencyCheck {
		return []DependencyCheck{{Component: "worker", State: "unavailable", Source: "local_worker", ObservedAt: time.Now()}}
	})
	r, err := s.ValidateRevision(context.Background(), revision)
	if err != nil || r.Valid || !hasIssue(r, "worker_unavailable") {
		t.Fatal("unavailable worker accepted", r, err)
	}
}

func TestPreflightDoesNotResolveCredentialsOrCallModel(t *testing.T) {
	s, _, revision := validationFixture(t)
	revision.ToolPolicy = json.RawMessage(`{"allowed_tools":["skill_load","skill_run"],"max_tool_calls":4}`)
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests.Add(1) }))
	defer server.Close()
	authorizer, err := secret.NewEnvStore([]secret.Grant{{TenantID: revision.TenantID, Purpose: secret.Model, Reference: "env://PREFLIGHT_TEST_KEY"}})
	if err != nil {
		t.Fatal(err)
	}
	s.WithSecretAuthorizer(authorizer)
	t.Setenv("PREFLIGHT_TEST_KEY", "") // authorization exists; value is deliberately unavailable
	revision.ModelConfig, _ = json.Marshal(map[string]any{"source": "revision", "provider": "openai", "name": "fixture", "base_url": server.URL, "api_key_ref": "env://PREFLIGHT_TEST_KEY"})
	r, err := s.ValidateRevision(context.Background(), revision)
	if err != nil || !r.Valid || requests.Load() != 0 || r.RuntimeStatus != "unknown" {
		t.Fatalf("preflight performed execution: %+v %v", r, err)
	}
	s.WithSecretAuthorizer(nil)
	r, err = s.ValidateRevision(context.Background(), revision)
	if err != nil || !hasIssue(r, "secret_not_authorized") {
		t.Fatal("revoked grant accepted", r, err)
	}
}

func TestPreflightFieldErrorsNeverEchoSensitiveInput(t *testing.T) {
	s, _, revision := validationFixture(t)
	revision.ModelConfig = json.RawMessage(`{"source":"private-canary","unknown-private-canary":"private-canary"}`)
	revision.GuardrailConfig = json.RawMessage(`{"private-canary":true}`)
	r, err := s.ValidateRevision(context.Background(), revision)
	if err != nil || !hasIssue(r, "invalid_model_config") || !hasIssue(r, "invalid_guardrail_config") {
		t.Fatal(r, err)
	}
	raw, _ := json.Marshal(r)
	if bytes.Contains(raw, []byte("private-canary")) {
		t.Fatal("validation leaked user input")
	}
	revision.ModelConfig = json.RawMessage(`null`)
	r, err = s.ValidateRevision(context.Background(), revision)
	if err != nil || !hasIssue(r, "invalid_config_object") {
		t.Fatal("null configuration accepted")
	}
}

func TestPreflightKnowledgeBackendAndPermissionChanges(t *testing.T) {
	s, repo, revision := validationFixture(t)
	revision.ToolPolicy = json.RawMessage(`{"allowed_tools":["skill_load","skill_run"],"max_tool_calls":4}`)
	revision.KnowledgeConfig = json.RawMessage(`{"enabled":true,"embedding":{"provider":"hash","dimensions":32}}`)
	r, err := s.ValidateRevision(context.Background(), revision)
	if err != nil || !hasIssue(r, "knowledge_backend_missing") {
		t.Fatal(r, err)
	}
	if err := repo.CreateBackendBinding(context.Background(), controlplane.BackendBinding{ID: "fixture-knowledge", TenantID: revision.TenantID, AppID: revision.AppID, ResourceType: "knowledge", BackendType: "inmemory", MigrationState: "active", Config: json.RawMessage(`{"dimensions":64}`)}); err != nil {
		t.Fatal(err)
	}
	r, err = s.ValidateRevision(context.Background(), revision)
	if err != nil || !hasIssue(r, "knowledge_dimensions_mismatch") {
		t.Fatal(r, err)
	}
	revision.KnowledgeConfig = json.RawMessage(`{}`)
	revision.ToolPolicy = json.RawMessage(`{"allowed_tools":["skill_run"],"max_tool_calls":4}`)
	r, err = s.ValidateRevision(context.Background(), revision)
	if err != nil || !hasIssue(r, "skill_load_not_allowed") {
		t.Fatal(r, err)
	}
	// Loading instructions without enabling execution needs no sandbox or second call.
	revision.ToolPolicy = json.RawMessage(`{"allowed_tools":["skill_load"],"max_tool_calls":1}`)
	r, err = s.ValidateRevision(context.Background(), revision)
	if err != nil || !r.Valid {
		t.Fatal("read-only Skill rejected", r, err)
	}
}

func TestPreflightEndpointRBACAndStructuredMutationError(t *testing.T) {
	s, _, revision := validationFixture(t)
	const writer = "preflight-writer-token-1234567890"
	const reader = "preflight-reader-token-1234567890"
	h, err := NewHandlerWithPrincipals(s, []Principal{{Name: "writer", Token: writer, Role: RoleTenantAdmin, TenantIDs: []string{"tutorial-tenant"}}, {Name: "reader", Token: reader, Role: RoleAuditor, TenantIDs: []string{"tutorial-tenant"}}})
	if err != nil {
		t.Fatal(err)
	}
	call := func(route, token, origin string, rev controlplane.AgentRevision) *httptest.ResponseRecorder {
		body, _ := json.Marshal(rev)
		req := httptest.NewRequest("POST", route, bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+token)
		if origin != "" {
			req.Header.Set("Origin", origin)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		return w
	}
	for _, tc := range []struct {
		token, origin string
		status        int
	}{{"", "", 401}, {reader, "", 403}, {writer, "https://evil.invalid", 403}, {writer, "", 200}} {
		if w := call("/admin/revisions/validate", tc.token, tc.origin, revision); w.Code != tc.status {
			t.Fatalf("status=%d expected=%d", w.Code, tc.status)
		}
	}
	w := call("/admin/revisions", writer, "", revision)
	if w.Code != 400 || !strings.Contains(w.Body.String(), `"code":"configuration_invalid"`) || !strings.Contains(w.Body.String(), "skill_tool_budget_too_low") {
		t.Fatal(w.Code, w.Body.String())
	}
	revision.TenantID = "other-tenant"
	if w := call("/admin/revisions/validate", writer, "", revision); w.Code != 403 {
		t.Fatal("cross-tenant validation permitted")
	}
}

func TestPublishRechecksModelAndRevokedCredentials(t *testing.T) {
	for _, testCase := range []string{"invalid_model_config", "secret_not_authorized"} {
		t.Run(testCase, func(t *testing.T) {
			s, repo, revision := validationFixture(t)
			revision.ToolPolicy = json.RawMessage(`{"allowed_tools":["skill_load","skill_run"],"max_tool_calls":4}`)
			if testCase == "invalid_model_config" {
				revision.ModelConfig = json.RawMessage(`{"source":"unsupported-source"}`)
			} else {
				revision.ModelConfig = json.RawMessage(`{"source":"revision","provider":"openai","name":"fixture","api_key_ref":"env://REVOKED_KEY"}`)
			}
			ctx := context.Background()
			if err := repo.CreateRevision(ctx, revision); err != nil {
				t.Fatal(err)
			}
			app, _ := repo.GetAgentApp(ctx, revision.TenantID, revision.AppID)
			_, err := s.PublishRevision(ctx, revision.TenantID, revision.AppID, revision.ID, app.Version)
			var invalid *ValidationError
			if !errors.As(err, &invalid) || !hasIssue(invalid.Report, testCase) {
				t.Fatalf("publication bypassed full validation: %v", err)
			}
			_, err = s.UpdateRolloutPolicy(ctx, revision.TenantID, revision.AppID, json.RawMessage(`{"canary_revision_id":"preflight-revision","canary_percent":10}`), app.Version)
			if !errors.As(err, &invalid) || !hasIssue(invalid.Report, testCase) {
				t.Fatalf("canary bypassed full validation: %v", err)
			}
		})
	}
}
