package tenant_test

import (
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestRuntimeContextValidateAndScopedKey(t *testing.T) {
	tc := tenant.RuntimeContext{
		TenantID:           "tenant-a",
		AppID:              "support",
		ConfigVersion:      "v1",
		SessionID:          "group/thread-1",
		SessionPrincipalID: "group-1",
		UserID:             "user-1",
		TraceID:            "trace-1",
	}
	if err := tc.Validate(); err != nil {
		t.Fatalf("validate runtime context: %v", err)
	}

	key, err := tc.Scope().Key("session", tc.SessionPrincipalID, tc.SessionID)
	if err != nil {
		t.Fatalf("build scoped key: %v", err)
	}
	const want = "tenant:tenant-a:app:support:session:group-1:group%2Fthread-1"
	if key != want {
		t.Fatalf("scoped key = %q, want %q", key, want)
	}
}

func TestScopeKeyEscapesTenantAndAppSegments(t *testing.T) {
	scope, err := tenant.NewScope("tenant/a", "support:b")
	if err != nil {
		t.Fatalf("new scope: %v", err)
	}
	key, err := scope.Key("runner")
	if err != nil {
		t.Fatalf("build runner key: %v", err)
	}
	const want = "tenant:tenant%2Fa:app:support%3Ab:runner"
	if key != want {
		t.Fatalf("runner key = %q, want %q", key, want)
	}
}

func TestRuntimeContextValidateRejectsMissingTenant(t *testing.T) {
	tc := tenant.RuntimeContext{
		AppID:              "support",
		ConfigVersion:      "v1",
		SessionID:          "session-1",
		SessionPrincipalID: "user-1",
	}
	if err := tc.Validate(); err == nil {
		t.Fatal("validate runtime context succeeded with missing tenant_id")
	}
}

func TestRuntimeContextValidateRejectsMissingSenderOrTrace(t *testing.T) {
	tests := []struct {
		name string
		edit func(*tenant.RuntimeContext)
		want string
	}{
		{
			name: "user id",
			edit: func(tc *tenant.RuntimeContext) { tc.UserID = "" },
			want: "user_id is required",
		},
		{
			name: "trace id",
			edit: func(tc *tenant.RuntimeContext) { tc.TraceID = "" },
			want: "trace_id is required",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tc := tenant.RuntimeContext{
				TenantID:           "tenant-a",
				AppID:              "support",
				ConfigVersion:      "v1",
				SessionID:          "session-1",
				SessionPrincipalID: "user-1",
				UserID:             "user-1",
				TraceID:            "trace-1",
			}
			tt.edit(&tc)
			if err := tc.Validate(); err == nil || err.Error() != tt.want {
				t.Fatalf("validate runtime context error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestModelConfigValidateRejectsIncompleteAPIKeyRef(t *testing.T) {
	cfg := validAppConfig()
	cfg.Model.APIKeyRef = tenant.SecretRef{Version: "v1"}
	if err := cfg.Validate(); err == nil {
		t.Fatal("validate app config succeeded with incomplete model api key ref")
	}
}

func TestModelConfigValidateRejectsUnsupportedProvider(t *testing.T) {
	config := tenant.ModelConfig{
		Provider:  "anthropic",
		Model:     "claude",
		APIKeyRef: tenant.SecretRef{Name: "model-key"},
	}
	if err := config.Validate(); err == nil {
		t.Fatal("model config accepted provider without a production resolver")
	}
}

func TestAppConfigValidateRejectsDuplicateTool(t *testing.T) {
	cfg := validAppConfig()
	cfg.Tools.VisibleTools = []string{"search", "search"}

	if err := cfg.Validate(); err == nil {
		t.Fatal("validate app config succeeded with duplicate visible tools")
	}
}

func TestToolPolicyDefaultsToDeny(t *testing.T) {
	policy := tenant.ToolPolicy{
		VisibleTools:    []string{"search", "read"},
		ExecutableTools: []string{"search"},
	}

	if !policy.CanView("read") {
		t.Fatal("read tool is not visible")
	}
	if policy.CanExecute("read") {
		t.Fatal("read tool is executable without permission")
	}
	if !policy.CanExecute("search") {
		t.Fatal("search tool is not executable")
	}
	if policy.CanView("write") || policy.CanExecute("write") {
		t.Fatal("unknown tool is permitted")
	}
	if (tenant.ToolPolicy{}).CanView("") || (tenant.ToolPolicy{}).CanExecute("") {
		t.Fatal("zero policy permits an empty tool name")
	}
}

func TestToolPolicyReviewRequiresExecutionPermission(t *testing.T) {
	policy := tenant.ToolPolicy{
		VisibleTools:        []string{"search", "delete"},
		ExecutableTools:     []string{"search", "delete"},
		ReviewRequiredTools: []string{"delete"},
	}
	if err := policy.Validate(); err != nil {
		t.Fatalf("validate tool policy: %v", err)
	}
	if !policy.CanView("delete") || !policy.CanExecute("delete") || !policy.RequiresReview("delete") {
		t.Fatal("review-required tool did not retain visible/executable/review permissions")
	}
	if policy.RequiresReview("search") {
		t.Fatal("ordinary executable tool unexpectedly requires review")
	}

	policy.ReviewRequiredTools = []string{"hidden"}
	if err := policy.Validate(); err == nil {
		t.Fatal("review-required non-executable tool was accepted")
	}
}

func TestIMAccessPolicyEmptyAndAllowlistedSemantics(t *testing.T) {
	if !(tenant.IMAccessPolicy{}).Allows("user-a", "conversation-a") {
		t.Fatal("empty IM access policy imposed an unexpected restriction")
	}
	policy := tenant.IMAccessPolicy{
		AllowedUsers:         []string{"user-a"},
		AllowedConversations: []string{"conversation-b"},
	}
	if !policy.Allows("user-a", "conversation-x") {
		t.Fatal("allowlisted user was rejected")
	}
	if !policy.Allows("user-x", "conversation-b") {
		t.Fatal("allowlisted conversation was rejected")
	}
	if policy.Allows("user-x", "conversation-x") {
		t.Fatal("unlisted user and conversation were accepted")
	}
}

func TestBudgetPolicyValidation(t *testing.T) {
	if err := (tenant.BudgetPolicy{}).Validate(); err != nil {
		t.Fatalf("validate unlimited budget: %v", err)
	}
	if err := (tenant.BudgetPolicy{MaxTokensPerExecution: -1}).Validate(); err == nil {
		t.Fatal("negative token budget was accepted")
	}
}

func TestAuditPolicyCannotWidenTenantPolicy(t *testing.T) {
	tests := []struct {
		name string
		app  tenant.AuditPolicy
		want string
	}{
		{
			name: "tool decisions",
			app:  tenant.AuditPolicy{Enabled: true, RecordToolDecisions: true, RedactPII: true},
			want: "tool audit",
		},
		{
			name: "retention",
			app:  tenant.AuditPolicy{Enabled: true, RetentionDays: 31, RedactPII: true},
			want: "retention",
		},
		{
			name: "redaction",
			app:  tenant.AuditPolicy{Enabled: true},
			want: "redaction",
		},
	}
	parent := tenant.AuditPolicy{
		Enabled:             true,
		RecordToolDecisions: false,
		RecordExecutions:    true,
		RetentionDays:       30,
		RedactPII:           true,
	}
	if err := (tenant.AuditPolicy{}).ValidateAppConfig(tenant.AuditPolicy{Enabled: true}); err == nil || !strings.Contains(err.Error(), "tenant audit is disabled") {
		t.Fatalf("disabled tenant error = %v", err)
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := parent.ValidateAppConfig(tt.app); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want substring %q", err, tt.want)
			}
		})
	}
}

func TestAuditPolicyZeroRetentionInheritsAtApplicationScope(t *testing.T) {
	parent := tenant.AuditPolicy{Enabled: true, RecordExecutions: true}
	if err := parent.ValidateAppConfig(tenant.AuditPolicy{Enabled: true, RecordExecutions: true, RetentionDays: 365}); err != nil {
		t.Fatalf("zero tenant retention rejected finite app retention: %v", err)
	}
	if got := (tenant.AuditPolicy{RetentionDays: 30}).EffectiveRetentionDays(tenant.AuditPolicy{}); got != 30 {
		t.Fatalf("zero app retention = %d, want tenant retention 30", got)
	}
	if got := parent.EffectiveRetentionDays(tenant.AuditPolicy{RetentionDays: 365}); got != 365 {
		t.Fatalf("finite app retention under unlimited tenant = %d, want 365", got)
	}
}

func validAppConfig() tenant.AppConfig {
	return tenant.AppConfig{
		TenantID: "tenant-a",
		AppID:    "support",
		Version:  "v1",
		Model: tenant.ModelConfig{
			Provider:   "openai",
			Model:      "gpt-4.1-mini",
			APIKeyRef:  tenant.SecretRef{Name: "model-api-key", Version: "v1"},
			Parameters: map[string]string{"temperature": "0"},
		},
		Tools: tenant.ToolPolicy{
			VisibleTools:    []string{"search"},
			ExecutableTools: []string{"search"},
		},
		BackendConfig: validBackendConfig(),
		SecretRefs: []tenant.SecretRef{
			{Name: "model-api-key", Version: "v1"},
		},
		ChannelBinding: []string{"binding-1"},
	}
}

func validBackendConfig() tenant.BackendConfig {
	return tenant.BackendConfig{
		Name: "default",
		Session: tenant.BackendRef{
			Kind:     tenant.BackendSQL,
			Provider: "postgres",
			Name:     "session-sql",
			Options:  map[string]string{"schema": "agent"},
		},
		Memory: tenant.BackendRef{
			Kind:     tenant.BackendRedis,
			Provider: "redis",
			Name:     "memory-redis",
		},
	}
}
