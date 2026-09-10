package admin_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/admin"
	platformaudit "github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/auth"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestAPIValidateAppConfigChecksChannelBindings(t *testing.T) {
	cfg := testAppConfig()
	binding := testBinding()
	bindings, err := config.NewStaticBindingResolver(binding)
	if err != nil {
		t.Fatalf("new binding resolver: %v", err)
	}
	api := admin.API{Bindings: bindings}

	if err := api.ValidateAppConfig(context.Background(), cfg); err != nil {
		t.Fatalf("validate app config: %v", err)
	}
}

func TestAPIValidateAppConfigRejectsMissingChannelBinding(t *testing.T) {
	api := admin.API{}
	if err := api.ValidateAppConfig(context.Background(), testAppConfig()); err == nil {
		t.Fatal("validate app config succeeded without binding resolver")
	}
}

func TestAPIValidateAppConfigAllowsKnowledgeWithoutArtifact(t *testing.T) {
	cfg := testAppConfig()
	cfg.BackendConfig.Knowledge = tenant.BackendRef{
		Kind:     tenant.BackendVector,
		Provider: "qdrant",
		Name:     "shared-qdrant",
		Options: map[string]string{
			"embedding_model":      "text-embedding-3-small",
			"embedding_dimensions": "1536",
			"embedding_profile":    "text-embedding-3-small",
			"index_generation":     "g1",
		},
	}
	cfg.KnowledgeBaseIDs = []string{"kb-1"}
	bindings, err := config.NewStaticBindingResolver(testBinding())
	if err != nil {
		t.Fatalf("new binding resolver: %v", err)
	}
	if err := (admin.API{Bindings: bindings}).ValidateAppConfig(context.Background(), cfg); err != nil {
		t.Fatalf("validate app config with Knowledge but no Artifact COS: %v", err)
	}
}

func TestAPIRejectsCallerSuppliedChannelBindingRoute(t *testing.T) {
	api := admin.API{Repository: &recordingRepository{}}
	binding := testBinding()
	if _, err := api.ProvisionChannelBinding(context.Background(), binding); err == nil {
		t.Fatal("provision channel binding accepted caller-supplied route")
	}
}

func TestAPIProvisionLongConnectionBindingDoesNotCreatePublicRoute(t *testing.T) {
	repository := &recordingRepository{}
	binding := testBinding()
	binding.PublicRouteID = ""
	binding.BindingRevision = 0
	prepared, err := (admin.API{Repository: repository}).ProvisionChannelBinding(context.Background(), binding)
	if err != nil {
		t.Fatalf("provision long-connection binding: %v", err)
	}
	if prepared.PublicRouteID != "" || prepared.BindingRevision != 1 {
		t.Fatalf("prepared binding = %#v, want no route and revision 1", prepared)
	}
}

func TestAPIRejectsWeChatCustomerBindingWithoutIngress(t *testing.T) {
	binding := testBinding()
	binding.Channel = channels.ChannelWeChatCustomer
	binding.PublicRouteID = ""
	binding.BindingRevision = 0
	if _, err := (admin.API{Repository: &recordingRepository{}}).ProvisionChannelBinding(context.Background(), binding); !errors.Is(err, channels.ErrChannelNotProvisionable) {
		t.Fatalf("provision wechat customer binding error = %v, want no-ingress error", err)
	}
}

func TestAPIRejectsActivatingLegacyWeChatCustomerBinding(t *testing.T) {
	repository := &recordingRepository{binding: testBinding()}
	repository.binding.Channel = channels.ChannelWeChatCustomer
	repository.binding.Status = channels.BindingSuspended
	_, err := (admin.API{Repository: repository}).SetChannelBindingStatus(
		context.Background(), tenant.Scope{TenantID: repository.binding.TenantID, AppID: repository.binding.AppID},
		repository.binding.BindingID, channels.BindingActive,
	)
	if !errors.Is(err, channels.ErrChannelNotProvisionable) {
		t.Fatalf("activate legacy wechat customer binding error = %v, want no-ingress error", err)
	}
}

func TestAPIIssueCredentialRejectsExpiredCredential(t *testing.T) {
	api := admin.API{Repository: &recordingRepository{}}
	_, err := api.IssueCredential(
		context.Background(),
		tenant.Scope{TenantID: "tenant-a", AppID: "support"},
		time.Now().Add(-time.Second),
	)
	if err == nil {
		t.Fatal("issue credential succeeded with expired time")
	}
}

func TestAPIListAuditEventsRejectsInvalidLimit(t *testing.T) {
	api := admin.API{Repository: &recordingRepository{}}
	_, err := api.ListAuditEvents(
		context.Background(),
		tenant.Scope{TenantID: "tenant-a", AppID: "support"},
		1001,
	)
	if err == nil {
		t.Fatal("audit query accepted a limit above the API maximum")
	}
}

func TestAPIEvaluateAppCanaryAppliesVersionGuardedDecision(t *testing.T) {
	repository := &recordingRepository{}
	maxErrorRate := 0.1
	result, err := (admin.API{Repository: repository}).EvaluateAppCanary(
		context.Background(),
		tenant.Scope{TenantID: "tenant-a", AppID: "support"},
		"v2",
		tenant.CanaryObservations{Samples: 20, ErrorRate: 0.2},
		tenant.CanaryRule{
			MinimumSamples: 20,
			MaxErrorRate:   &maxErrorRate,
			Action:         tenant.CanaryActionRollback,
		},
	)
	if err != nil {
		t.Fatalf("evaluate app canary: %v", err)
	}
	if !result.Applied || result.Decision.Action != tenant.CanaryActionRollback {
		t.Fatalf("canary evaluation result = %#v", result)
	}
	if repository.canaryExpectedVersion != "v2" ||
		repository.canaryAction != tenant.CanaryActionRollback ||
		repository.canaryReason != "error_rate" {
		t.Fatalf("applied canary decision = version %q action %q reason %q",
			repository.canaryExpectedVersion, repository.canaryAction, repository.canaryReason)
	}
}

func TestAPIEvaluateAppCanaryDoesNotMutateBeforeMinimumSamples(t *testing.T) {
	repository := &recordingRepository{}
	maxErrorRate := 0.1
	result, err := (admin.API{Repository: repository}).EvaluateAppCanary(
		context.Background(),
		tenant.Scope{TenantID: "tenant-a", AppID: "support"},
		"v2",
		tenant.CanaryObservations{Samples: 19, ErrorRate: 1},
		tenant.CanaryRule{
			MinimumSamples: 20,
			MaxErrorRate:   &maxErrorRate,
			Action:         tenant.CanaryActionPause,
		},
	)
	if err != nil {
		t.Fatalf("evaluate app canary: %v", err)
	}
	if result.Applied || result.Decision.Action != tenant.CanaryActionNone {
		t.Fatalf("insufficient-sample result = %#v", result)
	}
	if repository.canaryAction != "" {
		t.Fatalf("canary action applied with insufficient samples: %q", repository.canaryAction)
	}
}

func testAppConfig() tenant.AppConfig {
	return tenant.AppConfig{
		TenantID: "tenant-a",
		AppID:    "support",
		Version:  "v1",
		Model: tenant.ModelConfig{
			Provider:  "openai",
			APIKeyRef: tenant.SecretRef{Name: "model-key"},
			Model:     "gpt-4.1-mini",
		},
		BackendConfig: tenant.BackendConfig{
			Name: "shared",
			Session: tenant.BackendRef{
				Kind:     tenant.BackendSQL,
				Provider: "postgres",
				Name:     "session-sql",
			},
		},
		ChannelBinding: []string{"binding-1"},
	}
}

func testBinding() channels.Binding {
	return channels.Binding{
		TenantID:        "tenant-a",
		AppID:           "support",
		BindingID:       "binding-1",
		Channel:         channels.ChannelWeCom,
		ExternalAccount: "corp-agent-1",
		Secret: tenant.SecretRef{
			Name: "wecom-bot-secret",
		},
		PublicRouteID:   "route-binding-1",
		BindingRevision: 1,
		Status:          channels.BindingActive,
	}
}

type recordingRepository struct {
	tenant                tenant.Tenant
	app                   tenant.AgentApp
	binding               channels.Binding
	initial               tenant.AppConfig
	published             tenant.AppConfig
	activatedTenantID     string
	activatedAppID        string
	activatedVersion      string
	digest                auth.APIKeyDigest
	credential            auth.Credential
	revokedTenantID       string
	revokedAppID          string
	revokedCredentialID   string
	auditEvents           []platformaudit.Event
	auditTenantID         string
	auditAppID            string
	auditLimit            int
	auditWrites           []platformaudit.Event
	canaryAction          tenant.CanaryAction
	canaryExpectedVersion string
	canaryReason          string
	canaryErr             error
}

func (r *recordingRepository) CreateTenant(_ context.Context, value tenant.Tenant) error {
	r.tenant = value
	return nil
}

func (r *recordingRepository) CreateAgentApp(
	_ context.Context,
	app tenant.AgentApp,
	initial tenant.AppConfig,
) error {
	r.app = app
	r.initial = initial
	return nil
}

func (r *recordingRepository) CreateChannelBinding(_ context.Context, binding channels.Binding) error {
	r.binding = binding
	return nil
}

func (r *recordingRepository) SetChannelBindingStatus(
	_ context.Context,
	_ string,
	_ string,
	_ string,
	status channels.BindingStatus,
) (channels.Binding, error) {
	r.binding.Status = status
	return r.binding, nil
}

func (r *recordingRepository) ResolveBinding(
	_ context.Context,
	tenantID, appID, bindingID string,
) (channels.Binding, error) {
	if r.binding.TenantID != tenantID || r.binding.AppID != appID || r.binding.BindingID != bindingID {
		return channels.Binding{}, errors.New("channel binding not found")
	}
	return r.binding, nil
}

func (r *recordingRepository) InsertAppConfigVersion(_ context.Context, cfg tenant.AppConfig) error {
	r.published = cfg
	return nil
}

func (r *recordingRepository) ActivateAppConfig(
	_ context.Context,
	tenantID, appID, version string,
) error {
	r.activatedTenantID = tenantID
	r.activatedAppID = appID
	r.activatedVersion = version
	return nil
}

func (r *recordingRepository) CreateCredential(
	_ context.Context,
	digest auth.APIKeyDigest,
	credential auth.Credential,
) error {
	r.digest = digest
	r.credential = credential
	return nil
}

func (r *recordingRepository) RevokeCredential(
	_ context.Context,
	tenantID, appID, credentialID string,
) error {
	r.revokedTenantID = tenantID
	r.revokedAppID = appID
	r.revokedCredentialID = credentialID
	return nil
}

func (r *recordingRepository) ListAuditEvents(
	_ context.Context,
	tenantID, appID string,
	limit int,
) ([]platformaudit.Event, error) {
	r.auditTenantID = tenantID
	r.auditAppID = appID
	r.auditLimit = limit
	result := make([]platformaudit.Event, 0)
	for _, event := range r.auditEvents {
		if event.TenantID == tenantID && event.AppID == appID {
			result = append(result, event)
		}
	}
	return result, nil
}

func (r *recordingRepository) Record(_ context.Context, event platformaudit.Event) error {
	r.auditWrites = append(r.auditWrites, event)
	return nil
}

func (r *recordingRepository) ApplyCanaryDecision(
	_ context.Context,
	_ string,
	_ string,
	expectedVersion string,
	action tenant.CanaryAction,
	reason string,
) (tenant.AgentApp, error) {
	if r.canaryErr != nil {
		return tenant.AgentApp{}, r.canaryErr
	}
	r.canaryExpectedVersion = expectedVersion
	r.canaryAction = action
	r.canaryReason = reason
	return r.app, nil
}
