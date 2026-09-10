package gateway

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	agentinmemory "github.com/XnLemon/trpc-agent-service/trpcservice/app/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	backendinmemory "github.com/XnLemon/trpc-agent-service/trpcservice/backend/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	channelsinmemory "github.com/XnLemon/trpc-agent-service/trpcservice/channels/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/model"
	modelinmemory "github.com/XnLemon/trpc-agent-service/trpcservice/model/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
	tenantinmemory "github.com/XnLemon/trpc-agent-service/trpcservice/tenant/inmemory"
)

func TestPrincipalKindsCannotBeForgedAcrossAuthenticationPaths(t *testing.T) {
	tenantID := "t_01J1K9ZQTVE4PAWF1TSB2WMHNP"
	appID := "app_01J1K9ZQTVE4PAWF1TSB2WMHNP"
	authenticated, err := newAuthenticatedAPI(APIIdentity{TenantID: tenantID, AppID: appID, SubjectID: "api-subject"})
	if err != nil {
		t.Fatal(err)
	}
	apiPrincipal, err := newAPIPrincipal(authenticated)
	if err != nil {
		t.Fatal(err)
	}
	if err := apiPrincipal.Validate(); err != nil || apiPrincipal.Kind() != PrincipalAPI || apiPrincipal.TenantID() != tenantID || apiPrincipal.AppID() != appID || apiPrincipal.SubjectID() != "api-subject" {
		t.Fatalf("API principal = %+v, err=%v", apiPrincipal, err)
	}
	if _, ok := apiPrincipal.RoutingTarget(); ok {
		t.Fatal("API principal exposed a Channel routing target")
	}

	fabricated := channels.RoutingTarget{
		TenantID: tenantID, BindingID: "cb_01J1K9ZQTVE4PAWF1TSB2WMHNP", BindingVersion: 1,
		AppID: appID, Channel: channels.ChannelWeCom, ProviderAccountID: "corp-a", ConfigDigest: strings.Repeat("a", 64),
	}
	if _, err := NewChannelPrincipal(fabricated); err == nil {
		t.Fatal("shape-valid fabricated channel route unexpectedly succeeded")
	}

	fixture := newGatewayFixture(t)
	target := newTrustedRoutingTarget(t, fixture)
	channelPrincipal, err := NewChannelPrincipal(target)
	if err != nil {
		t.Fatal(err)
	}
	if err := channelPrincipal.Validate(); err != nil || channelPrincipal.Kind() != PrincipalChannel || channelPrincipal.SubjectID() != "" {
		t.Fatalf("Channel principal = %+v, err=%v", channelPrincipal, err)
	}
	if got, ok := channelPrincipal.RoutingTarget(); !ok || got != target {
		t.Fatalf("Channel target = %+v, ok=%v", got, ok)
	}

	bad := target
	bad.TenantID = "not-a-tenant"
	if _, err := NewChannelPrincipal(bad); err == nil {
		t.Fatal("invalid channel route unexpectedly succeeded")
	}
	if _, err := newAuthenticatedAPI(APIIdentity{TenantID: tenantID, AppID: "tenant-not-app", SubjectID: "subject"}); err == nil {
		t.Fatal("invalid API app ID unexpectedly succeeded")
	}
}

func TestAPIAuthenticatorIsTheOnlyPublicPrincipalIssuer(t *testing.T) {
	identity := APIIdentity{TenantID: "t_01J1K9ZQTVE4PAWF1TSB2WMHNP", AppID: "app_01J1K9ZQTVE4PAWF1TSB2WMHNP", SubjectID: "subject"}
	authenticator, err := NewStaticAPIAuthenticator(map[string]APIIdentity{"credential": identity})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest("POST", "/v1/chat", nil)
	request.Header.Set("Authorization", "Bearer credential")
	result, err := authenticator.Authenticate(request.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	principal, err := newAPIPrincipal(result)
	if err != nil || principal.Kind() != PrincipalAPI || principal.TenantID() != identity.TenantID || principal.AppID() != identity.AppID {
		t.Fatalf("authenticated principal = %+v, err=%v", principal, err)
	}
	if _, err := newAPIPrincipal(AuthenticatedAPI{}); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("zero authenticator result was accepted: %v", err)
	}
	mutated := result
	mutated.identity.TenantID = "t_01J1K9ZQTVE4PAWF1TSB2WMHNQ"
	if _, err := newAPIPrincipal(mutated); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("mutated authenticator result was accepted: %v", err)
	}
	request.Header.Set("Authorization", "Bearer unknown")
	if _, err := authenticator.Authenticate(request.Context(), request); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("unknown credential was accepted: %v", err)
	}
}

func TestInboundMessageNormalizesTextAndRejectsUnsupportedInput(t *testing.T) {
	message, err := (InboundMessage{
		Content: "  hello  ", ExternalUserID: "user", ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer",
	}).Normalize()
	if err != nil {
		t.Fatal(err)
	}
	if message.Content != "hello" || message.ContentType != ContentTypeText {
		t.Fatalf("normalized message = %+v", message)
	}
	for name, input := range map[string]InboundMessage{
		"unsupported content":  {Content: "hello", ContentType: "image", ExternalUserID: "user", ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer"},
		"missing user":         {Content: "hello", ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer"},
		"missing conversation": {Content: "hello", ExternalUserID: "user"},
		"control identity":     {Content: "hello", ExternalUserID: "user\n", ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer"},
		"blank identity":       {Content: "hello", ExternalUserID: " ", ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := input.Normalize(); !errors.Is(err, ErrInvalid) {
				t.Fatalf("Normalize error = %v", err)
			}
		})
	}
}

func TestPlanResolverUsesTenantCanaryRevision(t *testing.T) {
	fixture := newGatewayFixture(t)
	current, candidate := int64(1), int64(2)
	appRoot := fixture.app.Clone()
	appRoot.CurrentRevision = &current
	appRoot.CanaryRevision = &candidate
	if err := appRoot.Validate(); err != nil {
		t.Fatal(err)
	}
	config := resolverTestConfig(fixture)
	candidateRevision := fixture.revision.Clone()
	candidateRevision.Revision = candidate
	config.Apps = resolverAgentRepository{Repository: fixture.apps, getFn: func(context.Context, string, string) (*appmodel.App, error) { return &appRoot, nil }, getRevisionFn: func(_ context.Context, _ string, _ string, revision int64) (*appmodel.Revision, error) {
		if revision == candidate {
			return &candidateRevision, nil
		}
		return fixture.revision, nil
	}}
	resolver, err := NewPlanResolver(config)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := resolver.Resolve(context.Background(), mustAPIPrincipal(t, fixture.tenant.TenantID, appRoot.AppID))
	if err != nil {
		t.Fatal(err)
	}
	if got := plan.AgentSnapshot().Revision().Revision; got != candidate {
		t.Fatalf("resolved revision = %d, want %d", got, candidate)
	}
}

func TestPlanResolverBuildsFixedPlanFromRepositoryInterfaces(t *testing.T) {
	fixture := newGatewayFixture(t)
	resolver, err := NewPlanResolver(runtime.PlanResolverConfig{
		Tenants: fixture.tenants, Apps: fixture.apps, Models: fixture.models, Backends: fixture.backends,
		ModelCatalog: fixture.modelCatalog, BackendCatalog: fixture.backendCatalog,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !resolver.Ready() {
		t.Fatal("resolver is not ready with complete dependencies")
	}
	principal := mustAPIPrincipal(t, fixture.tenant.TenantID, fixture.app.AppID)
	plan, err := resolver.Resolve(context.Background(), principal)
	if err != nil {
		t.Fatal(err)
	}
	key, err := plan.CacheKey()
	if err != nil {
		t.Fatal(err)
	}
	if key.TenantID != fixture.tenant.TenantID || key.AppID != fixture.app.AppID || key.Revision != fixture.revision.Revision || key.ModelProfileID != fixture.model.ProfileID || key.BackendProfileID != fixture.backend.ProfileID {
		t.Fatalf("unexpected plan key: %+v", key)
	}

	routingTarget := newTrustedRoutingTarget(t, fixture)
	channelPrincipal, err := NewChannelPrincipal(routingTarget)
	if err != nil {
		t.Fatal(err)
	}
	channelPlan, err := resolver.Resolve(context.Background(), channelPrincipal)
	if err != nil {
		t.Fatal(err)
	}
	channelKey, err := channelPlan.CacheKey()
	if err != nil || channelKey != key {
		t.Fatalf("channel plan key = %+v, err=%v, API key=%+v", channelKey, err, key)
	}

	otherTenant := mustAPIPrincipal(t, "t_01J1K9ZQTVE4PAWF1TSB2WMHNQ", fixture.app.AppID)
	if _, err := resolver.Resolve(context.Background(), otherTenant); !errors.Is(err, ErrPlanUnavailable) {
		t.Fatalf("cross-tenant plan error = %v", err)
	}
}

func TestPlanResolverResolveAuthenticatedAPIRequiresProofAndResolvesPlan(t *testing.T) {
	fixture := newGatewayFixture(t)
	resolver, err := NewPlanResolver(runtime.PlanResolverConfig{
		Tenants: fixture.tenants, Apps: fixture.apps, Models: fixture.models, Backends: fixture.backends,
		ModelCatalog: fixture.modelCatalog, BackendCatalog: fixture.backendCatalog,
	})
	if err != nil {
		t.Fatal(err)
	}
	authenticated, err := newAuthenticatedAPI(APIIdentity{TenantID: fixture.tenant.TenantID, AppID: fixture.app.AppID, SubjectID: "restart-test"})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := resolver.ResolveAuthenticatedAPI(context.Background(), authenticated)
	if err != nil {
		t.Fatal(err)
	}
	key, err := plan.CacheKey()
	if err != nil || key.TenantID != fixture.tenant.TenantID || key.AppID != fixture.app.AppID {
		t.Fatalf("resolved authenticated plan key = %+v, err=%v", key, err)
	}
	if _, err := resolver.ResolveAuthenticatedAPI(context.Background(), AuthenticatedAPI{}); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("unproofed authenticated API result = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := resolver.ResolveAuthenticatedAPI(canceled, authenticated); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled authenticated resolution = %v", err)
	}
}

func TestPlanResolverPreservesCancellationAndRedactsDependencyFailures(t *testing.T) {
	fixture := newGatewayFixture(t)
	resolver, err := NewPlanResolver(runtime.PlanResolverConfig{
		Tenants: fixture.tenants, Apps: fixture.apps, Models: fixture.models, Backends: fixture.backends,
		ModelCatalog: fixture.modelCatalog, BackendCatalog: fixture.backendCatalog,
	})
	if err != nil {
		t.Fatal(err)
	}
	principal := mustAPIPrincipal(t, fixture.tenant.TenantID, fixture.app.AppID)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := resolver.Resolve(canceled, principal); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled resolver error = %v", err)
	}
	if _, err := resolver.Resolve(context.Background(), Principal{}); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("invalid principal error = %v", err)
	}
	if _, err := resolver.Resolve(context.Background(), mustAPIPrincipal(t, fixture.tenant.TenantID, "app_01J1K9ZQTVE4PAWF1TSB2WMHNQ")); !errors.Is(err, ErrPlanUnavailable) {
		t.Fatalf("missing app error = %v", err)
	}
	if strings.Contains(errString(ErrPlanUnavailable), "secret") || strings.Contains(errString(ErrPlanUnavailable), "provider") {
		t.Fatal("stable plan error contains sensitive detail")
	}
}

func TestPlanResolverStopsAfterLateRepositoryCancellation(t *testing.T) {
	for name, wrap := range map[string]func(gatewayFixture, context.CancelFunc) runtime.PlanResolverConfig{
		"model": func(fixture gatewayFixture, cancel context.CancelFunc) runtime.PlanResolverConfig {
			return runtime.PlanResolverConfig{
				Tenants: fixture.tenants, Apps: fixture.apps,
				Models:   cancelAfterModelGet{Repository: fixture.models, cancel: cancel},
				Backends: fixture.backends, ModelCatalog: fixture.modelCatalog, BackendCatalog: fixture.backendCatalog,
			}
		},
		"backend": func(fixture gatewayFixture, cancel context.CancelFunc) runtime.PlanResolverConfig {
			return runtime.PlanResolverConfig{
				Tenants: fixture.tenants, Apps: fixture.apps, Models: fixture.models,
				Backends:     cancelAfterBackendGet{Repository: fixture.backends, cancel: cancel},
				ModelCatalog: fixture.modelCatalog, BackendCatalog: fixture.backendCatalog,
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			fixture := newGatewayFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			resolver, err := NewPlanResolver(wrap(fixture, cancel))
			if err != nil {
				t.Fatal(err)
			}
			plan, err := resolver.Resolve(ctx, mustAPIPrincipal(t, fixture.tenant.TenantID, fixture.app.AppID))
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("Resolve() plan=%+v err=%v, want context.Canceled", plan, err)
			}
		})
	}
}

type cancelAfterModelGet struct {
	model.Repository
	cancel context.CancelFunc
}

func (repository cancelAfterModelGet) Get(ctx context.Context, tenantID, profileID string) (*model.Profile, error) {
	profile, err := repository.Repository.Get(ctx, tenantID, profileID)
	repository.cancel()
	return profile, err
}

type cancelAfterBackendGet struct {
	backend.Repository
	cancel context.CancelFunc
}

func (repository cancelAfterBackendGet) Get(ctx context.Context, tenantID, profileID string) (*backend.Profile, error) {
	profile, err := repository.Repository.Get(ctx, tenantID, profileID)
	repository.cancel()
	return profile, err
}

func mustAPIPrincipal(t *testing.T, tenantID, appID string) Principal {
	t.Helper()
	authenticated, err := newAuthenticatedAPI(APIIdentity{TenantID: tenantID, AppID: appID, SubjectID: "api-subject"})
	if err != nil {
		t.Fatal(err)
	}
	principal, err := newAPIPrincipal(authenticated)
	if err != nil {
		t.Fatal(err)
	}
	return principal
}

func errString(err error) string { return err.Error() }

func newTrustedRoutingTarget(t *testing.T, fixture gatewayFixture) channels.RoutingTarget {
	t.Helper()
	routeDigest, err := channels.DigestPublicRouteKey(channels.ChannelWeCom, "gateway-test-route")
	if err != nil {
		t.Fatal(err)
	}
	channelRepo := channelsinmemory.NewInMemoryRepository()
	binding, _, err := channelRepo.Create(context.Background(), channels.CreateInput{
		TenantID: fixture.tenant.TenantID, BindingKey: "gateway-test-binding", Channel: channels.ChannelWeCom,
		ProviderAccountID: "corp-gateway", PublicRouteKeyDigest: routeDigest, AppID: fixture.app.AppID,
		SecretRef: "secret/gateway-test", Protocol: channels.ProtocolConfiguration{
			WeCom: &channels.WeComProtocolConfiguration{CorpID: "corp-gateway", ReceiveID: "receive"},
		}, Metadata: channels.ChangeMetadata{ActorType: "test", ActorID: "gateway", Reason: "fixture", CorrelationID: "gateway"},
	})
	if err != nil {
		t.Fatal(err)
	}
	binding, _, err = channelRepo.Activate(context.Background(), channels.TransitionStatusInput{
		TenantID: binding.TenantID, BindingID: binding.BindingID, ExpectedVersion: binding.Version,
		Metadata: channels.ChangeMetadata{ActorType: "test", ActorID: "gateway", Reason: "fixture", CorrelationID: "gateway"},
	})
	if err != nil {
		t.Fatal(err)
	}
	secret := "offline-gateway-secret"
	resolver := channelsinmemory.NewFakeCandidateResolver(channelRepo, map[channels.SecretScope]string{{TenantID: binding.TenantID, SecretRef: binding.SecretRef}: secret})
	candidates, err := channelRepo.LookupCandidates(context.Background(), channels.ChannelWeCom, routeDigest)
	if err != nil || len(candidates) != 1 {
		t.Fatalf("gateway route discovery failed: candidates=%d err=%v", len(candidates), err)
	}
	handle, err := resolver.ResolveCandidate(context.Background(), channels.CandidateSecretRequest{Candidate: candidates[0], Purpose: channels.PurposeWebhookVerification})
	if err != nil {
		t.Fatal(err)
	}
	verification := channels.VerificationRequest{
		Purpose: channels.PurposeWebhookVerification, Timestamp: time.Now().UTC(), Nonce: "gateway-nonce",
		MessageDigest: strings.Repeat("a", 64), ReceiveID: "receive",
	}
	verification.Signature = channelsinmemory.SignFakeRequest(secret, verification)
	verified, err := resolver.Verify(context.Background(), handle, verification)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := tenant.NewConfigurationSnapshot(fixture.tenant)
	if err != nil {
		t.Fatal(err)
	}
	target, err := channels.NewRoutingTarget(snapshot, binding, fixture.app, verified)
	if err != nil {
		t.Fatal(err)
	}
	return target
}

type gatewayFixture struct {
	tenant         *tenant.Tenant
	app            *appmodel.App
	revision       *appmodel.Revision
	model          *model.Profile
	backend        *backend.Profile
	modelCatalog   *model.ProviderCatalog
	backendCatalog *backend.ProviderCatalog
	tenants        *tenantinmemory.InMemoryRepository
	apps           *agentinmemory.InMemoryRepository
	models         *modelinmemory.InMemoryRepository
	backends       *backendinmemory.InMemoryRepository
}

func newGatewayFixture(t *testing.T) gatewayFixture {
	t.Helper()
	modelCatalog, err := model.NewProviderCatalog(model.ProviderSpec{
		Provider: "fake", Models: []string{"deterministic"}, EndpointPolicy: model.FieldForbidden, SecretRefPolicy: model.FieldForbidden,
	})
	if err != nil {
		t.Fatal(err)
	}
	backendCatalog, err := backend.NewProviderCatalog(backend.ProviderSpec{
		Provider: "inmemory", Capabilities: []backend.Capability{backend.CapabilitySession}, EndpointPolicy: backend.FieldForbidden, SecretRefPolicy: backend.FieldForbidden,
		Options: map[string]backend.OptionSpec{"namespace": {Kind: backend.OptionString}},
	})
	if err != nil {
		t.Fatal(err)
	}
	tenants := tenantinmemory.NewRepository()
	root, err := tenants.Create(context.Background(), tenant.CreateInput{TenantKey: "gateway-tenant", DisplayName: "Gateway Tenant", AuditRetentionDays: 30, LogMaskingLevel: tenant.MaskingBasic, TraceSamplingRate: 1})
	if err != nil {
		t.Fatal(err)
	}
	models := modelinmemory.NewRepository(modelCatalog)
	modelProfile, _, err := models.Create(context.Background(), model.CreateInput{
		TenantID: root.TenantID, ProfileKey: "primary-model", DisplayName: "Primary Model",
		Configuration: model.Configuration{Provider: "fake", Model: "deterministic"}, Metadata: model.ChangeMetadata{ActorType: "test", ActorID: "fixture", Reason: "fixture", CorrelationID: "fixture"},
	})
	if err != nil {
		t.Fatal(err)
	}
	backends := backendinmemory.NewRepository(backendCatalog)
	backendProfile, _, err := backends.Create(context.Background(), backend.CreateInput{
		TenantID: root.TenantID, ProfileKey: "primary-backend", DisplayName: "Primary Backend",
		Bindings: []backend.CapabilityBinding{{Capability: backend.CapabilitySession, Provider: "inmemory", Options: map[string]string{"namespace": "gateway"}}},
		Metadata: backend.ChangeMetadata{ActorType: "test", ActorID: "fixture", Reason: "fixture", CorrelationID: "fixture"},
	})
	if err != nil {
		t.Fatal(err)
	}
	apps := agentinmemory.NewRepository()
	appRoot, err := apps.Create(context.Background(), appmodel.CreateInput{TenantID: root.TenantID, AppKey: "gateway-app", DisplayName: "Gateway App", Description: "Fixture"})
	if err != nil {
		t.Fatal(err)
	}
	draft, err := apps.CreateDraft(context.Background(), appmodel.CreateDraftInput{
		TenantID: root.TenantID, AppID: appRoot.AppID, ExpectedAppVersion: appRoot.Version,
		Configuration: appmodel.DraftConfiguration{Description: "Gateway revision", Instruction: "Answer clearly.", ModelProfileID: modelProfile.ProfileID, Runtime: appmodel.DefaultRuntimePolicy()},
	})
	if err != nil {
		t.Fatal(err)
	}
	publishedApp, published, _, err := apps.Publish(context.Background(), appmodel.PublishInput{
		TenantID: root.TenantID, AppID: appRoot.AppID, Revision: draft.Revision, ExpectedAppVersion: appRoot.Version,
		ExpectedDraftVersion: draft.DraftVersion, TenantActive: true,
		Metadata: appmodel.ChangeMetadata{ActorType: "test", ActorID: "fixture", Reason: "fixture", CorrelationID: "fixture"},
	})
	if err != nil {
		t.Fatal(err)
	}
	appID, backendID := publishedApp.AppID, backendProfile.ProfileID
	updatedRoot, err := tenants.UpdateConfiguration(context.Background(), tenant.UpdateConfigurationInput{
		TenantID: root.TenantID, ExpectedVersion: root.Version, DisplayName: root.DisplayName,
		AuditRetentionDays: 30, LogMaskingLevel: tenant.MaskingBasic, TraceSamplingRate: 1,
		DefaultAgentAppID: &appID, DefaultBackendProfileID: &backendID,
	})
	if err != nil {
		t.Fatal(err)
	}
	return gatewayFixture{
		tenant: updatedRoot, app: publishedApp, revision: published, model: modelProfile, backend: backendProfile,
		modelCatalog: modelCatalog, backendCatalog: backendCatalog, tenants: tenants, apps: apps, models: models, backends: backends,
	}
}

func TestGatewayAuthenticationAndPrincipalValidationEdges(t *testing.T) {
	identity := APIIdentity{TenantID: "t_01J1K9ZQTVE4PAWF1TSB2WMHNP", AppID: "app_01J1K9ZQTVE4PAWF1TSB2WMHNP", SubjectID: "subject"}
	authenticated, err := newAuthenticatedAPI(identity)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := authenticated.Identity(); err != nil || got != identity {
		t.Fatalf("authenticated identity = %+v, err=%v", got, err)
	}
	var nilAuthenticator APIAuthenticatorFunc
	if _, err := nilAuthenticator.Authenticate(context.Background(), httptest.NewRequest("GET", "/", nil)); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("nil authenticator error = %v", err)
	}
	called := false
	functionAuthenticator := APIAuthenticatorFunc(func(context.Context, *http.Request) (AuthenticatedAPI, error) {
		called = true
		return authenticated, nil
	})
	if _, err := functionAuthenticator.Authenticate(context.Background(), httptest.NewRequest("GET", "/", nil)); err != nil || !called {
		t.Fatalf("function authenticator result err=%v called=%v", err, called)
	}
	if _, err := (AuthenticatedAPI{}).Identity(); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("zero authenticated identity error = %v", err)
	}

	assertInvalidStaticCredentials(t, identity)
	authenticator, err := NewStaticAPIAuthenticator(map[string]APIIdentity{"credential": identity})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest("GET", "/", nil)
	var nilContext context.Context
	if _, err := authenticator.Authenticate(context.Background(), nil); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("nil request error = %v", err)
	}
	if _, err := authenticator.Authenticate(nilContext, request); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("nil context error = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := authenticator.Authenticate(canceled, request); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled authentication error = %v", err)
	}
	assertInvalidAuthorizationHeaders(t, authenticator, request)

	principal := mustAPIPrincipal(t, identity.TenantID, identity.AppID)
	mutated := principal
	mutated.apiProof = nil
	if !errors.Is(mutated.Validate(), ErrInvalid) {
		t.Fatal("principal without proof was accepted")
	}
	mutated = principal
	mutated.tenantID = "t_01J1K9ZQTVE4PAWF1TSB2WMHNQ"
	if !errors.Is(mutated.Validate(), ErrInvalid) {
		t.Fatal("principal with mismatched proof was accepted")
	}
	mutated = principal
	mutated.kind = PrincipalKind("unknown")
	if !errors.Is(mutated.Validate(), ErrInvalid) {
		t.Fatal("unknown principal kind was accepted")
	}
	fixture := newGatewayFixture(t)
	channelPrincipal, err := NewChannelPrincipal(newTrustedRoutingTarget(t, fixture))
	if err != nil {
		t.Fatal(err)
	}
	mutatedChannel := channelPrincipal
	mutatedChannel.tenantID = "t_01J1K9ZQTVE4PAWF1TSB2WMHNQ"
	if !errors.Is(mutatedChannel.Validate(), ErrInvalid) {
		t.Fatal("channel principal with mismatched scope was accepted")
	}
}

func assertInvalidStaticCredentials(t *testing.T, identity APIIdentity) {
	t.Helper()
	for _, credential := range []string{"", " ", "bad\ncredential", strings.Repeat("x", maxPrincipalIDRunes+1)} {
		if _, err := NewStaticAPIAuthenticator(map[string]APIIdentity{credential: identity}); !errors.Is(err, ErrInvalid) {
			t.Fatalf("invalid credential %q was accepted: %v", credential, err)
		}
	}
}

func assertInvalidAuthorizationHeaders(t *testing.T, authenticator APIAuthenticator, request *http.Request) {
	t.Helper()
	for _, header := range []string{"", "Basic credential", "Bearer ", "Bearer unknown"} {
		request.Header.Set("Authorization", header)
		if _, err := authenticator.Authenticate(context.Background(), request); !errors.Is(err, ErrUnauthenticated) {
			t.Fatalf("invalid authorization %q error = %v", header, err)
		}
	}
}

func TestInboundMessageAndResolverBoundaryEdges(t *testing.T) {
	base := InboundMessage{Content: "hello", ExternalUserID: "user", ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer"}
	validGroup := base
	validGroup.ConversationKind = channels.ConversationGroup
	validGroup.ExternalPeerID = ""
	validGroup.ExternalChatID = "chat"
	validGroup.ExternalMessageID = "message"
	validGroup.ExternalThreadID = "thread"
	if _, err := validGroup.Normalize(); err != nil {
		t.Fatal(err)
	}
	cases := map[string]InboundMessage{
		"empty content":        {Content: " ", ExternalUserID: "user", ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer"},
		"long content":         {Content: strings.Repeat("x", maxMessageRunes+1), ExternalUserID: "user", ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer"},
		"message id":           withMessageField(base, "ExternalMessageID", "bad\nid"),
		"long user":            withMessageField(base, "ExternalUserID", strings.Repeat("x", maxExternalIDRunes+1)),
		"missing direct peer":  {Content: "hello", ExternalUserID: "user", ConversationKind: channels.ConversationDirect},
		"missing group chat":   {Content: "hello", ExternalUserID: "user", ConversationKind: channels.ConversationGroup},
		"unknown conversation": {Content: "hello", ExternalUserID: "user", ConversationKind: channels.ConversationKind("other")},
		"bad thread":           withMessageField(base, "ExternalThreadID", "bad\nthread"),
	}
	for name, message := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := message.Normalize(); !errors.Is(err, ErrInvalid) {
				t.Fatalf("Normalize() error = %v", err)
			}
		})
	}

	if _, err := NewPlanResolver(runtime.PlanResolverConfig{}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("missing resolver dependencies error = %v", err)
	}
	var nilResolver *PlanResolver
	if nilResolver.Ready() {
		t.Fatal("nil resolver is ready")
	}
	if _, err := nilResolver.Resolve(context.Background(), Principal{}); !errors.Is(err, ErrNotReady) {
		t.Fatalf("nil resolver error = %v", err)
	}
	fixture := newGatewayFixture(t)
	resolver, err := NewPlanResolver(runtime.PlanResolverConfig{
		Tenants: fixture.tenants, Apps: fixture.apps, Models: fixture.models, Backends: fixture.backends,
		ModelCatalog: fixture.modelCatalog, BackendCatalog: fixture.backendCatalog,
	})
	if err != nil {
		t.Fatal(err)
	}
	var nilContext context.Context
	if _, err := resolver.Resolve(nilContext, mustAPIPrincipal(t, fixture.tenant.TenantID, fixture.app.AppID)); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil resolver context error = %v", err)
	}
	if !IsContextCancellation(context.Canceled) || !IsContextCancellation(context.DeadlineExceeded) || IsContextCancellation(errors.New("other")) {
		t.Fatal("context cancellation classification is incorrect")
	}
}

func withMessageField(message InboundMessage, field, value string) InboundMessage {
	switch field {
	case "ExternalMessageID":
		message.ExternalMessageID = value
	case "ExternalUserID":
		message.ExternalUserID = value
	case "ExternalThreadID":
		message.ExternalThreadID = value
	}
	return message
}
