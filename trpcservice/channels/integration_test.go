package channels_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"reflect"
	"testing"
	"time"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
)

func TestFakeTrustedInboundRouteAndBindingAwareIdentity(t *testing.T) {
	setup := setupTrustedInbound(t)
	verified, target := assertTrustedInboundVerification(t, setup)
	direct := assertRunnerIdentityBoundaries(t, setup, target)
	assertTrustedTargetRejectsInvalidStates(t, setup, verified, direct)
}

type trustedInboundSetup struct {
	root        *tenant.Tenant
	snapshot    tenant.ConfigurationSnapshot
	app         *appmodel.App
	repo        *inmemory.InMemoryRepository
	binding     *channels.Binding
	routeDigest string
	secret      string
	resolver    *inmemory.FakeCandidateResolver
}

func setupTrustedInbound(t *testing.T) trustedInboundSetup {
	t.Helper()
	root, snapshot, app := activeTenantApp(t, "trusted-route")
	repo := inmemory.NewInMemoryRepository()
	routeDigest, err := channels.DigestPublicRouteKey(channels.ChannelWeCom, "public-wecom-route")
	if err != nil {
		t.Fatal(err)
	}
	binding := createActiveBinding(t, repo, root.TenantID, app.AppID, "wecom", channels.ChannelWeCom, "corp-trusted", routeDigest, "secret/wecom")
	secret := "offline-fake-secret" // #nosec G101 -- deterministic fixture secret for an offline test.
	resolver := inmemory.NewFakeCandidateResolver(repo, map[channels.SecretScope]string{{TenantID: root.TenantID, SecretRef: binding.SecretRef}: secret})
	return trustedInboundSetup{root: root, snapshot: snapshot, app: app, repo: repo, binding: binding, routeDigest: routeDigest, secret: secret, resolver: resolver}
}

func assertTrustedInboundVerification(t *testing.T, setup trustedInboundSetup) (channels.VerifiedBinding, channels.RoutingTarget) {
	t.Helper()
	candidates, err := setup.repo.LookupCandidates(context.Background(), channels.ChannelWeCom, setup.routeDigest)
	if err != nil || len(candidates) != 1 {
		t.Fatalf("route discovery failed: candidates=%d err=%v", len(candidates), err)
	}
	request := fakeRequest("nonce-trusted", "message body")
	request.RouteHints = channels.UntrustedRouteHints{TenantID: "t_77777777777777777777777777", BindingID: "cb_77777777777777777777777777", AppID: "app_77777777777777777777777777"}
	request.Signature = inmemory.SignFakeRequest(setup.secret, request)
	handle, err := setup.resolver.ResolveCandidate(context.Background(), channels.CandidateSecretRequest{Candidate: candidates[0], Purpose: channels.PurposeWebhookVerification})
	if err != nil {
		t.Fatal(err)
	}
	verified, err := setup.resolver.Verify(context.Background(), handle, request)
	if err != nil {
		t.Fatal(err)
	}
	if verified.TenantID != setup.root.TenantID || verified.BindingID != setup.binding.BindingID || verified.AppID != setup.app.AppID || verified.BindingVersion != setup.binding.Version || verified.Channel != setup.binding.Channel || verified.ProviderAccountID != setup.binding.ProviderAccountID {
		t.Fatalf("verified result was not fixed to the binding: %+v", verified)
	}
	target, err := channels.NewRoutingTarget(setup.snapshot, setup.binding, setup.app, verified)
	if err != nil {
		t.Fatal(err)
	}
	if target.TenantID != setup.root.TenantID || target.AppID != setup.app.AppID || target.BindingID != setup.binding.BindingID {
		t.Fatalf("unexpected trusted target: %+v", target)
	}
	return verified, target
}

func assertRunnerIdentityBoundaries(t *testing.T, setup trustedInboundSetup, target channels.RoutingTarget) tenant.RunnerIdentity {
	t.Helper()
	direct, err := target.RunnerIdentity(channels.IdentityInput{ExternalUserID: "user-1", Kind: channels.ConversationDirect, ExternalPeerID: "peer-1"})
	if err != nil {
		t.Fatal(err)
	}
	directAgain, err := target.RunnerIdentity(channels.IdentityInput{ExternalUserID: "user-1", Kind: channels.ConversationDirect, ExternalPeerID: "peer-1"})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(direct, directAgain) {
		t.Fatal("same identity input was not stable")
	}
	group, err := target.RunnerIdentity(channels.IdentityInput{ExternalUserID: "user-1", Kind: channels.ConversationGroup, ExternalChatID: "chat-1"})
	if err != nil {
		t.Fatal(err)
	}
	thread, err := target.RunnerIdentity(channels.IdentityInput{ExternalUserID: "user-1", Kind: channels.ConversationGroup, ExternalChatID: "chat-1", ExternalThreadID: "topic-7"})
	if err != nil {
		t.Fatal(err)
	}
	if direct.SessionID == group.SessionID || group.SessionID == thread.SessionID || direct.UserID == "" || direct.SessionID == "" {
		t.Fatal("direct, group, and thread identities were not separated")
	}

	otherRoute, _ := channels.DigestPublicRouteKey(channels.ChannelTelegram, "public-telegram-route")
	otherBinding := createActiveBinding(t, setup.repo, setup.root.TenantID, setup.app.AppID, "telegram", channels.ChannelTelegram, "bot-trusted", otherRoute, "secret/telegram")
	otherSecret := "offline-telegram-secret"
	resolver := inmemory.NewFakeCandidateResolver(setup.repo, map[channels.SecretScope]string{
		{TenantID: setup.root.TenantID, SecretRef: setup.binding.SecretRef}: setup.secret,
		{TenantID: setup.root.TenantID, SecretRef: otherBinding.SecretRef}:  otherSecret,
	})
	otherVerified := resolveOne(t, setup.repo, resolver, channels.ChannelTelegram, otherRoute, otherSecret, "nonce-other")
	otherTarget, err := channels.NewRoutingTarget(setup.snapshot, otherBinding, setup.app, otherVerified)
	if err != nil {
		t.Fatal(err)
	}
	otherIdentity, err := otherTarget.RunnerIdentity(channels.IdentityInput{ExternalUserID: "user-1", Kind: channels.ConversationDirect, ExternalPeerID: "peer-1"})
	if err != nil {
		t.Fatal(err)
	}
	if direct.UserID == otherIdentity.UserID || direct.SessionID == otherIdentity.SessionID {
		t.Fatal("different channel bindings collided in Runner identity")
	}

	return direct
}

func assertTrustedTargetRejectsInvalidStates(t *testing.T, setup trustedInboundSetup, verified channels.VerifiedBinding, direct tenant.RunnerIdentity) {
	t.Helper()
	suspendedApp := setup.app.Clone()
	suspendedApp.Status = appmodel.StatusSuspended
	if _, err := channels.NewRoutingTarget(setup.snapshot, setup.binding, &suspendedApp, verified); !errors.Is(err, channels.ErrVerificationFailed) {
		t.Fatalf("suspended App was accepted by trusted target: %v", err)
	}
	disabledBinding := setup.binding.Clone()
	disabledBinding.Status = channels.StatusDisabled
	if _, err := channels.NewRoutingTarget(setup.snapshot, &disabledBinding, setup.app, verified); !errors.Is(err, channels.ErrVerificationFailed) {
		t.Fatalf("disabled Binding was accepted by trusted target: %v", err)
	}
	if _, err := channels.NewRoutingTarget(tenant.ConfigurationSnapshot{}, setup.binding, setup.app, verified); !errors.Is(err, channels.ErrVerificationFailed) {
		t.Fatalf("zero/non-active Tenant snapshot was accepted: %v", err)
	}
}

func TestResolveCandidateRoutingTargetSealsCurrentSnapshots(t *testing.T) {
	root, _, app := activeTenantApp(t, "resolve-target")
	repo := inmemory.NewInMemoryRepository()
	routeDigest, err := channels.DigestPublicRouteKey(channels.ChannelWeCom, "resolve-target-route")
	if err != nil {
		t.Fatal(err)
	}
	binding := createActiveBinding(t, repo, root.TenantID, app.AppID, "wecom", channels.ChannelWeCom, "corp-resolve", routeDigest, "secret/wecom")
	candidate := oneCandidate(t, repo, channels.ChannelWeCom, routeDigest)
	if candidate.CandidateToken == "" {
		t.Fatal("candidate token is empty")
	}
	tenantRepo := &singleTenantRepository{value: root}
	appRepo := &singleAppRepository{value: app}
	target, err := channels.ResolveCandidateRoutingTarget(context.Background(), repo, tenantRepo, appRepo, candidate, func(context.Context, channels.Binding) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := target.Validate(); err != nil || target.BindingID != binding.BindingID || target.TenantID != root.TenantID || target.AppID != app.AppID {
		t.Fatalf("target = %+v, err=%v", target, err)
	}
	if _, err := channels.ResolveCandidateRoutingTarget(context.Background(), repo, tenantRepo, appRepo, candidate, func(context.Context, channels.Binding) error { return nil }); !errors.Is(err, channels.ErrVerificationFailed) {
		t.Fatalf("candidate replay error = %v", err)
	}
}

func TestResolveCandidateRoutingTargetFailureBoundaries(t *testing.T) {
	tests := []struct {
		name         string
		prepare      func(*routingTargetFixture)
		want         error
		cancellation bool
	}{
		{name: "nil context", prepare: func(f *routingTargetFixture) { f.ctx = nil }, want: channels.ErrVerificationFailed},
		{name: "canceled context", prepare: func(f *routingTargetFixture) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			f.ctx = ctx
		}, cancellation: true},
		{name: "nil consumer", prepare: func(f *routingTargetFixture) { f.consumer = nil }, want: channels.ErrVerificationFailed},
		{name: "nil tenant repository", prepare: func(f *routingTargetFixture) { f.tenants = nil }, want: channels.ErrVerificationFailed},
		{name: "nil app repository", prepare: func(f *routingTargetFixture) { f.apps = nil }, want: channels.ErrVerificationFailed},
		{name: "nil verifier", prepare: func(f *routingTargetFixture) { f.verify = nil }, want: channels.ErrVerificationFailed},
		{name: "empty candidate channel", prepare: func(f *routingTargetFixture) { f.candidate.Channel = "" }, want: channels.ErrVerificationFailed},
		{name: "consume failure", prepare: func(f *routingTargetFixture) { f.consumerStub.consumeErr = errors.New("consume failed") }, want: channels.ErrVerificationFailed},
		{name: "consume cancellation", prepare: func(f *routingTargetFixture) { f.consumerStub.consumeErr = context.Canceled }, cancellation: true},
		{name: "nil binding", prepare: func(f *routingTargetFixture) { f.consumerStub.nilBinding = true }, want: channels.ErrVerificationFailed},
		{name: "verification failure", prepare: func(f *routingTargetFixture) { f.verifyErr = errors.New("verification failed") }, want: channels.ErrVerificationFailed},
		{name: "verification cancellation", prepare: func(f *routingTargetFixture) { f.verifyErr = context.Canceled }, cancellation: true},
		{name: "invalid binding", prepare: func(f *routingTargetFixture) { f.consumerStub.binding = &channels.Binding{} }, want: channels.ErrVerificationFailed},
		{name: "tenant lookup failure", prepare: func(f *routingTargetFixture) { f.tenantRepo.err = errors.New("tenant lookup failed") }, want: channels.ErrVerificationFailed},
		{name: "tenant lookup cancellation", prepare: func(f *routingTargetFixture) { f.tenantRepo.err = context.Canceled }, cancellation: true},
		{name: "invalid tenant snapshot", prepare: func(f *routingTargetFixture) { f.tenantRepo.value = &tenant.Tenant{} }, want: channels.ErrVerificationFailed},
		{name: "app lookup failure", prepare: func(f *routingTargetFixture) { f.appRepo.err = errors.New("app lookup failed") }, want: channels.ErrVerificationFailed},
		{name: "app lookup cancellation", prepare: func(f *routingTargetFixture) { f.appRepo.err = context.Canceled }, cancellation: true},
		{name: "invalid routing target", prepare: func(f *routingTargetFixture) {
			appValue := f.app.Clone()
			appValue.Status = appmodel.StatusSuspended
			f.appRepo.value = &appValue
		}, want: channels.ErrVerificationFailed},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fixture := newRoutingTargetFixture(t)
			tt.prepare(&fixture)
			if fixture.verify != nil {
				fixture.verify = func(context.Context, channels.Binding) error { return fixture.verifyErr }
			}
			_, err := channels.ResolveCandidateRoutingTarget(fixture.ctx, fixture.consumer, fixture.tenants, fixture.apps, fixture.candidate, fixture.verify)
			if tt.cancellation {
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("error = %v, want context cancellation", err)
				}
				return
			}
			if !errors.Is(err, tt.want) {
				t.Fatalf("error = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestResolveConfiguredRoutingTargetSealsCurrentBinding(t *testing.T) {
	fixture := newRoutingTargetFixture(t)
	target, err := channels.ResolveConfiguredRoutingTarget(fixture.ctx, fixture.consumer, fixture.tenants, fixture.apps, fixture.consumerStub.binding.TenantID, fixture.consumerStub.binding.BindingID)
	if err != nil {
		t.Fatal(err)
	}
	if target.TenantID != fixture.consumerStub.binding.TenantID || target.BindingID != fixture.consumerStub.binding.BindingID || target.Channel != fixture.consumerStub.binding.Channel {
		t.Fatalf("configured target = %+v", target)
	}
	if _, err := channels.ResolveConfiguredRoutingTarget(fixture.ctx, fixture.consumer, fixture.tenants, fixture.apps, fixture.consumerStub.binding.TenantID, "missing"); !errors.Is(err, channels.ErrVerificationFailed) {
		t.Fatalf("missing configured binding error = %v", err)
	}
}

type routingTargetFixture struct {
	ctx          context.Context
	consumer     channels.CandidateConsumer
	consumerStub *routingCandidateConsumerStub
	tenants      tenant.Repository
	apps         appmodel.Repository
	candidate    channels.CandidateBindingContext
	verify       func(context.Context, channels.Binding) error
	verifyErr    error
	tenantRepo   *singleTenantRepository
	appRepo      *singleAppRepository
	app          *appmodel.App
}

func newRoutingTargetFixture(t *testing.T) routingTargetFixture {
	t.Helper()
	root, _, app := activeTenantApp(t, "resolve-boundaries")
	repo := inmemory.NewInMemoryRepository()
	routeDigest, err := channels.DigestPublicRouteKey(channels.ChannelWeCom, "resolve-boundaries-route")
	if err != nil {
		t.Fatal(err)
	}
	binding := createActiveBinding(t, repo, root.TenantID, app.AppID, "wecom", channels.ChannelWeCom, "corp-boundaries", routeDigest, "secret/wecom")
	fixture := routingTargetFixture{
		ctx:        context.Background(),
		consumer:   &routingCandidateConsumerStub{binding: binding},
		candidate:  oneCandidate(t, repo, channels.ChannelWeCom, routeDigest),
		verifyErr:  nil,
		tenantRepo: &singleTenantRepository{value: root},
		appRepo:    &singleAppRepository{value: app},
		app:        app,
	}
	fixture.consumerStub = fixture.consumer.(*routingCandidateConsumerStub)
	fixture.tenants = fixture.tenantRepo
	fixture.apps = fixture.appRepo
	fixture.verify = func(context.Context, channels.Binding) error { return fixture.verifyErr }
	return fixture
}

type routingCandidateConsumerStub struct {
	binding    *channels.Binding
	consumeErr error
	nilBinding bool
}

func (s *routingCandidateConsumerStub) LookupCandidates(context.Context, channels.Channel, string) ([]channels.CandidateBindingContext, error) {
	return nil, errors.New("unsupported")
}
func (s *routingCandidateConsumerStub) Get(_ context.Context, tenantID, bindingID string) (*channels.Binding, error) {
	if s.binding == nil || s.binding.TenantID != tenantID || s.binding.BindingID != bindingID {
		return nil, channels.ErrNotFound
	}
	binding := s.binding.Clone()
	return &binding, nil
}
func (s *routingCandidateConsumerStub) ConsumeCandidate(context.Context, channels.CandidateBindingContext) (*channels.Binding, error) {
	if s.consumeErr != nil {
		return nil, s.consumeErr
	}
	if s.nilBinding {
		return nil, nil
	}
	if s.binding == nil {
		return nil, nil
	}
	binding := s.binding.Clone()
	return &binding, nil
}

type singleTenantRepository struct {
	value *tenant.Tenant
	err   error
}

func (r *singleTenantRepository) Create(context.Context, tenant.CreateInput) (*tenant.Tenant, error) {
	return nil, errors.New("unsupported")
}

func (r *singleTenantRepository) Get(context.Context, string) (*tenant.Tenant, error) {
	if r.err != nil {
		return nil, r.err
	}
	if r.value == nil {
		return nil, nil
	}
	value := r.value.Clone()
	return &value, nil
}
func (r *singleTenantRepository) UpdateConfiguration(context.Context, tenant.UpdateConfigurationInput) (*tenant.Tenant, error) {
	return nil, errors.New("unsupported")
}
func (r *singleTenantRepository) TransitionStatus(context.Context, tenant.TransitionStatusInput) (*tenant.Tenant, tenant.StatusChangeEvent, error) {
	return nil, tenant.StatusChangeEvent{}, errors.New("unsupported")
}

type singleAppRepository struct {
	value *appmodel.App
	err   error
}

func (r *singleAppRepository) Create(context.Context, appmodel.CreateInput) (*appmodel.App, error) {
	return nil, errors.New("unsupported")
}
func (r *singleAppRepository) Get(context.Context, string, string) (*appmodel.App, error) {
	if r.err != nil {
		return nil, r.err
	}
	if r.value == nil {
		return nil, nil
	}
	value := r.value.Clone()
	return &value, nil
}
func (r *singleAppRepository) UpdateMetadata(context.Context, appmodel.UpdateMetadataInput) (*appmodel.App, error) {
	return nil, errors.New("unsupported")
}
func (r *singleAppRepository) CreateDraft(context.Context, appmodel.CreateDraftInput) (*appmodel.Revision, error) {
	return nil, errors.New("unsupported")
}
func (r *singleAppRepository) UpdateDraft(context.Context, appmodel.UpdateDraftInput) (*appmodel.Revision, error) {
	return nil, errors.New("unsupported")
}
func (r *singleAppRepository) GetRevision(context.Context, string, string, int64) (*appmodel.Revision, error) {
	return nil, errors.New("unsupported")
}
func (r *singleAppRepository) Publish(context.Context, appmodel.PublishInput) (*appmodel.App, *appmodel.Revision, appmodel.ChangeEvent, error) {
	return nil, nil, appmodel.ChangeEvent{}, errors.New("unsupported")
}
func (r *singleAppRepository) Rollback(context.Context, appmodel.RollbackInput) (*appmodel.App, appmodel.ChangeEvent, error) {
	return nil, appmodel.ChangeEvent{}, errors.New("unsupported")
}
func (r *singleAppRepository) SetCanary(context.Context, appmodel.SetCanaryInput) (*appmodel.App, appmodel.ChangeEvent, error) {
	return nil, appmodel.ChangeEvent{}, errors.New("unsupported")
}
func (r *singleAppRepository) TransitionStatus(context.Context, appmodel.TransitionStatusInput) (*appmodel.App, appmodel.ChangeEvent, error) {
	return nil, appmodel.ChangeEvent{}, errors.New("unsupported")
}

func TestFakeResolverRejectsPurposeMismatchBadProofExpiryAndReplay(t *testing.T) {
	clock := &integrationClock{now: time.Now().UTC().Add(time.Hour)}
	repo := inmemory.NewInMemoryRepository(inmemory.Options{Clock: clock.Now, CandidateTTL: time.Second})
	root, _, app := activeTenantApp(t, "fake-failures")
	routeDigest, _ := channels.DigestPublicRouteKey(channels.ChannelWeCom, "failure-route")
	binding := createActiveBinding(t, repo, root.TenantID, app.AppID, "wecom", channels.ChannelWeCom, "corp-failure", routeDigest, "secret/failure")
	secret := "failure-secret"
	resolver := inmemory.NewFakeCandidateResolver(repo, map[channels.SecretScope]string{{TenantID: root.TenantID, SecretRef: binding.SecretRef}: secret}, inmemory.FakeResolverOptions{Clock: clock.Now})

	candidate := oneCandidate(t, repo, channels.ChannelWeCom, routeDigest)
	if _, err := resolver.ResolveCandidate(context.Background(), channels.CandidateSecretRequest{Candidate: candidate, Purpose: channels.VerificationPurpose("wrong-purpose")}); !errors.Is(err, channels.ErrVerificationFailed) {
		t.Fatalf("purpose mismatch was accepted: %v", err)
	}
	if _, err := resolver.ResolveCandidate(context.Background(), channels.CandidateSecretRequest{Candidate: candidate, Purpose: channels.PurposeWebhookVerification}); err != nil {
		t.Fatal(err)
	}
	// The candidate is consumed above, so a second resolution is a replay.
	if _, err := resolver.ResolveCandidate(context.Background(), channels.CandidateSecretRequest{Candidate: candidate, Purpose: channels.PurposeWebhookVerification}); !errors.Is(err, channels.ErrVerificationFailed) {
		t.Fatalf("candidate replay was accepted: %v", err)
	}

	badCandidate := oneCandidate(t, repo, channels.ChannelWeCom, routeDigest)
	badHandle, err := resolver.ResolveCandidate(context.Background(), channels.CandidateSecretRequest{Candidate: badCandidate, Purpose: channels.PurposeWebhookVerification})
	if err != nil {
		t.Fatal(err)
	}
	badRequest := fakeRequestAt(clock.now, "nonce-bad", "bad proof")
	badRequest.Signature = inmemory.SignFakeRequest("wrong-secret", badRequest)
	if _, err := resolver.Verify(context.Background(), badHandle, badRequest); !errors.Is(err, channels.ErrVerificationFailed) {
		t.Fatalf("bad signature was accepted: %v", err)
	}
	if _, err := resolver.Verify(context.Background(), badHandle, badRequest); !errors.Is(err, channels.ErrVerificationFailed) {
		t.Fatalf("verifier handle was replayable: %v", err)
	}

	successCandidate := oneCandidate(t, repo, channels.ChannelWeCom, routeDigest)
	successHandle, err := resolver.ResolveCandidate(context.Background(), channels.CandidateSecretRequest{Candidate: successCandidate, Purpose: channels.PurposeWebhookVerification})
	if err != nil {
		t.Fatal(err)
	}
	successRequest := fakeRequestAt(clock.now, "nonce-replay", "replay proof")
	successRequest.Signature = inmemory.SignFakeRequest(secret, successRequest)
	if _, err := resolver.Verify(context.Background(), successHandle, successRequest); err != nil {
		t.Fatal(err)
	}
	replayCandidate := oneCandidate(t, repo, channels.ChannelWeCom, routeDigest)
	replayHandle, err := resolver.ResolveCandidate(context.Background(), channels.CandidateSecretRequest{Candidate: replayCandidate, Purpose: channels.PurposeWebhookVerification})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := resolver.Verify(context.Background(), replayHandle, successRequest); !errors.Is(err, channels.ErrVerificationFailed) {
		t.Fatalf("nonce replay on a new candidate was accepted: %v", err)
	}

	expiredCandidate := oneCandidate(t, repo, channels.ChannelWeCom, routeDigest)
	expiredHandle, err := resolver.ResolveCandidate(context.Background(), channels.CandidateSecretRequest{Candidate: expiredCandidate, Purpose: channels.PurposeWebhookVerification})
	if err != nil {
		t.Fatal(err)
	}
	clock.now = clock.now.Add(2 * time.Second)
	expiredRequest := fakeRequestAt(clock.now, "nonce-expired", "expired proof")
	expiredRequest.Signature = inmemory.SignFakeRequest(secret, expiredRequest)
	if _, err := resolver.Verify(context.Background(), expiredHandle, expiredRequest); !errors.Is(err, channels.ErrVerificationFailed) {
		t.Fatalf("expired verifier handle was accepted: %v", err)
	}
}

func TestFakeResolverKeepsTwoTenantCandidatesSeparated(t *testing.T) {
	clock := &integrationClock{now: time.Now().UTC().Add(time.Hour)}
	repo := inmemory.NewInMemoryRepository(inmemory.Options{Clock: clock.Now})
	firstRoot, firstSnapshot, firstApp := activeTenantApp(t, "same-route-one")
	secondRoot, secondSnapshot, secondApp := activeTenantApp(t, "same-route-two")
	routeDigest, _ := channels.DigestPublicRouteKey(channels.ChannelWeCom, "same-public-route")
	first := createActiveBinding(t, repo, firstRoot.TenantID, firstApp.AppID, "shared", channels.ChannelWeCom, "corp-one", routeDigest, "secret/first")
	second := createActiveBinding(t, repo, secondRoot.TenantID, secondApp.AppID, "shared", channels.ChannelWeCom, "corp-two", routeDigest, "secret/second")
	const sharedSecret = "same-offline-secret"
	resolver := inmemory.NewFakeCandidateResolver(repo, map[channels.SecretScope]string{
		{TenantID: firstRoot.TenantID, SecretRef: first.SecretRef}:   sharedSecret,
		{TenantID: secondRoot.TenantID, SecretRef: second.SecretRef}: sharedSecret,
	}, inmemory.FakeResolverOptions{Clock: clock.Now})
	candidates, err := repo.LookupCandidates(context.Background(), channels.ChannelWeCom, routeDigest)
	if err != nil || len(candidates) != 2 {
		t.Fatalf("expected two candidates for the same public route: %d %v", len(candidates), err)
	}
	seen := map[string]bool{}
	for index, candidate := range candidates {
		request := fakeRequestAt(clock.now, "nonce-tenant-"+string(rune('a'+index)), "tenant proof")
		request.Signature = inmemory.SignFakeRequest(sharedSecret, request)
		handle, err := resolver.ResolveCandidate(context.Background(), channels.CandidateSecretRequest{Candidate: candidate, Purpose: channels.PurposeWebhookVerification})
		if err != nil {
			t.Fatal(err)
		}
		verified, err := resolver.Verify(context.Background(), handle, request)
		if err != nil {
			t.Fatal(err)
		}
		seen[verified.TenantID] = true
		switch verified.TenantID {
		case firstRoot.TenantID:
			if _, err := channels.NewRoutingTarget(firstSnapshot, first, firstApp, verified); err != nil {
				t.Fatal(err)
			}
		case secondRoot.TenantID:
			if _, err := channels.NewRoutingTarget(secondSnapshot, second, secondApp, verified); err != nil {
				t.Fatal(err)
			}
		default:
			t.Fatalf("candidate crossed tenant boundary: %+v", verified)
		}
	}
	if len(seen) != 2 || !seen[firstRoot.TenantID] || !seen[secondRoot.TenantID] {
		t.Fatalf("same-route candidates did not preserve both tenant scopes: %v", seen)
	}
}

type integrationClock struct {
	now time.Time
}

func (c *integrationClock) Now() time.Time { return c.now }

func activeTenantApp(t *testing.T, key string) (*tenant.Tenant, tenant.ConfigurationSnapshot, *appmodel.App) {
	t.Helper()
	root, err := tenant.NewTenant(tenant.CreateInput{TenantKey: key, DisplayName: "Integration Tenant", AuditRetentionDays: 30, LogMaskingLevel: tenant.MaskingBasic, TraceSamplingRate: 1})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := tenant.NewConfigurationSnapshot(root)
	if err != nil {
		t.Fatal(err)
	}
	appRoot, err := appmodel.NewApp(appmodel.CreateInput{TenantID: root.TenantID, AppKey: "support", DisplayName: "Support", Description: "offline integration"})
	if err != nil {
		t.Fatal(err)
	}
	revision := int64(1)
	appRoot.Status = appmodel.StatusActive
	appRoot.CurrentRevision = &revision
	appRoot.Version = 2
	appRoot.UpdatedAt = appRoot.CreatedAt.Add(time.Second)
	if err := appRoot.Validate(); err != nil {
		t.Fatal(err)
	}
	return root, snapshot, appRoot
}

func createActiveBinding(t *testing.T, repo *inmemory.InMemoryRepository, tenantID, appID, key string, channel channels.Channel, providerAccountID, routeDigest, secretRef string) *channels.Binding {
	t.Helper()
	protocol := channels.ProtocolConfiguration{}
	if channel == channels.ChannelWeCom {
		protocol.WeCom = &channels.WeComProtocolConfiguration{CorpID: providerAccountID, ReceiveID: "receive"}
	} else {
		protocol.Telegram = &channels.TelegramProtocolConfiguration{WebhookPath: "/callback"}
	}
	binding, _, err := repo.Create(context.Background(), channels.CreateInput{TenantID: tenantID, BindingKey: key, Channel: channel, ProviderAccountID: providerAccountID, PublicRouteKeyDigest: routeDigest, AppID: appID, SecretRef: secretRef, Protocol: protocol, Metadata: validMetadata()})
	if err != nil {
		t.Fatal(err)
	}
	active, _, err := repo.Activate(context.Background(), channels.TransitionStatusInput{TenantID: binding.TenantID, BindingID: binding.BindingID, ExpectedVersion: binding.Version, Metadata: validMetadata()})
	if err != nil {
		t.Fatal(err)
	}
	return active
}

func resolveOne(t *testing.T, repo *inmemory.InMemoryRepository, resolver *inmemory.FakeCandidateResolver, channel channels.Channel, routeDigest, secret, nonce string) channels.VerifiedBinding {
	t.Helper()
	candidate := oneCandidate(t, repo, channel, routeDigest)
	request := fakeRequest(nonce, "message")
	request.Signature = inmemory.SignFakeRequest(secret, request)
	handle, err := resolver.ResolveCandidate(context.Background(), channels.CandidateSecretRequest{Candidate: candidate, Purpose: channels.PurposeWebhookVerification})
	if err != nil {
		t.Fatal(err)
	}
	verified, err := resolver.Verify(context.Background(), handle, request)
	if err != nil {
		t.Fatal(err)
	}
	return verified
}

func oneCandidate(t *testing.T, repo *inmemory.InMemoryRepository, channel channels.Channel, routeDigest string) channels.CandidateBindingContext {
	t.Helper()
	candidates, err := repo.LookupCandidates(context.Background(), channel, routeDigest)
	if err != nil || len(candidates) != 1 {
		t.Fatalf("expected one candidate, got %d: %v", len(candidates), err)
	}
	return candidates[0]
}

func fakeRequest(nonce, body string) channels.VerificationRequest {
	return fakeRequestAt(time.Now().UTC(), nonce, body)
}

func fakeRequestAt(timestamp time.Time, nonce, body string) channels.VerificationRequest {
	digest := sha256.Sum256([]byte(body))
	return channels.VerificationRequest{Purpose: channels.PurposeWebhookVerification, Timestamp: timestamp.UTC(), Nonce: nonce, MessageDigest: hex.EncodeToString(digest[:]), ReceiveID: "receive"}
}

func validMetadata() channels.ChangeMetadata {
	return channels.ChangeMetadata{ActorType: "test", ActorID: "integration", Reason: "integration change", CorrelationID: "integration-correlation"}
}
