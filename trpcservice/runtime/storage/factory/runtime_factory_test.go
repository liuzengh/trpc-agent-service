package factory

import (
	"context"
	"errors"
	"sync"
	"testing"

	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	modelruntime "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/model"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	runtimeinmemory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/inmemory"
	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

func TestRegistryStorageFactoryMaterializesTenantSession(t *testing.T) {
	const tenantID = "t_00000000000000000000000000"
	providers := NewProviderRegistry()
	factory := &sessionCapabilityProvider{}
	if err := providers.Register(tenantID, CapabilitySession, "memory", factory); err != nil {
		t.Fatal(err)
	}
	secrets := modelruntime.NewSecretRegistry()
	storageFactory, err := NewRegistryStorageFactory(providers, secrets)
	if err != nil {
		t.Fatal(err)
	}
	input := StorageFactoryInput{TenantID: tenantID, Bindings: []CapabilityBinding{{Capability: CapabilitySession, Provider: "memory"}}}
	set, err := storageFactory.New(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if service, err := set.Session(); err != nil || service == nil {
		t.Fatalf("Session() = %v, %v", service, err)
	}
	if err := set.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := set.Session(); !errors.Is(err, ErrCapabilityUnavailable) {
		t.Fatalf("Session after Close() = %v", err)
	}
}

func TestCapabilitySetMaterializesExplicitSummary(t *testing.T) {
	const tenantID = "t_00000000000000000000000000"
	store := &summaryCapabilityStub{}
	set, err := NewCapabilitySet(tenantID, map[Capability]any{CapabilitySummary: store})
	if err != nil {
		t.Fatal(err)
	}
	got, err := set.Summary()
	if err != nil || got != store {
		t.Fatalf("Summary() = %v, %v", got, err)
	}
}

func TestCapabilitySetSummaryRequiresExplicitCapability(t *testing.T) {
	const tenantID = "t_00000000000000000000000000"
	store := runtimeinmemory.New()
	set, err := NewCapabilitySet(tenantID, map[Capability]any{CapabilityMemory: store})
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	defer set.Close()
	if _, err := set.Summary(); !errors.Is(err, ErrCapabilityUnavailable) {
		t.Fatalf("Summary() without explicit capability = %v", err)
	}
}

func TestCapabilitySetTypedAccessors(t *testing.T) {
	const tenantID = "t_00000000000000000000000000"
	store := runtimeinmemory.New()
	set, err := NewCapabilitySet(tenantID, map[Capability]any{
		CapabilitySession:   inmemory.NewSessionService(),
		CapabilityMemory:    store,
		CapabilitySummary:   store,
		CapabilityKnowledge: store,
		CapabilityArtifact:  store,
		CapabilityAudit:     store,
	})
	if err != nil {
		t.Fatal(err)
	}
	checks := []struct {
		name string
		call func() error
	}{
		{"Session", func() error { _, err := set.Session(); return err }},
		{"Memory", func() error { _, err := set.Memory(); return err }},
		{"Summary", func() error { _, err := set.Summary(); return err }},
		{"Knowledge", func() error { _, err := set.Knowledge(); return err }},
		{"Artifact", func() error { _, err := set.Artifact(); return err }},
		{"Audit", func() error { _, err := set.Audit(); return err }},
		{"Vector", func() error { _, err := set.Vector(); return err }},
		{"Object", func() error { _, err := set.Object(); return err }},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			if err := check.call(); err != nil {
				t.Fatalf("%s() = %v", check.name, err)
			}
		})
	}
	if err := set.Close(); err != nil {
		t.Fatal(err)
	}
	missing, err := NewCapabilitySet(tenantID, map[Capability]any{CapabilitySession: inmemory.NewSessionService()})
	if err != nil {
		t.Fatal(err)
	}
	defer missing.Close()
	for _, check := range checks[1:] {
		t.Run("missing-"+check.name, func(t *testing.T) {
			if err := check.call(); !errors.Is(err, ErrCapabilityUnavailable) {
				t.Fatalf("%s() = %v", check.name, err)
			}
		})
	}
}

func TestCapabilitySetTypedAccessorsRejectWrongTypes(t *testing.T) {
	const tenantID = "t_00000000000000000000000000"
	cases := []struct {
		name string
		kind Capability
		call func(*CapabilitySet) error
	}{
		{"Session", CapabilitySession, func(set *CapabilitySet) error { _, err := set.Session(); return err }},
		{"Memory", CapabilityMemory, func(set *CapabilitySet) error { _, err := set.Memory(); return err }},
		{"Summary", CapabilitySummary, func(set *CapabilitySet) error { _, err := set.Summary(); return err }},
		{"Knowledge", CapabilityKnowledge, func(set *CapabilitySet) error { _, err := set.Knowledge(); return err }},
		{"Artifact", CapabilityArtifact, func(set *CapabilitySet) error { _, err := set.Artifact(); return err }},
		{"Audit", CapabilityAudit, func(set *CapabilitySet) error { _, err := set.Audit(); return err }},
		{"Vector", CapabilityKnowledge, func(set *CapabilitySet) error { _, err := set.Vector(); return err }},
		{"Object", CapabilityArtifact, func(set *CapabilitySet) error { _, err := set.Object(); return err }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			set, err := NewCapabilitySet(tenantID, map[Capability]any{tc.kind: struct{}{}})
			if err != nil {
				t.Fatal(err)
			}
			defer set.Close()
			if !errors.Is(tc.call(set), ErrCapabilityUnavailable) {
				t.Fatalf("%s accepted wrong capability type", tc.name)
			}
		})
	}
}

func TestMatchesCapabilityRejectsUnsupportedAndNilValues(t *testing.T) {
	if matchesCapability(CapabilitySession, (*session.Service)(nil)) {
		t.Fatal("typed nil accepted")
	}
	if matchesCapability(Capability("unknown"), struct{}{}) {
		t.Fatal("unknown capability accepted")
	}
	if _, ok := (&CapabilitySet{}).Capability(CapabilitySession); ok {
		t.Fatal("missing capability reported")
	}
}

func TestMatchesCapabilityContracts(t *testing.T) {
	store := runtimeinmemory.New()
	t.Cleanup(func() { _ = store.Close() })
	sessionService := inmemory.NewSessionService()
	t.Cleanup(func() { _ = sessionService.Close() })
	knowledgeOnly := struct{ runtimestorage.KnowledgeStore }{KnowledgeStore: store}
	artifactOnly := struct{ runtimestorage.ArtifactStore }{ArtifactStore: store}
	for _, test := range []struct {
		name  string
		kind  Capability
		value any
		want  bool
	}{
		{name: "session", kind: CapabilitySession, value: sessionService, want: true},
		{name: "memory", kind: CapabilityMemory, value: store, want: true},
		{name: "summary", kind: CapabilitySummary, value: store, want: true},
		{name: "knowledge requires vector", kind: CapabilityKnowledge, value: knowledgeOnly},
		{name: "knowledge with vector", kind: CapabilityKnowledge, value: store, want: true},
		{name: "artifact requires object", kind: CapabilityArtifact, value: artifactOnly},
		{name: "artifact with object", kind: CapabilityArtifact, value: store, want: true},
		{name: "audit", kind: CapabilityAudit, value: store, want: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := matchesCapability(test.kind, test.value); got != test.want {
				t.Fatalf("matchesCapability(%q, %T) = %t, want %t", test.kind, test.value, got, test.want)
			}
		})
	}
}

func TestRegistryStorageFactoryCancellationAndMissingSession(t *testing.T) {
	providers := NewProviderRegistry()
	secrets := modelruntime.NewSecretRegistry()
	factory, err := NewRegistryStorageFactory(providers, secrets)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := factory.New(ctx, StorageFactoryInput{TenantID: "t_00000000000000000000000000", Bindings: []CapabilityBinding{{Capability: CapabilitySession, Provider: "missing"}}}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled New() = %v", err)
	}
	if _, err := factory.New(context.Background(), StorageFactoryInput{TenantID: "t_00000000000000000000000000", Bindings: []CapabilityBinding{{Capability: CapabilityMemory, Provider: "missing"}}}); !errors.Is(err, ErrStorageFactory) {
		t.Fatalf("missing provider New() = %v", err)
	}
}

func TestRegistryStorageFactoryCancellationAfterProviderSuccess(t *testing.T) {
	providers := NewProviderRegistry()
	closed := make(chan struct{})
	provider := &sessionCapabilityProvider{closed: closed}
	const tenantID = "t_00000000000000000000000000"
	if err := providers.Register(tenantID, CapabilitySession, "memory", provider); err != nil {
		t.Fatal(err)
	}
	factory, err := NewRegistryStorageFactory(providers, modelruntime.NewSecretRegistry())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	provider.cancel = cancel
	if _, err := factory.New(ctx, StorageFactoryInput{TenantID: tenantID, Bindings: []CapabilityBinding{{Capability: CapabilitySession, Provider: "memory"}}}); !errors.Is(err, context.Canceled) {
		t.Fatalf("provider-success cancellation = %v", err)
	}
	select {
	case <-closed:
	default:
		t.Fatal("provider capability was not closed after cancellation")
	}
}

func TestRegistryStorageFactoryBuildsTenantCapabilitiesConcurrently(t *testing.T) {
	const (
		tenantOne = "t_00000000000000000000000000"
		tenantTwo = "t_00000000000000000000000001"
	)
	providers := NewProviderRegistry()
	provider := &recordingSessionCapabilityProvider{}
	for _, tenantID := range []string{tenantOne, tenantTwo} {
		if err := providers.Register(tenantID, CapabilitySession, "memory", provider); err != nil {
			t.Fatal(err)
		}
	}
	factory, err := NewRegistryStorageFactory(providers, modelruntime.NewSecretRegistry())
	if err != nil {
		t.Fatal(err)
	}
	const workers = 20
	sets := make(chan *CapabilitySet, workers)
	errorsCh := make(chan error, workers)
	var group sync.WaitGroup
	for index := 0; index < workers; index++ {
		tenantID := tenantOne
		if index%2 == 1 {
			tenantID = tenantTwo
		}
		group.Add(1)
		go func(tenantID string) {
			defer group.Done()
			set, newErr := factory.New(context.Background(), StorageFactoryInput{TenantID: tenantID, Bindings: []CapabilityBinding{{Capability: CapabilitySession, Provider: "memory"}}})
			if newErr != nil {
				errorsCh <- newErr
				return
			}
			sets <- set
		}(tenantID)
	}
	group.Wait()
	close(sets)
	close(errorsCh)
	for newErr := range errorsCh {
		t.Fatal(newErr)
	}
	for set := range sets {
		if service, sessionErr := set.Session(); sessionErr != nil || service == nil {
			t.Fatalf("materialized Session() = %v, %v", service, sessionErr)
		}
		if err := set.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if provider.Count(tenantOne) != workers/2 || provider.Count(tenantTwo) != workers/2 {
		t.Fatalf("provider tenant calls = one:%d two:%d", provider.Count(tenantOne), provider.Count(tenantTwo))
	}
}

func TestCapabilitySetOwnsValuesAndAggregatesCloseFailures(t *testing.T) {
	const tenantID = "t_00000000000000000000000000"
	if _, err := NewCapabilitySet("", map[Capability]any{CapabilitySession: struct{}{}}); !errors.Is(err, ErrStorageFactory) {
		t.Fatalf("empty tenant set = %v", err)
	}
	if _, err := NewCapabilitySet(tenantID, nil); !errors.Is(err, ErrStorageFactory) {
		t.Fatalf("empty set = %v", err)
	}
	if _, err := NewCapabilitySet(tenantID, map[Capability]any{"": struct{}{}}); !errors.Is(err, ErrStorageFactory) {
		t.Fatalf("invalid capability set = %v", err)
	}

	closer := &failingCapabilityCloser{}
	values := map[Capability]any{CapabilityMemory: closer}
	set, err := NewCapabilitySet(tenantID, values)
	if err != nil {
		t.Fatal(err)
	}
	values[CapabilitySession] = inmemory.NewSessionService()
	if _, ok := set.Capability(CapabilitySession); ok {
		t.Fatal("capability set retained caller map")
	}
	if err := set.Close(); !errors.Is(err, ErrStorageFactory) {
		t.Fatalf("Close() = %v", err)
	}
	if err := set.Close(); !errors.Is(err, ErrStorageFactory) || closer.calls != 1 {
		t.Fatalf("second Close() = %v, calls=%d", err, closer.calls)
	}
	if _, ok := set.Capability(CapabilityMemory); ok {
		t.Fatal("closed set exposed capability")
	}
	var nilSet *CapabilitySet
	if value, ok := nilSet.Capability(CapabilitySession); value != nil || ok {
		t.Fatalf("nil Capability() = %v, %v", value, ok)
	}
	if _, err := nilSet.Session(); !errors.Is(err, ErrCapabilityUnavailable) {
		t.Fatalf("nil Session() = %v", err)
	}
	if err := nilSet.Close(); err != nil {
		t.Fatal(err)
	}
	wrongType, err := NewCapabilitySet(tenantID, map[Capability]any{CapabilitySession: struct{}{}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wrongType.Session(); !errors.Is(err, ErrCapabilityUnavailable) {
		t.Fatalf("wrong-type Session() = %v", err)
	}
}

func TestCapabilitySetClosesSharedValueOnce(t *testing.T) {
	const tenantID = "t_00000000000000000000000000"
	closer := &failingCapabilityCloser{}
	set, err := NewCapabilitySet(tenantID, map[Capability]any{
		CapabilitySession: closer,
		CapabilityMemory:  closer,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := set.Close(); !errors.Is(err, ErrStorageFactory) {
		t.Fatalf("Close() = %v", err)
	}
	if closer.calls != 1 {
		t.Fatalf("shared capability close calls = %d", closer.calls)
	}
}

func TestRegistryStorageFactoryClosesEarlierCapabilityAndScopesSecrets(t *testing.T) {
	const tenantID = "t_00000000000000000000000000"
	providers := NewProviderRegistry()
	secrets := modelruntime.NewSecretRegistry()
	if err := secrets.RegisterValue(modelprofile.SecretScope{TenantID: tenantID, SecretRef: "secret/session"}, "session-secret"); err != nil {
		t.Fatal(err)
	}
	closed := make(chan struct{})
	first := capabilityProviderFunc(func(_ context.Context, input StorageFactoryInput, binding CapabilityBinding, secret modelprofile.SecretValue) (any, error) {
		if input.TenantID != tenantID || binding.SecretRef != "secret/session" || secret.Value() != "session-secret" {
			t.Fatalf("provider input = %+v, %+v, %q", input, binding, secret.Value())
		}
		return &closeTrackingSession{Service: inmemory.NewSessionService(), closed: closed}, nil
	})
	second := capabilityProviderFunc(func(context.Context, StorageFactoryInput, CapabilityBinding, modelprofile.SecretValue) (any, error) {
		return nil, errors.New("provider detail")
	})
	if err := providers.Register(tenantID, CapabilitySession, "session", first); err != nil {
		t.Fatal(err)
	}
	if err := providers.Register(tenantID, CapabilityMemory, "broken", second); err != nil {
		t.Fatal(err)
	}
	factory, err := NewRegistryStorageFactory(providers, secrets)
	if err != nil {
		t.Fatal(err)
	}
	_, err = factory.New(context.Background(), StorageFactoryInput{TenantID: tenantID, Bindings: []CapabilityBinding{
		{Capability: CapabilitySession, Provider: "session", SecretRef: "secret/session"},
		{Capability: CapabilityMemory, Provider: "broken"},
	}})
	if !errors.Is(err, ErrStorageFactory) {
		t.Fatalf("New() = %v", err)
	}
	select {
	case <-closed:
	default:
		t.Fatal("earlier capability was not closed")
	}
}

func TestRegistryStorageFactoryRejectsDuplicateCapabilities(t *testing.T) {
	const tenantID = "t_00000000000000000000000000"
	providers := NewProviderRegistry()
	closed := make(chan struct{})
	first := capabilityProviderFunc(func(context.Context, StorageFactoryInput, CapabilityBinding, modelprofile.SecretValue) (any, error) {
		return &closeTrackingSession{Service: inmemory.NewSessionService(), closed: closed}, nil
	})
	called := false
	second := capabilityProviderFunc(func(context.Context, StorageFactoryInput, CapabilityBinding, modelprofile.SecretValue) (any, error) {
		called = true
		return inmemory.NewSessionService(), nil
	})
	if err := providers.Register(tenantID, CapabilitySession, "one", first); err != nil {
		t.Fatal(err)
	}
	if err := providers.Register(tenantID, CapabilitySession, "two", second); err != nil {
		t.Fatal(err)
	}
	factory, err := NewRegistryStorageFactory(providers, modelruntime.NewSecretRegistry())
	if err != nil {
		t.Fatal(err)
	}
	_, err = factory.New(context.Background(), StorageFactoryInput{TenantID: tenantID, Bindings: []CapabilityBinding{
		{Capability: CapabilitySession, Provider: "one"},
		{Capability: CapabilitySession, Provider: "two"},
	}})
	if !errors.Is(err, ErrStorageFactory) {
		t.Fatalf("duplicate capability New() = %v", err)
	}
	if called {
		t.Fatal("duplicate capability was materialized")
	}
	select {
	case <-closed:
	default:
		t.Fatal("materialized capability was not closed")
	}
}

func TestRegistryStorageFactoryValidationAndResolverFailures(t *testing.T) {
	if _, err := NewRegistryStorageFactory(nil, modelruntime.NewSecretRegistry()); !errors.Is(err, ErrStorageFactory) {
		t.Fatalf("nil providers factory = %v", err)
	}
	providers := NewProviderRegistry()
	factory, err := NewRegistryStorageFactory(providers, modelruntime.NewSecretRegistry())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := factory.New(context.Background(), StorageFactoryInput{}); !errors.Is(err, ErrStorageFactory) {
		t.Fatalf("invalid input New() = %v", err)
	}
	const tenantID = "t_00000000000000000000000000"
	if err := providers.Register(tenantID, CapabilitySession, "session", capabilityProviderFunc(func(context.Context, StorageFactoryInput, CapabilityBinding, modelprofile.SecretValue) (any, error) {
		return inmemory.NewSessionService(), nil
	})); err != nil {
		t.Fatal(err)
	}
	if _, err := factory.New(context.Background(), StorageFactoryInput{TenantID: tenantID, Bindings: []CapabilityBinding{{Capability: CapabilitySession, Provider: "session", SecretRef: "missing"}}}); !errors.Is(err, ErrStorageFactory) {
		t.Fatalf("missing secret New() = %v", err)
	}
	if _, err := factory.New(nil, StorageFactoryInput{TenantID: tenantID, Bindings: []CapabilityBinding{{Capability: CapabilitySession, Provider: "session"}}}); !errors.Is(err, ErrStorageFactory) {
		t.Fatalf("nil context New() = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := factory.New(canceled, StorageFactoryInput{TenantID: tenantID, Bindings: []CapabilityBinding{{Capability: CapabilitySession, Provider: "session"}}}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled New() = %v", err)
	}
	if _, err := factory.New(context.Background(), StorageFactoryInput{TenantID: tenantID, Bindings: []CapabilityBinding{{}}}); !errors.Is(err, ErrStorageFactory) {
		t.Fatalf("invalid binding New() = %v", err)
	}
}

type sessionCapabilityProvider struct {
	cancel context.CancelFunc
	closed chan struct{}
}

func (provider *sessionCapabilityProvider) New(context.Context, StorageFactoryInput, CapabilityBinding, modelprofile.SecretValue) (any, error) {
	if provider.cancel != nil {
		provider.cancel()
	}
	service := inmemory.NewSessionService()
	if provider.closed != nil {
		return &closeTrackingSession{Service: service, closed: provider.closed}, nil
	}
	return service, nil
}

type closeTrackingSession struct {
	session.Service
	closed chan struct{}
	once   sync.Once
}

func (service *closeTrackingSession) Close() error {
	service.once.Do(func() { close(service.closed) })
	return nil
}

type recordingSessionCapabilityProvider struct {
	mu    sync.Mutex
	calls map[string]int
}

type failingCapabilityCloser struct{ calls int }

func (closer *failingCapabilityCloser) Close() error {
	closer.calls++
	return errors.New("close detail")
}

type capabilityProviderFunc func(context.Context, StorageFactoryInput, CapabilityBinding, modelprofile.SecretValue) (any, error)

type summaryCapabilityStub struct{}

func (*summaryCapabilityStub) PutSummary(context.Context, runtimestorage.SummaryRecord) (runtimestorage.SummaryRecord, error) {
	return runtimestorage.SummaryRecord{}, nil
}
func (*summaryCapabilityStub) GetSummary(context.Context, string, string, string) (runtimestorage.SummaryRecord, error) {
	return runtimestorage.SummaryRecord{}, nil
}
func (*summaryCapabilityStub) EnqueueSummary(context.Context, runtimestorage.SummaryRecord) error {
	return nil
}

func (function capabilityProviderFunc) New(ctx context.Context, input StorageFactoryInput, binding CapabilityBinding, secret modelprofile.SecretValue) (any, error) {
	return function(ctx, input, binding, secret)
}

func (provider *recordingSessionCapabilityProvider) New(_ context.Context, input StorageFactoryInput, _ CapabilityBinding, _ modelprofile.SecretValue) (any, error) {
	provider.mu.Lock()
	if provider.calls == nil {
		provider.calls = make(map[string]int)
	}
	provider.calls[input.TenantID]++
	provider.mu.Unlock()
	return inmemory.NewSessionService(), nil
}

func (provider *recordingSessionCapabilityProvider) Count(tenantID string) int {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	return provider.calls[tenantID]
}
