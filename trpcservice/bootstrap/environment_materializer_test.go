package bootstrap

import (
	"context"
	"errors"
	"testing"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	appmemory "github.com/XnLemon/trpc-agent-service/trpcservice/app/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	backendmemory "github.com/XnLemon/trpc-agent-service/trpcservice/backend/inmemory"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	modelmemory "github.com/XnLemon/trpc-agent-service/trpcservice/model/inmemory"
	modelruntime "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/model"
	storagefactory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/factory"
	runtimestorageinmemory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
	tenantmemory "github.com/XnLemon/trpc-agent-service/trpcservice/tenant/inmemory"
)

func TestEnvironmentRuntimeProviderLookupBoundaries(t *testing.T) {
	store := runtimestorageinmemory.New()
	defer func() { _ = store.Close() }()
	providers := []environmentRuntimeProviderSpec{{name: "inmemory", store: store}}
	if provider, ok := environmentRuntimeProvider(providers, "inmemory"); !ok || provider.store != store {
		t.Fatalf("runtime provider lookup = %+v, %v", provider, ok)
	}
	if _, ok := environmentRuntimeProvider(providers, "missing"); ok {
		t.Fatal("missing runtime provider was found")
	}
}

func TestEnvironmentWebRootUsesConfiguredValueOrDefault(t *testing.T) {
	t.Setenv("TRPC_WEB_ROOT", "/tmp/trpc-web")
	if got := environmentWebRoot(); got != "/tmp/trpc-web" {
		t.Fatalf("configured web root = %q", got)
	}
	t.Setenv("TRPC_WEB_ROOT", " \t")
	if got := environmentWebRoot(); got != defaultWebRoot {
		t.Fatalf("default web root = %q, want %q", got, defaultWebRoot)
	}
}

func TestNewEnvironmentTenantMaterializerValidatesDependencies(t *testing.T) {
	options, root, _, _ := newEnvironmentMaterializerFixture(t)
	cases := []struct {
		name   string
		mutate func(*environmentTenantRuntimeOptions)
	}{
		{name: "missing runtime provider", mutate: func(value *environmentTenantRuntimeOptions) {
			value.runtimeStores.providers = nil
		}},
		{name: "missing secret registry", mutate: func(value *environmentTenantRuntimeOptions) {
			value.secretRegistry = nil
		}},
		{name: "missing model registry", mutate: func(value *environmentTenantRuntimeOptions) {
			value.modelRegistry = nil
		}},
		{name: "missing backend registry", mutate: func(value *environmentTenantRuntimeOptions) {
			value.backendRegistry = nil
		}},
		{name: "incomplete control plane", mutate: func(value *environmentTenantRuntimeOptions) {
			value.controlPlane = &environmentTenantRuntimeDependencies{}
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			candidate := options
			test.mutate(&candidate)
			if _, err := newEnvironmentTenantMaterializer(candidate); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("materializer error = %v", err)
			}
		})
	}

	options.controlPlane = nil
	options.config = environmentConfig{runtimeStorage: "inmemory", modelProvider: defaultModelProvider, secretRef: "env/model"}
	options.config.modelAPIKey = "global-key"
	materialize, err := newEnvironmentTenantMaterializer(options)
	if err != nil {
		t.Fatal(err)
	}
	var nilContext context.Context
	if err := materialize(nilContext, root.TenantID); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil context materialization error = %v", err)
	}
	if err := materialize(context.Background(), " \t"); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("blank tenant materialization error = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := materialize(canceled, root.TenantID); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled materialization error = %v", err)
	}

	options.config.modelAPIKey = ""
	materialize, err = newEnvironmentTenantMaterializer(options)
	if err != nil {
		t.Fatal(err)
	}
	if err := materialize(context.Background(), root.TenantID); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("missing model key materialization error = %v", err)
	}
}

func TestEnvironmentTenantMaterializerBuildsDemoAndConfiguredRuntime(t *testing.T) {
	options, root, _, _ := newEnvironmentMaterializerFixture(t)
	options.controlPlane = nil
	options.config = environmentConfig{
		runtimeStorage: "inmemory", demoMode: true, modelProvider: defaultModelProvider,
		modelAPIKeys: map[string]string{root.TenantID: "tenant-key"}, secretRef: "env/model",
	}
	materialize, err := newEnvironmentTenantMaterializer(options)
	if err != nil {
		t.Fatal(err)
	}
	if err := materialize(context.Background(), root.TenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := options.modelRegistry.New(context.Background(), modelprofile.ModelFactoryInput{TenantID: root.TenantID, Provider: demoModelProvider, Model: demoModelName}, modelprofile.SecretValue{}); err != nil {
		t.Fatalf("demo model registry = %v", err)
	}

	options = newEnvironmentMaterializerOptions(t, options.runtimeStores.providers["inmemory"])
	options.controlPlane = nil
	options.config = environmentConfig{
		runtimeStorage: "inmemory", modelProvider: defaultModelProvider, modelAPIKey: "global-key", secretRef: "env/model",
		modelAPIKeys: map[string]string{root.TenantID: "tenant-key"},
	}
	materialize, err = newEnvironmentTenantMaterializer(options)
	if err != nil {
		t.Fatal(err)
	}
	if err := materialize(context.Background(), root.TenantID); err != nil {
		t.Fatal(err)
	}
	secret, err := options.secretRegistry.Resolve(context.Background(), modelprofile.SecretScope{TenantID: root.TenantID, SecretRef: "env/model"})
	if err != nil || secret.Value() != "tenant-key" {
		t.Fatalf("tenant-scoped model key = %q, err=%v", secret.Value(), err)
	}
	secondTenant := "t_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	if err := materialize(context.Background(), secondTenant); err != nil {
		t.Fatal(err)
	}
	secret, err = options.secretRegistry.Resolve(context.Background(), modelprofile.SecretScope{TenantID: secondTenant, SecretRef: "env/model"})
	if err != nil || secret.Value() != "global-key" {
		t.Fatalf("global fallback model key = %q, err=%v", secret.Value(), err)
	}
	if _, err := options.modelRegistry.New(context.Background(), modelprofile.ModelFactoryInput{TenantID: root.TenantID, Provider: defaultModelProvider, Model: "gpt-test"}, secret); err != nil {
		t.Fatalf("configured model registry = %v", err)
	}
	if _, err := options.backendRegistry.Resolve(context.Background(), backend.StorageFactoryInput{TenantID: root.TenantID}, backend.CapabilityBinding{Capability: backend.CapabilitySession, Provider: "inmemory"}); err != nil {
		t.Fatalf("configured backend registry = %v", err)
	}
}

func TestEnvironmentTenantMaterializerUsesControlPlaneAndFailsClosed(t *testing.T) {
	fixture := newEnvironmentMaterializerControlPlaneFixture(t)
	options := newEnvironmentMaterializerOptions(t, fixture.store)
	options.config = environmentConfig{runtimeStorage: "inmemory", modelAPIKey: "fallback-key", secretRef: fixture.model.Configuration.SecretRef, modelProvider: defaultModelProvider}
	options.controlPlane = fixture.dependencies
	options.config.secretRef = fixture.model.Configuration.SecretRef
	materialize, err := newEnvironmentTenantMaterializer(options)
	if err != nil {
		t.Fatal(err)
	}
	if err := materialize(context.Background(), fixture.root.TenantID); err != nil {
		t.Fatal(err)
	}
	secret, err := options.secretRegistry.Resolve(context.Background(), modelprofile.SecretScope{TenantID: fixture.root.TenantID, SecretRef: fixture.model.Configuration.SecretRef})
	if err != nil || secret.Value() != "fallback-key" {
		t.Fatalf("fallback control-plane secret = %q, err=%v", secret.Value(), err)
	}

	resolvedSecret, err := modelprofile.NewSecretValue("resolved-key")
	if err != nil {
		t.Fatal(err)
	}
	options = newEnvironmentMaterializerOptions(t, options.runtimeStores.providers["inmemory"])
	options.config = environmentConfig{runtimeStorage: "inmemory", secretRef: fixture.model.Configuration.SecretRef, modelProvider: defaultModelProvider}
	fixture.dependencies.secrets = environmentMaterializerSecretResolver{value: resolvedSecret}
	options.controlPlane = fixture.dependencies
	materialize, err = newEnvironmentTenantMaterializer(options)
	if err != nil {
		t.Fatal(err)
	}
	if err := materialize(context.Background(), fixture.root.TenantID); err != nil {
		t.Fatal(err)
	}
	secret, err = options.secretRegistry.Resolve(context.Background(), modelprofile.SecretScope{TenantID: fixture.root.TenantID, SecretRef: fixture.model.Configuration.SecretRef})
	if err != nil || secret.Value() != "resolved-key" {
		t.Fatalf("resolved control-plane secret = %q, err=%v", secret.Value(), err)
	}
	if _, err := options.modelRegistry.New(context.Background(), modelprofile.ModelFactoryInput{TenantID: fixture.root.TenantID, Provider: defaultModelProvider, Model: "gpt-test"}, secret); err != nil {
		t.Fatalf("control-plane model registry = %v", err)
	}

	badSecret := newEnvironmentMaterializerControlPlaneFixture(t)
	badOptions := newEnvironmentMaterializerOptions(t, options.runtimeStores.providers["inmemory"])
	badOptions.config = environmentConfig{runtimeStorage: "inmemory", secretRef: "different-ref", modelAPIKey: "fallback-key", modelProvider: defaultModelProvider}
	badOptions.controlPlane = badSecret.dependencies
	materialize, err = newEnvironmentTenantMaterializer(badOptions)
	if err != nil {
		t.Fatal(err)
	}
	if err := materialize(context.Background(), badSecret.root.TenantID); err == nil {
		t.Fatal("secret reference mismatch was accepted")
	}

	badProvider := newEnvironmentMaterializerControlPlaneFixture(t)
	badProvider.backend.Bindings[0].Provider = "missing"
	badOptions = newEnvironmentMaterializerOptions(t, options.runtimeStores.providers["inmemory"])
	badOptions.config = environmentConfig{runtimeStorage: "inmemory", secretRef: badProvider.model.Configuration.SecretRef, modelAPIKey: "fallback-key", modelProvider: defaultModelProvider}
	badOptions.controlPlane = badProvider.dependencies
	materialize, err = newEnvironmentTenantMaterializer(badOptions)
	if err != nil {
		t.Fatal(err)
	}
	if err := materialize(context.Background(), badProvider.root.TenantID); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("missing backend provider error = %v", err)
	}

	notReady := newEnvironmentMaterializerControlPlaneFixture(t)
	notReady.root.Status = tenant.StatusSuspended
	notReady.dependencies.tenants = environmentMaterializerTenantRepository{value: notReady.root}
	badOptions = newEnvironmentMaterializerOptions(t, options.runtimeStores.providers["inmemory"])
	badOptions.config = environmentConfig{runtimeStorage: "inmemory", secretRef: notReady.model.Configuration.SecretRef, modelAPIKey: "fallback-key", modelProvider: defaultModelProvider}
	badOptions.controlPlane = notReady.dependencies
	materialize, err = newEnvironmentTenantMaterializer(badOptions)
	if err != nil {
		t.Fatal(err)
	}
	if err := materialize(context.Background(), notReady.root.TenantID); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("not-ready tenant error = %v", err)
	}
}

func TestEnvironmentTenantMaterializerRejectsIncompleteControlPlaneState(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(environmentMaterializerControlPlaneFixture)
	}{
		{name: "tenant missing", mutate: func(fixture environmentMaterializerControlPlaneFixture) {
			fixture.dependencies.tenants = environmentMaterializerTenantRepository{}
		}},
		{name: "tenant missing default app", mutate: func(fixture environmentMaterializerControlPlaneFixture) {
			root := fixture.root.Clone()
			root.DefaultAgentAppID = nil
			fixture.dependencies.tenants = environmentMaterializerTenantRepository{value: &root}
		}},
		{name: "tenant missing default backend", mutate: func(fixture environmentMaterializerControlPlaneFixture) {
			root := fixture.root.Clone()
			root.DefaultBackendProfileID = nil
			fixture.dependencies.tenants = environmentMaterializerTenantRepository{value: &root}
		}},
		{name: "app missing", mutate: func(fixture environmentMaterializerControlPlaneFixture) {
			fixture.dependencies.apps = environmentMaterializerAppRepository{revision: fixture.dependencies.apps.(environmentMaterializerAppRepository).revision}
		}},
		{name: "app suspended", mutate: func(fixture environmentMaterializerControlPlaneFixture) {
			app := fixture.dependencies.apps.(environmentMaterializerAppRepository).app.Clone()
			app.Status = appmodel.StatusSuspended
			fixture.dependencies.apps = environmentMaterializerAppRepository{app: &app, revision: fixture.dependencies.apps.(environmentMaterializerAppRepository).revision}
		}},
		{name: "app missing current revision", mutate: func(fixture environmentMaterializerControlPlaneFixture) {
			app := fixture.dependencies.apps.(environmentMaterializerAppRepository).app.Clone()
			app.CurrentRevision = nil
			fixture.dependencies.apps = environmentMaterializerAppRepository{app: &app, revision: fixture.dependencies.apps.(environmentMaterializerAppRepository).revision}
		}},
		{name: "revision missing", mutate: func(fixture environmentMaterializerControlPlaneFixture) {
			fixture.dependencies.apps = environmentMaterializerAppRepository{app: fixture.dependencies.apps.(environmentMaterializerAppRepository).app}
		}},
		{name: "model missing", mutate: func(fixture environmentMaterializerControlPlaneFixture) {
			fixture.dependencies.models = environmentMaterializerModelRepository{}
		}},
		{name: "backend missing", mutate: func(fixture environmentMaterializerControlPlaneFixture) {
			fixture.dependencies.backends = environmentMaterializerBackendRepository{}
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			fixture := newEnvironmentMaterializerControlPlaneFixture(t)
			test.mutate(fixture)
			options := newEnvironmentMaterializerOptions(t, fixture.store)
			options.config = environmentConfig{runtimeStorage: "inmemory", modelAPIKey: "fallback-key", secretRef: fixture.model.Configuration.SecretRef, modelProvider: defaultModelProvider}
			options.controlPlane = fixture.dependencies
			materialize, err := newEnvironmentTenantMaterializer(options)
			if err != nil {
				t.Fatal(err)
			}
			if err := materialize(context.Background(), fixture.root.TenantID); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("materialization error = %v, want ErrInvalidConfig", err)
			}
		})
	}
}

func newEnvironmentMaterializerFixture(t *testing.T) (environmentTenantRuntimeOptions, *tenant.Tenant, *modelprofile.Profile, *backend.Profile) {
	t.Helper()
	modelCatalog, err := modelprofile.NewProviderCatalog(modelprofile.ProviderSpec{Provider: "fake", Models: []string{"web-model"}, EndpointPolicy: modelprofile.FieldForbidden, SecretRefPolicy: modelprofile.FieldRequired})
	if err != nil {
		t.Fatal(err)
	}
	backendCatalog, err := backend.NewProviderCatalog(backend.ProviderSpec{Provider: "memory", Capabilities: []backend.Capability{backend.CapabilitySession}, EndpointPolicy: backend.FieldForbidden, SecretRefPolicy: backend.FieldForbidden, Options: map[string]backend.OptionSpec{"namespace": {Kind: backend.OptionString}}})
	if err != nil {
		t.Fatal(err)
	}
	tenants := tenantmemory.NewRepository()
	apps := appmemory.NewRepository()
	models := modelmemory.NewRepository(modelCatalog)
	backends := backendmemory.NewRepository(backendCatalog)
	root, app := createBootstrapTenantExecutionState(t, tenants, apps, models, backends, "materializer", "materializer", "web-model", "env/model")
	revision, err := apps.GetRevision(context.Background(), root.TenantID, app.AppID, *app.CurrentRevision)
	if err != nil {
		t.Fatal(err)
	}
	model, err := models.Get(context.Background(), root.TenantID, revision.ModelProfileID)
	if err != nil {
		t.Fatal(err)
	}
	modelValue := model.Clone()
	modelValue.Configuration.Provider = defaultModelProvider
	backendValue, err := backends.Get(context.Background(), root.TenantID, *root.DefaultBackendProfileID)
	if err != nil {
		t.Fatal(err)
	}
	backendValueClone := backendValue.Clone()
	for index := range backendValueClone.Bindings {
		backendValueClone.Bindings[index].Provider = "inmemory"
	}
	store := runtimestorageinmemory.New()
	t.Cleanup(func() { _ = store.Close() })
	options := newEnvironmentMaterializerOptions(t, store)
	options.controlPlane = &environmentTenantRuntimeDependencies{
		// These repositories are replaced by the control-plane-specific helper
		// in tests that exercise the branch. Keeping the normal fixture here
		// makes the options useful for the non-control-plane branch as well.
		tenants:  environmentMaterializerTenantRepository{value: root},
		apps:     environmentMaterializerAppRepository{app: app, revision: revision},
		models:   environmentMaterializerModelRepository{profile: &modelValue},
		backends: environmentMaterializerBackendRepository{profile: &backendValueClone},
		secrets:  environmentMaterializerSecretResolver{err: errors.New("secret manager unavailable")},
	}
	return options, root, &modelValue, &backendValueClone
}

func newEnvironmentMaterializerOptions(t *testing.T, store environmentStorage) environmentTenantRuntimeOptions {
	t.Helper()
	return environmentTenantRuntimeOptions{
		config:         environmentConfig{runtimeStorage: "inmemory", modelProvider: defaultModelProvider, secretRef: "env/model", modelAPIKey: "global-key"},
		runtimeStores:  environmentRuntimeStores{providers: map[string]environmentStorage{"inmemory": store}},
		secretRegistry: modelruntime.NewSecretRegistry(), modelRegistry: modelruntime.NewModelProviderRegistry(), backendRegistry: storagefactory.NewProviderRegistry(),
	}
}

type environmentMaterializerControlPlaneFixture struct {
	root         *tenant.Tenant
	model        *modelprofile.Profile
	backend      *backend.Profile
	store        environmentStorage
	dependencies *environmentTenantRuntimeDependencies
}

func newEnvironmentMaterializerControlPlaneFixture(t *testing.T) environmentMaterializerControlPlaneFixture {
	options, root, model, backendProfile := newEnvironmentMaterializerFixture(t)
	return environmentMaterializerControlPlaneFixture{root: root, model: model, backend: backendProfile, store: options.runtimeStores.providers["inmemory"], dependencies: options.controlPlane}
}

type environmentMaterializerTenantRepository struct {
	tenant.Repository
	value *tenant.Tenant
	err   error
}

func (repository environmentMaterializerTenantRepository) Get(context.Context, string) (*tenant.Tenant, error) {
	if repository.err != nil {
		return nil, repository.err
	}
	if repository.value == nil {
		return nil, nil
	}
	value := repository.value.Clone()
	return &value, nil
}

type environmentMaterializerAppRepository struct {
	appmodel.Repository
	app      *appmodel.App
	revision *appmodel.Revision
}

func (repository environmentMaterializerAppRepository) Get(context.Context, string, string) (*appmodel.App, error) {
	if repository.app == nil {
		return nil, nil
	}
	value := repository.app.Clone()
	return &value, nil
}

func (repository environmentMaterializerAppRepository) GetRevision(context.Context, string, string, int64) (*appmodel.Revision, error) {
	if repository.revision == nil {
		return nil, nil
	}
	value := repository.revision.Clone()
	return &value, nil
}

type environmentMaterializerModelRepository struct {
	modelprofile.Repository
	profile *modelprofile.Profile
}

func (repository environmentMaterializerModelRepository) Get(context.Context, string, string) (*modelprofile.Profile, error) {
	if repository.profile == nil {
		return nil, nil
	}
	value := repository.profile.Clone()
	return &value, nil
}

type environmentMaterializerBackendRepository struct {
	backend.Repository
	profile *backend.Profile
}

func (repository environmentMaterializerBackendRepository) Get(context.Context, string, string) (*backend.Profile, error) {
	if repository.profile == nil {
		return nil, nil
	}
	value := repository.profile.Clone()
	return &value, nil
}

type environmentMaterializerSecretResolver struct {
	value modelprofile.SecretValue
	err   error
}

func (resolver environmentMaterializerSecretResolver) Resolve(context.Context, modelprofile.SecretScope) (modelprofile.SecretValue, error) {
	if resolver.err != nil {
		return modelprofile.SecretValue{}, resolver.err
	}
	return resolver.value, nil
}
