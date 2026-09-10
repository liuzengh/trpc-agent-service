package bootstrap

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	agentsessionstore "github.com/XnLemon/trpc-agent-service/trpcservice/agent/sessionstore"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	"github.com/XnLemon/trpc-agent-service/trpcservice/gateway"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	storagefactory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/factory"
	runtimestorageinmemory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/inmemory"
	runtimestorageredis "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/redis"
	"github.com/XnLemon/trpc-agent-service/trpcservice/storage/postgres"
	"github.com/alicebob/miniredis/v2"
	"trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

func TestParseEnvironmentOTLPHeadersAndTelemetryBoundaries(t *testing.T) {
	headers, err := parseEnvironmentOTLPHeaders("authorization=Bearer secret, x-tenant=platform")
	if err != nil || headers["authorization"] != "Bearer secret" || headers["x-tenant"] != "platform" {
		t.Fatalf("OTLP headers = %#v, %v", headers, err)
	}
	for _, invalid := range []string{"missing-equals", "=value", "key=", "key=value,key=other", "key=value\n"} {
		if _, err := parseEnvironmentOTLPHeaders(invalid); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("invalid OTLP headers %q error = %v", invalid, err)
		}
	}
	t.Setenv(envOTLPEndpoint, "http://collector:4318/v1/otlp")
	t.Setenv(envOTLPHeaders, "authorization=Bearer secret")
	t.Setenv(envOTLPInsecure, "true")
	t.Setenv(envOTELServiceName, "service-test")
	config := environmentConfig{}
	if err := config.loadTelemetry(); err != nil {
		t.Fatal(err)
	}
	if config.otlp.Endpoint != "http://collector:4318/v1/otlp" || !config.otlp.Insecure || config.otlp.Headers["authorization"] != "Bearer secret" || config.otlp.ServiceName != "service-test" {
		t.Fatalf("OTLP config = %+v", config.otlp)
	}
	t.Setenv(envOTLPInsecure, "not-bool")
	if err := config.loadTelemetry(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("invalid OTLP insecure error = %v", err)
	}
	t.Setenv(envOTLPInsecure, "false")
	t.Setenv(envOTELServiceName, "bad\nname")
	if err := config.loadTelemetry(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("invalid OTEL service name error = %v", err)
	}
}

func TestParseEnvironmentAPIIdentities(t *testing.T) {
	value := "token-a|t_00000000000000000000000000|app_00000000000000000000000000|service-a, token-b|t_00000000000000000000000001|app_00000000000000000000000001|service-b"
	identities, err := parseEnvironmentAPIIdentities(value)
	if err != nil || len(identities) != 2 {
		t.Fatalf("parse identities = %+v, %v", identities, err)
	}
	if identities["token-b"].TenantID != "t_00000000000000000000000001" {
		t.Fatalf("second identity = %+v", identities["token-b"])
	}
	for _, invalid := range []string{"token|tenant|app", "token-a|t_00000000000000000000000000|app_00000000000000000000000000|service,token-a|t_00000000000000000000000001|app_00000000000000000000000001|service", "token| |app|subject"} {
		if _, err := parseEnvironmentAPIIdentities(invalid); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("invalid identities %q error = %v", invalid, err)
		}
	}
}

func TestLoadEnvironmentSupportsIdentityListWithoutFixedTenantFields(t *testing.T) {
	setRequiredEnvironment(t)
	t.Setenv(envAPIIdentities, "")
	t.Setenv(envAPIToken, "")
	t.Setenv(envTenantID, "")
	t.Setenv(envAppID, "")
	t.Setenv(envModelAPIKey, "")
	t.Setenv(envModelAPIKeys, "t_00000000000000000000000000=key-a,t_00000000000000000000000001=key-b")
	t.Setenv(envAPIIdentities, "token-a|t_00000000000000000000000000|app_00000000000000000000000000|service-a,token-b|t_00000000000000000000000001|app_00000000000000000000000001|service-b")
	config, err := loadEnvironment()
	if err != nil || len(config.apiIdentities) != 2 {
		t.Fatalf("multi-tenant environment = %+v, %v", config, err)
	}
	if config.tenantID != "" || config.appID != "" {
		t.Fatal("multi-tenant environment selected a process-fixed identity")
	}
	if _, err := gateway.NewStaticAPIAuthenticator(config.apiIdentities); err != nil {
		t.Fatal(err)
	}
}

func TestLoadEnvironmentUsesTenantModelAPIKeysForMultipleIdentities(t *testing.T) {
	setRequiredEnvironment(t)
	t.Setenv(envAPIIdentities, "token-a|t_00000000000000000000000000|app_00000000000000000000000000|service-a,token-b|t_00000000000000000000000001|app_00000000000000000000000001|service-b")
	t.Setenv(envAPIToken, "")
	t.Setenv(envTenantID, "")
	t.Setenv(envAppID, "")
	t.Setenv(envModelAPIKey, "")
	t.Setenv(envModelAPIKeys, "t_00000000000000000000000000=key-a,t_00000000000000000000000001=key-b")
	config, err := loadEnvironment()
	if err != nil {
		t.Fatal(err)
	}
	if config.modelAPIKey != "" || config.modelAPIKeys["t_00000000000000000000000000"] != "key-a" || config.modelAPIKeys["t_00000000000000000000000001"] != "key-b" {
		t.Fatalf("tenant model keys = %#v, global=%q", config.modelAPIKeys, config.modelAPIKey)
	}
}

func TestLoadEnvironmentUsesIdentityListForSingleFixedIdentity(t *testing.T) {
	setRequiredEnvironment(t)
	t.Setenv(envAPIIdentities, "token-a|t_00000000000000000000000000|app_00000000000000000000000000|service-a")
	t.Setenv(envAPIToken, "")
	t.Setenv(envTenantID, "")
	t.Setenv(envAppID, "")
	config, err := loadEnvironment()
	if err != nil {
		t.Fatal(err)
	}
	if config.tenantID != "t_00000000000000000000000000" || config.appID != "app_00000000000000000000000000" {
		t.Fatalf("single identity fixed fields = %q/%q", config.tenantID, config.appID)
	}
}

func TestEnvironmentRegistriesKeepModelSecretsTenantScoped(t *testing.T) {
	const (
		tenantA = "t_00000000000000000000000000"
		tenantB = "t_00000000000000000000000001"
	)
	delegate := inmemory.NewSessionService()
	store := runtimestorageinmemory.New()
	defer func() {
		_ = delegate.Close()
		_ = store.Close()
	}()
	config := environmentConfig{
		apiIdentities: map[string]gateway.APIIdentity{
			"token-a": {TenantID: tenantA, AppID: "app-a", SubjectID: "subject-a"},
			"token-b": {TenantID: tenantB, AppID: "app-b", SubjectID: "subject-b"},
		},
		modelAPIKeys:  map[string]string{tenantA: "key-a", tenantB: "key-b"},
		modelProvider: defaultModelProvider,
		secretRef:     "env/model",
	}
	secrets, models, backends, err := environmentRegistries(config, delegate, store)
	if err != nil {
		t.Fatal(err)
	}
	for tenantID, want := range map[string]string{tenantA: "key-a", tenantB: "key-b"} {
		value, resolveErr := secrets.Resolve(context.Background(), modelprofile.SecretScope{TenantID: tenantID, SecretRef: config.secretRef})
		if resolveErr != nil || value.Value() != want {
			t.Fatalf("tenant %s model secret = %q, %v", tenantID, value.Value(), resolveErr)
		}
	}
	modelSecret, err := secrets.Resolve(context.Background(), modelprofile.SecretScope{TenantID: tenantA, SecretRef: config.secretRef})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := models.New(context.Background(), modelprofile.ModelFactoryInput{TenantID: tenantA, Provider: defaultModelProvider, Model: "gpt-4o-mini"}, modelSecret); err != nil {
		t.Fatalf("tenant model registry = %v", err)
	}
	if _, err := storagefactory.NewRegistryStorageFactory(backends, secrets); err != nil {
		t.Fatalf("tenant backend registry = %v", err)
	}
}

func TestEnvironmentRegistriesRejectMissingTenantModelKey(t *testing.T) {
	delegate := inmemory.NewSessionService()
	store := runtimestorageinmemory.New()
	defer func() {
		_ = delegate.Close()
		_ = store.Close()
	}()
	_, _, _, err := environmentRegistries(environmentConfig{
		apiIdentities: map[string]gateway.APIIdentity{"token": {TenantID: "t_00000000000000000000000000", AppID: "app", SubjectID: "subject"}},
		modelAPIKeys:  map[string]string{},
	}, delegate, store)
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("missing tenant model key = %v", err)
	}
}

func TestParseEnvironmentModelAPIKeysRejectsMalformedOrDuplicateEntries(t *testing.T) {
	for _, value := range []string{"", ",", "tenant=", "=key", "tenant\n=key", "tenant=one,tenant=two"} {
		if _, err := parseEnvironmentModelAPIKeys(value); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("parse %q error = %v", value, err)
		}
	}
	keys, err := parseEnvironmentModelAPIKeys("tenant-a=key-a, tenant-b=key-b")
	if err != nil || keys["tenant-a"] != "key-a" || keys["tenant-b"] != "key-b" {
		t.Fatalf("parsed model keys = %#v, err=%v", keys, err)
	}
}

func TestLoadEnvironmentRequiresEveryTenantModelAPIKey(t *testing.T) {
	setRequiredEnvironment(t)
	t.Setenv(envAPIIdentities, "token-a|t_00000000000000000000000000|app_00000000000000000000000000|service-a,token-b|t_00000000000000000000000001|app_00000000000000000000000001|service-b")
	t.Setenv(envAPIToken, "")
	t.Setenv(envTenantID, "")
	t.Setenv(envAppID, "")
	t.Setenv(envModelAPIKey, "")
	t.Setenv(envModelAPIKeys, "t_00000000000000000000000000=key-a")
	if _, err := loadEnvironment(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("incomplete tenant model keys error = %v", err)
	}
}

func TestLoadEnvironmentRejectsGlobalModelKeyForMultipleIdentities(t *testing.T) {
	setRequiredEnvironment(t)
	t.Setenv(envAPIIdentities, "token-a|t_00000000000000000000000000|app_00000000000000000000000000|service-a,token-b|t_00000000000000000000000001|app_00000000000000000000000001|service-b")
	t.Setenv(envAPIToken, "")
	t.Setenv(envTenantID, "")
	t.Setenv(envAppID, "")
	t.Setenv(envModelAPIKey, "shared-key")
	t.Setenv(envModelAPIKeys, "")
	if _, err := loadEnvironment(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("global model key in multi-tenant mode error = %v", err)
	}
}

func TestLoadEnvironmentRejectsSingleWeComCredentialSetForMultipleIdentities(t *testing.T) {
	setRequiredEnvironment(t)
	t.Setenv(envAPIIdentities, "token-a|t_00000000000000000000000000|app_00000000000000000000000000|service-a,token-b|t_00000000000000000000000001|app_00000000000000000000000001|service-b")
	t.Setenv(envWeComCallbackToken, "callback")
	t.Setenv(envWeComEncodingAESKey, "aes")
	t.Setenv(envWeComAppSecret, "secret")
	t.Setenv(envWeComSecretRef, "env/wecom")
	if _, err := loadEnvironment(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("multi-identity WeCom config error = %v", err)
	}
}

func TestEnvironmentCredentialResolversFailClosedByScope(t *testing.T) {
	wecomResolver := environmentWeComCredentialResolver{tenantID: "t_00000000000000000000000000", config: environmentWeComConfig{callbackToken: "callback", encodingAESKey: "aes", appSecret: "app", secretRef: "env/wecom"}}
	scope := channels.SecretScope{TenantID: "t_00000000000000000000000000", SecretRef: "env/wecom"}
	credentials, err := wecomResolver.Resolve(context.Background(), scope)
	if err != nil || credentials.CallbackToken != "callback" {
		t.Fatalf("WeCom Resolve() = %+v, %v", credentials, err)
	}
	foreign := scope
	foreign.TenantID = "t_00000000000000000000000001"
	if _, err := wecomResolver.Resolve(context.Background(), foreign); err == nil {
		t.Fatal("foreign WeCom scope was accepted")
	}
	if _, err := wecomResolver.Resolve(nil, scope); err == nil {
		t.Fatal("nil WeCom context was accepted")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := wecomResolver.Resolve(canceled, scope); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled WeCom Resolve() = %v", err)
	}

	modelResolver := environmentSecretResolver{reference: "env/model", value: "model-secret"}
	modelScope := modelprofile.SecretScope{TenantID: "t_00000000000000000000000000", SecretRef: "env/model"}
	value, err := modelResolver.Resolve(context.Background(), modelScope)
	if err != nil || value.Value() != "model-secret" {
		t.Fatalf("model Resolve() = %q, %v", value.Value(), err)
	}
	modelScope.SecretRef = "other"
	if _, err := modelResolver.Resolve(context.Background(), modelScope); err == nil {
		t.Fatal("foreign model scope was accepted")
	}
}

func TestEnvironmentSessionCapabilityProviderBoundaries(t *testing.T) {
	provider := environmentSessionCapabilityProvider{}
	if _, err := provider.New(nil, backend.StorageFactoryInput{TenantID: "t_00000000000000000000000000"}, backend.CapabilityBinding{}, modelprofile.SecretValue{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("nil provider context = %v", err)
	}
	store := runtimestorageinmemory.New()
	delegate := inmemory.NewSessionService()
	value, err := (environmentSessionCapabilityProvider{delegate: delegate, store: store}).New(context.Background(), backend.StorageFactoryInput{TenantID: "t_00000000000000000000000000"}, backend.CapabilityBinding{}, modelprofile.SecretValue{})
	if err != nil || value == nil {
		t.Fatalf("session capability = %v, %v", value, err)
	}
	if err := value.(interface{ Close() error }).Close(); err != nil {
		t.Fatal(err)
	}
	_ = delegate.Close()
	_ = store.Close()
}

func TestEnvironmentRuntimeCapabilityProviderNew(t *testing.T) {
	const tenantID = "t_00000000000000000000000000"
	store := runtimestorageinmemory.New()
	delegate := inmemory.NewSessionService()
	t.Cleanup(func() {
		_ = delegate.Close()
		_ = store.Close()
	})
	input := backend.StorageFactoryInput{TenantID: tenantID}

	tests := []struct {
		name       string
		capability backend.Capability
		matches    func(any) bool
	}{
		{name: "session", capability: backend.CapabilitySession, matches: func(value any) bool { _, ok := value.(*agentsessionstore.Service); return ok }},
		{name: "memory", capability: backend.CapabilityMemory, matches: func(value any) bool { _, ok := value.(borrowedMemoryStore); return ok }},
		{name: "summary", capability: backend.CapabilitySummary, matches: func(value any) bool { _, ok := value.(borrowedSummaryStore); return ok }},
		{name: "knowledge", capability: backend.CapabilityKnowledge, matches: func(value any) bool { _, ok := value.(borrowedKnowledgeStore); return ok }},
		{name: "artifact", capability: backend.CapabilityArtifact, matches: func(value any) bool { _, ok := value.(borrowedArtifactStore); return ok }},
		{name: "audit", capability: backend.CapabilityAudit, matches: func(value any) bool { _, ok := value.(borrowedAuditStore); return ok }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value, err := (environmentRuntimeCapabilityProvider{capability: test.capability, delegate: delegate, store: store, backend: "inmemory"}).New(context.Background(), input, backend.CapabilityBinding{}, modelprofile.SecretValue{})
			if err != nil || value == nil || !test.matches(value) {
				t.Fatalf("capability value = %T, %v", value, err)
			}
			closer, ok := value.(interface{ Close() error })
			if !ok {
				t.Fatalf("capability %T does not expose Close", value)
			}
			if err := closer.Close(); err != nil {
				t.Fatalf("borrowed capability close = %v", err)
			}
		})
	}

	if _, err := (environmentRuntimeCapabilityProvider{capability: backend.CapabilityMemory, store: store}).New(nil, input, backend.CapabilityBinding{}, modelprofile.SecretValue{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("nil provider context = %v", err)
	}
	if _, err := (environmentRuntimeCapabilityProvider{capability: backend.CapabilitySession, delegate: delegate, store: store}).New(nil, input, backend.CapabilityBinding{}, modelprofile.SecretValue{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("nil session context = %v", err)
	}
	for _, capability := range []backend.Capability{backend.CapabilityMemory, backend.CapabilitySummary, backend.CapabilityKnowledge, backend.CapabilityArtifact, backend.CapabilityAudit, backend.Capability("unknown")} {
		if _, err := (environmentRuntimeCapabilityProvider{capability: capability}).New(context.Background(), input, backend.CapabilityBinding{}, modelprofile.SecretValue{}); !errors.Is(err, storagefactory.ErrStorageFactory) {
			t.Fatalf("missing %s store error = %v", capability, err)
		}
	}
	if _, err := (environmentRuntimeCapabilityProvider{capability: backend.CapabilitySession, delegate: delegate}).New(context.Background(), input, backend.CapabilityBinding{}, modelprofile.SecretValue{}); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("invalid session dependencies error = %v", err)
	}

	knowledgeOnly := &environmentKnowledgeOnlyStore{environmentStorage: store, knowledge: store}
	if _, err := (environmentRuntimeCapabilityProvider{capability: backend.CapabilityKnowledge, store: knowledgeOnly}).New(context.Background(), input, backend.CapabilityBinding{}, modelprofile.SecretValue{}); !errors.Is(err, storagefactory.ErrStorageFactory) {
		t.Fatalf("missing vector store error = %v", err)
	}
	artifactOnly := &environmentArtifactOnlyStore{environmentStorage: store, artifact: store}
	if _, err := (environmentRuntimeCapabilityProvider{capability: backend.CapabilityArtifact, store: artifactOnly}).New(context.Background(), input, backend.CapabilityBinding{}, modelprofile.SecretValue{}); !errors.Is(err, storagefactory.ErrStorageFactory) {
		t.Fatalf("missing object store error = %v", err)
	}
	if _, err := (environmentRuntimeCapabilityProvider{capability: backend.Capability("unknown"), store: store}).New(context.Background(), input, backend.CapabilityBinding{}, modelprofile.SecretValue{}); !errors.Is(err, storagefactory.ErrStorageFactory) {
		t.Fatalf("unsupported capability error = %v", err)
	}

	if _, err := store.PutMemory(context.Background(), runtimestorage.MemoryInput{TenantID: tenantID, UserID: "user", Content: "content"}); err != nil {
		t.Fatalf("borrowed capability close stopped shared store = %v", err)
	}
}

func TestEnvironmentBackendCatalogIncludesTenantScopedS3ArtifactOnly(t *testing.T) {
	catalog, err := newEnvironmentBackendCatalog("postgres")
	if err != nil {
		t.Fatal(err)
	}
	bucket := "tenant-artifacts"
	bindings, err := catalog.NormalizeBindings([]backend.CapabilityBinding{{
		Capability: backend.CapabilityArtifact,
		Provider:   "s3",
		Endpoint:   "https://s3.example.test",
		SecretRef:  "env/s3",
		Options:    map[string]string{"bucket": bucket},
	}})
	if err != nil || len(bindings) != 1 {
		t.Fatalf("S3 artifact binding = %#v, %v", bindings, err)
	}
	if bindings[0].Options["region"] != "us-east-1" || bindings[0].Options["path_style"] != "false" || bindings[0].Options["max_bytes"] != "33554432" {
		t.Fatalf("S3 defaults = %#v", bindings[0].Options)
	}
	if _, err := catalog.NormalizeBindings([]backend.CapabilityBinding{{
		Capability: backend.CapabilityArtifact,
		Provider:   "s3",
		Endpoint:   "http://minio:9000",
		SecretRef:  "env/s3",
		Options:    map[string]string{"bucket": bucket},
	}}); !errors.Is(err, backend.ErrInvalid) {
		t.Fatalf("insecure S3 endpoint without opt-in = %v", err)
	}
	if _, err := catalog.NormalizeBindings([]backend.CapabilityBinding{{
		Capability: backend.CapabilityArtifact,
		Provider:   "s3",
		Endpoint:   "http://minio:9000",
		SecretRef:  "env/s3",
		Options:    map[string]string{"bucket": bucket, "allow_insecure": "true"},
	}}); err != nil {
		t.Fatalf("insecure S3 endpoint with opt-in = %v", err)
	}
	for _, unsafeBucket := range []string{"BAD_BUCKET", "foo..bar", "192.168.1.1"} {
		if _, err := catalog.NormalizeBindings([]backend.CapabilityBinding{{
			Capability: backend.CapabilityArtifact,
			Provider:   "s3",
			Endpoint:   "https://s3.example.test",
			SecretRef:  "env/s3",
			Options:    map[string]string{"bucket": unsafeBucket},
		}}); !errors.Is(err, backend.ErrInvalid) {
			t.Fatalf("unsafe S3 bucket %q = %v", unsafeBucket, err)
		}
	}
	if _, err := catalog.NormalizeBindings([]backend.CapabilityBinding{{Capability: backend.CapabilityMemory, Provider: "s3", Endpoint: "https://s3.example.test", SecretRef: "env/s3", Options: map[string]string{"bucket": bucket}}}); !errors.Is(err, backend.ErrInvalid) {
		t.Fatalf("S3 memory binding = %v", err)
	}
	for _, options := range []map[string]string{{"bucket": bucket, "secret": "leak"}, {"bucket": bucket, "max_bytes": "0"}} {
		if _, err := catalog.NormalizeBindings([]backend.CapabilityBinding{{Capability: backend.CapabilityArtifact, Provider: "s3", Endpoint: "https://s3.example.test", SecretRef: "env/s3", Options: options}}); !errors.Is(err, backend.ErrInvalid) {
			t.Fatalf("invalid S3 options %#v = %v", options, err)
		}
	}
	for _, options := range []map[string]string{{"bucket": "BAD"}, {"bucket": "a..b"}, {"bucket": bucket, "unknown": "value"}, {"bucket": bucket, "path_style": "maybe"}, {"bucket": bucket, "allow_insecure": "maybe"}, {"bucket": bucket, "max_bytes": "0"}, {"bucket": bucket, "connect_timeout_ms": "0"}} {
		if _, err := parseEnvironmentS3Options(options); !errors.Is(err, storagefactory.ErrStorageFactory) {
			t.Fatalf("invalid S3 bucket %#v = %v", options, err)
		}
	}
}

func TestLoadEnvironmentS3CredentialsAreOptionalAndTenantScoped(t *testing.T) {
	setRequiredEnvironment(t)
	for _, name := range []string{envS3AccessKeyID, envS3SecretKey, envS3SecretRef} {
		t.Setenv(name, "")
	}
	config, err := loadEnvironment()
	if err != nil {
		t.Fatalf("S3-disabled environment = %v", err)
	}
	if config.s3AccessKeyID != "" || config.s3SecretKey != "" || config.s3SecretRef != "" {
		t.Fatalf("S3-disabled config = %+v", config)
	}
	t.Setenv(envS3AccessKeyID, "access")
	t.Setenv(envS3SecretKey, "secret")
	t.Setenv(envS3SecretRef, "env/custom-s3")
	config, err = loadEnvironment()
	if err != nil || config.s3AccessKeyID != "access" || config.s3SecretKey != "secret" || config.s3SecretRef != "env/custom-s3" {
		t.Fatalf("S3-enabled config = %+v, %v", config, err)
	}
}

func TestEnvironmentS3CapabilityProviderValidatesScopeAndProbe(t *testing.T) {
	original := newEnvironmentS3Store
	t.Cleanup(func() { newEnvironmentS3Store = original })
	store := &testS3CapabilityStore{}
	newEnvironmentS3Store = func(context.Context, string, backend.CapabilityBinding, modelprofile.SecretValue) (environmentS3Store, error) {
		return store, nil
	}
	secret, err := modelprofile.NewSecretValue("access:secret")
	if err != nil {
		t.Fatal(err)
	}
	provider := environmentS3CapabilityProvider{tenantID: "t_00000000000000000000000000", secretRef: "env/s3"}
	input := backend.StorageFactoryInput{TenantID: "t_00000000000000000000000000"}
	binding := backend.CapabilityBinding{Capability: backend.CapabilityArtifact, Provider: "s3", Endpoint: "https://s3.example.test", SecretRef: "env/s3", Options: map[string]string{"bucket": "tenant-artifacts"}}
	if _, err := provider.New(nil, input, binding, secret); !errors.Is(err, context.Canceled) {
		t.Fatalf("nil S3 provider context = %v", err)
	}
	if _, err := provider.New(canceledContext(), input, binding, secret); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled S3 provider context = %v", err)
	}
	value, err := provider.New(context.Background(), input, binding, secret)
	if err != nil || value != store || store.probes != 1 {
		t.Fatalf("S3 provider = %T, %v, probes=%d", value, err, store.probes)
	}
	if _, err := provider.New(context.Background(), backend.StorageFactoryInput{TenantID: "t_00000000000000000000000001"}, binding, secret); !errors.Is(err, storagefactory.ErrStorageFactory) {
		t.Fatalf("foreign S3 tenant = %v", err)
	}
	store.probeErr = errors.New("unavailable")
	if _, err := provider.New(context.Background(), input, binding, secret); !errors.Is(err, storagefactory.ErrStorageFactory) || store.closes != 1 {
		t.Fatalf("S3 probe failure = %v, closes=%d", err, store.closes)
	}
	store.probeErr = nil
	newEnvironmentS3Store = func(context.Context, string, backend.CapabilityBinding, modelprofile.SecretValue) (environmentS3Store, error) {
		return nil, errors.New("factory unavailable")
	}
	if _, err := provider.New(context.Background(), input, binding, secret); !errors.Is(err, storagefactory.ErrStorageFactory) {
		t.Fatalf("S3 factory failure = %v", err)
	}
	newEnvironmentS3Store = func(context.Context, string, backend.CapabilityBinding, modelprofile.SecretValue) (environmentS3Store, error) {
		return nil, nil
	}
	if _, err := provider.New(context.Background(), input, binding, secret); !errors.Is(err, storagefactory.ErrStorageFactory) {
		t.Fatalf("nil S3 store = %v", err)
	}
}

func TestEnvironmentS3ConfigurationBoundaries(t *testing.T) {
	secret, err := modelprofile.NewSecretValue("access:secret")
	if err != nil {
		t.Fatal(err)
	}
	validBinding := backend.CapabilityBinding{Endpoint: "https://s3.example.test", SecretRef: "env/s3", Options: map[string]string{"bucket": "tenant-artifacts"}}
	if store, err := newEnvironmentS3StoreFromConfig(context.Background(), "t_00000000000000000000000000", validBinding, secret); err != nil || store == nil {
		t.Fatalf("valid S3 store = %T, %v", store, err)
	} else {
		_ = store.Close()
	}
	for _, test := range []struct {
		name   string
		ctx    context.Context
		tenant string
		bind   backend.CapabilityBinding
		secret modelprofile.SecretValue
	}{
		{name: "nil context", tenant: "tenant", bind: validBinding, secret: secret},
		{name: "canceled context", ctx: canceledContext(), tenant: "tenant", bind: validBinding, secret: secret},
		{name: "empty tenant", ctx: context.Background(), bind: validBinding, secret: secret},
		{name: "missing secret", ctx: context.Background(), tenant: "tenant", bind: validBinding},
		{name: "invalid credentials", ctx: context.Background(), tenant: "tenant", bind: validBinding, secret: mustEnvironmentSecret(t, "access")},
		{name: "invalid endpoint", ctx: context.Background(), tenant: "tenant", bind: backend.CapabilityBinding{Endpoint: "http://minio:9000", SecretRef: "env/s3", Options: map[string]string{"bucket": "tenant-artifacts"}}, secret: secret},
		{name: "invalid options", ctx: context.Background(), tenant: "tenant", bind: backend.CapabilityBinding{Endpoint: "https://s3.example.test", SecretRef: "env/s3", Options: map[string]string{"bucket": "BAD"}}, secret: secret},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := test.ctx
			if ctx == nil && test.name != "nil context" {
				ctx = context.Background()
			}
			if _, err := newEnvironmentS3StoreFromConfig(ctx, test.tenant, test.bind, test.secret); !errors.Is(err, storagefactory.ErrStorageFactory) {
				t.Fatalf("newEnvironmentS3StoreFromConfig() = %v", err)
			}
		})
	}

	for _, test := range []struct {
		name  string
		ref   string
		value string
		want  bool
	}{
		{name: "valid", ref: "env/s3", value: "access:secret", want: true},
		{name: "missing ref", value: "access:secret"},
		{name: "missing separator", ref: "env/s3", value: "access"},
		{name: "empty access", ref: "env/s3", value: ":secret"},
		{name: "empty secret", ref: "env/s3", value: "access:"},
		{name: "newline", ref: "env/s3", value: "access:sec\nret"},
	} {
		t.Run("credentials/"+test.name, func(t *testing.T) {
			value, err := modelprofile.NewSecretValue(test.value)
			if test.value == "" {
				value = modelprofile.SecretValue{}
			} else if err != nil {
				t.Fatal(err)
			}
			access, key, parseErr := parseEnvironmentS3Credentials(test.ref, value)
			if (parseErr == nil) != test.want || (test.want && (access != "access" || key != "secret")) {
				t.Fatalf("parseEnvironmentS3Credentials() = %q, %q, %v", access, key, parseErr)
			}
		})
	}
}

func canceledContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

func mustEnvironmentSecret(t *testing.T, value string) modelprofile.SecretValue {
	t.Helper()
	secret, err := modelprofile.NewSecretValue(value)
	if err != nil {
		t.Fatal(err)
	}
	return secret
}

type testS3CapabilityStore struct {
	runtimestorage.ArtifactStore
	runtimestorage.ObjectStore
	probes   int
	closes   int
	probeErr error
}

func (store *testS3CapabilityStore) Probe(context.Context) error {
	store.probes++
	return store.probeErr
}
func (store *testS3CapabilityStore) Close() error { store.closes++; return nil }

type environmentKnowledgeOnlyStore struct {
	environmentStorage
	knowledge runtimestorage.KnowledgeStore
}

func (store *environmentKnowledgeOnlyStore) PutKnowledge(ctx context.Context, document runtimestorage.KnowledgeDocument) (runtimestorage.KnowledgeDocument, error) {
	return store.knowledge.PutKnowledge(ctx, document)
}

func (store *environmentKnowledgeOnlyStore) GetKnowledge(ctx context.Context, tenantID, documentID string) (runtimestorage.KnowledgeDocument, error) {
	return store.knowledge.GetKnowledge(ctx, tenantID, documentID)
}

func (store *environmentKnowledgeOnlyStore) SearchKnowledge(ctx context.Context, tenantID string, embedding []float64, limit int) ([]runtimestorage.KnowledgeSearchResult, error) {
	return store.knowledge.SearchKnowledge(ctx, tenantID, embedding, limit)
}

func (store *environmentKnowledgeOnlyStore) DeleteKnowledge(ctx context.Context, tenantID, documentID string) error {
	return store.knowledge.DeleteKnowledge(ctx, tenantID, documentID)
}

type environmentArtifactOnlyStore struct {
	environmentStorage
	artifact runtimestorage.ArtifactStore
}

func (store *environmentArtifactOnlyStore) PutArtifact(ctx context.Context, artifact runtimestorage.ArtifactRecord) (runtimestorage.ArtifactRecord, error) {
	return store.artifact.PutArtifact(ctx, artifact)
}

func (store *environmentArtifactOnlyStore) GetArtifact(ctx context.Context, tenantID, artifactID string) (runtimestorage.ArtifactRecord, error) {
	return store.artifact.GetArtifact(ctx, tenantID, artifactID)
}

func (store *environmentArtifactOnlyStore) ListArtifacts(ctx context.Context, tenantID, sessionID string) ([]runtimestorage.ArtifactRecord, error) {
	return store.artifact.ListArtifacts(ctx, tenantID, sessionID)
}

func (store *environmentArtifactOnlyStore) DeleteArtifact(ctx context.Context, tenantID, artifactID string) error {
	return store.artifact.DeleteArtifact(ctx, tenantID, artifactID)
}

func TestNewFromEnvironmentMigrationAndReadinessFailures(t *testing.T) {
	setRequiredEnvironment(t)
	registerBootstrapPingDriver.Do(func() {
		sql.Register("trpc-service-bootstrap-ping", bootstrapPingDriver{})
	})
	previousOpen := openEnvironmentDatabase
	previousApply := applyEnvironmentMigrations
	previousVerify := verifyEnvironmentMigrations
	defer func() {
		openEnvironmentDatabase = previousOpen
		applyEnvironmentMigrations = previousApply
		verifyEnvironmentMigrations = previousVerify
	}()
	for _, test := range []struct {
		name     string
		applyErr error
	}{
		{name: "migration", applyErr: errors.New("migration failed")},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, err := sql.Open("trpc-service-bootstrap-ping", "")
			if err != nil {
				t.Fatal(err)
			}
			openEnvironmentDatabase = func(context.Context, string, postgres.Options) (*sql.DB, error) { return db, nil }
			applyEnvironmentMigrations = func(context.Context, *sql.DB) error { return test.applyErr }
			verifyEnvironmentMigrations = func(context.Context, *sql.DB) error { return nil }
			if _, err := NewFromEnvironment(context.Background()); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("migration error = %v", err)
			}
			_ = db.Close()
		})
	}

	db, err := sql.Open("trpc-service-bootstrap-ping", "")
	if err != nil {
		t.Fatal(err)
	}
	openEnvironmentDatabase = func(context.Context, string, postgres.Options) (*sql.DB, error) { return db, nil }
	applyEnvironmentMigrations = func(context.Context, *sql.DB) error { return nil }
	verifyEnvironmentMigrations = func(context.Context, *sql.DB) error { return errors.New("verification failed") }
	if _, err := NewFromEnvironment(context.Background()); !errors.Is(err, ErrInvalidConfig) {
		_ = db.Close()
		t.Fatalf("migration verification error = %v", err)
	}
}

func TestNewFromEnvironmentMigratesBeforeResolvingWeComAIBotBindings(t *testing.T) {
	setRequiredEnvironment(t)
	t.Setenv(envWeComAIBotConnections, `[{"binding_id":"cb_00000000000000000000000000","secret_ref":"env/wecom-aibot","bot_secret":"test-secret"}]`)
	registerBootstrapPingDriver.Do(func() {
		sql.Register("trpc-service-bootstrap-ping", bootstrapPingDriver{})
	})
	db, err := sql.Open("trpc-service-bootstrap-ping", "")
	if err != nil {
		t.Fatal(err)
	}
	previousOpen := openEnvironmentDatabase
	previousApply := applyEnvironmentMigrations
	previousVerify := verifyEnvironmentMigrations
	t.Cleanup(func() {
		openEnvironmentDatabase = previousOpen
		applyEnvironmentMigrations = previousApply
		verifyEnvironmentMigrations = previousVerify
		_ = db.Close()
	})
	migrated := false
	openEnvironmentDatabase = func(context.Context, string, postgres.Options) (*sql.DB, error) { return db, nil }
	applyEnvironmentMigrations = func(context.Context, *sql.DB) error {
		migrated = true
		return nil
	}
	verifyEnvironmentMigrations = func(context.Context, *sql.DB) error { return nil }
	if _, err := NewFromEnvironment(context.Background()); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("environment assembly error = %v", err)
	}
	if !migrated {
		t.Fatal("AI Bot binding lookup ran before PostgreSQL migrations")
	}
}

func TestEnvironmentCatalogsAndIdentityListBoundaries(t *testing.T) {
	config := environmentConfig{modelProvider: defaultModelProvider, modelNames: []string{"gpt-4o-mini"}, endpointHosts: []string{"api.openai.com"}, secretRef: "env/model"}
	if _, _, err := environmentCatalogs(config); err != nil {
		t.Fatal(err)
	}
	config.modelProvider = "unknown"
	if _, _, err := environmentCatalogs(config); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("unsupported catalog = %v", err)
	}
	if _, err := parseEnvironmentAPIIdentities(""); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("empty identity list = %v", err)
	}
}

func TestLoadEnvironmentRedisConfigurationIsExplicitAndBounded(t *testing.T) {
	setRequiredEnvironment(t)
	t.Setenv(envSessionBackend, "redis")
	t.Setenv(envRedisAddr, "127.0.0.1:6379")
	t.Setenv(envRedisPassword, "redis-secret")
	t.Setenv(envRedisDB, "3")
	t.Setenv(envRedisKeyPrefix, "service:runtime")
	t.Setenv(envRedisSecretRef, "env/redis-password")
	t.Setenv(envRedisDialTimeout, "150ms")
	t.Setenv(envRedisReadTimeout, "250ms")
	t.Setenv(envRedisWriteTimeout, "350ms")
	t.Setenv(envRedisPoolSize, "12")

	config, err := loadEnvironment()
	if err != nil {
		t.Fatal(err)
	}
	if config.redis.Addr != "127.0.0.1:6379" || config.redis.Password != "redis-secret" || config.redis.DB != 3 || config.redis.KeyPrefix != "service:runtime" || config.redis.PoolSize != 12 {
		t.Fatalf("redis config = %+v", config.redis)
	}
	if config.redisEndpoint != "redis://127.0.0.1:6379" || config.redisSecretRef != "env/redis-password" || config.redis.DialTimeout.String() != "150ms" || config.redis.ReadTimeout.String() != "250ms" || config.redis.WriteTimeout.String() != "350ms" {
		t.Fatalf("redis endpoint/timeouts = %q/%q/%s/%s/%s", config.redisEndpoint, config.redisSecretRef, config.redis.DialTimeout, config.redis.ReadTimeout, config.redis.WriteTimeout)
	}

	for _, test := range []struct {
		name  string
		value string
	}{
		{name: "missing address", value: ""},
		{name: "invalid db", value: "not-an-integer"},
		{name: "invalid timeout", value: "not-a-duration"},
	} {
		t.Run(test.name, func(t *testing.T) {
			setRequiredEnvironment(t)
			t.Setenv(envSessionBackend, "redis")
			t.Setenv(envRedisAddr, "127.0.0.1:6379")
			switch test.name {
			case "missing address":
				t.Setenv(envRedisAddr, test.value)
			case "invalid db":
				t.Setenv(envRedisDB, test.value)
			case "invalid timeout":
				t.Setenv(envRedisDialTimeout, test.value)
			}
			if _, err := loadEnvironment(); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("redis configuration error = %v", err)
			}
		})
	}
}

func TestEnvironmentRedisCatalogAndRegistryBoundaries(t *testing.T) {
	const (
		tenantA = "t_00000000000000000000000000"
		tenantB = "t_00000000000000000000000001"
	)
	config := environmentConfig{runtimeStorage: "redis", redisEndpoint: "redis://127.0.0.1:6379", redisSecretRef: "env/redis-password", redis: runtimestorageredis.Config{Password: "redis-secret"}, modelProvider: defaultModelProvider, modelNames: []string{"gpt-4o-mini"}, endpointHosts: []string{"api.openai.com"}, secretRef: "env/model", apiIdentities: map[string]gateway.APIIdentity{
		"token-a": {TenantID: tenantA, AppID: "app-a", SubjectID: "subject-a"},
		"token-b": {TenantID: tenantB, AppID: "app-b", SubjectID: "subject-b"},
	}, modelAPIKeys: map[string]string{tenantA: "model-a", tenantB: "model-b"}}
	_, backendCatalog, err := environmentCatalogs(config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := backend.NewProfile(backend.CreateInput{TenantID: tenantA, ProfileKey: "redis", DisplayName: "Redis", Bindings: []backend.CapabilityBinding{{Capability: backend.CapabilitySession, Provider: "redis", Endpoint: config.redisEndpoint, SecretRef: config.redisSecretRef}}}, backendCatalog); err != nil {
		t.Fatalf("redis session binding = %v", err)
	}
	if _, err := backend.NewProfile(backend.CreateInput{TenantID: tenantA, ProfileKey: "redis-summary", DisplayName: "Redis Summary", Bindings: []backend.CapabilityBinding{{Capability: backend.CapabilitySummary, Provider: "redis", Endpoint: config.redisEndpoint}}}, backendCatalog); !errors.Is(err, backend.ErrInvalid) {
		t.Fatalf("unsupported redis capability = %v", err)
	}
	if _, err := backend.NewProfile(backend.CreateInput{TenantID: tenantA, ProfileKey: "redis-endpoint", DisplayName: "Redis Endpoint", Bindings: []backend.CapabilityBinding{{Capability: backend.CapabilitySession, Provider: "redis", Endpoint: "redis://other:6379"}}}, backendCatalog); err != nil {
		t.Fatalf("catalog should accept a valid redis endpoint before provider binding: %v", err)
	}

	delegate := inmemory.NewSessionService()
	redisStore := runtimestorageinmemory.New()
	inMemoryStore := runtimestorageinmemory.New()
	t.Cleanup(func() { _ = delegate.Close(); _ = redisStore.Close(); _ = inMemoryStore.Close() })
	secrets, _, providers, err := environmentRegistriesForStores(config, delegate, environmentRuntimeStores{
		primary: redisStore,
		providers: map[string]environmentStorage{
			"redis":    redisStore,
			"inmemory": inMemoryStore,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	secret, err := secrets.Resolve(context.Background(), modelprofile.SecretScope{TenantID: tenantA, SecretRef: config.redisSecretRef})
	if err != nil || secret.Value() != "redis-secret" {
		t.Fatalf("tenant redis secret = %q, %v", secret.Value(), err)
	}
	if _, err := secrets.Resolve(context.Background(), modelprofile.SecretScope{TenantID: tenantA, SecretRef: "env/other"}); err == nil {
		t.Fatal("foreign redis secret reference was accepted")
	}
	provider, err := providers.Resolve(context.Background(), backend.StorageFactoryInput{TenantID: tenantA}, backend.CapabilityBinding{Capability: backend.CapabilitySession, Provider: "redis"})
	if err != nil || provider == nil {
		t.Fatalf("tenant redis provider = %v", err)
	}
	if _, err := providers.Resolve(context.Background(), backend.StorageFactoryInput{TenantID: tenantA}, backend.CapabilityBinding{Capability: backend.CapabilitySummary, Provider: "redis"}); !errors.Is(err, storagefactory.ErrProviderUnavailable) {
		t.Fatalf("unsupported redis provider capability = %v", err)
	}
	if _, err := providers.Resolve(context.Background(), backend.StorageFactoryInput{TenantID: "t_00000000000000000000000002"}, backend.CapabilityBinding{Capability: backend.CapabilitySession, Provider: "redis"}); !errors.Is(err, storagefactory.ErrProviderUnavailable) {
		t.Fatalf("unregistered tenant redis provider = %v", err)
	}
	value, err := provider.New(context.Background(), backend.StorageFactoryInput{TenantID: tenantA}, backend.CapabilityBinding{Capability: backend.CapabilitySession, Provider: "redis", Endpoint: "redis://other:6379"}, secret)
	if !errors.Is(err, storagefactory.ErrStorageFactory) || value != nil {
		t.Fatalf("mismatched redis endpoint = %T, %v", value, err)
	}
	if strings.Contains(err.Error(), "redis://other:6379") {
		t.Fatal("redis endpoint leaked in provider error")
	}
	value, err = provider.New(context.Background(), backend.StorageFactoryInput{TenantID: tenantA}, backend.CapabilityBinding{Capability: backend.CapabilitySession, Provider: "redis", Endpoint: config.redisEndpoint, SecretRef: "env/other"}, secret)
	if !errors.Is(err, storagefactory.ErrStorageFactory) || value != nil {
		t.Fatalf("mismatched redis secret reference = %T, %v", value, err)
	}
}

func TestEnvironmentRedisProfilesUseSeparateInMemoryProvider(t *testing.T) {
	const (
		tenantA   = "t_00000000000000000000000000"
		tenantB   = "t_00000000000000000000000001"
		sessionID = "same-session"
		memoryID  = "same-memory"
	)
	config := environmentConfig{runtimeStorage: "redis", redisEndpoint: "redis://127.0.0.1:6379", redisSecretRef: "env/redis-password", redis: runtimestorageredis.Config{Password: "redis-secret"}, modelProvider: defaultModelProvider, modelNames: []string{"gpt-4o-mini"}, endpointHosts: []string{"api.openai.com"}, secretRef: "env/model", apiIdentities: map[string]gateway.APIIdentity{
		"token-a": {TenantID: tenantA, AppID: "app-a", SubjectID: "subject-a"},
		"token-b": {TenantID: tenantB, AppID: "app-b", SubjectID: "subject-b"},
	}, modelAPIKeys: map[string]string{tenantA: "model-a", tenantB: "model-b"}}
	_, catalog, err := environmentCatalogs(config)
	if err != nil {
		t.Fatal(err)
	}
	redisProfile, err := backend.NewProfile(backend.CreateInput{TenantID: tenantA, ProfileKey: "redis-runtime", DisplayName: "Redis Runtime", Bindings: []backend.CapabilityBinding{
		{Capability: backend.CapabilitySession, Provider: "redis", Endpoint: config.redisEndpoint, SecretRef: config.redisSecretRef},
		{Capability: backend.CapabilityMemory, Provider: "redis", Endpoint: config.redisEndpoint, SecretRef: config.redisSecretRef},
	}}, catalog)
	if err != nil {
		t.Fatalf("redis profile = %v", err)
	}
	inMemoryProfile, err := backend.NewProfile(backend.CreateInput{TenantID: tenantB, ProfileKey: "inmemory-runtime", DisplayName: "InMemory Runtime", Bindings: []backend.CapabilityBinding{
		{Capability: backend.CapabilitySession, Provider: "inmemory"},
		{Capability: backend.CapabilityMemory, Provider: "inmemory"},
	}}, catalog)
	if err != nil {
		t.Fatalf("in-memory profile = %v", err)
	}

	delegate := inmemory.NewSessionService()
	redisStore := runtimestorageinmemory.New()
	inMemoryStore := runtimestorageinmemory.New()
	t.Cleanup(func() { _ = delegate.Close(); _ = redisStore.Close(); _ = inMemoryStore.Close() })
	secrets, _, providers, err := environmentRegistriesForStores(config, delegate, environmentRuntimeStores{
		primary: redisStore,
		providers: map[string]environmentStorage{
			"redis":    redisStore,
			"inmemory": inMemoryStore,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	factory, err := storagefactory.NewRegistryStorageFactory(providers, secrets)
	if err != nil {
		t.Fatal(err)
	}
	redisCapabilities, err := factory.New(context.Background(), backend.StorageFactoryInput{TenantID: tenantA, Bindings: redisProfile.Bindings})
	if err != nil {
		t.Fatalf("redis capabilities = %v", err)
	}
	t.Cleanup(func() { _ = redisCapabilities.Close() })
	inMemoryCapabilities, err := factory.New(context.Background(), backend.StorageFactoryInput{TenantID: tenantB, Bindings: inMemoryProfile.Bindings})
	if err != nil {
		t.Fatalf("in-memory capabilities = %v", err)
	}
	t.Cleanup(func() { _ = inMemoryCapabilities.Close() })
	if _, err := redisCapabilities.Session(); err != nil {
		t.Fatalf("redis session capability = %v", err)
	}
	if _, err := inMemoryCapabilities.Session(); err != nil {
		t.Fatalf("in-memory session capability = %v", err)
	}
	redisMemory, err := redisCapabilities.Memory()
	if err != nil {
		t.Fatalf("redis memory capability = %v", err)
	}
	inMemoryMemory, err := inMemoryCapabilities.Memory()
	if err != nil {
		t.Fatalf("in-memory memory capability = %v", err)
	}
	ctx := context.Background()
	if _, err := redisMemory.PutMemory(ctx, runtimestorage.MemoryInput{TenantID: tenantA, MemoryID: memoryID, UserID: "user", Content: "redis"}); err != nil {
		t.Fatalf("redis memory write = %v", err)
	}
	if _, err := inMemoryMemory.PutMemory(ctx, runtimestorage.MemoryInput{TenantID: tenantB, MemoryID: memoryID, UserID: "user", Content: "in-memory"}); err != nil {
		t.Fatalf("in-memory memory write = %v", err)
	}
	if _, err := redisStore.CreateSession(ctx, tenantA, sessionID, map[string]any{"provider": "redis"}); err != nil {
		t.Fatalf("redis session write = %v", err)
	}
	if _, err := inMemoryStore.CreateSession(ctx, tenantB, sessionID, map[string]any{"provider": "in-memory"}); err != nil {
		t.Fatalf("in-memory session write = %v", err)
	}
	assertEnvironmentRuntimeStoreIsolation(t, redisStore, inMemoryStore, tenantA, tenantB, sessionID, memoryID)
}

func TestEnvironmentRedisRuntimeStoresOwnPrimaryAndFallback(t *testing.T) {
	primary := &environmentRuntimeStoreSpy{}
	fallback := &environmentRuntimeStoreSpy{}
	previousRedis := newEnvironmentRedisRuntimeStore
	previousFallback := newEnvironmentInMemoryFallback
	newEnvironmentRedisRuntimeStore = func(context.Context, environmentConfig) (environmentStorage, error) {
		return primary, nil
	}
	newEnvironmentInMemoryFallback = func() environmentStorage { return fallback }
	t.Cleanup(func() {
		newEnvironmentRedisRuntimeStore = previousRedis
		newEnvironmentInMemoryFallback = previousFallback
	})
	stores, err := newEnvironmentRuntimeStoresForConfig(context.Background(), environmentConfig{runtimeStorage: "redis"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if stores.primary != primary || stores.providers["redis"] != primary || stores.providers["inmemory"] != fallback || len(stores.owned) != 2 {
		t.Fatalf("redis runtime stores = %#v", stores)
	}
	if err := stores.Close(); err != nil || primary.closed != 1 || fallback.closed != 1 {
		t.Fatalf("runtime store close = %v, primary closes=%d fallback closes=%d", err, primary.closed, fallback.closed)
	}
	if _, err := environmentRuntimeProviders(environmentConfig{runtimeStorage: "redis"}, environmentRuntimeStores{providers: map[string]environmentStorage{"redis": primary}}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("missing in-memory fallback = %v", err)
	}
	if _, err := environmentRuntimeProviders(environmentConfig{runtimeStorage: "redis"}, environmentRuntimeStores{}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("missing redis primary = %v", err)
	}
}

func TestEnvironmentRedisRuntimeStoreFailsClosed(t *testing.T) {
	server := miniredis.RunT(t)
	config := environmentConfig{redis: runtimestorageredis.Config{Addr: server.Addr()}}
	store, err := environmentRedisRuntimeStore(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	addr := server.Addr()
	server.Close()
	if _, err := environmentRedisRuntimeStore(context.Background(), environmentConfig{redis: runtimestorageredis.Config{Addr: addr}}); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("unavailable Redis runtime store = %v", err)
	}
}

type environmentRuntimeStoreSpy struct {
	environmentStorage
	closed int
}

func (store *environmentRuntimeStoreSpy) Close() error {
	store.closed++
	return nil
}

func assertEnvironmentRuntimeStoreIsolation(t *testing.T, redisStore, inMemoryStore environmentStorage, tenantA, tenantB, sessionID, memoryID string) {
	t.Helper()
	ctx := context.Background()
	redisSession, err := redisStore.GetSession(ctx, tenantA, sessionID)
	if err != nil || redisSession.State["provider"] != "redis" {
		t.Fatalf("redis session = %#v, %v", redisSession, err)
	}
	inMemorySession, err := inMemoryStore.GetSession(ctx, tenantB, sessionID)
	if err != nil || inMemorySession.State["provider"] != "in-memory" {
		t.Fatalf("in-memory session = %#v, %v", inMemorySession, err)
	}
	redisMemory, err := redisStore.(runtimestorage.MemoryStore).GetMemory(ctx, tenantA, memoryID)
	if err != nil || redisMemory.Content != "redis" {
		t.Fatalf("redis memory = %#v, %v", redisMemory, err)
	}
	inMemoryMemory, err := inMemoryStore.(runtimestorage.MemoryStore).GetMemory(ctx, tenantB, memoryID)
	if err != nil || inMemoryMemory.Content != "in-memory" {
		t.Fatalf("in-memory memory = %#v, %v", inMemoryMemory, err)
	}
	if _, err := redisStore.GetSession(ctx, tenantB, sessionID); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("redis session leaked into in-memory tenant = %v", err)
	}
	if _, err := inMemoryStore.GetSession(ctx, tenantA, sessionID); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("in-memory session leaked into redis tenant = %v", err)
	}
	if _, err := redisStore.(runtimestorage.MemoryStore).GetMemory(ctx, tenantB, memoryID); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("redis memory leaked into in-memory tenant = %v", err)
	}
	if _, err := inMemoryStore.(runtimestorage.MemoryStore).GetMemory(ctx, tenantA, memoryID); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("in-memory memory leaked into redis tenant = %v", err)
	}
}

func TestNewFromEnvironmentRedisConnectionFailureIsRedacted(t *testing.T) {
	setRequiredEnvironment(t)
	t.Setenv(envSessionBackend, "redis")
	t.Setenv(envRedisAddr, "redis.internal:6379")
	t.Setenv(envRedisPassword, "redis-password")
	registerBootstrapPingDriver.Do(func() { sql.Register("trpc-service-bootstrap-ping", bootstrapPingDriver{}) })
	db, err := sql.Open("trpc-service-bootstrap-ping", "")
	if err != nil {
		t.Fatal(err)
	}
	previousOpen := openEnvironmentDatabase
	previousApply := applyEnvironmentMigrations
	previousVerify := verifyEnvironmentMigrations
	previousRedis := newEnvironmentRedisRuntimeStore
	t.Cleanup(func() {
		openEnvironmentDatabase = previousOpen
		applyEnvironmentMigrations = previousApply
		verifyEnvironmentMigrations = previousVerify
		newEnvironmentRedisRuntimeStore = previousRedis
		_ = db.Close()
	})
	openEnvironmentDatabase = func(context.Context, string, postgres.Options) (*sql.DB, error) { return db, nil }
	applyEnvironmentMigrations = func(context.Context, *sql.DB) error { return nil }
	verifyEnvironmentMigrations = func(context.Context, *sql.DB) error { return nil }
	newEnvironmentRedisRuntimeStore = func(context.Context, environmentConfig) (environmentStorage, error) {
		return nil, errors.New("dial redis.internal:6379 with password redis-password failed")
	}
	_, err = NewFromEnvironment(context.Background())
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("redis connection error = %v", err)
	}
	if strings.Contains(err.Error(), "redis.internal:6379") || strings.Contains(err.Error(), "redis-password") {
		t.Fatalf("redis connection details leaked: %v", err)
	}
}

func TestNewFromEnvironmentRuntimeStoreFailureBoundaries(t *testing.T) {
	tests := []struct {
		name           string
		runtimeStorage string
		configureStore func(context.CancelFunc)
		wantError      error
	}{
		{
			name:           "redis cancellation wins",
			runtimeStorage: "redis",
			configureStore: func(cancel context.CancelFunc) {
				newEnvironmentRedisRuntimeStore = func(context.Context, environmentConfig) (environmentStorage, error) {
					cancel()
					return nil, errors.New("redis dial failure")
				}
			},
			wantError: context.Canceled,
		},
		{
			name:           "non redis preserves source error",
			runtimeStorage: "inmemory",
			configureStore: func(context.CancelFunc) {
				newEnvironmentRuntimeStore = func(string, *sql.DB) (environmentStorage, error) {
					return nil, errEnvironmentRuntimeStore
				}
			},
			wantError: errEnvironmentRuntimeStore,
		},
		{
			name:           "runtime store lacks reply batches",
			runtimeStorage: "inmemory",
			configureStore: func(context.CancelFunc) {
				newEnvironmentRuntimeStore = func(string, *sql.DB) (environmentStorage, error) {
					return &environmentRuntimeStoreSpy{}, nil
				}
			},
			wantError: ErrInvalidConfig,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setRequiredEnvironment(t)
			t.Setenv(envSessionBackend, tt.runtimeStorage)
			if tt.runtimeStorage == "redis" {
				t.Setenv(envRedisAddr, "redis.internal:6379")
			}
			registerBootstrapPingDriver.Do(func() { sql.Register("trpc-service-bootstrap-ping", bootstrapPingDriver{}) })
			db, err := sql.Open("trpc-service-bootstrap-ping", "")
			if err != nil {
				t.Fatal(err)
			}
			previousOpen := openEnvironmentDatabase
			previousApply := applyEnvironmentMigrations
			previousVerify := verifyEnvironmentMigrations
			previousRedis := newEnvironmentRedisRuntimeStore
			previousStore := newEnvironmentRuntimeStore
			t.Cleanup(func() {
				openEnvironmentDatabase = previousOpen
				applyEnvironmentMigrations = previousApply
				verifyEnvironmentMigrations = previousVerify
				newEnvironmentRedisRuntimeStore = previousRedis
				newEnvironmentRuntimeStore = previousStore
				_ = db.Close()
			})
			openEnvironmentDatabase = func(context.Context, string, postgres.Options) (*sql.DB, error) { return db, nil }
			applyEnvironmentMigrations = func(context.Context, *sql.DB) error { return nil }
			verifyEnvironmentMigrations = func(context.Context, *sql.DB) error { return nil }
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			tt.configureStore(cancel)

			_, err = NewFromEnvironment(ctx)
			if !errors.Is(err, tt.wantError) {
				t.Fatalf("runtime store failure = %v, want %v", err, tt.wantError)
			}
			if pingErr := db.Ping(); pingErr == nil {
				t.Fatal("database remained open after runtime store failure")
			}
		})
	}
}

func TestNewFromEnvironmentDoesNotInitializeRedisStoresBeforeMigrationSucceeds(t *testing.T) {
	setRequiredEnvironment(t)
	t.Setenv(envSessionBackend, "redis")
	t.Setenv(envRedisAddr, "redis.internal:6379")
	registerBootstrapPingDriver.Do(func() { sql.Register("trpc-service-bootstrap-ping", bootstrapPingDriver{}) })
	db, err := sql.Open("trpc-service-bootstrap-ping", "")
	if err != nil {
		t.Fatal(err)
	}
	primary := &environmentRuntimeStoreSpy{}
	fallback := &environmentRuntimeStoreSpy{}
	primaryCreated, fallbackCreated := 0, 0
	previousOpen := openEnvironmentDatabase
	previousApply := applyEnvironmentMigrations
	previousRedis := newEnvironmentRedisRuntimeStore
	previousFallback := newEnvironmentInMemoryFallback
	t.Cleanup(func() {
		openEnvironmentDatabase = previousOpen
		applyEnvironmentMigrations = previousApply
		newEnvironmentRedisRuntimeStore = previousRedis
		newEnvironmentInMemoryFallback = previousFallback
		_ = db.Close()
	})
	openEnvironmentDatabase = func(context.Context, string, postgres.Options) (*sql.DB, error) { return db, nil }
	applyEnvironmentMigrations = func(context.Context, *sql.DB) error { return errors.New("migration failed") }
	newEnvironmentRedisRuntimeStore = func(context.Context, environmentConfig) (environmentStorage, error) {
		primaryCreated++
		return primary, nil
	}
	newEnvironmentInMemoryFallback = func() environmentStorage {
		fallbackCreated++
		return fallback
	}

	if _, err := NewFromEnvironment(context.Background()); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("bootstrap failure = %v", err)
	}
	if primaryCreated != 0 || fallbackCreated != 0 {
		t.Fatalf("runtime stores initialized before migration success: primary=%d fallback=%d", primaryCreated, fallbackCreated)
	}
	if pingErr := db.Ping(); pingErr == nil {
		t.Fatal("database remained open after bootstrap failure")
	}
}

var errEnvironmentRuntimeStore = errors.New("runtime store initialization failed")

func TestDemoEnvironmentConfigurationBranches(t *testing.T) {
	setRequiredEnvironment(t)
	t.Setenv(envDemoMode, "not-bool")
	if _, err := loadEnvironment(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("invalid demo mode = %v", err)
	}

	t.Setenv(envDemoMode, "true")
	t.Setenv(envModelProvider, demoModelProvider)
	t.Setenv(envModelNames, ",")
	config := environmentConfig{demoMode: true, modelProvider: demoModelProvider}
	if err := config.loadModel(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("invalid demo model list = %v", err)
	}

	config = environmentConfig{demoMode: true, driver: ControlPlaneDriverPostgres, runtimeStorage: "postgres"}
	if err := config.loadRuntime(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("demo postgres runtime storage = %v", err)
	}
	config.driver = ControlPlaneDriverMySQL
	config.runtimeStorage = "inmemory"
	if err := config.loadRuntime(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("demo MySQL control plane = %v", err)
	}

	if _, _, err := environmentCatalogs(environmentConfig{demoMode: true, modelProvider: defaultModelProvider, modelNames: []string{demoModelName}}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("demo production provider = %v", err)
	}
	if _, _, err := environmentCatalogs(environmentConfig{demoMode: true, modelProvider: demoModelProvider}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("empty demo model catalog = %v", err)
	}

	if value, err := environmentBool("TEST_DEMO_BOOL"); err != nil || value {
		t.Fatalf("empty environment bool = %v, %v", value, err)
	}
	t.Setenv("TEST_DEMO_BOOL", "true")
	if value, err := environmentBool("TEST_DEMO_BOOL"); err != nil || !value {
		t.Fatalf("true environment bool = %v, %v", value, err)
	}
	t.Setenv("TEST_DEMO_BOOL", "false")
	if value, err := environmentBool("TEST_DEMO_BOOL"); err != nil || value {
		t.Fatalf("false environment bool = %v, %v", value, err)
	}
	t.Setenv("TEST_DEMO_BOOL", "invalid")
	if _, err := environmentBool("TEST_DEMO_BOOL"); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("invalid environment bool = %v", err)
	}
}

func TestEnvironmentDemoRegistriesAreCredentialFree(t *testing.T) {
	const tenantID = "t_00000000000000000000000000"
	delegate := inmemory.NewSessionService()
	store := runtimestorageinmemory.New()
	t.Cleanup(func() {
		_ = delegate.Close()
		_ = store.Close()
	})
	config := environmentConfig{
		demoMode: true, modelProvider: demoModelProvider, modelNames: []string{demoModelName}, runtimeStorage: "inmemory",
		apiIdentities: map[string]gateway.APIIdentity{"demo-token": {TenantID: tenantID, AppID: "app_00000000000000000000000000", SubjectID: "demo"}},
	}
	secrets, models, backends, err := environmentRegistries(config, delegate, store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := secrets.Resolve(context.Background(), modelprofile.SecretScope{TenantID: tenantID, SecretRef: "env/model"}); err == nil {
		t.Fatal("demo registry unexpectedly stored a model secret")
	}
	model, err := models.New(context.Background(), modelprofile.ModelFactoryInput{TenantID: tenantID, Provider: demoModelProvider, Model: demoModelName}, modelprofile.SecretValue{})
	if err != nil || model == nil || model.Info().Name != demoModelName {
		t.Fatalf("demo model registry = %T, %v", model, err)
	}
	for _, capability := range []backend.Capability{backend.CapabilitySession, backend.CapabilityMemory, backend.CapabilitySummary, backend.CapabilityKnowledge, backend.CapabilityArtifact, backend.CapabilityAudit} {
		provider, resolveErr := backends.Resolve(context.Background(), backend.StorageFactoryInput{TenantID: tenantID}, backend.CapabilityBinding{Capability: capability, Provider: "inmemory"})
		if resolveErr != nil || provider == nil {
			t.Fatalf("demo backend provider %s = %v", capability, resolveErr)
		}
	}
	if _, _, _, err := environmentRegistries(environmentConfig{demoMode: true, modelProvider: demoModelProvider, apiIdentities: map[string]gateway.APIIdentity{"bad": {TenantID: "invalid", AppID: "app", SubjectID: "subject"}}}, delegate, store); err == nil {
		t.Fatal("invalid demo tenant registration was accepted")
	}
}

func TestNewFromEnvironmentBuildsDemoGraphWithoutCredentials(t *testing.T) {
	setRequiredEnvironment(t)
	t.Setenv(envDemoMode, "true")
	t.Setenv(envModelProvider, demoModelProvider)
	t.Setenv(envModelAPIKey, "")
	t.Setenv(envModelAPIKeys, "")
	t.Setenv(envSessionBackend, "inmemory")
	registerBootstrapPingDriver.Do(func() {
		sql.Register("trpc-service-bootstrap-ping", bootstrapPingDriver{})
	})
	db, err := sql.Open("trpc-service-bootstrap-ping", "")
	if err != nil {
		t.Fatal(err)
	}
	previousOpen := openEnvironmentDatabase
	previousApply := applyEnvironmentMigrations
	previousVerify := verifyEnvironmentMigrations
	openEnvironmentDatabase = func(context.Context, string, postgres.Options) (*sql.DB, error) { return db, nil }
	applyEnvironmentMigrations = func(context.Context, *sql.DB) error { return nil }
	verifyEnvironmentMigrations = func(context.Context, *sql.DB) error { return nil }
	t.Cleanup(func() {
		openEnvironmentDatabase = previousOpen
		applyEnvironmentMigrations = previousApply
		verifyEnvironmentMigrations = previousVerify
		_ = db.Close()
	})
	graph, err := NewFromEnvironment(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !graph.Ready() {
		t.Fatal("demo environment graph is not ready")
	}
	if err := graph.Close(); err != nil {
		t.Fatal(err)
	}
}
