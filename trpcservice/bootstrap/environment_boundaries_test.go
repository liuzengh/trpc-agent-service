package bootstrap

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	"github.com/XnLemon/trpc-agent-service/trpcservice/gateway"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	storagefactory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/factory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/inmemory"
)

func TestEnvironmentConfigErrorBranches(t *testing.T) {
	t.Run("load environment telemetry", func(t *testing.T) {
		setRequiredEnvironment(t)
		t.Setenv(envControlPlaneDriver, string(ControlPlaneDriverPostgres))
		t.Setenv(envModelProvider, defaultModelProvider)
		t.Setenv(envModelSecretRef, "env/model")
		t.Setenv(envModelNames, "gpt-4o-mini")
		t.Setenv(envModelEndpointHost, "api.openai.com")
		t.Setenv(envDemoMode, "false")
		t.Setenv(envOTLPHeaders, "invalid-header")
		if _, err := loadEnvironment(); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("invalid telemetry headers error = %v", err)
		}
	})

	t.Run("S3 credentials and scope", func(t *testing.T) {
		t.Setenv(envS3AccessKeyID, "access")
		t.Setenv(envS3SecretKey, "")
		if err := (&environmentConfig{}).loadS3(); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("partial S3 credentials error = %v", err)
		}
		t.Setenv(envS3SecretKey, "secret")
		config := environmentConfig{apiIdentities: map[string]gateway.APIIdentity{"token": {TenantID: "invalid", AppID: "app", SubjectID: "subject"}}}
		if err := config.loadS3(); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("invalid S3 scope error = %v", err)
		}
	})

	t.Run("telemetry parser", func(t *testing.T) {
		t.Setenv(envOTLPHeaders, "key=value,key=other")
		if err := (&environmentConfig{}).loadTelemetry(); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("duplicate telemetry header error = %v", err)
		}
	})

	t.Run("identity and admin parsers", func(t *testing.T) {
		t.Setenv(envAPIIdentities, "malformed")
		config := environmentConfig{}
		if err := config.loadIdentities(); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("malformed API identity error = %v", err)
		}
		t.Setenv(envAdminToken, "admin")
		t.Setenv(envAdminTenants, ",")
		if err := (&environmentConfig{}).loadAdmin(); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("malformed admin tenant list error = %v", err)
		}
	})

	t.Run("model and runtime modes", func(t *testing.T) {
		t.Setenv(envModelAPIKey, "model-key")
		t.Setenv(envModelNames, "model")
		t.Setenv(envModelEndpointHost, "api.openai.com")
		if err := (&environmentConfig{}).loadModel(); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("missing model provider and secret reference error = %v", err)
		}
		if err := (&environmentConfig{demoMode: true, runtimeStorage: "redis"}).loadRuntime(); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("demo Redis runtime error = %v", err)
		}
	})

	t.Run("Redis option validation", func(t *testing.T) {
		for _, test := range []struct {
			name  string
			value string
			set   func(string)
		}{
			{name: "address control character", value: "redis\ninternal:6379", set: func(value string) { t.Setenv(envRedisAddr, value) }},
			{name: "read timeout", value: "bad", set: func(value string) { t.Setenv(envRedisReadTimeout, value) }},
			{name: "write timeout", value: "bad", set: func(value string) { t.Setenv(envRedisWriteTimeout, value) }},
			{name: "pool size", value: "bad", set: func(value string) { t.Setenv(envRedisPoolSize, value) }},
			{name: "key prefix", value: "runtime\nprefix", set: func(value string) { t.Setenv(envRedisKeyPrefix, value) }},
			{name: "password control character", value: "secret\nvalue", set: func(value string) { t.Setenv(envRedisPassword, value) }},
		} {
			t.Run(test.name, func(t *testing.T) {
				t.Setenv(envRedisAddr, "redis.internal:6379")
				t.Setenv(envRedisReadTimeout, "")
				t.Setenv(envRedisWriteTimeout, "")
				t.Setenv(envRedisPoolSize, "")
				t.Setenv(envRedisKeyPrefix, "runtime")
				t.Setenv(envRedisPassword, "")
				test.set(test.value)
				if err := (&environmentConfig{}).loadRedis(); !errors.Is(err, ErrInvalidConfig) {
					t.Fatalf("Redis %s error = %v", test.name, err)
				}
			})
		}
		if err := (&environmentConfig{demoMode: true, runtimeStorage: "redis"}).loadRuntime(); err == nil {
			t.Fatal("demo Redis runtime unexpectedly succeeded")
		}
	})

	t.Run("WeCom configuration", func(t *testing.T) {
		for _, name := range []string{envWeComCallbackToken, envWeComEncodingAESKey, envWeComAppSecret, envWeComSecretRef} {
			t.Setenv(name, "configured")
		}
		if err := (&environmentConfig{}).loadWeCom(); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("multi-identity WeCom configuration error = %v", err)
		}
		validIdentity := gateway.APIIdentity{TenantID: "t_00000000000000000000000000", AppID: "app_00000000000000000000000000", SubjectID: "subject"}
		config := environmentConfig{apiIdentities: map[string]gateway.APIIdentity{"one": validIdentity}}
		t.Setenv(envWeComAIBotConnections, `[{"binding_id":"binding","secret_ref":"env/one","bot_secret":"secret"}] {}`)
		if err := config.loadWeComAIBots(); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("trailing AI Bot JSON error = %v", err)
		}
		config.demoMode = true
		t.Setenv(envWeComAIBotConnections, `[{"binding_id":"binding","secret_ref":"env/one","bot_secret":"secret"}]`)
		if err := config.loadWeComAIBots(); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("demo AI Bot configuration error = %v", err)
		}
		config.demoMode = false
		config.apiIdentities = map[string]gateway.APIIdentity{"one": validIdentity, "two": validIdentity}
		if err := config.loadWeComAIBots(); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("multi-identity AI Bot configuration error = %v", err)
		}
		config.apiIdentities = map[string]gateway.APIIdentity{"one": validIdentity}
		t.Setenv(envWeComAIBotConnections, `[{"binding_id":"one","secret_ref":"bad","bot_secret":"secret"}]`)
		if err := config.loadWeComAIBots(); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("invalid AI Bot scope error = %v", err)
		}
		t.Setenv(envWeComAIBotConnections, `[{"binding_id":"one","secret_ref":"env/duplicate","bot_secret":"first"},{"binding_id":"two","secret_ref":"env/duplicate","bot_secret":"second"}]`)
		if err := config.loadWeComAIBots(); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("duplicate AI Bot secret reference error = %v", err)
		}
	})
}

func TestEnvironmentPingAndCatalogBoundaryBranches(t *testing.T) {
	if err := environmentPing(nil, ControlPlaneDriverPostgres, nil, nil); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil ping context error = %v", err)
	}
	canceled := canceledContext()
	if err := environmentPing(canceled, ControlPlaneDriverPostgres, nil, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled ping error = %v", err)
	}
	registerBootstrapPingDriver.Do(func() { sql.Register("trpc-service-bootstrap-ping", bootstrapPingDriver{}) })
	db, err := sql.Open("trpc-service-bootstrap-ping", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := environmentPing(context.Background(), ControlPlaneDriverMySQL, db, nil); err != nil {
		t.Fatalf("MySQL ping error = %v", err)
	}
	if err := environmentPing(context.Background(), ControlPlaneDriverPostgres, db, nil); err != nil {
		t.Fatalf("PostgreSQL ping error = %v", err)
	}
	wantErr := errors.New("runtime ping failed")
	if err := environmentPing(context.Background(), ControlPlaneDriverPostgres, db, bootstrapPingRuntimeStore{err: wantErr}); !errors.Is(err, wantErr) {
		t.Fatalf("runtime ping error = %v", err)
	}
	if _, _, err := environmentCatalogs(environmentConfig{demoMode: true, modelProvider: "openai", modelNames: []string{"model"}}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("demo catalog provider error = %v", err)
	}
}

func TestEnvironmentComponentSelectionErrorBranches(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, _, _, auditWriter, err := environmentRepositories(environmentConfig{driver: ControlPlaneDriverPostgres, apiIdentities: map[string]gateway.APIIdentity{
		"one": {TenantID: "t_00000000000000000000000000"}, "two": {TenantID: "t_00000000000000000000000001"},
	}}, db)
	if err != nil || auditWriter == nil {
		t.Fatalf("multi-tenant audit writer = %v, %v", auditWriter, err)
	}
	if factory := environmentOutboxWorkerFactory(environmentOutboxWorkerDependencies{}); factory != nil {
		t.Fatal("empty outbox dependencies returned a worker factory")
	}
	store := inmemory.New()
	t.Cleanup(func() { _ = store.Close() })
	missingStoreFactory := environmentOutboxWorkerFactory(environmentOutboxWorkerDependencies{config: environmentConfig{tenantID: "t_00000000000000000000000000"}, aiBotBindings: map[string]struct{}{"binding": {}}})
	if _, err := missingStoreFactory(nil); err == nil {
		t.Fatal("AI Bot worker accepted a runtime store without delivery acknowledgements")
	}
	invalidAdapterFactory := environmentOutboxWorkerFactory(environmentOutboxWorkerDependencies{config: environmentConfig{tenantID: "t_00000000000000000000000000"}, replyStore: store, messageStore: store, deliveryStore: store, aiBotBindings: map[string]struct{}{"binding": {}}})
	if _, err := invalidAdapterFactory([]channels.PollingAdapter{failingPollingAdapter{}}); err == nil {
		t.Fatal("AI Bot worker accepted an adapter with the wrong concrete type")
	}
	countFactory := environmentOutboxWorkerFactory(environmentOutboxWorkerDependencies{config: environmentConfig{tenantID: "t_00000000000000000000000000"}, replyStore: store, messageStore: store, deliveryStore: store, aiBotBindings: map[string]struct{}{"binding": {}}})
	if _, err := countFactory(nil); err == nil {
		t.Fatal("AI Bot worker accepted a mismatched manager count")
	}
	router := environmentReplyProvider{legacy: bootstrapStaticProvider{receipt: "legacy"}, aiBot: bootstrapStaticProvider{receipt: "ai"}, aiBotBindingIDs: map[string]struct{}{"ai": {}}}
	if status, receipt, err := router.Reconcile(context.Background(), runtimestorage.ReplyOutbox{ReplyTarget: runtimestorage.ReplyTarget{BindingID: "legacy"}}); err != nil || status != "accepted" || receipt != "legacy" {
		t.Fatalf("legacy reply reconciliation = %q %q %v", status, receipt, err)
	}
	if _, err := environmentRuntimeProviders(environmentConfig{runtimeStorage: "inmemory"}, environmentRuntimeStores{providers: map[string]environmentStorage{}}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("missing primary runtime provider error = %v", err)
	}
	if _, err := environmentRuntimeProviders(environmentConfig{runtimeStorage: "redis"}, environmentRuntimeStores{providers: map[string]environmentStorage{"redis": store}}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("missing in-memory runtime fallback error = %v", err)
	}
	registry := storagefactory.NewProviderRegistry()
	if err := registerEnvironmentRuntimeProviders(registry, "invalid", nil, environmentConfig{}, []environmentRuntimeProviderSpec{{name: "inmemory", capabilities: []backend.Capability{backend.CapabilitySession}, store: store}}); !errors.Is(err, storagefactory.ErrInvalid) {
		t.Fatalf("invalid runtime provider scope error = %v", err)
	}
}

func TestEnvironmentProviderValidationBranches(t *testing.T) {
	const tenantID = "t_00000000000000000000000000"
	resolver := environmentWeComAIBotCredentialResolver{tenantID: tenantID, secrets: map[string]string{"env/bot": "secret"}}
	if _, err := resolver.Resolve(nil, channels.SecretScope{TenantID: tenantID, SecretRef: "env/bot"}); err == nil {
		t.Fatal("nil AI Bot resolver context unexpectedly succeeded")
	}
	if _, err := resolver.Resolve(canceledContext(), channels.SecretScope{TenantID: tenantID, SecretRef: "env/bot"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled AI Bot resolver error = %v", err)
	}
	if _, err := resolver.Resolve(context.Background(), channels.SecretScope{TenantID: "other", SecretRef: "env/bot"}); err == nil {
		t.Fatal("foreign AI Bot resolver scope unexpectedly succeeded")
	}
	if _, err := resolver.Resolve(context.Background(), channels.SecretScope{TenantID: tenantID, SecretRef: "env/missing"}); err == nil {
		t.Fatal("missing AI Bot secret unexpectedly succeeded")
	}

	for _, test := range []struct {
		name          string
		endpoint      string
		allowInsecure bool
		want          bool
	}{
		{name: "https", endpoint: "https://s3.example.test", want: true},
		{name: "http opt in", endpoint: "http://minio:9000", allowInsecure: true, want: true},
		{name: "http without opt in", endpoint: "http://minio:9000"},
		{name: "userinfo", endpoint: "https://user@s3.example.test"},
		{name: "query", endpoint: "https://s3.example.test?secret=value"},
		{name: "path", endpoint: "https://s3.example.test/bucket"},
		{name: "missing host", endpoint: "https://"},
	} {
		t.Run("endpoint/"+test.name, func(t *testing.T) {
			if got := validEnvironmentS3Endpoint(test.endpoint, test.allowInsecure); got != test.want {
				t.Fatalf("validEnvironmentS3Endpoint(%q, %t) = %t, want %t", test.endpoint, test.allowInsecure, got, test.want)
			}
		})
	}
	for _, bucket := range []string{"BAD", "a..b", "-bucket", "bucket-", "192.168.0.1", "bad_bucket"} {
		if validS3Bucket(bucket) {
			t.Fatalf("invalid S3 bucket %q was accepted", bucket)
		}
	}

	password := mustEnvironmentSecret(t, "redis-password")
	for _, test := range []struct {
		name     string
		provider environmentRuntimeCapabilityProvider
		binding  backend.CapabilityBinding
		secret   modelprofile.SecretValue
		wantErr  bool
	}{
		{name: "unsupported capability", provider: environmentRuntimeCapabilityProvider{backend: "redis", capability: backend.CapabilityAudit}, binding: backend.CapabilityBinding{}, secret: password, wantErr: true},
		{name: "endpoint mismatch", provider: environmentRuntimeCapabilityProvider{backend: "redis", capability: backend.CapabilityMemory, redisEndpoint: "redis://configured"}, binding: backend.CapabilityBinding{Endpoint: "redis://other"}, secret: password, wantErr: true},
		{name: "secret reference mismatch", provider: environmentRuntimeCapabilityProvider{backend: "redis", capability: backend.CapabilityMemory, redisSecretRef: "env/redis"}, binding: backend.CapabilityBinding{SecretRef: "env/other"}, secret: password, wantErr: true},
		{name: "required password missing", provider: environmentRuntimeCapabilityProvider{backend: "redis", capability: backend.CapabilityMemory, redisPasswordRequired: true}, binding: backend.CapabilityBinding{}, wantErr: true},
		{name: "valid", provider: environmentRuntimeCapabilityProvider{backend: "redis", capability: backend.CapabilityMemory, redisEndpoint: "redis://configured", redisSecretRef: "env/redis", redisPasswordRequired: true}, binding: backend.CapabilityBinding{Endpoint: "redis://configured", SecretRef: "env/redis"}, secret: password},
	} {
		t.Run("redis/"+test.name, func(t *testing.T) {
			err := test.provider.validateRedisBinding(test.binding, test.secret)
			if (err != nil) != test.wantErr {
				t.Fatalf("validateRedisBinding() = %v, want error=%t", err, test.wantErr)
			}
		})
	}
}

type bootstrapPingRuntimeStore struct {
	environmentStorage
	err error
}

func (store bootstrapPingRuntimeStore) Ping(context.Context) error { return store.err }
