package storage

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"trpc.group/trpc-go/trpc-agent-go/artifact"
)

func TestRouterFailClosedAndDefaultResolution(t *testing.T) {
	if _, err := NewRouter("", nil, nil); err == nil {
		t.Fatal("incomplete router accepted")
	}
	db := &sql.DB{}
	resolverErr := errors.New("synthetic resolver failure")
	router, err := NewRouter("synthetic-dsn", db, func(tenant.SecretRef) (string, error) { return "", resolverErr })
	if err != nil {
		t.Fatal(err)
	}
	if router.MigrationLedgerDB() != db {
		t.Fatal("migration ledger did not use platform database")
	}
	target, err := router.Resolve(context.Background(), tenant.BackendConfig{Type: tenant.BackendPostgres})
	if err != nil || target.DB != db || target.DSN != "synthetic-dsn" {
		t.Fatalf("default target=%+v err=%v", target, err)
	}
	credential := tenant.SecretRef{Provider: tenant.SecretProviderEnv, Key: "SYNTHETIC_DSN"}
	if _, err := router.Resolve(context.Background(), tenant.BackendConfig{Type: tenant.BackendPostgres, Credential: credential}); !errors.Is(err, resolverErr) && err == nil {
		t.Fatalf("resolver error=%v", err)
	}
	if _, err := router.Resolve(nil, tenant.BackendConfig{Type: tenant.BackendPostgres}); err == nil {
		t.Fatal("nil context accepted")
	}
	if _, err := router.ServicesForApp(context.Background(), "", tenant.AgentApp{}); err == nil {
		t.Fatal("empty app scope accepted")
	}
	if _, err := router.KnowledgeForApp(context.Background(), "tenant", tenant.AgentApp{ID: "app"}); err == nil {
		t.Fatal("disabled knowledge accepted")
	}
	if err := router.Close(); err != nil {
		t.Fatal(err)
	}
	if err := router.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := router.Resolve(context.Background(), tenant.BackendConfig{Type: tenant.BackendPostgres, Credential: credential}); err == nil {
		t.Fatal("closed router accepted external route")
	}
	var nilRouter *Router
	nilRouter.SetOperationObserver(nil)
	if nilRouter.MigrationLedgerDB() != nil || nilRouter.Close() != nil {
		t.Fatal("nil router methods must be safe")
	}
}

func TestRouterUsesScopedResolverForTenantRoutes(t *testing.T) {
	router, err := NewRouter("synthetic-dsn", &sql.DB{}, func(tenant.SecretRef) (string, error) {
		return "", errors.New("unscoped resolver must not run")
	})
	if err != nil {
		t.Fatal(err)
	}
	called := false
	router.SetScopedResolver(func(_ context.Context, tenantID, appID string, _ tenant.SecretRef) (string, error) {
		called = true
		if tenantID != "tenant-a" || appID != "app-a" {
			t.Fatalf("scope = %q/%q", tenantID, appID)
		}
		return "", errors.New("scope rejected")
	})
	_, err = router.ResolveForScope(context.Background(), "tenant-a", "app-a", tenant.BackendConfig{
		Type:       tenant.BackendPostgres,
		Credential: tenant.SecretRef{Provider: tenant.SecretProviderVault, Key: "tenant-a/app-a/postgres"},
	})
	if err == nil || !called {
		t.Fatalf("scoped resolver error = %v", err)
	}
}

func TestRoutedProfileValidationCoversMigrationBoundaries(t *testing.T) {
	postgres := tenant.BackendConfig{Type: tenant.BackendPostgres}
	valid := tenant.StorageProfile{Session: postgres, Memory: postgres, Summary: postgres, Artifact: postgres, Knowledge: postgres, Audit: postgres}
	secret := tenant.SecretRef{Provider: tenant.SecretProviderEnv, Key: "SYNTHETIC_TARGET"}
	external := tenant.BackendConfig{Type: tenant.BackendExternal, Endpoint: "https://memory.example", Credential: secret}
	s3 := tenant.BackendConfig{Type: tenant.BackendS3, Endpoint: "https://objects.example", Namespace: "artifacts", Credential: secret}
	qdrant := tenant.BackendConfig{Type: tenant.BackendQdrant, Endpoint: "grpcs://vector.example:6334", Namespace: "knowledge", Credential: secret}
	tests := []struct {
		name   string
		mutate func(*tenant.StorageProfile)
	}{
		{"session migration type", func(profile *tenant.StorageProfile) {
			target := tenant.BackendConfig{Type: tenant.BackendRedis, Credential: secret}
			profile.Session.MigrationTarget = &target
			profile.Summary.MigrationTarget = &target
		}},
		{"session migration credential", func(profile *tenant.StorageProfile) {
			target := postgres
			profile.Session.MigrationTarget = &target
			profile.Summary.MigrationTarget = &target
		}},
		{"audit archive endpoint", func(profile *tenant.StorageProfile) {
			target := external
			target.Endpoint = "http://audit.example"
			profile.Audit.MigrationTarget = &target
		}},
		{"memory primary", func(profile *tenant.StorageProfile) { profile.Memory.Type = tenant.BackendRedis }},
		{"external memory endpoint", func(profile *tenant.StorageProfile) {
			profile.Memory = external
			profile.Memory.Endpoint = "http://memory.example"
		}},
		{"external memory reverse migration", func(profile *tenant.StorageProfile) {
			profile.Memory = external
			target := postgres
			profile.Memory.MigrationTarget = &target
		}},
		{"artifact migration", func(profile *tenant.StorageProfile) {
			target := tenant.BackendConfig{Type: tenant.BackendLocal}
			profile.Artifact.MigrationTarget = &target
		}},
		{"knowledge migration", func(profile *tenant.StorageProfile) {
			target := tenant.BackendConfig{Type: tenant.BackendMilvus}
			profile.Knowledge.MigrationTarget = &target
		}},
		{"audit primary credential", func(profile *tenant.StorageProfile) { profile.Audit.Credential = secret }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			profile := valid.Clone()
			test.mutate(&profile)
			if err := ValidateRoutedProfile(profile); err == nil {
				t.Fatal("invalid routed profile accepted")
			}
		})
	}
	profile := valid.Clone()
	profile.Memory = external
	profile.Artifact = s3
	profile.Knowledge = qdrant
	archive := external
	profile.Audit.MigrationTarget = &archive
	if err := ValidateRoutedProfile(profile); err != nil {
		t.Fatalf("supported multi-backend profile rejected: %v", err)
	}
	redisSession := tenant.BackendConfig{Type: tenant.BackendRedis, Endpoint: "redis://redis:6379/0", Namespace: "runner-session"}
	profile = valid.Clone()
	profile.Session, profile.Summary = redisSession, redisSession
	if err := ValidateRoutedProfile(profile); err != nil {
		t.Fatalf("Redis session profile rejected: %v", err)
	}
	profile.Summary.Namespace = "other-namespace"
	if err := ValidateRoutedProfile(profile); err == nil {
		t.Fatal("different Redis Session/Summary namespaces accepted")
	}
	redisTarget := tenant.BackendConfig{Type: tenant.BackendRedis, Endpoint: "redis://redis:6379/0", Namespace: "migration-target"}
	profile = valid.Clone()
	profile.Session.MigrationTarget = &redisTarget
	profile.Summary.MigrationTarget = &redisTarget
	if err := ValidateRoutedProfile(profile); err != nil {
		t.Fatalf("PostgreSQL to Redis Session migration rejected: %v", err)
	}
	postgresTarget := tenant.BackendConfig{Type: tenant.BackendPostgres}
	profile.Session, profile.Summary = redisSession, redisSession
	profile.Session.MigrationTarget = &postgresTarget
	profile.Summary.MigrationTarget = &postgresTarget
	if err := ValidateRoutedProfile(profile); err != nil {
		t.Fatalf("Redis to PostgreSQL Session migration rejected: %v", err)
	}
}

func TestRedisSessionRouteValidation(t *testing.T) {
	if !validRedisURL("redis://redis:6379/0") || !validRedisURL("rediss://redis.example:6380/0") {
		t.Fatal("valid Redis URL rejected")
	}
	for _, value := range []string{"", "http://redis:6379", "redis:///0", "redis://redis:6379/0#fragment"} {
		if validRedisURL(value) {
			t.Fatalf("invalid Redis URL accepted: %q", value)
		}
	}
	for _, value := range []string{"redis://user:password@redis:6379/0", "redis://redis:6379/0?protocol=3"} {
		if validRedisEndpoint(value) {
			t.Fatalf("credential-bearing endpoint accepted: %q", value)
		}
	}
	if _, err := newRedisSession("", ""); err == nil {
		t.Fatal("incomplete Redis session accepted")
	}
	const secret = "synthetic-secret-must-not-leak"
	if _, err := newRedisSession("redis://user:"+secret+"@redis:6379/not-a-db", "tenant-prefix"); err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("Redis constructor error was missing or leaked its credential: %v", err)
	}
}

func TestArtifactScopeIsTenantAndSessionSafe(t *testing.T) {
	info := artifact.SessionInfo{AppName: "tenant/a/app/b", UserID: "user", SessionID: "session"}
	tenantID, appID, sessionID, err := artifactScope(info, "result.txt")
	if err != nil || tenantID != "a" || appID != "b" || sessionID != "session" {
		t.Fatalf("scope tenant=%q app=%q session=%q err=%v", tenantID, appID, sessionID, err)
	}
	_, _, userSession, err := artifactScope(info, "user:profile.txt")
	if err != nil || userSession != userArtifactSession {
		t.Fatalf("user scope=%q err=%v", userSession, err)
	}
	for _, invalid := range []artifact.SessionInfo{{}, {AppName: "invalid", UserID: "user"}, {AppName: info.AppName, UserID: "user"}} {
		if _, _, _, err := artifactScope(invalid, "result.txt"); err == nil {
			t.Fatalf("invalid info accepted: %+v", invalid)
		}
	}
	if _, _, _, err := artifactScope(info, ""); err == nil {
		t.Fatal("empty filename accepted")
	}
}
