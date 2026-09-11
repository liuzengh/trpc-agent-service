package storage

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
)

func TestBackendDriverDomains(t *testing.T) {
	t.Parallel()
	tests := map[string][]string{
		"inmemory":      {BackendDomainSession, BackendDomainMemory, BackendDomainArtifact},
		"postgres":      {BackendDomainSession, BackendDomainMemory, BackendDomainArtifact},
		" POSTGRES ":    {BackendDomainSession, BackendDomainMemory, BackendDomainArtifact},
		"redis":         {BackendDomainSession, BackendDomainMemory},
		"mysql":         {BackendDomainSession, BackendDomainMemory},
		"sqlite":        {BackendDomainSession},
		"mongodb":       {BackendDomainSession},
		"clickhouse":    {BackendDomainSession},
		"pgvector":      {BackendDomainKnowledge},
		"qdrant":        {BackendDomainKnowledge},
		"elasticsearch": {BackendDomainKnowledge},
		"s3":            {BackendDomainArtifact},
		"cos":           {BackendDomainArtifact},
		"mem0":          {BackendDomainMemory},
		"chromadb":      {BackendDomainMemory},
		"tencentdb":     {BackendDomainMemory},
		"unknown":       nil,
	}
	for driver, want := range tests {
		driver, want := driver, want
		t.Run(driver, func(t *testing.T) {
			t.Parallel()
			if got := BackendDriverDomains(driver); !reflect.DeepEqual(got, want) {
				t.Fatalf("BackendDriverDomains(%q) = %#v, want %#v", driver, got, want)
			}
		})
	}
}

func TestBackendDriverCatalogReturnsIndependentCapabilityMatrix(t *testing.T) {
	t.Parallel()
	catalog := BackendDriverCatalog()
	if len(catalog) != len(backendDriverSpecs) || len(catalog) == 0 {
		t.Fatalf("BackendDriverCatalog() length = %d, want %d", len(catalog), len(backendDriverSpecs))
	}
	seen := map[string]BackendDriverSpec{}
	for _, spec := range catalog {
		seen[spec.Driver] = spec
	}
	for _, driver := range []string{"postgres", "redis", "mysql", "sqlite", "mongodb", "clickhouse", "elasticsearch", "s3", "mem0", "chromadb", "tencentdb"} {
		if _, ok := seen[driver]; !ok {
			t.Fatalf("BackendDriverCatalog() missing %q", driver)
		}
	}
	if seen["inmemory"].Capabilities.MultiNode || seen["sqlite"].Capabilities.MultiNode {
		t.Fatal("local-only backends advertised multi-node support")
	}
	if !seen["postgres"].Capabilities.MultiNode || !seen["redis"].Capabilities.MultiNode {
		t.Fatal("shared backends did not advertise multi-node support")
	}
	if !seen["mem0"].Capabilities.MemoryConsoleBrowsing {
		t.Fatal("Mem0 should advertise direct Memory console browsing")
	}
	if seen["tencentdb"].Capabilities.MemoryConsoleBrowsing {
		t.Fatal("TencentDB should not advertise direct Memory console browsing")
	}
	// Callers may edit the returned catalog for UI purposes without mutating
	// the process-wide capability matrix used for validation.
	catalog[0].Domains[0] = "mutated"
	again := BackendDriverCatalog()
	if again[0].Domains[0] == "mutated" {
		t.Fatal("BackendDriverCatalog() returned aliased domain storage")
	}
}

func TestBackendDriverCapabilitiesForUnknownDriverFailsClosed(t *testing.T) {
	t.Parallel()
	if got := BackendDriverCapabilitiesFor("missing"); got != (BackendDriverCapabilities{}) {
		t.Fatalf("BackendDriverCapabilitiesFor(missing) = %#v", got)
	}
}

func TestNormalizeBackendProfileUsesSelectedDataDomains(t *testing.T) {
	t.Parallel()
	valid := []struct {
		name string
		in   BackendProfile
		want BackendProfile
	}{
		{
			name: "postgres subset",
			in: BackendProfile{ProfileID: " support-db ", DisplayName: " 客服数据库 ", Driver: " POSTGRES ",
				Domains: []string{BackendDomainArtifact, BackendDomainSession}},
			want: BackendProfile{ProfileID: "support-db", DisplayName: "客服数据库", Driver: "postgres", Status: BackendProfileActive,
				Domains: []string{BackendDomainSession, BackendDomainArtifact}, Capabilities: BackendDriverCapabilities{MultiNode: true, MemoryConsoleBrowsing: true}},
		},
		{
			name: "external redis",
			in: BackendProfile{ProfileID: "support-cache", DisplayName: "客服缓存", Driver: "redis", ConnectionRef: " env:SUPPORT_REDIS ", Status: "DISABLED",
				Domains: []string{BackendDomainSession}},
			want: BackendProfile{ProfileID: "support-cache", DisplayName: "客服缓存", Driver: "redis", ConnectionRef: "env:SUPPORT_REDIS", Status: BackendProfileDisabled,
				Domains: []string{BackendDomainSession}, Capabilities: BackendDriverCapabilities{MultiNode: true, MemoryConsoleBrowsing: true}},
		},
		{
			name: "in-memory session only",
			in:   BackendProfile{ProfileID: "ephemeral-session", DisplayName: "临时会话", Driver: "inmemory", Domains: []string{BackendDomainSession}},
			want: BackendProfile{ProfileID: "ephemeral-session", DisplayName: "临时会话", Driver: "inmemory", Status: BackendProfileActive,
				Domains: []string{BackendDomainSession}, Capabilities: BackendDriverCapabilities{MemoryConsoleBrowsing: true}},
		},
	}
	for _, tt := range valid {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := NormalizeBackendProfile(tt.in)
			if err != nil {
				t.Fatalf("NormalizeBackendProfile() error = %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("NormalizeBackendProfile() = %#v, want %#v", got, tt.want)
			}
		})
	}

	invalid := []BackendProfile{
		{},
		{ProfileID: "support db", DisplayName: "客服数据库", Driver: "postgres", Domains: []string{BackendDomainSession}},
		{ProfileID: "support-db", DisplayName: "客服数据库", Driver: "unknown", Domains: []string{BackendDomainSession}},
		{ProfileID: "support-db", DisplayName: "客服数据库", Driver: "postgres"},
		{ProfileID: "support-db", DisplayName: "客服数据库", Driver: "postgres", Domains: []string{BackendDomainKnowledge}},
		{ProfileID: "support-db", DisplayName: "客服数据库", Driver: "postgres", Domains: []string{BackendDomainSession}, Status: "broken"},
		{ProfileID: "support-db", DisplayName: "客服数据库", Driver: "postgres", Domains: []string{BackendDomainSession}, ConnectionRef: "plain-secret"},
		{ProfileID: "support-cache", DisplayName: "客服缓存", Driver: "redis", Domains: []string{BackendDomainSession}},
		{ProfileID: "support-search", DisplayName: "客服搜索", Driver: "qdrant", Domains: []string{BackendDomainKnowledge}},
		{ProfileID: "support-files", DisplayName: "客服文件", Driver: "s3", Domains: []string{BackendDomainArtifact}},
		{ProfileID: "support-memory", DisplayName: "客服偏好", Driver: "mem0", Domains: []string{BackendDomainMemory}},
		{ProfileID: "bad-memory", DisplayName: "错误内存", Driver: "inmemory", Domains: []string{BackendDomainMemory}, ConnectionRef: "env:SHOULD_NOT_EXIST"},
	}
	for index, input := range invalid {
		if _, err := NormalizeBackendProfile(input); err == nil {
			t.Fatalf("NormalizeBackendProfile(invalid %d) error = nil", index)
		}
	}
}

func TestUniqueTrimmed(t *testing.T) {
	t.Parallel()
	got := uniqueTrimmed([]string{" support-b ", "", "support-a", "support-b", " support-a"})
	want := []string{"support-a", "support-b"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("uniqueTrimmed() = %#v, want %#v", got, want)
	}
}

func TestMemoryBackendProfileStoreLifecycle(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := NewMemoryBackendProfileStore()

	profiles, err := store.ListBackendProfiles(ctx)
	if err != nil || len(profiles) != 2 || profiles[0].ProfileID != "platform-pgvector" || profiles[1].ProfileID != "platform-postgres" {
		t.Fatalf("ListBackendProfiles() = %#v, %v", profiles, err)
	}
	profiles[0].Domains[0] = "mutated"
	again, _ := store.GetBackendProfile(ctx, "platform-pgvector")
	if again.Domains[0] != BackendDomainKnowledge {
		t.Fatal("returned profile domains alias store state")
	}
	if _, err := store.GetBackendProfile(ctx, "missing"); !errors.Is(err, ErrBackendProfileNotFound) {
		t.Fatalf("GetBackendProfile(missing) error = %v", err)
	}

	created, err := store.CreateBackendProfile(ctx, BackendProfile{
		ProfileID: "support-cache", DisplayName: "客服缓存", Driver: "redis", ConnectionRef: "env:SUPPORT_REDIS", Domains: []string{BackendDomainSession},
	})
	if err != nil || created.CreatedAt.IsZero() || created.UpdatedAt.IsZero() {
		t.Fatalf("CreateBackendProfile() = %#v, %v", created, err)
	}
	updated, err := store.UpdateBackendProfile(ctx, "support-cache", BackendProfileUpdate{DisplayName: "客服会话缓存", Status: BackendProfileActive})
	if err != nil || updated.DisplayName != "客服会话缓存" || !updated.CreatedAt.Equal(created.CreatedAt) {
		t.Fatalf("UpdateBackendProfile() = %#v, %v", updated, err)
	}
	if _, err := store.CreateBackendProfile(ctx, BackendProfile{
		ProfileID: "support-cache", DisplayName: "重复后端", Driver: "redis", ConnectionRef: "env:OTHER_REDIS", Domains: []string{BackendDomainSession},
	}); !errors.Is(err, ErrBackendProfileAlreadyExists) {
		t.Fatalf("duplicate CreateBackendProfile() error = %v, want ErrBackendProfileAlreadyExists", err)
	}
	if _, err := store.UpdateBackendProfile(ctx, "support-cache", BackendProfileUpdate{DisplayName: "", Status: BackendProfileActive}); err == nil {
		t.Fatal("empty display name should be rejected")
	}
	if _, err := store.CreateBackendProfile(ctx, BackendProfile{ProfileID: "bad", DisplayName: "坏配置", Driver: "redis", Domains: []string{BackendDomainSession}}); err == nil {
		t.Fatal("invalid profile should be rejected")
	}

	if err := store.ReplaceTenantBackendProfiles(ctx, "support", []string{" support-cache ", "support-cache", "platform-postgres"}); err != nil {
		t.Fatalf("ReplaceTenantBackendProfiles() error = %v", err)
	}
	allowed, err := store.ListTenantBackendProfiles(ctx, "support")
	if err != nil || len(allowed) != 2 || allowed[0].ProfileID != "platform-postgres" || allowed[1].ProfileID != "support-cache" {
		t.Fatalf("ListTenantBackendProfiles() = %#v, %v", allowed, err)
	}
	if !allowed[0].Capabilities.MultiNode || !allowed[0].Capabilities.MemoryConsoleBrowsing || !allowed[1].Capabilities.MultiNode {
		t.Fatalf("tenant backend capabilities = %#v", allowed)
	}
	resolved, err := store.ResolveTenantBackend(ctx, "support", BackendDomainSession, "support-cache")
	if err != nil || !reflect.DeepEqual(resolved, config.BackendConfig{Driver: "redis", ConnectionRef: "env:SUPPORT_REDIS"}) {
		t.Fatalf("ResolveTenantBackend() = %#v, %v", resolved, err)
	}
	if _, err := store.ResolveTenantBackend(ctx, "other", BackendDomainSession, "support-cache"); !errors.Is(err, ErrBackendProfileUnauthorized) {
		t.Fatalf("ResolveTenantBackend(unauthorized) error = %v", err)
	}
	if _, err := store.ResolveTenantBackend(ctx, "support", BackendDomainArtifact, "support-cache"); err == nil {
		t.Fatal("ResolveTenantBackend(unsupported domain) error = nil")
	}

	if err := store.DeleteBackendProfile(ctx, "platform-postgres"); err == nil {
		t.Fatal("DeleteBackendProfile(built-in) error = nil")
	}
	if err := store.DeleteBackendProfile(ctx, "missing"); !errors.Is(err, ErrBackendProfileNotFound) {
		t.Fatalf("DeleteBackendProfile(missing) error = %v", err)
	}
	if err := store.DeleteBackendProfile(ctx, "support-cache"); err == nil {
		t.Fatal("DeleteBackendProfile(authorized) error = nil")
	}
	if err := store.ReplaceTenantBackendProfiles(ctx, "support", nil); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteBackendProfile(ctx, "support-cache"); err != nil {
		t.Fatalf("DeleteBackendProfile() error = %v", err)
	}
}

func TestMemoryBackendProfileStoreRejectsInvalidTenantPolicy(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := NewMemoryBackendProfileStore()
	if err := store.ReplaceTenantBackendProfiles(ctx, "", nil); err == nil {
		t.Fatal("empty tenant should be rejected")
	}
	if err := store.ReplaceTenantBackendProfiles(ctx, "support", []string{"missing"}); err == nil {
		t.Fatal("missing profile should be rejected")
	}
	if _, err := store.CreateBackendProfile(ctx, BackendProfile{ProfileID: "disabled-cache", DisplayName: "停用缓存", Driver: "redis", ConnectionRef: "env:CACHE", Domains: []string{BackendDomainSession}, Status: BackendProfileDisabled}); err != nil {
		t.Fatal(err)
	}
	if err := store.ReplaceTenantBackendProfiles(ctx, "support", []string{"disabled-cache"}); err == nil {
		t.Fatal("disabled profile should be rejected")
	}

	store.tenantAllow["support"] = map[string]struct{}{"missing": {}}
	listed, err := store.ListTenantBackendProfiles(ctx, "support")
	if err != nil || len(listed) != 0 {
		t.Fatalf("ListTenantBackendProfiles(stale) = %#v, %v", listed, err)
	}
	store.tenantAllow["support"] = map[string]struct{}{"disabled-cache": {}}
	if _, err := store.ResolveTenantBackend(ctx, "support", BackendDomainSession, "disabled-cache"); !errors.Is(err, ErrBackendProfileUnavailable) {
		t.Fatalf("ResolveTenantBackend(disabled) error = %v", err)
	}
	store.tenantAllow["support"] = map[string]struct{}{"missing": {}}
	if _, err := store.ResolveTenantBackend(ctx, "support", BackendDomainSession, "missing"); !errors.Is(err, ErrBackendProfileNotFound) {
		t.Fatalf("ResolveTenantBackend(stale) error = %v", err)
	}
}

func TestNewPostgresBackendProfileStoreRequiresDatabase(t *testing.T) {
	t.Parallel()
	if _, err := NewPostgresBackendProfileStore(nil); err == nil {
		t.Fatal("NewPostgresBackendProfileStore(nil) error = nil")
	}
}

func TestPostgresBackendProfileStoreValidatesBeforeDatabaseAccess(t *testing.T) {
	t.Parallel()
	database, err := sql.Open("pgx", "postgres://unused:unused@127.0.0.1:1/unused")
	if err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	store, err := NewPostgresBackendProfileStore(database)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := store.CreateBackendProfile(ctx, BackendProfile{ProfileID: "bad", DisplayName: "坏配置", Driver: "redis", Domains: []string{BackendDomainSession}}); err == nil {
		t.Fatal("CreateBackendProfile() reached database for invalid profile")
	}
	if _, err := store.UpdateBackendProfile(ctx, "", BackendProfileUpdate{DisplayName: "名称", Status: BackendProfileActive}); err == nil {
		t.Fatal("UpdateBackendProfile() accepted empty profile ID")
	}
	if _, err := store.UpdateBackendProfile(ctx, "profile-a", BackendProfileUpdate{DisplayName: "", Status: BackendProfileActive}); err == nil {
		t.Fatal("UpdateBackendProfile() reached database for invalid update")
	}
	for _, builtin := range []string{"platform-postgres", "platform-pgvector"} {
		if err := store.DeleteBackendProfile(ctx, builtin); err == nil {
			t.Fatalf("DeleteBackendProfile(%q) error = nil", builtin)
		}
	}
	if err := store.ReplaceTenantBackendProfiles(ctx, "", []string{"profile-a"}); err == nil {
		t.Fatal("ReplaceTenantBackendProfiles() accepted empty tenant")
	}
	if _, err := store.ResolveTenantBackend(ctx, "tenant-a", BackendDomainSession, ""); err == nil {
		t.Fatal("ResolveTenantBackend() accepted empty profile ID")
	}
}

func TestBackendProfileDomainParsing(t *testing.T) {
	t.Parallel()
	if got := splitBackendDomains(" session,memory,artifact "); !reflect.DeepEqual(got, []string{" session", "memory", "artifact "}) {
		t.Fatalf("splitBackendDomains() = %#v", got)
	}
	if got := splitBackendDomains("  "); got != nil {
		t.Fatalf("splitBackendDomains(empty) = %#v", got)
	}
}
