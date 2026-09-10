package storage

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
)

func TestBackendProfileDomains(t *testing.T) {
	t.Parallel()
	tests := map[string][]string{
		"postgres":   {BackendDomainSession, BackendDomainMemory, BackendDomainArtifact},
		" POSTGRES ": {BackendDomainSession, BackendDomainMemory, BackendDomainArtifact},
		"redis":      {BackendDomainSession},
		"pgvector":   {BackendDomainKnowledge},
		"qdrant":     {BackendDomainKnowledge},
		"s3":         {BackendDomainArtifact},
		"cos":        {BackendDomainArtifact},
		"mem0":       {BackendDomainMemory},
		"unknown":    nil,
	}
	for driver, want := range tests {
		driver, want := driver, want
		t.Run(driver, func(t *testing.T) {
			t.Parallel()
			if got := BackendProfileDomains(driver); !reflect.DeepEqual(got, want) {
				t.Fatalf("BackendProfileDomains(%q) = %#v, want %#v", driver, got, want)
			}
		})
	}
}

func TestBackendProfileSupportsDomain(t *testing.T) {
	t.Parallel()
	if !BackendProfileSupportsDomain("postgres", BackendDomainMemory) {
		t.Fatal("postgres should support memory")
	}
	if BackendProfileSupportsDomain("redis", BackendDomainArtifact) {
		t.Fatal("redis should not support artifact")
	}
}

func TestNormalizeBackendProfile(t *testing.T) {
	t.Parallel()
	valid := []struct {
		name string
		in   BackendProfile
		want BackendProfile
	}{
		{
			name: "platform postgres",
			in:   BackendProfile{ProfileID: " support-db ", DisplayName: " 客服数据库 ", Driver: " POSTGRES "},
			want: BackendProfile{ProfileID: "support-db", DisplayName: "客服数据库", Driver: "postgres", Status: BackendProfileActive,
				Domains: []string{BackendDomainSession, BackendDomainMemory, BackendDomainArtifact}},
		},
		{
			name: "external redis",
			in:   BackendProfile{ProfileID: "support-cache", DisplayName: "客服缓存", Driver: "redis", ConnectionRef: " env:SUPPORT_REDIS ", Status: "DISABLED"},
			want: BackendProfile{ProfileID: "support-cache", DisplayName: "客服缓存", Driver: "redis", ConnectionRef: "env:SUPPORT_REDIS", Status: BackendProfileDisabled,
				Domains: []string{BackendDomainSession}},
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
		{ProfileID: "support db", DisplayName: "客服数据库", Driver: "postgres"},
		{ProfileID: "support-db", DisplayName: "客服数据库", Driver: "unknown"},
		{ProfileID: "support-db", DisplayName: "客服数据库", Driver: "postgres", Status: "broken"},
		{ProfileID: "support-db", DisplayName: "客服数据库", Driver: "postgres", ConnectionRef: "plain-secret"},
		{ProfileID: "support-cache", DisplayName: "客服缓存", Driver: "redis"},
		{ProfileID: "support-search", DisplayName: "客服搜索", Driver: "qdrant"},
		{ProfileID: "support-files", DisplayName: "客服文件", Driver: "s3"},
		{ProfileID: "support-files", DisplayName: "客服文件", Driver: "cos"},
		{ProfileID: "support-memory", DisplayName: "客服偏好", Driver: "mem0"},
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

	created, err := store.UpsertBackendProfile(ctx, BackendProfile{
		ProfileID: "support-cache", DisplayName: "客服缓存", Driver: "redis", ConnectionRef: "env:SUPPORT_REDIS",
	})
	if err != nil || created.CreatedAt.IsZero() || created.UpdatedAt.IsZero() {
		t.Fatalf("UpsertBackendProfile(create) = %#v, %v", created, err)
	}
	updated, err := store.UpsertBackendProfile(ctx, BackendProfile{
		ProfileID: "support-cache", DisplayName: "客服会话缓存", Driver: "redis", ConnectionRef: "env:SUPPORT_REDIS",
	})
	if err != nil || updated.DisplayName != "客服会话缓存" || !updated.CreatedAt.Equal(created.CreatedAt) {
		t.Fatalf("UpsertBackendProfile(update) = %#v, %v", updated, err)
	}
	if _, err := store.UpsertBackendProfile(ctx, BackendProfile{
		ProfileID: "support-cache", DisplayName: "错误路由", Driver: "redis", ConnectionRef: "env:OTHER_REDIS",
	}); err == nil {
		t.Fatal("routing mutation should be rejected")
	}
	if _, err := store.UpsertBackendProfile(ctx, BackendProfile{ProfileID: "bad", DisplayName: "坏配置", Driver: "redis"}); err == nil {
		t.Fatal("invalid profile should be rejected")
	}

	if err := store.ReplaceTenantBackendProfiles(ctx, "support", []string{" support-cache ", "support-cache", "platform-postgres"}); err != nil {
		t.Fatalf("ReplaceTenantBackendProfiles() error = %v", err)
	}
	allowed, err := store.ListTenantBackendProfiles(ctx, "support")
	if err != nil || len(allowed) != 2 || allowed[0].ProfileID != "platform-postgres" || allowed[1].ProfileID != "support-cache" {
		t.Fatalf("ListTenantBackendProfiles() = %#v, %v", allowed, err)
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
	if _, err := store.UpsertBackendProfile(ctx, BackendProfile{ProfileID: "disabled-cache", DisplayName: "停用缓存", Driver: "redis", ConnectionRef: "env:CACHE", Status: BackendProfileDisabled}); err != nil {
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
