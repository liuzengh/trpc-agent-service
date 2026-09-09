// Tencent is pleased to support the open source community by making
// trpc-agent-go available.
//
// Copyright (C) 2025 Tencent. All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.

package storage

import (
	"context"
	"errors"
	"strings"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
)

type typedNilBackendProfileResolver struct{ BackendProfileResolver }

func TestBackendFactoryRejectsTypedNilProfileResolver(t *testing.T) {
	var typedNil *typedNilBackendProfileResolver
	factory := NewBackendFactoryWithProfiles(typedNil)
	_, err := factory.CreateBackendForTenant("tenant-a", &tenant.StorageConfig{
		SessionBackend: "redis", SessionProfile: "redis-profile",
		MemoryBackend: "inmemory",
	})
	if !errors.Is(err, ErrBackendProfileNotFound) {
		t.Fatalf("typed-nil resolver error=%v, want ErrBackendProfileNotFound", err)
	}
}

func TestBackendProfileHealthCheckHonorsCancellation(t *testing.T) {
	catalog := &BackendProfileCatalog{profiles: map[string]BackendProfile{
		"redis-profile": {Backend: "redis", ConnectionString: "rediss://example.invalid"},
	}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := catalog.HealthCheckBackendProfile(ctx, "tenant-a", "redis-profile"); !errors.Is(err, context.Canceled) {
		t.Fatalf("health check error=%v, want context.Canceled", err)
	}
}

func TestBackendProfileHealthCheckDoesNotExposeConnectionMaterial(t *testing.T) {
	catalog := &BackendProfileCatalog{profiles: map[string]BackendProfile{
		"redis-profile": {Backend: "redis", ConnectionString: "redis://user:super-secret@%invalid"},
	}}
	err := catalog.HealthCheckBackendProfile(context.Background(), "tenant-a", "redis-profile")
	if !errors.Is(err, ErrBackendProfileHealthCheckFailed) {
		t.Fatalf("health check error=%v, want ErrBackendProfileHealthCheckFailed", err)
	}
	if strings.Contains(err.Error(), "super-secret") || strings.Contains(err.Error(), "%invalid") {
		t.Fatalf("health check leaked connection material: %v", err)
	}
}

func TestLoadBackendProfilesResolvesOperatorOwnedSecrets(t *testing.T) {
	manifest := `[
		{"id":"shared-postgres","backend":"postgres","connectionEnv":"TENANT_POSTGRES_DSN"},
		{"id":"shared-redis","backend":"redis","connectionEnv":"TENANT_REDIS_URL"}
	]`
	values := map[string]string{
		"TENANT_POSTGRES_DSN": "postgresql://runtime:password@db.internal/agent?sslmode=verify-full",
		"TENANT_REDIS_URL":    "rediss://:password@cache.internal:6380/0",
	}
	profiles, err := LoadBackendProfiles(manifest, func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	})
	if err != nil {
		t.Fatal(err)
	}
	profile, err := profiles.ResolveBackendProfile("shared-postgres")
	if err != nil {
		t.Fatal(err)
	}
	if profile.Backend != "postgres" || profile.ConnectionString != values["TENANT_POSTGRES_DSN"] {
		t.Fatalf("resolved profile mismatch: backend=%q", profile.Backend)
	}
}

func TestBackendProfileValidatorUsesOnlyPublicMetadata(t *testing.T) {
	validator, err := LoadBackendProfileValidator(`[
		{"id":"tenant-a-postgres","backend":"postgres","connectionEnv":"TENANT_POSTGRES_DSN","tenantIds":["tenant-a"]},
		{"id":"shared-redis","backend":"redis","connectionEnv":"TENANT_REDIS_URL"}
	]`)
	if err != nil {
		t.Fatal(err)
	}
	if err := validator.ValidateTenantStorage("tenant-a", tenant.StorageConfig{
		SessionBackend: "postgres", SessionProfile: "tenant-a-postgres",
		MemoryBackend: "redis", MemoryProfile: "shared-redis",
	}); err != nil {
		t.Fatalf("public profile validation failed: %v", err)
	}
	if err := validator.ValidateTenantStorage("tenant-b", tenant.StorageConfig{
		SessionBackend: "postgres", SessionProfile: "tenant-a-postgres",
		MemoryBackend: "redis", MemoryProfile: "shared-redis",
	}); !errors.Is(err, ErrBackendProfileNotFound) {
		t.Fatalf("tenant allowlist error=%v, want ErrBackendProfileNotFound", err)
	}
	if err := validator.ValidateTenantStorage("tenant-a", tenant.StorageConfig{
		SessionBackend: "redis", SessionProfile: "tenant-a-postgres",
		MemoryBackend: "redis", MemoryProfile: "shared-redis",
	}); !errors.Is(err, ErrBackendProfileTypeMismatch) {
		t.Fatalf("backend type error=%v, want ErrBackendProfileTypeMismatch", err)
	}
}

func TestLoadBackendProfilesRejectsInvalidEndpointWithoutCredentialLeak(t *testing.T) {
	const credential = "profile-password-must-not-leak"
	manifest := `[{"id":"shared-redis","backend":"redis","connectionEnv":"TENANT_REDIS_URL"}]`
	profiles, err := LoadBackendProfiles(manifest, func(string) (string, bool) {
		return "redis://:" + credential + "@bad host:6379/0", true
	})
	if profiles != nil || !errors.Is(err, ErrBackendEndpointInvalid) {
		t.Fatalf("invalid endpoint error=%v", err)
	}
	if strings.Contains(err.Error(), credential) || strings.Contains(err.Error(), "bad host") {
		t.Fatalf("credential-bearing endpoint leaked through error: %v", err)
	}
}

func TestValidateBackendEndpointRequiresVerifiedPostgresTLS(t *testing.T) {
	for _, test := range []struct {
		name          string
		connection    string
		allowInsecure bool
		wantErr       bool
	}{
		{
			name:       "verified server identity",
			connection: "postgresql://runtime:password@db.internal/agent?sslmode=verify-full",
		},
		{
			name:       "encryption without identity verification",
			connection: "postgresql://runtime:password@db.internal/agent?sslmode=require",
			wantErr:    true,
		},
		{
			name:       "CA verification without hostname verification",
			connection: "postgresql://runtime:password@db.internal/agent?sslmode=verify-ca",
			wantErr:    true,
		},
		{
			name:          "explicit local development exception",
			connection:    "postgresql://runtime:password@db.internal/agent?sslmode=disable",
			allowInsecure: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := validateBackendEndpoint(BackendProfile{
				Backend:          "postgres",
				ConnectionString: test.connection,
				AllowInsecure:    test.allowInsecure,
			})
			if (err != nil) != test.wantErr {
				t.Fatalf("validate backend endpoint error=%v, wantErr=%v", err, test.wantErr)
			}
		})
	}
}

func TestValidateServicePostgresURLDefaultsToVerifiedTLS(t *testing.T) {
	if err := ValidateServicePostgresURL(
		"postgresql://control:password@db.internal/platform?sslmode=verify-full", false,
	); err != nil {
		t.Fatalf("verified service URL: %v", err)
	}
	if err := ValidateServicePostgresURL(
		"postgresql://control:password@db.internal/platform?sslmode=disable", false,
	); !errors.Is(err, ErrBackendEndpointInvalid) {
		t.Fatalf("insecure service URL error=%v, want ErrBackendEndpointInvalid", err)
	}
	if err := ValidateServicePostgresURL(
		"postgresql://control:password@db.internal/platform?sslmode=disable", true,
	); err != nil {
		t.Fatalf("explicit local development exception: %v", err)
	}
}

func TestLoadBackendProfilesRejectsAmbiguousManifest(t *testing.T) {
	tests := []struct {
		name     string
		manifest string
	}{
		{"duplicate profile", `[
			{"id":"shared","backend":"redis","connectionEnv":"REDIS_ONE","allowInsecure":true},
			{"id":"shared","backend":"redis","connectionEnv":"REDIS_TWO","allowInsecure":true}
		]`},
		{"unknown field", `[{"id":"shared","backend":"redis","connectionEnv":"REDIS_ONE","allowInsecure":true,"url":"secret"}]`},
		{"trailing value", `[{"id":"shared","backend":"redis","connectionEnv":"REDIS_ONE","allowInsecure":true}] {}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			profiles, err := LoadBackendProfiles(test.manifest, func(string) (string, bool) {
				return "redis://cache.internal:6379/0", true
			})
			if profiles != nil || !errors.Is(err, ErrBackendProfileManifestInvalid) {
				t.Fatalf("ambiguous manifest accepted: profiles=%v err=%v", profiles, err)
			}
		})
	}
}

func TestBackendProfileTenantAllowlistIsEnforcedByScopedResolver(t *testing.T) {
	profiles, err := LoadBackendProfiles(
		`[{"id":"tenant-a-postgres","backend":"postgres","connectionEnv":"POSTGRES_DSN","tenantIds":["tenant-a"]}]`,
		func(string) (string, bool) {
			return "postgresql://runtime:password@db.internal/agent?sslmode=verify-full", true
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := profiles.ResolveBackendProfileForTenant("tenant-b", "tenant-a-postgres"); !errors.Is(err, ErrBackendProfileNotFound) {
		t.Fatalf("unlisted tenant resolved profile: %v", err)
	}
	if profile, err := profiles.ResolveBackendProfileForTenant("tenant-a", "tenant-a-postgres"); err != nil || profile.Backend != "postgres" {
		t.Fatalf("listed tenant could not resolve profile: profile=%+v err=%v", profile, err)
	}
}

func TestBackendFactoryRequiresMatchingOperatorProfile(t *testing.T) {
	profiles, err := LoadBackendProfiles(
		`[{"id":"shared-postgres","backend":"postgres","connectionEnv":"POSTGRES_DSN"}]`,
		func(string) (string, bool) {
			return "postgresql://runtime:password@db.internal/agent?sslmode=verify-full", true
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	factory := NewBackendFactoryWithProfiles(profiles)
	_, err = factory.CreateBackend(&tenant.StorageConfig{
		SessionBackend: "redis",
		SessionProfile: "shared-postgres",
		MemoryBackend:  "inmemory",
	})
	if !errors.Is(err, ErrBackendProfileTypeMismatch) {
		t.Fatalf("profile/backend mismatch error=%v", err)
	}

	_, err = NewBackendFactory().CreateBackend(&tenant.StorageConfig{
		SessionBackend: "redis",
		SessionProfile: "missing",
		MemoryBackend:  "inmemory",
	})
	if !errors.Is(err, ErrBackendProfileNotFound) {
		t.Fatalf("missing profile error=%v", err)
	}
}

func TestBackendFactoryRejectsTenantConnectionMaterialWithoutLeak(t *testing.T) {
	const credential = "tenant-password-must-not-leak"
	_, err := NewBackendFactory().CreateBackend(&tenant.StorageConfig{
		SessionBackend: "postgres",
		SessionProfile: "shared-postgres",
		SessionConfig: map[string]string{
			"dsn": "postgresql://tenant:" + credential + "@internal/agent",
		},
		MemoryBackend: "inmemory",
	})
	if !errors.Is(err, ErrBackendOptionInvalid) {
		t.Fatalf("tenant connection material error=%v", err)
	}
	if strings.Contains(err.Error(), credential) {
		t.Fatalf("tenant connection material leaked through error: %v", err)
	}
}
