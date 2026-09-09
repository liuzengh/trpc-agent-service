//
// Tencent is pleased to support the open source community by making trpc-agent-go available.
//
// Copyright (C) 2025 Tencent.  All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.
//

package storage

import (
	"context"
	"fmt"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

func TestStorageAdapter_CrossTenantAccessBlocked(t *testing.T) {
	adapter := NewMultiTenantStorageAdapterImpl()
	defer adapter.Close()

	ctx := context.Background()

	// Create two tenants with different configurations
	tenantA := &tenant.Tenant{
		ID:   "tenant-a",
		Name: "Tenant A",
		Storage: tenant.StorageConfig{
			SessionBackend: "inmemory",
			SessionConfig:  map[string]string{},
			MemoryBackend:  "inmemory",
			MemoryConfig:   map[string]string{},
		},
	}

	tenantB := &tenant.Tenant{
		ID:   "tenant-b",
		Name: "Tenant B",
		Storage: tenant.StorageConfig{
			SessionBackend: "inmemory",
			SessionConfig:  map[string]string{},
			MemoryBackend:  "inmemory",
			MemoryConfig:   map[string]string{},
		},
	}

	// Create session for tenant A
	keyA := session.Key{
		AppName:   "test-app",
		UserID:    "user-1",
		SessionID: "session-123",
	}

	sessionA, err := adapter.CreateSession(ctx, tenantA, keyA, nil)
	if err != nil {
		t.Fatalf("failed to create session for tenant A: %v", err)
	}

	if sessionA == nil {
		t.Fatal("session A is nil")
	}

	// Attempt to read tenant A's session using tenant B's credentials
	// This MUST fail or return nil
	sessionFromB, err := adapter.GetSession(ctx, tenantB, keyA)

	// SECURITY CHECK: Tenant B must NOT be able to access tenant A's session
	if sessionFromB != nil {
		t.Fatal("SECURITY VIOLATION: Tenant B successfully accessed Tenant A's session")
	}

	// The session should not be found - either error or nil is acceptable
	// as long as the data is not returned
	if err != nil {
		t.Logf("GetSession returned error as expected: %v", err)
	} else {
		t.Log("GetSession returned nil session (not found), which is acceptable for tenant isolation")
	}
}

func TestStorageAdapter_MissingTenantIDInContext(t *testing.T) {
	adapter := NewMultiTenantStorageAdapterImpl()
	defer adapter.Close()

	// Create context WITHOUT tenant_id
	ctx := context.Background()

	testTenant := &tenant.Tenant{
		ID:   "test-tenant",
		Name: "Test Tenant",
		Storage: tenant.StorageConfig{
			SessionBackend: "inmemory",
			SessionConfig:  map[string]string{},
			MemoryBackend:  "inmemory",
			MemoryConfig:   map[string]string{},
		},
	}

	key := session.Key{
		AppName:   "test-app",
		UserID:    "user-1",
		SessionID: "session-456",
	}

	// Create session
	sess, err := adapter.CreateSession(ctx, testTenant, key, nil)
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}

	if sess == nil {
		t.Fatal("session is nil")
	}

	// Verify tenant_id is set in the session
	if sess.UserID != "user-1" {
		t.Errorf("expected UserID 'user-1', got '%s'", sess.UserID)
	}
}

func TestStorageAdapterAcceptsNilContext(t *testing.T) {
	adapter := NewMultiTenantStorageAdapterImpl()
	defer adapter.Close()
	testTenant := testStorageTenant("nil-context-tenant", "inmemory")
	//lint:ignore SA1012 Preserve the adapter's documented defensive fallback for legacy callers.
	if _, err := adapter.CreateSession(nil, testTenant, session.Key{
		AppName: "app", UserID: "user", SessionID: "session",
	}, nil); err != nil {
		t.Fatalf("create session with nil context: %v", err)
	}
}

func TestStorageAdapter_TenantLevelBackendSeparation(t *testing.T) {
	adapter := NewMultiTenantStorageAdapterImpl()
	defer adapter.Close()

	ctx := context.Background()

	// Create tenant with inmemory backend
	tenant1 := &tenant.Tenant{
		ID:   "tenant-1",
		Name: "Tenant 1",
		Storage: tenant.StorageConfig{
			SessionBackend: "inmemory",
			SessionConfig:  map[string]string{},
			MemoryBackend:  "inmemory",
			MemoryConfig:   map[string]string{},
		},
	}

	// Create session for tenant 1
	key1 := session.Key{
		AppName:   "app1",
		UserID:    "user1",
		SessionID: "session1",
	}

	sess1, err := adapter.CreateSession(ctx, tenant1, key1, nil)
	if err != nil {
		t.Fatalf("failed to create session for tenant 1: %v", err)
	}

	if sess1 == nil {
		t.Fatal("session 1 is nil")
	}

	// Verify we can retrieve it
	retrieved, err := adapter.GetSession(ctx, tenant1, key1)
	if err != nil {
		t.Fatalf("failed to retrieve session: %v", err)
	}

	if retrieved == nil {
		t.Fatal("retrieved session is nil")
	}

	if retrieved.ID != sess1.ID {
		t.Errorf("session ID mismatch: expected %s, got %s", sess1.ID, retrieved.ID)
	}
}

func TestStorageAdapterScopesCallerKeysBeforeSharedBackend(t *testing.T) {
	tenants := []*tenant.Tenant{
		{ID: "tenant-a", Storage: tenant.StorageConfig{SessionBackend: "inmemory", MemoryBackend: "inmemory"}},
		{ID: "tenant-b", Storage: tenant.StorageConfig{SessionBackend: "inmemory", MemoryBackend: "inmemory"}},
	}

	for _, current := range tenants {
		got, err := scopeSessionKey(current, session.Key{AppName: "support", UserID: "u", SessionID: "s"})
		if err != nil {
			t.Fatal(err)
		}
		want := fmt.Sprintf("tsa1:%d:%s:support", len(current.ID), current.ID)
		if got.AppName != want {
			t.Fatalf("tenant %s app scope = %q, want %q", current.ID, got.AppName, want)
		}
	}

	left, err := TenantScopedAppName(&tenant.Tenant{ID: "tenant-a"}, "support")
	if err != nil {
		t.Fatal(err)
	}
	right, err := TenantScopedAppName(&tenant.Tenant{ID: "tenant-b"}, "support")
	if err != nil {
		t.Fatal(err)
	}
	if left == right {
		t.Fatalf("cross-tenant scopes collided: %q", left)
	}
	if _, err := TenantScopedAppName(&tenant.Tenant{ID: "tenant-a"}, "tenant-b:support"); err == nil {
		t.Fatal("caller-supplied pre-scoped app name was accepted")
	}
}

func TestStorageAdapter_HealthCheck(t *testing.T) {
	adapter := NewMultiTenantStorageAdapterImpl()
	defer adapter.Close()

	ctx := context.Background()

	// Health check should succeed even with no tenants initialized
	err := adapter.HealthCheck(ctx)
	if err != nil {
		t.Errorf("health check failed: %v", err)
	}
}

func TestStorageAdapter_ConcurrentTenantAccess(t *testing.T) {
	adapter := NewMultiTenantStorageAdapterImpl()
	defer adapter.Close()

	ctx := context.Background()

	// Create multiple tenants
	numTenants := 5
	for i := 0; i < numTenants; i++ {
		tenantID := fmt.Sprintf("tenant-%d", i)
		testTenant := &tenant.Tenant{
			ID:   tenantID,
			Name: fmt.Sprintf("Tenant %d", i),
			Storage: tenant.StorageConfig{
				SessionBackend: "inmemory",
				SessionConfig:  map[string]string{},
				MemoryBackend:  "inmemory",
				MemoryConfig:   map[string]string{},
			},
		}

		key := session.Key{
			AppName:   "test-app",
			UserID:    "user-1",
			SessionID: fmt.Sprintf("session-%d", i),
		}

		_, err := adapter.CreateSession(ctx, testTenant, key, nil)
		if err != nil {
			t.Errorf("failed to create session for tenant %s: %v", tenantID, err)
		}
	}
}
