package config

import (
	"context"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/repository"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestPublishedCacheEvictsOldVersionsWithinBound(t *testing.T) {
	store := repository.NewMemoryStore()
	for expected := tenant.ConfigVersion(0); expected < 3; expected++ {
		if _, err := store.PublishConfig(context.Background(), repository.ConfigRecord{
			TenantID: "tenant-a",
			Payload:  []byte(validYAML),
		}, expected); err != nil {
			t.Fatal(err)
		}
	}
	cache, err := NewPublishedCacheWithLimit(store, 2)
	if err != nil {
		t.Fatal(err)
	}
	for version := tenant.ConfigVersion(1); version <= 3; version++ {
		if _, err := cache.Version(context.Background(), "tenant-a", version); err != nil {
			t.Fatalf("load version %d: %v", version, err)
		}
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if len(cache.versions) != 2 {
		t.Fatalf("cached versions=%d, want 2", len(cache.versions))
	}
	if _, ok := cache.versions[publishedKey{tenantID: "tenant-a", version: 1}]; ok {
		t.Fatal("oldest version was not evicted")
	}
}

func TestPublishedCacheRejectsInvalidLimit(t *testing.T) {
	if _, err := NewPublishedCacheWithLimit(repository.NewMemoryStore(), 0); err == nil {
		t.Fatal("zero cache limit was accepted")
	}
}

func TestPublishedCacheAppliesDeploymentPolicyToLegacyRecords(t *testing.T) {
	store := repository.NewMemoryStore()
	if _, err := store.PublishConfig(context.Background(), repository.ConfigRecord{
		TenantID: "tenant-a",
		Payload:  []byte(validYAML),
	}, 0); err != nil {
		t.Fatal(err)
	}
	cache, err := NewPublishedCache(store)
	if err != nil {
		t.Fatal(err)
	}
	cache.SetValidator((*File).ValidateProduction)
	if _, err := cache.Version(context.Background(), "tenant-a", 1); err == nil {
		t.Fatal("legacy env-based published secret bypassed production policy")
	}
}
