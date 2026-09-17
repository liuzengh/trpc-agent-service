package inmemory

import (
	"context"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/redaction"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func meta() tenant.ChangeMetadata {
	return tenant.ChangeMetadata{ActorType: "admin", ActorID: "operator", ReasonCode: "test", CorrelationID: "correlation", TraceID: "trace"}
}

func TestConfigurationCASPublishesImmutableRedactionRules(t *testing.T) {
	ctx := context.Background()
	repo := New()
	created, err := repo.Create(ctx, tenant.CreateInput{Tenant: tenant.Tenant{TenantID: "tenant-a", TenantKey: "tenant-a", DisplayName: "A"}, ChangeMetadata: meta()})
	if err != nil {
		t.Fatal(err)
	}
	next := created
	next.RedactionRules = []redaction.Rule{{ID: "case", KeyFragments: []string{"case_ref"}, TextPattern: `CASE-[0-9]{6}`}}
	updated, err := repo.UpdateConfiguration(ctx, tenant.UpdateConfigurationInput{Tenant: next, ExpectedVersion: created.Version, ChangeMetadata: meta()})
	if err != nil {
		t.Fatal(err)
	}
	next.RedactionRules[0].KeyFragments[0] = "mutated"
	loaded, err := repo.Get(ctx, created.TenantID)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Tenant.Version != created.Version+1 || loaded.RedactionRules[0].KeyFragments[0] != "case_ref" {
		t.Fatalf("updated=%#v loaded=%#v", updated.Tenant, loaded)
	}
	if _, err := repo.UpdateConfiguration(ctx, tenant.UpdateConfigurationInput{Tenant: next, ExpectedVersion: created.Version, ChangeMetadata: meta()}); !errors.Is(err, tenant.ErrVersionConflict) {
		t.Fatalf("expected stale configuration conflict, got %v", err)
	}
}

func TestStatusCASAndAtomicFacts(t *testing.T) {
	ctx := context.Background()
	repo := New()
	created, err := repo.Create(ctx, tenant.CreateInput{Tenant: tenant.Tenant{TenantID: "tenant-a", TenantKey: "tenant-a", DisplayName: "A"}, ChangeMetadata: meta()})
	if err != nil {
		t.Fatal(err)
	}
	changed, err := repo.TransitionStatus(ctx, tenant.TransitionStatusInput{TenantID: "tenant-a", ExpectedVersion: created.Version, NextStatus: tenant.StatusSuspended, ChangeMetadata: meta()})
	if err != nil {
		t.Fatal(err)
	}
	if changed.Tenant.Version != 2 {
		t.Fatalf("version=%d", changed.Tenant.Version)
	}
	_, err = repo.TransitionStatus(ctx, tenant.TransitionStatusInput{TenantID: "tenant-a", ExpectedVersion: 1, NextStatus: tenant.StatusDisabled, ChangeMetadata: meta()})
	if !errors.Is(err, tenant.ErrVersionConflict) {
		t.Fatalf("got %v", err)
	}
	changes, outbox := repo.Facts("tenant-a")
	if len(changes) != 2 || len(outbox) != 4 {
		t.Fatalf("changes=%d outbox=%d", len(changes), len(outbox))
	}
}
