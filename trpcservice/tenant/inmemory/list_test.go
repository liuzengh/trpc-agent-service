package inmemory

import (
	"context"
	"testing"

	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
)

func TestListScopesFiltersAndPaginatesTenants(t *testing.T) {
	repository := NewRepository()
	first, err := repository.Create(context.Background(), tenant.CreateInput{TenantKey: "first-list", DisplayName: "First List"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := repository.Create(context.Background(), tenant.CreateInput{TenantKey: "second-list", DisplayName: "Second List"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repository.Create(context.Background(), tenant.CreateInput{TenantKey: "hidden-list", DisplayName: "Hidden List"}); err != nil {
		t.Fatal(err)
	}

	page, next, err := repository.List(context.Background(), []string{"", first.TenantID, second.TenantID, first.TenantID}, "", "active", "", 1)
	if err != nil || len(page) != 1 || next == "" {
		t.Fatalf("first tenant page = items=%+v next=%q err=%v", page, next, err)
	}
	page, next, err = repository.List(context.Background(), []string{first.TenantID, second.TenantID}, "", "", next, 1)
	if err != nil || len(page) != 1 || next != "" {
		t.Fatalf("second tenant page = items=%+v next=%q err=%v", page, next, err)
	}
	filtered, _, err := repository.List(context.Background(), []string{first.TenantID, second.TenantID}, "hidden", "", "", 0)
	if err != nil || len(filtered) != 0 {
		t.Fatalf("hidden tenant search = items=%+v err=%v", filtered, err)
	}
	if _, _, err := repository.List(context.Background(), []string{first.TenantID}, "", "", "bad", 50); err == nil {
		t.Fatal("invalid tenant cursor was accepted")
	}
	if items, _, err := repository.List(context.Background(), []string{first.TenantID, second.TenantID}, "", "", "", 201); err != nil || len(items) != 2 {
		t.Fatalf("maximum tenant page = items=%+v err=%v", items, err)
	}
}

func TestListWildcardIncludesAllTenants(t *testing.T) {
	repository := NewRepository()
	for _, key := range []string{"wildcard-first", "wildcard-second"} {
		if _, err := repository.Create(context.Background(), tenant.CreateInput{TenantKey: key, DisplayName: key}); err != nil {
			t.Fatal(err)
		}
	}
	items, next, err := repository.List(context.Background(), []string{" * "}, "", "", "", 50)
	if err != nil || len(items) != 2 || next != "" {
		t.Fatalf("wildcard tenant list = items=%+v next=%q err=%v", items, next, err)
	}
}
