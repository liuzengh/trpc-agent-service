package inmemory_test

import (
	"context"
	"errors"
	"testing"

	"github.com/XnLemon/trpc-agent-service/trpcservice/backend/inmemory"
)

func TestListScopesFiltersAndPaginatesProfiles(t *testing.T) {
	repository := inmemory.NewRepository(testCatalog(t))
	first, _, err := repository.Create(context.Background(), createInput(tenantOne, "primary"))
	if err != nil {
		t.Fatal(err)
	}
	second, _, err := repository.Create(context.Background(), createInput(tenantOne, "secondary"))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := repository.Create(context.Background(), createInput(tenantTwo, "primary")); err != nil {
		t.Fatal(err)
	}

	page, next, err := repository.List(context.Background(), tenantOne, "", "", "", 1)
	if err != nil || len(page) != 1 || next == "" {
		t.Fatalf("first backend page = items=%+v next=%q err=%v", page, next, err)
	}
	page, next, err = repository.List(context.Background(), tenantOne, "", "", next, 1)
	if err != nil || len(page) != 1 || next != "" {
		t.Fatalf("second backend page = items=%+v next=%q err=%v", page, next, err)
	}
	filtered, _, err := repository.List(context.Background(), tenantOne, "secondary", "", "", 50)
	if err != nil || len(filtered) != 1 || filtered[0].ProfileID != second.ProfileID || filtered[0].ProfileID == first.ProfileID {
		t.Fatalf("filtered backend list = items=%+v err=%v", filtered, err)
	}
}

func TestListProfileBoundaryReturnsCursorAndContextErrors(t *testing.T) {
	repository := inmemory.NewRepository(testCatalog(t))
	profile, _, err := repository.Create(context.Background(), createInput(tenantOne, "boundary"))
	if err != nil {
		t.Fatal(err)
	}
	if items, next, err := repository.List(context.Background(), tenantOne, "", "disabled", "", 1); err != nil || len(items) != 0 || next != "" {
		t.Fatalf("status-filtered list = items=%+v next=%q err=%v", items, next, err)
	}
	if items, next, err := repository.List(context.Background(), tenantOne, "", "", "0", 0); err != nil || len(items) != 1 || next != "" || items[0].ProfileID != profile.ProfileID {
		t.Fatalf("default list = items=%+v next=%q err=%v", items, next, err)
	}
	if items, next, err := repository.List(context.Background(), tenantOne, "", "", "99", 201); err != nil || items == nil || len(items) != 0 || next != "" {
		t.Fatalf("past-end list = items=%+v next=%q err=%v", items, next, err)
	}
	if _, _, err := repository.List(context.Background(), tenantOne, "", "", "bad", 1); err == nil {
		t.Fatal("invalid cursor was accepted")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := repository.List(canceled, tenantOne, "", "", "", 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled list error = %v", err)
	}
}
