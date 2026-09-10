package inmemory

import (
	"context"
	"testing"
)

func TestListScopesFiltersAndPaginatesProfiles(t *testing.T) {
	repository := NewRepository(inmemoryTestCatalog(t))
	first, _, err := repository.Create(context.Background(), inmemoryCreateInput("tenant-one", "primary"))
	if err != nil {
		t.Fatal(err)
	}
	second, _, err := repository.Create(context.Background(), inmemoryCreateInput("tenant-one", "secondary"))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := repository.Create(context.Background(), inmemoryCreateInput("tenant-two", "primary")); err != nil {
		t.Fatal(err)
	}

	page, next, err := repository.List(context.Background(), first.TenantID, "", "", "", 1)
	if err != nil || len(page) != 1 || next == "" {
		t.Fatalf("first model page = items=%+v next=%q err=%v", page, next, err)
	}
	page, next, err = repository.List(context.Background(), first.TenantID, "", "", next, 1)
	if err != nil || len(page) != 1 || next != "" {
		t.Fatalf("second model page = items=%+v next=%q err=%v", page, next, err)
	}
	filtered, _, err := repository.List(context.Background(), first.TenantID, "secondary", "", "", 50)
	if err != nil || len(filtered) != 1 || filtered[0].ProfileID != second.ProfileID || filtered[0].ProfileID == first.ProfileID {
		t.Fatalf("filtered model list = items=%+v err=%v", filtered, err)
	}
}
