package tenant

import (
	"context"
	"testing"
)

func TestTenantCRUD(t *testing.T) {
	ctx := context.Background()
	s := NewManager()

	t1 := &Tenant{ID: "t1", Name: "acme", Status: StatusActive}
	if err := s.Create(ctx, t1); err != nil {
		t.Fatalf("Create: %v", err)
	}

	got, err := s.Get(ctx, "t1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Name != "acme" {
		t.Errorf("Name = %q, want %q", got.Name, "acme")
	}

	all, err := s.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(all) != 1 {
		t.Errorf("len(List) = %d, want 1", len(all))
	}

	got.Name = "acme-corp"
	if err := s.Update(ctx, got); err != nil {
		t.Fatalf("Update: %v", err)
	}
	got, _ = s.Get(ctx, "t1")
	if got.Name != "acme-corp" {
		t.Errorf("after update Name = %q, want %q", got.Name, "acme-corp")
	}

	if err := s.Delete(ctx, "t1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := s.Get(ctx, "t1"); err == nil {
		t.Error("Get after Delete should fail")
	}
}

func TestTenantDataBackendRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := NewManager()

	t1 := &Tenant{
		ID:          "t1",
		Name:        "acme",
		Status:      StatusActive,
		DataBackend: map[string]string{DomainSession: BackendRedis, DomainMemory: BackendMySQL},
	}
	if err := s.Create(ctx, t1); err != nil {
		t.Fatalf("Create: %v", err)
	}
	got, err := s.Get(ctx, "t1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.DataBackend[DomainSession] != BackendRedis {
		t.Errorf("DataBackend[%q] = %q, want %q", DomainSession, got.DataBackend[DomainSession], BackendRedis)
	}
	if got.DataBackend[DomainMemory] != BackendMySQL {
		t.Errorf("DataBackend[%q] = %q, want %q", DomainMemory, got.DataBackend[DomainMemory], BackendMySQL)
	}

	got.DataBackend[DomainSession] = BackendInMemory
	if err := s.Update(ctx, got); err != nil {
		t.Fatalf("Update: %v", err)
	}
	got, _ = s.Get(ctx, "t1")
	if got.DataBackend[DomainSession] != BackendInMemory {
		t.Errorf("after Update DataBackend[%q] = %q, want %q", DomainSession, got.DataBackend[DomainSession], BackendInMemory)
	}
}

func TestTenantCreateDuplicate(t *testing.T) {
	ctx := context.Background()
	s := NewManager()
	if err := s.Create(ctx, &Tenant{ID: "t1", Name: "a", Status: StatusActive}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := s.Create(ctx, &Tenant{ID: "t1", Name: "b", Status: StatusActive}); err == nil {
		t.Error("duplicate Create should fail")
	}
}

func TestTenantValidate(t *testing.T) {
	tests := []struct {
		name    string
		tenant  Tenant
		wantErr bool
	}{
		{"valid", Tenant{ID: "t1", Name: "acme", Status: StatusActive}, false},
		{"empty id", Tenant{ID: "", Name: "acme", Status: StatusActive}, true},
		{"empty name", Tenant{ID: "t1", Name: "", Status: StatusActive}, true},
		{"bad status", Tenant{ID: "t1", Name: "acme", Status: "weird"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.tenant.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() err = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
