package postgres

import "testing"

func TestNewRepositoryCreatesTenantAdapter(t *testing.T) {
	if repository := NewRepository(nil); repository == nil {
		t.Fatal("tenant PostgreSQL repository is nil")
	}
}
