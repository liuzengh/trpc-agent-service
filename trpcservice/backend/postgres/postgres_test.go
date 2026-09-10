package postgres

import "testing"

func TestNewRepositoryCreatesBackendAdapter(t *testing.T) {
	if repository := NewRepository(nil, nil); repository == nil {
		t.Fatal("backend PostgreSQL repository is nil")
	}
}
