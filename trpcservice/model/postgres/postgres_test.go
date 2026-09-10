package postgres

import "testing"

func TestNewRepositoryCreatesModelAdapter(t *testing.T) {
	if repository := NewRepository(nil, nil); repository == nil {
		t.Fatal("model PostgreSQL repository is nil")
	}
}
