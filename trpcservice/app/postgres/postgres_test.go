package postgres

import "testing"

func TestNewRepositoryCreatesAgentAdapter(t *testing.T) {
	if repository := NewAppRepository(nil); repository == nil {
		t.Fatal("agent PostgreSQL repository is nil")
	}
}
