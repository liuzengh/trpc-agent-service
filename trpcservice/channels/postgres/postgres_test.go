package postgres

import "testing"

func TestNewRepositoryCreatesChannelAdapter(t *testing.T) {
	if repository := NewRepository(nil); repository == nil {
		t.Fatal("channel PostgreSQL repository is nil")
	}
}
