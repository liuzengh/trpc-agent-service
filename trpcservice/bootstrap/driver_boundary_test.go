package bootstrap

import (
	"context"
	"errors"
	"testing"
)

func TestNewWithDatabaseDriverPreservesContextAndDriverSelectionBoundary(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := NewWithDatabaseDriver(ctx, nil, ControlPlaneDriverMySQL, Config{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled MySQL bootstrap = %v", err)
	}
	if _, err := NewWithDatabaseDriver(ctx, nil, ControlPlaneDriverPostgres, Config{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled PostgreSQL bootstrap = %v", err)
	}
}
