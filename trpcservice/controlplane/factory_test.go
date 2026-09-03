package controlplane

import (
	"context"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
)

func TestNewMemoryRepository(t *testing.T) {
	repository, err := New(context.Background(), config.ControlPlaneConfig{
		Backend: config.ControlPlaneBackendInMemory,
	})
	if err != nil {
		t.Fatalf("create memory repository: %v", err)
	}
	t.Cleanup(func() { _ = repository.Close() })
	if err := repository.Ready(context.Background()); err != nil {
		t.Fatalf("repository ready: %v", err)
	}
}

func TestNewRejectsUnsupportedBackend(t *testing.T) {
	_, err := New(context.Background(), config.ControlPlaneConfig{Backend: "unknown"})
	if err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("error = %v", err)
	}
}
