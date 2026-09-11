package storage

import (
	"context"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
)

func TestNewSessionServiceInMemory(t *testing.T) {
	service, err := NewSessionService(context.Background(), config.SessionConfig{
		Backend: config.SessionBackendInMemory,
	})
	if err != nil {
		t.Fatalf("create in-memory session service: %v", err)
	}
	t.Cleanup(func() {
		if err := service.Close(); err != nil {
			t.Fatalf("close in-memory session service: %v", err)
		}
	})
	if err := ProbeSessionService(context.Background(), service); err != nil {
		t.Fatalf("probe in-memory session service: %v", err)
	}
}

func TestNewSessionServiceRedis(t *testing.T) {
	server := miniredis.RunT(t)
	service, err := NewSessionService(context.Background(), config.SessionConfig{
		Backend:        config.SessionBackendRedis,
		RedisURL:       "redis://" + server.Addr() + "/0",
		RedisKeyPrefix: "factory-test",
	})
	if err != nil {
		t.Fatalf("create Redis session service: %v", err)
	}
	t.Cleanup(func() {
		if err := service.Close(); err != nil {
			t.Fatalf("close Redis session service: %v", err)
		}
	})
	if err := ProbeSessionService(context.Background(), service); err != nil {
		t.Fatalf("probe Redis session service: %v", err)
	}
}

func TestNewSessionServiceRejectsUnsupportedBackend(t *testing.T) {
	_, err := NewSessionService(context.Background(), config.SessionConfig{Backend: "unknown"})
	if err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("error = %v", err)
	}
}

func TestProbeSessionServiceRejectsNil(t *testing.T) {
	if err := ProbeSessionService(context.Background(), nil); err == nil {
		t.Fatal("expected nil session service error")
	}
}
