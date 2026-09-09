//
// Tencent is pleased to support the open source community by making trpc-agent-go available.
//
// Copyright (C) 2025 Tencent.  All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.

package health

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

type stubRedisClient struct {
	pingErr error
}

func (s *stubRedisClient) PingContext(ctx context.Context) error { return s.pingErr }

type failingPingClient struct{}

func (failingPingClient) PingContext(_ context.Context) error { return errors.New("database down") }

func TestHealthChecker_Check_reportsHealthyDependencies(t *testing.T) {
	h := New()
	checks := h.Check(context.Background())
	if len(checks) != 0 {
		t.Fatalf("expected empty checks when no dependencies configured, got %v", checks)
	}
}

func TestNilHealthCheckerFailsClosed(t *testing.T) {
	var checker *HealthChecker
	checks := checker.Check(context.Background())
	if checks["health_checker"] != "unhealthy" {
		t.Fatalf("nil checker checks=%v, want unhealthy", checks)
	}
	if checker.IsHealthy(context.Background()) {
		t.Fatal("nil checker reported healthy")
	}
	body, code := checker.Report(context.Background())
	if code != http.StatusServiceUnavailable || body["status"] != "unhealthy" {
		t.Fatalf("nil checker report=(%v,%d), want unhealthy/503", body, code)
	}
}

func TestHealthChecker_Check_reportsRedisStatus(t *testing.T) {
	h := New(WithDatabase(&stubRedisClient{pingErr: nil}))
	checks := h.Check(context.Background())
	if checks["database"] != StatusHealthy {
		t.Fatalf("expected database healthy, got %v", checks["database"])
	}

	h = New(WithDatabase(&stubRedisClient{pingErr: errors.New("timeout")}))
	checks = h.Check(context.Background())
	if checks["database"] == StatusHealthy {
		t.Fatal("expected unhealthy database status")
	}
}

func TestHealthChecker_Report_returnsUnavailableWhenUnhealthy(t *testing.T) {
	h := New(WithDatabase(&failingPingClient{}))
	body, code := h.Report(context.Background())
	if code != http.StatusServiceUnavailable {
		t.Fatalf("expected %d, got %d", http.StatusServiceUnavailable, code)
	}
	if body["status"] != "unhealthy" {
		t.Fatalf("expected unhealthy overall status, got %v", body["status"])
	}
}

func TestHealthCheckerDoesNotExposeDependencyErrorDetails(t *testing.T) {
	secret := "postgres://operator:super-secret@private-db.internal/app"
	h := New(WithDatabase(&stubRedisClient{pingErr: fmt.Errorf("dial %s", secret)}))
	body, _ := h.Report(context.Background())
	checks, ok := body["checks"].(map[string]string)
	if !ok || checks["database"] != "unhealthy" {
		t.Fatalf("unexpected health checks: %#v", body["checks"])
	}
	if strings.Contains(fmt.Sprint(body), "super-secret") || strings.Contains(fmt.Sprint(body), "private-db.internal") {
		t.Fatalf("health response exposed dependency details: %v", body)
	}
}

func TestHealthChecker_IsHealthy_matchesReport(t *testing.T) {
	h := New(WithDatabase(&stubRedisClient{pingErr: nil}))
	if !h.IsHealthy(context.Background()) {
		t.Fatal("expected healthy")
	}
	h = New(WithDatabase(&failingPingClient{}))
	if h.IsHealthy(context.Background()) {
		t.Fatal("expected unhealthy")
	}
}

type blockingHealthPinger struct {
	started chan struct{}
	release chan struct{}
}

func (p *blockingHealthPinger) PingContext(context.Context) error {
	close(p.started)
	<-p.release
	return nil
}

func TestHealthChecker_ReportShortCircuitsProbesWhileDraining(t *testing.T) {
	coordinator := NewCoordinator()
	pinger := &blockingHealthPinger{started: make(chan struct{}), release: make(chan struct{})}
	h := New(WithDatabase(pinger), WithDrainState(coordinator))
	coordinator.BeginDrain()

	done := make(chan struct{})
	var body map[string]interface{}
	var code int
	go func() {
		body, code = h.Report(context.Background())
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		close(pinger.release)
		t.Fatal("draining readiness waited for a dependency probe")
	}
	if code != http.StatusServiceUnavailable || body["status"] != "draining" {
		t.Fatalf("draining report=(%v,%d), want 503 draining", body, code)
	}
	select {
	case <-pinger.started:
		t.Fatal("dependency probe ran while readiness was draining")
	default:
	}
}
