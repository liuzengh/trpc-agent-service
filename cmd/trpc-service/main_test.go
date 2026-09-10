package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestConfigFromEnvironmentRequiresExplicitWorkerIdentity(t *testing.T) {
	config, err := configFromEnvironment(environmentReader(map[string]string{
		envRole:            string(roleWorker),
		envPostgresDSN:     "postgres://example",
		envRedisURL:        "redis://example:6379/0",
		envWorkerID:        "worker-a",
		envHTTPAddr:        "127.0.0.1:8081",
		envShutdownTimeout: "45s",
		envModelTimeout:    "12s",
	}))
	if err != nil {
		t.Fatalf("config from environment: %v", err)
	}
	if config.Role != roleWorker || config.WorkerID != "worker-a" ||
		config.HTTPAddr != "127.0.0.1:8081" || config.ShutdownTimeout != 45*time.Second ||
		config.ModelTimeout != 12*time.Second || config.WorkerConcurrency != defaultWorkerConcurrency {
		t.Fatalf("config = %#v", config)
	}

	_, err = configFromEnvironment(environmentReader(map[string]string{
		envRole:        string(roleWorker),
		envPostgresDSN: "postgres://example",
		envRedisURL:    "redis://example:6379/0",
	}))
	if err == nil {
		t.Fatal("config without worker ID succeeded")
	}
}

func TestChannelRoleOwnsAdaptersWithoutGatewayCredentials(t *testing.T) {
	config, err := configFromEnvironment(environmentReader(map[string]string{
		envRole:        string(roleChannel),
		envPostgresDSN: "postgres://example",
		envRedisURL:    "redis://example:6379/0",
	}))
	if err != nil {
		t.Fatalf("channel configuration: %v", err)
	}
	if !config.Role.runsChannel() || config.Role.runsGateway() || config.Role.runsWorker() {
		t.Fatalf("channel role ownership = gateway=%t channel=%t worker=%t", config.Role.runsGateway(), config.Role.runsChannel(), config.Role.runsWorker())
	}
}

func TestReplySenderOwnershipMatchesChannelRole(t *testing.T) {
	tests := []struct {
		role serviceRole
		want bool
	}{
		{role: roleGateway, want: false},
		{role: roleChannel, want: true},
		{role: roleWorker, want: false},
		{role: roleAll, want: true},
	}
	for _, test := range tests {
		if got := test.role.runsChannel(); got != test.want {
			t.Fatalf("role %q runs channel = %t, want %t", test.role, got, test.want)
		}
	}
}

func TestChannelReplyOwnerUsesStableOwnerWhenWorkerIDIsAbsent(t *testing.T) {
	if got := channelReplyOwner(serviceConfig{Role: roleChannel}); got != defaultChannelReplyOwner {
		t.Fatalf("channel reply owner = %q, want %q", got, defaultChannelReplyOwner)
	}
	if got := channelReplyOwner(serviceConfig{Role: roleAll, WorkerID: "all-1"}); got != "all-1" {
		t.Fatalf("combined reply owner = %q, want all-1", got)
	}
}

func TestShutdownResourceCloseSkippedAfterShutdownTimeout(t *testing.T) {
	if !shutdownResourceCloseSkipped(errWorkerShutdownTimeout) {
		t.Fatal("worker shutdown timeout did not skip shared resource close")
	}
	if !shutdownResourceCloseSkipped(context.DeadlineExceeded) {
		t.Fatal("deadline exceeded did not skip shared resource close")
	}
	if shutdownResourceCloseSkipped(errors.New("ordinary shutdown error")) {
		t.Fatal("ordinary shutdown error incorrectly skipped shared resource close")
	}
}

func TestConfigFromEnvironmentParsesWorkerConcurrency(t *testing.T) {
	config, err := configFromEnvironment(environmentReader(map[string]string{
		envRole:              string(roleWorker),
		envPostgresDSN:       "postgres://example",
		envRedisURL:          "redis://example:6379/0",
		envWorkerID:          "worker-a",
		envWorkerConcurrency: "16",
	}))
	if err != nil || config.WorkerConcurrency != 16 {
		t.Fatalf("worker concurrency = %d, err=%v", config.WorkerConcurrency, err)
	}
	_, err = configFromEnvironment(environmentReader(map[string]string{
		envRole:              string(roleWorker),
		envPostgresDSN:       "postgres://example",
		envRedisURL:          "redis://example:6379/0",
		envWorkerID:          "worker-a",
		envWorkerConcurrency: "0",
	}))
	if err == nil {
		t.Fatal("zero worker concurrency was accepted")
	}
}

func TestConfigFromEnvironmentParsesAdmissionControls(t *testing.T) {
	config, err := configFromEnvironment(environmentReader(map[string]string{
		envRole:                 string(roleGateway),
		envPostgresDSN:          "postgres://example",
		envRedisURL:             "redis://example:6379/0",
		envAdminToken:           "admin-token",
		envDispatcherID:         "gateway-1",
		envAdmissionRateLimit:   "7",
		envAdmissionRateWindow:  "2s",
		envAdmissionConcurrency: "3",
		envHTTPEventWaitTimeout: "4s",
	}))
	if err != nil {
		t.Fatalf("admission configuration: %v", err)
	}
	if config.AdmissionRateLimit != 7 || config.AdmissionRateWindow != 2*time.Second ||
		config.AdmissionConcurrency != 3 || config.HTTPEventWaitTimeout != 4*time.Second {
		t.Fatalf("admission controls = %#v", config)
	}
	for _, test := range []struct {
		name  string
		env   string
		value string
	}{
		{name: "zero rate", env: envAdmissionRateLimit, value: "0"},
		{name: "zero window", env: envAdmissionRateWindow, value: "0s"},
		{name: "zero concurrency", env: envAdmissionConcurrency, value: "0"},
		{name: "zero HTTP wait", env: envHTTPEventWaitTimeout, value: "0s"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := configFromEnvironment(environmentReader(map[string]string{
				envRole:         string(roleGateway),
				envPostgresDSN:  "postgres://example",
				envRedisURL:     "redis://example:6379/0",
				envAdminToken:   "admin-token",
				envDispatcherID: "gateway-1",
				test.env:        test.value,
			}))
			if err == nil {
				t.Fatalf("invalid %s=%q was accepted", test.env, test.value)
			}
		})
	}
}

func TestConfigFromEnvironmentParsesFaultPauseAfterClaim(t *testing.T) {
	config, err := configFromEnvironment(environmentReader(map[string]string{
		envRole:                 string(roleWorker),
		envPostgresDSN:          "postgres://example",
		envRedisURL:             "redis://example:6379/0",
		envWorkerID:             "worker-a",
		envFaultPauseAfterClaim: "10m",
	}))
	if err != nil || config.PauseAfterClaim != 10*time.Minute {
		t.Fatalf("fault pause = %s, err=%v", config.PauseAfterClaim, err)
	}
	_, err = configFromEnvironment(environmentReader(map[string]string{
		envRole:                 string(roleWorker),
		envPostgresDSN:          "postgres://example",
		envRedisURL:             "redis://example:6379/0",
		envWorkerID:             "worker-a",
		envFaultPauseAfterClaim: "0s",
	}))
	if err == nil {
		t.Fatal("zero fault pause was accepted")
	}
}

func TestConfigFromEnvironmentParsesFaultPauseAfterMigrationCopy(t *testing.T) {
	config, err := configFromEnvironment(environmentReader(map[string]string{
		envRole:                         string(roleWorker),
		envPostgresDSN:                  "postgres://example",
		envRedisURL:                     "redis://example:6379/0",
		envWorkerID:                     "worker-a",
		envFaultPauseAfterMigrationCopy: "10s",
	}))
	if err != nil || config.PauseAfterMigrationCopy != 10*time.Second {
		t.Fatalf("migration copy fault pause = %s, err=%v", config.PauseAfterMigrationCopy, err)
	}
	_, err = configFromEnvironment(environmentReader(map[string]string{
		envRole:                         string(roleWorker),
		envPostgresDSN:                  "postgres://example",
		envRedisURL:                     "redis://example:6379/0",
		envWorkerID:                     "worker-a",
		envFaultPauseAfterMigrationCopy: "0s",
	}))
	if err == nil {
		t.Fatal("zero migration copy fault pause was accepted")
	}
}

func TestConfigFromEnvironmentParsesFaultPauseAfterMigrationCopyItem(t *testing.T) {
	config, err := configFromEnvironment(environmentReader(map[string]string{
		envRole:                             string(roleWorker),
		envPostgresDSN:                      "postgres://example",
		envRedisURL:                         "redis://example:6379/0",
		envWorkerID:                         "worker-a",
		envFaultPauseAfterMigrationCopyItem: "10s",
	}))
	if err != nil || config.PauseAfterMigrationCopyItem != 10*time.Second {
		t.Fatalf("migration copy item fault pause = %s, err=%v", config.PauseAfterMigrationCopyItem, err)
	}
	_, err = configFromEnvironment(environmentReader(map[string]string{
		envRole:                             string(roleWorker),
		envPostgresDSN:                      "postgres://example",
		envRedisURL:                         "redis://example:6379/0",
		envWorkerID:                         "worker-a",
		envFaultPauseAfterMigrationCopyItem: "0s",
	}))
	if err == nil {
		t.Fatal("zero migration copy item fault pause was accepted")
	}
}

func TestConfigFromEnvironmentRejectsInvalidModelTimeout(t *testing.T) {
	_, err := configFromEnvironment(environmentReader(map[string]string{
		envRole:         string(roleWorker),
		envPostgresDSN:  "postgres://example",
		envRedisURL:     "redis://example:6379/0",
		envWorkerID:     "worker-a",
		envModelTimeout: "0s",
	}))
	if err == nil {
		t.Fatal("config accepted a non-positive model timeout")
	}
}

func TestConfigFromEnvironmentParsesArtifactRetention(t *testing.T) {
	config, err := configFromEnvironment(environmentReader(map[string]string{
		envRole:              string(roleWorker),
		envPostgresDSN:       "postgres://example",
		envRedisURL:          "redis://example:6379/0",
		envWorkerID:          "worker-a",
		envArtifactRetention: "48h",
	}))
	if err != nil || config.ArtifactRetention != 48*time.Hour {
		t.Fatalf("artifact retention = %s, err=%v", config.ArtifactRetention, err)
	}

	config, err = configFromEnvironment(environmentReader(map[string]string{
		envRole:              string(roleWorker),
		envPostgresDSN:       "postgres://example",
		envRedisURL:          "redis://example:6379/0",
		envWorkerID:          "worker-a",
		envArtifactRetention: "0",
	}))
	if err != nil || config.ArtifactRetention != 0 {
		t.Fatalf("disabled artifact retention = %s, err=%v", config.ArtifactRetention, err)
	}

	_, err = configFromEnvironment(environmentReader(map[string]string{
		envRole:              string(roleWorker),
		envPostgresDSN:       "postgres://example",
		envRedisURL:          "redis://example:6379/0",
		envWorkerID:          "worker-a",
		envArtifactRetention: "-1h",
	}))
	if err == nil {
		t.Fatal("negative artifact retention was accepted")
	}
}

func TestRenewLeaseCancelsWorkOnFailure(t *testing.T) {
	t.Parallel()
	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	want := errors.New("lease lost")
	go renewLease(runCtx, cancel, 10*time.Millisecond, done, func(context.Context) error {
		return want
	})

	select {
	case err := <-done:
		if !errors.Is(err, want) {
			t.Fatalf("renew error = %v, want %v", err, want)
		}
	case <-time.After(time.Second):
		t.Fatal("lease renewal did not finish")
	}
	if runCtx.Err() == nil {
		t.Fatal("lease renewal failure did not cancel work")
	}
}

func TestRenewLeaseBoundsRenewalCall(t *testing.T) {
	t.Parallel()
	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go renewLease(runCtx, cancel, 30*time.Millisecond, done, func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	})

	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("renew error = %v, want deadline exceeded", err)
		}
	case <-time.After(time.Second):
		t.Fatal("bounded lease renewal did not finish")
	}
	if runCtx.Err() == nil {
		t.Fatal("bounded renewal failure did not cancel work")
	}
}

func TestDataMigrationRetryDelayIsBounded(t *testing.T) {
	for attempt, base := range map[int]time.Duration{
		0: dataMigrationRetryInitial,
		1: 2 * dataMigrationRetryInitial,
		2: 4 * dataMigrationRetryInitial,
		7: dataMigrationRetryMax,
	} {
		for range 10 {
			delay := dataMigrationRetryDelay(attempt)
			if delay < base/2 || delay > base {
				t.Fatalf("attempt=%d delay=%s, want [%s,%s]", attempt, delay, base/2, base)
			}
		}
	}
}

func TestConfigFromEnvironmentRequiresAdminTokenForGateway(t *testing.T) {
	_, err := configFromEnvironment(environmentReader(map[string]string{
		envRole:        string(roleGateway),
		envPostgresDSN: "postgres://example",
		envRedisURL:    "redis://example:6379/0",
	}))
	if err == nil {
		t.Fatal("gateway configuration without admin token succeeded")
	}

	config, err := configFromEnvironment(environmentReader(map[string]string{
		envRole:         string(roleGateway),
		envPostgresDSN:  "postgres://example",
		envRedisURL:     "redis://example:6379/0",
		envAdminToken:   "admin-token",
		envDispatcherID: "gateway-1",
	}))
	if err != nil || config.AdminToken != "admin-token" {
		t.Fatalf("gateway configuration = %#v, %v", config, err)
	}
}

func TestConfigFromEnvironmentParsesScopedAdminRoles(t *testing.T) {
	config, err := configFromEnvironment(environmentReader(map[string]string{
		envRole:              string(roleGateway),
		envPostgresDSN:       "postgres://example",
		envRedisURL:          "redis://example:6379/0",
		envAdminToken:        "admin-token",
		envDispatcherID:      "gateway-1",
		envOperatorToken:     "operator-token",
		envOperatorTenantIDs: "tenant-a, tenant-b",
		envAuditorToken:      "auditor-token",
		envAuditorTenantIDs:  "tenant-b",
	}))
	if err != nil {
		t.Fatalf("gateway configuration: %v", err)
	}
	if len(config.OperatorTenantIDs) != 2 || config.OperatorTenantIDs[0] != "tenant-a" ||
		config.OperatorTenantIDs[1] != "tenant-b" || len(config.AuditorTenantIDs) != 1 ||
		config.AuditorTenantIDs[0] != "tenant-b" {
		t.Fatalf("scoped admin roles = %#v", config.adminAuthConfig())
	}
}

func TestConfigFromEnvironmentRejectsUnscopedAdminRole(t *testing.T) {
	_, err := configFromEnvironment(environmentReader(map[string]string{
		envRole:          string(roleGateway),
		envPostgresDSN:   "postgres://example",
		envRedisURL:      "redis://example:6379/0",
		envAdminToken:    "admin-token",
		envDispatcherID:  "gateway-1",
		envOperatorToken: "operator-token",
	}))
	if err == nil {
		t.Fatal("gateway configuration accepted an operator without a tenant scope")
	}
}

func TestServiceServerHasResourceLimits(t *testing.T) {
	service, err := startServiceServer("127.0.0.1:0", nil, nil, nil)
	if err != nil {
		t.Fatalf("start service server: %v", err)
	}
	defer func() {
		if err := service.shutdown(context.Background()); err != nil {
			t.Errorf("shutdown service server: %v", err)
		}
	}()

	if service.server.ReadHeaderTimeout != serviceReadHeaderTimeout ||
		service.server.ReadTimeout != serviceReadTimeout ||
		service.server.IdleTimeout != serviceIdleTimeout ||
		service.server.MaxHeaderBytes != serviceMaxHeaderBytes {
		t.Fatalf("HTTP server limits = %#v", service.server)
	}
}

func TestServiceHandlerRoutesGatewayIngress(t *testing.T) {
	handler := serviceHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			t.Fatalf("ingress path = %q", r.URL.Path)
		}
		w.WriteHeader(http.StatusNoContent)
	}), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/admin/v1/tenants" {
			t.Fatalf("admin path = %q", r.URL.Path)
		}
		w.WriteHeader(http.StatusNoContent)
	}))

	assertHTTPStatus(t, handler, "/v1/chat/completions", http.StatusNoContent)
	assertHTTPStatus(t, handler, "/admin/v1/tenants", http.StatusNoContent)
}

func TestServiceHandlerReadinessGatesGatewayIngress(t *testing.T) {
	state := &readinessState{}
	ingressCalls := 0
	handler := serviceHandlerWithReadiness(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		ingressCalls++
		w.WriteHeader(http.StatusNoContent)
	}), nil, state)

	assertHTTPStatus(t, handler, "/v1/chat/completions", http.StatusServiceUnavailable)
	if ingressCalls != 0 {
		t.Fatal("unready ingress reached Gateway")
	}
	state.setReady(true)
	assertHTTPStatus(t, handler, "/v1/chat/completions", http.StatusNoContent)
	if ingressCalls != 1 {
		t.Fatalf("ready ingress calls = %d, want 1", ingressCalls)
	}
}

func TestServiceHealthAndReadiness(t *testing.T) {
	checkErr := errors.New("backend unavailable")
	state := &readinessState{check: func(context.Context) error { return checkErr }}
	handler := serviceHandlerWithReadiness(nil, nil, state)

	assertHTTPStatus(t, handler, "/healthz", http.StatusOK)
	assertHTTPStatus(t, handler, "/readyz", http.StatusServiceUnavailable)
	state.setReady(true)
	assertHTTPStatus(t, handler, "/readyz", http.StatusServiceUnavailable)
	checkErr = nil
	assertHTTPStatus(t, handler, "/readyz", http.StatusOK)
	state.setReady(false)
	assertHTTPStatus(t, handler, "/readyz", http.StatusServiceUnavailable)
}

func TestAwaitWorkerExitReturnsAtShutdownDeadline(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	canceled := make(chan struct{})
	err, stopped := awaitWorkerExit(ctx, make(chan error), func() { close(canceled) })
	if stopped || !errors.Is(err, context.Canceled) {
		t.Fatalf("await worker exit = %v, %t", err, stopped)
	}
	select {
	case <-canceled:
	default:
		t.Fatal("worker cancellation was not requested")
	}
	shutdownErr := workerShutdownResult(nil, err, stopped)
	if !errors.Is(shutdownErr, errWorkerShutdownTimeout) {
		t.Fatalf("shutdown error = %v", shutdownErr)
	}
}

func TestAwaitWorkerExitBoundsNonCooperativeRunner(t *testing.T) {
	runner := &nonCooperativeShutdownRunner{
		started:         make(chan struct{}),
		cancelRequested: make(chan struct{}),
		release:         make(chan struct{}),
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	done := make(chan error, 1)
	go func() { done <- runner.Run(runCtx) }()
	select {
	case <-runner.started:
	case <-time.After(time.Second):
		t.Fatal("non-cooperative runner did not start")
	}

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancelShutdown()
	started := time.Now()
	err, stopped := awaitWorkerExit(shutdownCtx, done, func() {
		close(runner.cancelRequested)
		cancelRun()
	})
	if time.Since(started) > time.Second {
		t.Fatal("shutdown exceeded its timeout boundary")
	}
	if stopped || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("await worker exit = %v, %t, want deadline without stop", err, stopped)
	}
	select {
	case <-runner.cancelRequested:
	default:
		t.Fatal("shutdown did not request runner cancellation")
	}
	if runCtx.Err() == nil {
		t.Fatal("shutdown did not cancel the runner context")
	}
	close(runner.release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("released non-cooperative runner did not finish")
	}
}

func TestAwaitWorkerExitReturnsWorkerResult(t *testing.T) {
	done := make(chan error, 1)
	want := errors.New("worker stopped")
	done <- want
	called := false
	err, stopped := awaitWorkerExit(context.Background(), done, func() { called = true })
	if !stopped || !errors.Is(err, want) || called {
		t.Fatalf("await worker exit = %v, %t, canceled=%t", err, stopped, called)
	}
}

func TestAwaitDataMigrationExitReturnsAtShutdownDeadline(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err, stopped := awaitDataMigrationExit(ctx, make(chan error))
	if stopped || !errors.Is(err, context.Canceled) {
		t.Fatalf("await data migration exit = %v, %t", err, stopped)
	}
	shutdownErr := dataMigrationShutdownResult(nil, err, stopped)
	if !errors.Is(shutdownErr, errWorkerShutdownTimeout) {
		t.Fatalf("shutdown error = %v", shutdownErr)
	}
}

func TestAwaitDataMigrationExitReturnsResult(t *testing.T) {
	done := make(chan error, 1)
	want := errors.New("migration stopped")
	done <- want
	err, stopped := awaitDataMigrationExit(context.Background(), done)
	if !stopped || !errors.Is(err, want) {
		t.Fatalf("await data migration exit = %v, %t", err, stopped)
	}
}

func TestReplyShutdownTimeoutIsObservable(t *testing.T) {
	shutdownErr := replyShutdownError(context.DeadlineExceeded, false)
	if !errors.Is(shutdownErr, errWorkerShutdownTimeout) {
		t.Fatalf("reply shutdown error = %v, want worker shutdown timeout", shutdownErr)
	}
}

func TestServiceServerShutdownIsIdempotent(t *testing.T) {
	done := make(chan struct{})
	close(done)
	serveErr := errors.New("serve stopped")
	service := &serviceServer{
		server:    &http.Server{},
		done:      done,
		readiness: &readinessState{},
		serveErr:  serveErr,
	}

	if err := service.shutdown(context.Background()); !errors.Is(err, serveErr) {
		t.Fatalf("first shutdown = %v, want %v", err, serveErr)
	}
	second := make(chan error, 1)
	go func() { second <- service.shutdown(context.Background()) }()
	select {
	case err := <-second:
		if !errors.Is(err, serveErr) {
			t.Fatalf("second shutdown = %v, want %v", err, serveErr)
		}
	case <-time.After(time.Second):
		t.Fatal("second shutdown blocked")
	}
}

func TestWaitForGatewayShutdownPreservesReplyExit(t *testing.T) {
	wantErr := errors.New("reply sender stopped")
	replyDone := make(chan error, 1)
	replyDone <- wantErr
	service := &serviceServer{
		done:      make(chan struct{}),
		readiness: &readinessState{},
	}

	result, replyErr, replyStopped := waitForGatewayShutdown(
		context.Background(), service, time.Second, nil, nil, replyDone,
	)
	if result != nil || !errors.Is(replyErr, wantErr) || !replyStopped {
		t.Fatalf("gateway shutdown = result=%v reply=%v stopped=%t", result, replyErr, replyStopped)
	}
	if got := replyShutdownError(replyErr, replyStopped); !errors.Is(got, wantErr) {
		t.Fatalf("reply shutdown error = %v, want %v", got, wantErr)
	}
}

func TestWaitForGatewayShutdownIgnoresComponentCancellation(t *testing.T) {
	tests := []struct {
		name   string
		assign func(<-chan error) (<-chan error, <-chan error)
	}{
		{
			name: "relay",
			assign: func(done <-chan error) (<-chan error, <-chan error) {
				return done, nil
			},
		},
		{
			name: "provider",
			assign: func(done <-chan error) (<-chan error, <-chan error) {
				return nil, done
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			componentDone := make(chan error, 1)
			componentDone <- context.Canceled
			relayDone, providerDone := tt.assign(componentDone)
			service := &serviceServer{
				done:      make(chan struct{}),
				readiness: &readinessState{},
			}

			result, replyErr, replyStopped := waitForGatewayShutdown(
				context.Background(), service, time.Second, relayDone, providerDone, nil,
			)
			if result != nil || replyErr != nil || replyStopped {
				t.Fatalf("gateway shutdown = result=%v reply=%v stopped=%t", result, replyErr, replyStopped)
			}
		})
	}
}

func TestEnvironmentSecretsAreScoped(t *testing.T) {
	scope := tenant.Scope{TenantID: "tenant-a", AppID: "app-a"}
	ref := tenant.SecretRef{Name: "model-key", Version: "v1"}
	key := scopedSecretEnvironmentKey(scope, ref)
	sessionRef := tenant.SecretRef{Name: "session-dsn", Version: "v1"}
	sessionKey := scopedSecretEnvironmentKey(scope, sessionRef)
	provider := environmentSecretProvider{getenv: environmentReader(map[string]string{
		key:        "secret",
		sessionKey: "postgres://metadata",
	})}

	value, err := provider.ResolveSecret(context.Background(), scope, ref)
	if err != nil || value != "secret" {
		t.Fatalf("resolve scoped secret = %q, %v", value, err)
	}
	_, err = provider.ResolveSecret(context.Background(), tenant.Scope{TenantID: "tenant-b", AppID: "app-a"}, ref)
	if err == nil {
		t.Fatal("secret resolved for another tenant scope")
	}

	value, err = provider.ResolveSecret(context.Background(), scope, sessionRef)
	if err != nil || value != "postgres://metadata" {
		t.Fatalf("resolve session dsn = %q, %v", value, err)
	}
}

func TestEnvironmentTencentDBGatewayResolverUsesBackendName(t *testing.T) {
	resolver := environmentTencentDBGatewayResolver{getenv: environmentReader(map[string]string{
		envTencentDBGateways: `{"memory-tenant-a":"https://memory-a.example"}`,
	})}
	value, err := resolver.ResolveTencentDBGateway(context.Background(), "memory-tenant-a")
	if err != nil || value != "https://memory-a.example" {
		t.Fatalf("resolve gateway = %q, %v", value, err)
	}
	if _, err := resolver.ResolveTencentDBGateway(context.Background(), "memory-tenant-b"); err == nil {
		t.Fatal("resolved an unconfigured TencentDB gateway")
	}
}

func environmentReader(values map[string]string) func(string) string {
	return func(key string) string { return values[key] }
}

func assertHTTPStatus(t *testing.T, handler http.Handler, path string, want int) {
	t.Helper()
	request := httptest.NewRequest(http.MethodGet, path, nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != want {
		t.Fatalf("GET %s status = %d, want %d", path, response.Code, want)
	}
}

type nonCooperativeShutdownRunner struct {
	started         chan struct{}
	cancelRequested chan struct{}
	release         chan struct{}
}

func (r *nonCooperativeShutdownRunner) Run(context.Context) error {
	close(r.started)
	<-r.release
	return nil
}
