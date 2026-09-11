package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/trace"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type recordingAuditRetentionStore struct {
	calls map[string]time.Time
	fail  map[string]error
}

type outboxRetentionCall struct {
	tenantID string
	before   time.Time
	limit    int
}

type recordingOutboxRetentionStore struct {
	calls []outboxRetentionCall
	fail  map[string]error
}

type outboxBacklogCall struct {
	tenantID  string
	eventType string
}

type recordingOutboxBacklogStore struct {
	calls []outboxBacklogCall
}

func (s *recordingOutboxBacklogStore) OutboxBacklog(_ context.Context, tenantID, eventType string) (storage.OutboxBacklog, error) {
	s.calls = append(s.calls, outboxBacklogCall{tenantID: tenantID, eventType: eventType})
	return storage.OutboxBacklog{Pending: 2, OldestCreatedAt: time.Now().Add(-time.Minute)}, nil
}

func (s *recordingOutboxRetentionStore) PurgeDeliveredOutboxBefore(_ context.Context, tenantID string, before time.Time, limit int) (int64, error) {
	s.calls = append(s.calls, outboxRetentionCall{tenantID: tenantID, before: before, limit: limit})
	if err := s.fail[tenantID]; err != nil {
		return 0, err
	}
	return 0, nil
}

func (s *recordingAuditRetentionStore) PurgeAuditBefore(_ context.Context, tenantID string, before time.Time) (int64, error) {
	if s.calls == nil {
		s.calls = make(map[string]time.Time)
	}
	s.calls[tenantID] = before
	if err := s.fail[tenantID]; err != nil {
		return 0, err
	}
	return 1, nil
}

type failingApplicationLister struct {
	tenant.Repository
	err error
}

func (r failingApplicationLister) ListApplications(context.Context, string) ([]tenant.Snapshot, error) {
	return nil, r.err
}

type staticApplicationLister struct {
	tenant.Repository
	snapshots []tenant.Snapshot
}

func (r staticApplicationLister) ListApplications(context.Context, string) ([]tenant.Snapshot, error) {
	return append([]tenant.Snapshot(nil), r.snapshots...), nil
}

type configurableTenantDispatcher struct {
	tenants []string
	fail    map[string]error
	cancel  context.CancelFunc
}

func (d *configurableTenantDispatcher) DispatchTenant(_ context.Context, tenantID string, limit int) (int, error) {
	if limit != 100 {
		return 0, fmt.Errorf("unexpected limit %d", limit)
	}
	d.tenants = append(d.tenants, tenantID)
	if err := d.fail[tenantID]; err != nil {
		return 0, err
	}
	if d.cancel != nil {
		d.cancel()
	}
	return 1, nil
}

type cancelingUsageRetention struct {
	cancel context.CancelFunc
	calls  int
}

func (s *cancelingUsageRetention) PurgeUsageReservationsBefore(context.Context, string, string, time.Time, int) (int64, error) {
	s.calls++
	s.cancel()
	return 0, nil
}

type cancelingAuditRetention struct {
	cancel context.CancelFunc
	calls  int
}

func (s *cancelingAuditRetention) PurgeAuditBefore(context.Context, string, time.Time) (int64, error) {
	s.calls++
	s.cancel()
	return 1, nil
}

type cancelingOutboxRetention struct {
	cancel context.CancelFunc
	calls  int
}

func (s *cancelingOutboxRetention) PurgeDeliveredOutboxBefore(context.Context, string, time.Time, int) (int64, error) {
	s.calls++
	s.cancel()
	return 0, nil
}

type cancelingSessionArchiver struct {
	cancel context.CancelFunc
	calls  int
}

func (s *cancelingSessionArchiver) ArchiveIdleSessions(context.Context, time.Time, int) (int64, error) {
	s.calls++
	s.cancel()
	return 1, nil
}

func TestModelCredentialRefsTrimsDeduplicatesAndPreservesOrder(t *testing.T) {
	got := modelCredentialRefs(config.ModelProviderConfig{
		APIKeyRef:    " env:MODEL_TOKEN ",
		SecretIDRef:  "env:MODEL_TOKEN",
		SecretKeyRef: " env:MODEL_SECRET ",
	})
	if joined := strings.Join(got, ","); joined != "env:MODEL_TOKEN,env:MODEL_SECRET" {
		t.Fatalf("modelCredentialRefs() = %q", joined)
	}
	if got := modelCredentialRefs(config.ModelProviderConfig{}); len(got) != 0 {
		t.Fatalf("modelCredentialRefs(empty) = %#v", got)
	}
}

func TestEnforceAuditRetentionUsesStrictestTenantPolicy(t *testing.T) {
	repository := tenant.NewMemoryRepository()
	for _, tenantConfig := range []config.TenantConfig{
		{TenantID: "tenant-a", AppCode: "support", Status: config.AgentActive, ConfigVersion: 1, Audit: config.AuditPolicy{RetentionDays: 30}},
		{TenantID: "tenant-a", AppCode: "support-secondary", Status: config.AgentActive, ConfigVersion: 1, Audit: config.AuditPolicy{RetentionDays: 7}},
		{TenantID: "tenant-b", AppCode: "support", Status: config.AgentActive, ConfigVersion: 1, Audit: config.AuditPolicy{RetentionDays: 14}},
		{TenantID: "tenant-c", AppCode: "support", Status: config.AgentActive, ConfigVersion: 1},
	} {
		if _, err := repository.Publish(context.Background(), tenantConfig); err != nil {
			t.Fatal(err)
		}
	}
	retention := &recordingAuditRetentionStore{}
	app := &application{configurations: repository, auditRetention: retention}
	now := time.Date(2026, 9, 11, 1, 0, 0, 0, time.UTC)
	if err := app.enforceAuditRetentionOnce(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	if len(retention.calls) != 2 {
		t.Fatalf("purged tenants = %#v", retention.calls)
	}
	if want := now.Add(-7 * 24 * time.Hour); !retention.calls["tenant-a"].Equal(want) {
		t.Fatalf("tenant-a cutoff = %v, want %v", retention.calls["tenant-a"], want)
	}
	if want := now.Add(-14 * 24 * time.Hour); !retention.calls["tenant-b"].Equal(want) {
		t.Fatalf("tenant-b cutoff = %v, want %v", retention.calls["tenant-b"], want)
	}
}

func TestEnforceAuditRetentionPropagatesListAndPurgeErrors(t *testing.T) {
	base := tenant.NewMemoryRepository()
	app := &application{
		configurations: failingApplicationLister{Repository: base, err: errors.New("list failed")},
		auditRetention: &recordingAuditRetentionStore{},
	}
	if err := app.enforceAuditRetentionOnce(context.Background(), time.Now()); err == nil || !strings.Contains(err.Error(), "list applications") {
		t.Fatalf("list error = %v", err)
	}

	if _, err := base.Publish(context.Background(), config.TenantConfig{
		TenantID: "tenant-a", AppCode: "support", Status: config.AgentActive, ConfigVersion: 1,
		Audit: config.AuditPolicy{RetentionDays: 7},
	}); err != nil {
		t.Fatal(err)
	}
	app.configurations = base
	app.auditRetention = &recordingAuditRetentionStore{fail: map[string]error{"tenant-a": errors.New("purge failed")}}
	if err := app.enforceAuditRetentionOnce(context.Background(), time.Now()); err == nil || !strings.Contains(err.Error(), "purge audit") {
		t.Fatalf("purge error = %v", err)
	}
}

func TestEnforceOutboxRetentionUsesUniqueTenantsAndConfiguredAge(t *testing.T) {
	repository := staticApplicationLister{snapshots: []tenant.Snapshot{
		{Config: config.TenantConfig{TenantID: "tenant-a", AppCode: "one"}},
		{Config: config.TenantConfig{TenantID: "tenant-a", AppCode: "two"}},
		{Config: config.TenantConfig{TenantID: "tenant-b", AppCode: "one"}},
	}}
	retention := &recordingOutboxRetentionStore{}
	app := &application{configurations: repository, outboxRetention: retention, outboxRetentionAge: 48 * time.Hour}
	now := time.Date(2026, 9, 11, 1, 0, 0, 0, time.UTC)
	if err := app.enforceOutboxRetentionOnce(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	if len(retention.calls) != 2 {
		t.Fatalf("outbox retention calls = %#v", retention.calls)
	}
	for _, call := range retention.calls {
		if !call.before.Equal(now.Add(-48*time.Hour)) || call.limit != outboxRetentionBatch {
			t.Fatalf("outbox retention call = %#v", call)
		}
	}
}

func TestDispatchTenantOutboxesContinuesAfterTenantFailures(t *testing.T) {
	dispatcher := &configurableTenantDispatcher{fail: map[string]error{
		"tenant-a": errors.New("first failed"),
		"tenant-c": errors.New("third failed"),
	}}
	err := dispatchTenantOutboxes(context.Background(), dispatcher, []string{"tenant-a", "tenant-b", "tenant-c"}, "reply")
	if err == nil || !strings.Contains(err.Error(), "tenant-a") || !strings.Contains(err.Error(), "tenant-c") {
		t.Fatalf("dispatchTenantOutboxes() error = %v", err)
	}
	if got := strings.Join(dispatcher.tenants, ","); got != "tenant-a,tenant-b,tenant-c" {
		t.Fatalf("dispatch order = %q", got)
	}
}

func TestBackgroundCycleHelpersRunOneDeterministicCycle(t *testing.T) {
	repository := tenant.NewMemoryRepository()
	if _, err := repository.Publish(context.Background(), config.TenantConfig{
		TenantID: "tenant-a", AppCode: "support", Status: config.AgentActive, ConfigVersion: 1,
		Audit: config.AuditPolicy{RetentionDays: 7},
	}); err != nil {
		t.Fatal(err)
	}
	retention := &recordingAuditRetentionStore{}
	outboxRetention := &recordingOutboxRetentionStore{}
	archiver := &recordingSessionArchiver{}
	dispatcher := &configurableTenantDispatcher{}
	state := storage.NewMemoryStateStore()
	invalidation, err := tenant.NewConfigInvalidationDispatcher(state, tenant.NewMemoryCache())
	if err != nil {
		t.Fatal(err)
	}
	app := &application{
		role: roleGateway,
		configurations: staticApplicationLister{
			Repository: repository,
			snapshots: []tenant.Snapshot{{Config: config.TenantConfig{
				TenantID: "tenant-a", AppCode: "support", Audit: config.AuditPolicy{RetentionDays: 7},
			}}},
		},
		auditRetention:           retention,
		outboxRetention:          outboxRetention,
		sessionArchiver:          archiver,
		replyOutbox:              dispatcher,
		configInvalidationOutbox: invalidation,
	}
	ctx := context.Background()
	now := time.Now().UTC()
	if err := app.enforceAuditRetentionOnce(ctx, now); err != nil {
		t.Fatal(err)
	}
	if err := app.enforceOutboxRetentionOnce(ctx, now); err != nil {
		t.Fatal(err)
	}
	if _, err := app.archiveIdleSessionsOnce(ctx, now); err != nil {
		t.Fatal(err)
	}
	if err := app.dispatchReplyOutboxOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if err := app.dispatchConfigInvalidationOutboxOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if len(retention.calls) != 1 || len(outboxRetention.calls) != 1 || archiver.limit != 1000 || len(dispatcher.tenants) != 1 {
		t.Fatalf("background cycle calls: audit=%d outbox=%d archive_limit=%d dispatch=%v", len(retention.calls), len(outboxRetention.calls), archiver.limit, dispatcher.tenants)
	}
}

func TestObserveOutboxBacklogSamplesRoleChannels(t *testing.T) {
	observer, err := metrics.NewOTelObserver(trace.NewTracerProvider().Tracer("test"), metric.NewMeterProvider().Meter("test"))
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name     string
		role     serviceRole
		channels []channels.Channel
	}{
		{name: "gateway", role: roleGateway, channels: []channels.Channel{channels.Web}},
		{name: "channel", role: roleChannel, channels: []channels.Channel{channels.Telegram, channels.WeCom, channels.Feishu}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			backlog := &recordingOutboxBacklogStore{}
			app := &application{role: test.role, outboxBacklog: backlog, capacityObserver: observer}
			if err := app.observeOutboxBacklog(context.Background(), []string{"tenant-a", "tenant-b"}); err != nil {
				t.Fatal(err)
			}
			if len(backlog.calls) != len(test.channels)*2 {
				t.Fatalf("backlog calls = %#v", backlog.calls)
			}
			seen := make(map[string]bool)
			for _, call := range backlog.calls {
				seen[call.tenantID+"\x00"+call.eventType] = true
			}
			for _, tenantID := range []string{"tenant-a", "tenant-b"} {
				for _, channel := range test.channels {
					key := tenantID + "\x00" + "channel_reply." + string(channel)
					if !seen[key] {
						t.Fatalf("missing backlog sample %q: %#v", key, backlog.calls)
					}
				}
			}
		})
	}
}

func TestBackgroundLoopsRunSuccessfulCycleThenStopOnCancellation(t *testing.T) {
	repository := staticApplicationLister{snapshots: []tenant.Snapshot{{Config: config.TenantConfig{
		TenantID: "tenant-a", AppCode: "support", Audit: config.AuditPolicy{RetentionDays: 7},
	}}}}

	t.Run("usage retention", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		store := &cancelingUsageRetention{cancel: cancel}
		app := &application{configurations: repository, usageRetention: store}
		app.enforceUsageReservationRetention(ctx)
		if store.calls != 1 {
			t.Fatalf("usage retention calls = %d, want 1", store.calls)
		}
	})

	t.Run("audit retention", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		store := &cancelingAuditRetention{cancel: cancel}
		app := &application{configurations: repository, auditRetention: store}
		app.enforceAuditRetention(ctx)
		if store.calls != 1 {
			t.Fatalf("audit retention calls = %d, want 1", store.calls)
		}
	})

	t.Run("outbox retention", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		store := &cancelingOutboxRetention{cancel: cancel}
		app := &application{configurations: repository, outboxRetention: store}
		app.enforceOutboxRetention(ctx)
		if store.calls != 1 {
			t.Fatalf("outbox retention calls = %d, want 1", store.calls)
		}
	})

	t.Run("session archive", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		store := &cancelingSessionArchiver{cancel: cancel}
		app := &application{sessionArchiver: store}
		app.archiveIdleSessions(ctx)
		if store.calls != 1 {
			t.Fatalf("session archive calls = %d, want 1", store.calls)
		}
	})

	t.Run("reply outbox", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		dispatcher := &configurableTenantDispatcher{cancel: cancel}
		app := &application{role: roleGateway, configurations: repository, replyOutbox: dispatcher}
		app.dispatchReplyOutbox(ctx)
		if len(dispatcher.tenants) != 1 || dispatcher.tenants[0] != "tenant-a" {
			t.Fatalf("reply dispatch tenants = %v", dispatcher.tenants)
		}
	})
}

var _ governance.UsageReservationRetention = (*cancelingUsageRetention)(nil)
var _ storage.AuditRetentionStore = (*cancelingAuditRetention)(nil)
var _ storage.OutboxRetentionStore = (*cancelingOutboxRetention)(nil)
var _ storage.IdleSessionArchiver = (*cancelingSessionArchiver)(nil)

func TestApplicationCloseRunsInReverseOrderAndAllowsNil(t *testing.T) {
	var order []int
	app := &application{closeFuncs: []func(){
		func() { order = append(order, 1) },
		func() { order = append(order, 2) },
		func() { order = append(order, 3) },
	}}
	app.Close()
	if got := fmt.Sprint(order); got != "[3 2 1]" {
		t.Fatalf("close order = %s", got)
	}
	var nilApp *application
	nilApp.Close()
}

func TestApplicationRunRejectsIncompleteConfiguration(t *testing.T) {
	var nilApp *application
	if err := nilApp.Run(context.Background()); err == nil {
		t.Fatal("nil application Run() succeeded")
	}
	if err := (&application{}).Run(context.Background()); err == nil {
		t.Fatal("empty application Run() succeeded")
	}
	workerApp := &application{
		role:          roleWorker,
		listenAddress: "127.0.0.1:0",
		handler:       http.NotFoundHandler(),
	}
	if err := workerApp.Run(context.Background()); err == nil {
		t.Fatal("worker application without worker succeeded")
	}
}

func TestApplicationRunPropagatesListenFailure(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	app := &application{
		role:           roleGateway,
		listenAddress:  listener.Addr().String(),
		requestTimeout: time.Second,
		handler:        http.NotFoundHandler(),
	}
	if err := app.Run(context.Background()); err == nil || !strings.Contains(err.Error(), "listen HTTP") {
		t.Fatalf("Run() error = %v, want listen failure", err)
	}
}

func TestApplicationRunGatewayStopsCleanlyOnContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	app := &application{
		role:           roleGateway,
		listenAddress:  "127.0.0.1:0",
		requestTimeout: time.Second,
		handler:        http.NotFoundHandler(),
	}
	if err := app.Run(ctx); err != nil {
		t.Fatalf("Run(canceled) error = %v", err)
	}
}

var _ storage.AuditRetentionStore = (*recordingAuditRetentionStore)(nil)
var _ storage.OutboxRetentionStore = (*recordingOutboxRetentionStore)(nil)
