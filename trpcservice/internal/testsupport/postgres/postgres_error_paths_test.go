package postgres

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	"github.com/XnLemon/trpc-agent-service/trpcservice/model"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
)

var registerPostgresErrorDriver sync.Once

type postgresErrorDriver struct{}

func (postgresErrorDriver) Open(string) (driver.Conn, error) { return postgresErrorConn{}, nil }

type postgresErrorConn struct{}

func (postgresErrorConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare failure")
}
func (postgresErrorConn) Close() error              { return nil }
func (postgresErrorConn) Begin() (driver.Tx, error) { return postgresErrorTx{}, nil }
func (postgresErrorConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return postgresErrorTx{}, nil
}
func (postgresErrorConn) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	return nil, errors.New("exec failure")
}
func (postgresErrorConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return nil, errors.New("query failure")
}
func (postgresErrorConn) Ping(context.Context) error { return nil }

type postgresErrorTx struct{}

func (postgresErrorTx) Commit() error   { return errors.New("commit failure") }
func (postgresErrorTx) Rollback() error { return nil }

func openPostgresErrorDB(t *testing.T) *sql.DB {
	t.Helper()
	registerPostgresErrorDriver.Do(func() {
		sql.Register("trpc-postgres-error", postgresErrorDriver{})
	})
	db, err := sql.Open("trpc-postgres-error", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestPostgreSQLRepositoriesMapDriverFailures(t *testing.T) {
	db := openPostgresErrorDB(t)
	ctx := context.Background()
	testPostgreSQLTenantDriverFailures(t, db, ctx)

	testPostgreSQLModelDriverFailures(t, db, ctx)
	testPostgreSQLBackendDriverFailures(t, db, ctx)

	testPostgreSQLAgentDriverFailures(t, db, ctx)

	testPostgreSQLChannelDriverFailures(t, db, ctx)
}

func testPostgreSQLTenantDriverFailures(t *testing.T, db *sql.DB, ctx context.Context) {
	t.Helper()
	metadata := tenant.TransitionMetadata{ActorType: "test", ActorID: "unit", Reason: "exercise failure", CorrelationID: "error-path"}
	repo := NewTenantRepository(db)
	checks := []struct {
		name string
		run  func() error
	}{
		{"Create", func() error {
			_, err := repo.Create(ctx, tenant.CreateInput{TenantKey: "error-path", DisplayName: "Error Path", Status: tenant.StatusActive, AuditRetentionDays: 90, LogMaskingLevel: tenant.MaskingBasic, TraceSamplingRate: 1})
			return err
		}},
		{"Get", func() error { _, err := repo.Get(ctx, "tenant"); return err }},
		{"Update", func() error {
			_, err := repo.UpdateConfiguration(ctx, tenant.UpdateConfigurationInput{TenantID: "tenant", ExpectedVersion: 1, DisplayName: "Tenant", AuditRetentionDays: 90, LogMaskingLevel: tenant.MaskingBasic, TraceSamplingRate: 1})
			return err
		}},
		{"Transition", func() error {
			_, _, err := repo.TransitionStatus(ctx, tenant.TransitionStatusInput{TenantID: "tenant", ExpectedVersion: 1, NextStatus: tenant.StatusSuspended, Metadata: metadata})
			return err
		}},
	}
	assertPostgreSQLStorageErrors(t, checks)
}

func assertPostgreSQLStorageErrors(t *testing.T, checks []struct {
	name string
	run  func() error
}) {
	t.Helper()
	for _, check := range checks {
		if err := check.run(); !errors.Is(err, ErrStorage) {
			t.Fatalf("%s error = %v", check.name, err)
		}
	}
}

func testPostgreSQLModelDriverFailures(t *testing.T, db *sql.DB, ctx context.Context) {
	t.Helper()
	catalog, err := model.NewProviderCatalog(model.ProviderSpec{Provider: "public", Models: []string{"chat"}, EndpointPolicy: model.FieldOptional, EndpointSchemes: []string{"https"}, EndpointHosts: []string{"example.test"}, SecretRefPolicy: model.FieldOptional})
	if err != nil {
		t.Fatal(err)
	}
	repo := NewModelRepository(db, catalog)
	md := model.ChangeMetadata{ActorType: "test", ActorID: "unit", Reason: "exercise failure", CorrelationID: "error-path"}
	assertPostgreSQLStorageErrors(t, []struct {
		name string
		run  func() error
	}{
		{"Create", func() error {
			_, _, err := repo.Create(ctx, model.CreateInput{TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAW", ProfileKey: "primary", DisplayName: "Model", Status: model.StatusActive, Configuration: model.Configuration{Provider: "public", Model: "chat"}, Metadata: md})
			return err
		}},
		{"Get", func() error { _, err := repo.Get(ctx, "tenant", "profile"); return err }},
		{"Update", func() error { _, _, err := repo.UpdateConfiguration(ctx, model.UpdateConfigurationInput{}); return err }},
		{"Transition", func() error { _, _, err := repo.TransitionStatus(ctx, model.TransitionStatusInput{}); return err }},
	})
}

func testPostgreSQLBackendDriverFailures(t *testing.T, db *sql.DB, ctx context.Context) {
	t.Helper()
	catalog, err := backend.NewProviderCatalog(backend.ProviderSpec{Provider: "inmemory", Capabilities: []backend.Capability{backend.CapabilitySession}, EndpointPolicy: backend.FieldForbidden, SecretRefPolicy: backend.FieldForbidden})
	if err != nil {
		t.Fatal(err)
	}
	repo := NewBackendRepository(db, catalog)
	md := backend.ChangeMetadata{ActorType: "test", ActorID: "unit", Reason: "exercise failure", CorrelationID: "error-path"}
	assertPostgreSQLStorageErrors(t, []struct {
		name string
		run  func() error
	}{
		{"Create", func() error {
			_, _, err := repo.Create(ctx, backend.CreateInput{TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAW", ProfileKey: "primary", DisplayName: "Backend", Status: backend.StatusActive, Bindings: []backend.CapabilityBinding{{Capability: backend.CapabilitySession, Provider: "inmemory"}}, Metadata: md})
			return err
		}},
		{"Get", func() error { _, err := repo.Get(ctx, "tenant", "profile"); return err }},
		{"Update", func() error {
			_, _, err := repo.UpdateConfiguration(ctx, backend.UpdateConfigurationInput{})
			return err
		}},
		{"Transition", func() error { _, _, err := repo.TransitionStatus(ctx, backend.TransitionStatusInput{}); return err }},
	})
}

func testPostgreSQLAgentDriverFailures(t *testing.T, db *sql.DB, ctx context.Context) {
	t.Helper()
	repo := NewAgentRepository(db)
	md := appmodel.ChangeMetadata{ActorType: "test", ActorID: "unit", Reason: "exercise failure", CorrelationID: "error-path"}
	assertPostgreSQLStorageErrors(t, []struct {
		name string
		run  func() error
	}{
		{"Create", func() error {
			_, err := repo.Create(ctx, appmodel.CreateInput{TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAW", AppKey: "primary", DisplayName: "Agent"})
			return err
		}},
		{"Get", func() error { _, err := repo.Get(ctx, "tenant", "app"); return err }},
		{"Metadata", func() error { _, err := repo.UpdateMetadata(ctx, appmodel.UpdateMetadataInput{}); return err }},
		{"Draft", func() error { _, err := repo.CreateDraft(ctx, appmodel.CreateDraftInput{}); return err }},
		{"DraftUpdate", func() error { _, err := repo.UpdateDraft(ctx, appmodel.UpdateDraftInput{}); return err }},
		{"Revision", func() error { _, err := repo.GetRevision(ctx, "tenant", "app", 1); return err }},
		{"Publish", func() error {
			_, _, _, err := repo.Publish(ctx, appmodel.PublishInput{TenantID: "tenant", AppID: "app", Revision: 1, ExpectedAppVersion: 1, ExpectedDraftVersion: 1, TenantActive: true, Metadata: md})
			return err
		}},
		{"Rollback", func() error {
			_, _, err := repo.Rollback(ctx, appmodel.RollbackInput{TenantID: "tenant", AppID: "app", TargetRevision: 1, ExpectedAppVersion: 1, Metadata: md})
			return err
		}},
		{"Transition", func() error {
			_, _, err := repo.TransitionStatus(ctx, appmodel.TransitionStatusInput{TenantID: "tenant", AppID: "app", ExpectedVersion: 1, NextStatus: appmodel.StatusSuspended, Metadata: md})
			return err
		}},
	})
}

func testPostgreSQLChannelDriverFailures(t *testing.T, db *sql.DB, ctx context.Context) {
	t.Helper()
	routeDigest, err := channels.DigestPublicRouteKey(channels.ChannelTelegram, "error-path")
	if err != nil {
		t.Fatal(err)
	}
	repo := NewChannelRepository(db)
	md := channels.ChangeMetadata{ActorType: "test", ActorID: "unit", Reason: "exercise failure", CorrelationID: "error-path"}
	assertPostgreSQLStorageErrors(t, []struct {
		name string
		run  func() error
	}{
		{"Create", func() error {
			_, _, err := repo.Create(ctx, channels.CreateInput{TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAW", BindingKey: "primary", Channel: channels.ChannelTelegram, ProviderAccountID: "account", PublicRouteKeyDigest: routeDigest, AppID: "app_01ARZ3NDEKTSV4RRFFQ69G5FAW", SecretRef: "secret://binding", Protocol: channels.ProtocolConfiguration{Telegram: &channels.TelegramProtocolConfiguration{}}, Status: channels.StatusDraft, Metadata: md})
			return err
		}},
		{"Get", func() error { _, err := repo.Get(ctx, "tenant", "binding"); return err }},
		{"Update", func() error {
			_, _, err := repo.UpdateConfiguration(ctx, channels.UpdateConfigurationInput{})
			return err
		}},
		{"Transition", func() error { _, _, err := repo.TransitionStatus(ctx, channels.TransitionStatusInput{}); return err }},
		{"Activate", func() error { _, _, err := repo.Activate(ctx, channels.TransitionStatusInput{}); return err }},
		{"Suspend", func() error { _, _, err := repo.Suspend(ctx, channels.TransitionStatusInput{}); return err }},
		{"Resume", func() error { _, _, err := repo.Resume(ctx, channels.TransitionStatusInput{}); return err }},
		{"Disable", func() error { _, _, err := repo.Disable(ctx, channels.TransitionStatusInput{}); return err }},
	})
	if _, err := repo.LookupCandidates(ctx, channels.ChannelTelegram, routeDigest); !errors.Is(err, channels.ErrCandidateUnavailable) && !errors.Is(err, ErrStorage) {
		t.Fatalf("channel lookup error = %v", err)
	}
	if _, err := repo.ConsumeCandidate(ctx, validCandidate(routeDigest)); !errors.Is(err, channels.ErrCandidateUnavailable) {
		t.Fatalf("channel consume error = %v", err)
	}
}

func validCandidate(routeDigest string) channels.CandidateBindingContext {
	now := time.Now().UTC()
	return channels.CandidateBindingContext{
		Channel: channels.ChannelTelegram, PublicRouteKeyDigest: routeDigest, BindingVersion: 1,
		ConfigDigest: strings.Repeat("a", 64), Purpose: channels.PurposeWebhookVerification,
		CandidateToken: "candidate-token", IssuedAt: now, ExpiresAt: now.Add(time.Minute),
	}
}
