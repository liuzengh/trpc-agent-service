package governance

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/liuzengh/trpc-agent-service/migrations"
)

func postgresUsageGovernor(t *testing.T) (*PostgresUsageGovernor, *sql.DB, string) {
	t.Helper()
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN is not set")
	}
	database, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if err := migrations.Apply(context.Background(), database); err != nil {
		t.Fatalf("migrations.Apply() error = %v", err)
	}
	if _, err := database.ExecContext(context.Background(), `
GRANT USAGE ON SCHEMA public TO trpc_tenant;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO trpc_tenant;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO trpc_tenant`); err != nil {
		t.Fatalf("grant tenant role access: %v", err)
	}
	tenantID := "support-usage-" + uuid.NewString()
	if _, err := database.ExecContext(context.Background(), "INSERT INTO tenants (id) VALUES ($1)", tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(context.Background(), "INSERT INTO applications (tenant_id, app_code, status) VALUES ($1,'support','active')", tenantID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = database.ExecContext(context.Background(), "DELETE FROM tenants WHERE id=$1", tenantID) })
	governor, err := NewPostgresUsageGovernor(database)
	if err != nil {
		t.Fatal(err)
	}
	return governor, database, tenantID
}

func usageRequest(tenantID, messageID string) UsageReservationRequest {
	return UsageReservationRequest{
		TenantID: tenantID, AppCode: "support", Channel: "web", BindingID: "console",
		MessageID: messageID, TraceID: "trace-" + messageID,
		MaxConcurrentRuns: 2, TokenBudget: 1000, ReservedTokens: 200, LeaseTTL: time.Minute,
	}
}

func TestPostgresUsageGovernorKnownAndUnknownLifecycle(t *testing.T) {
	governor, database, tenantID := postgresUsageGovernor(t)
	ctx := context.Background()
	known, err := governor.Reserve(ctx, usageRequest(tenantID, "known"))
	if err != nil || known.ID == "" || known.TenantID != tenantID || known.ReservedTokens != 200 {
		t.Fatalf("Reserve(known) = %#v, %v", known, err)
	}
	if err := governor.Renew(ctx, known, 2*time.Minute); err != nil {
		t.Fatalf("Renew() error = %v", err)
	}
	if err := governor.SettleKnown(ctx, known, SettledUsage{PromptTokens: 60, CompletionTokens: 40, TotalTokens: 100, CostMicros: 15}); err != nil {
		t.Fatalf("SettleKnown() error = %v", err)
	}

	unknown, err := governor.Reserve(ctx, usageRequest(tenantID, "unknown"))
	if err != nil {
		t.Fatal(err)
	}
	if err := governor.SettleUnknown(ctx, unknown); err != nil {
		t.Fatalf("SettleUnknown() error = %v", err)
	}

	var knownStatus, unknownStatus string
	var total sql.NullInt64
	if err := database.QueryRowContext(ctx, "SELECT status,total_tokens FROM model_usage_reservations WHERE reservation_id=$1", known.ID).Scan(&knownStatus, &total); err != nil {
		t.Fatal(err)
	}
	if knownStatus != "settled_known" || !total.Valid || total.Int64 != 100 {
		t.Fatalf("known settlement = status %q total %#v", knownStatus, total)
	}
	if err := database.QueryRowContext(ctx, "SELECT status FROM model_usage_reservations WHERE reservation_id=$1", unknown.ID).Scan(&unknownStatus); err != nil {
		t.Fatal(err)
	}
	if unknownStatus != "settled_unknown" {
		t.Fatalf("unknown status = %q", unknownStatus)
	}
	if err := governor.Renew(ctx, known, time.Minute); !errors.Is(err, ErrUsageLeaseLost) {
		t.Fatalf("Renew(settled) error = %v", err)
	}
	if err := governor.SettleUnknown(ctx, known); !errors.Is(err, ErrUsageLeaseLost) {
		t.Fatalf("SettleUnknown(settled) error = %v", err)
	}
}

func TestPostgresUsageGovernorEnforcesConcurrencyAndHourlyBudget(t *testing.T) {
	governor, _, tenantID := postgresUsageGovernor(t)
	ctx := context.Background()
	now := time.Date(2026, 9, 11, 1, 20, 0, 0, time.UTC)
	governor.now = func() time.Time { return now }

	firstRequest := usageRequest(tenantID, "run-1")
	firstRequest.MaxConcurrentRuns = 1
	firstRequest.TokenBudget = 300
	firstRequest.ReservedTokens = 200
	first, err := governor.Reserve(ctx, firstRequest)
	if err != nil {
		t.Fatal(err)
	}
	second := firstRequest
	second.MessageID, second.TraceID = "run-2", "trace-run-2"
	if _, err := governor.Reserve(ctx, second); !errors.Is(err, ErrConcurrentRunLimit) {
		t.Fatalf("Reserve(concurrent) error = %v", err)
	}
	if err := governor.SettleUnknown(ctx, first); err != nil {
		t.Fatal(err)
	}
	if _, err := governor.Reserve(ctx, second); !errors.Is(err, ErrTokenBudget) {
		t.Fatalf("Reserve(over budget) error = %v", err)
	}

	now = now.Add(time.Hour)
	reservation, err := governor.Reserve(ctx, second)
	if err != nil {
		t.Fatalf("Reserve(next period) error = %v", err)
	}
	if !reservation.PeriodStart.Equal(now.Truncate(time.Hour)) {
		t.Fatalf("period start = %v, want %v", reservation.PeriodStart, now.Truncate(time.Hour))
	}
}

func TestPostgresUsageGovernorLeaseExpiryIsRejected(t *testing.T) {
	governor, _, tenantID := postgresUsageGovernor(t)
	now := time.Date(2026, 9, 11, 2, 0, 0, 0, time.UTC)
	governor.now = func() time.Time { return now }
	request := usageRequest(tenantID, "lease-expiry")
	request.LeaseTTL = time.Second
	reservation, err := governor.Reserve(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Second)
	if err := governor.Renew(context.Background(), reservation, time.Minute); !errors.Is(err, ErrUsageLeaseLost) {
		t.Fatalf("Renew(expired) error = %v", err)
	}
}

func TestPostgresUsageGovernorValidatesRenewAndSettlement(t *testing.T) {
	governor, _, tenantID := postgresUsageGovernor(t)
	if err := governor.Renew(context.Background(), UsageReservation{TenantID: tenantID}, 0); err == nil {
		t.Fatal("Renew() accepted zero TTL")
	}
	if err := governor.SettleKnown(context.Background(), UsageReservation{ID: "id", TenantID: tenantID}, SettledUsage{PromptTokens: 1, CompletionTokens: 1, TotalTokens: 3}); err == nil {
		t.Fatal("SettleKnown() accepted inconsistent token total")
	}
}

func TestPostgresUsageGovernorPropagatesClosedDatabaseErrors(t *testing.T) {
	database, err := sql.Open("pgx", "postgres://unused")
	if err != nil {
		t.Fatal(err)
	}
	_ = database.Close()
	governor, err := NewPostgresUsageGovernor(database)
	if err != nil {
		t.Fatal(err)
	}
	request := usageRequest("tenant-a", "closed")
	if _, err := governor.Reserve(context.Background(), request); err == nil || !strings.Contains(err.Error(), "begin usage reservation") {
		t.Fatalf("Reserve(closed DB) error = %v", err)
	}
	reservation := UsageReservation{ID: "reservation", TenantID: "tenant-a"}
	if err := governor.Renew(context.Background(), reservation, time.Minute); err == nil || !strings.Contains(err.Error(), "begin usage reservation renewal") {
		t.Fatalf("Renew(closed DB) error = %v", err)
	}
	if err := governor.SettleUnknown(context.Background(), reservation); err == nil || !strings.Contains(err.Error(), "begin usage settlement") {
		t.Fatalf("SettleUnknown(closed DB) error = %v", err)
	}
}
