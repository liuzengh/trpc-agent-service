package governance

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/liuzengh/trpc-agent-service/trpcservice/dbscope"
)

var (
	ErrConcurrentRunLimit = errors.New("tenant application concurrent run limit reached")
	ErrTokenBudget        = errors.New("tenant application token budget exceeded")
	ErrUsageLeaseLost     = errors.New("model usage reservation lease lost")
)

type UsageReservationRequest struct {
	TenantID          string
	AppCode           string
	Channel           string
	BindingID         string
	MessageID         string
	TraceID           string
	MaxConcurrentRuns int
	TokenBudget       int64
	ReservedTokens    int64
	LeaseTTL          time.Duration
}

type UsageReservation struct {
	ID             string
	TenantID       string
	AppCode        string
	ReservedTokens int64
	PeriodStart    time.Time
}

type SettledUsage struct {
	PromptTokens     int
	CompletionTokens int
	TotalTokens      int
	CostMicros       int64
}

// UsageGovernor coordinates model-call concurrency and token reservations
// across Worker replicas. Unknown and interrupted calls retain their reserved
// tokens until the fixed hourly window expires.
type UsageGovernor interface {
	Reserve(context.Context, UsageReservationRequest) (UsageReservation, error)
	Renew(context.Context, UsageReservation, time.Duration) error
	SettleKnown(context.Context, UsageReservation, SettledUsage) error
	SettleUnknown(context.Context, UsageReservation) error
}

// UsageReservationRetention owns bounded cleanup of the ephemeral reservation
// table. Durable historical usage lives in model_usage_ledger; reservations
// exist only to coordinate concurrency and the current token-budget window.
type UsageReservationRetention interface {
	PurgeUsageReservationsBefore(context.Context, string, string, time.Time, int) (int64, error)
}

type PostgresUsageGovernor struct {
	database *sql.DB
	now      func() time.Time
}

func NewPostgresUsageGovernor(database *sql.DB) (*PostgresUsageGovernor, error) {
	if database == nil {
		return nil, errors.New("usage governor database is required")
	}
	return &PostgresUsageGovernor{database: database, now: time.Now}, nil
}

func (g *PostgresUsageGovernor) Reserve(ctx context.Context, request UsageReservationRequest) (UsageReservation, error) {
	if err := validateUsageReservationRequest(request); err != nil {
		return UsageReservation{}, err
	}
	now := g.now().UTC()
	periodStart := now.Truncate(time.Hour)
	reservation := UsageReservation{
		ID: uuid.NewString(), TenantID: request.TenantID, AppCode: request.AppCode,
		ReservedTokens: request.ReservedTokens, PeriodStart: periodStart,
	}
	tx, err := dbscope.BeginTenantTransaction(ctx, g.database, request.TenantID)
	if err != nil {
		return UsageReservation{}, fmt.Errorf("begin usage reservation: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	lockKey := request.TenantID + "/" + request.AppCode + "/usage"
	if _, err := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", lockKey); err != nil {
		return UsageReservation{}, fmt.Errorf("lock usage reservation: %w", err)
	}
	if request.MaxConcurrentRuns > 0 {
		var active int
		if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM model_usage_reservations
WHERE tenant_id = $1 AND app_code = $2 AND status = 'pending' AND lease_until > $3`,
			request.TenantID, request.AppCode, now).Scan(&active); err != nil {
			return UsageReservation{}, fmt.Errorf("count active model runs: %w", err)
		}
		if active >= request.MaxConcurrentRuns {
			return UsageReservation{}, ErrConcurrentRunLimit
		}
	}
	if request.TokenBudget > 0 {
		var accounted int64
		if err := tx.QueryRowContext(ctx, `
SELECT COALESCE(SUM(
    CASE WHEN status = 'settled_known' THEN total_tokens ELSE reserved_tokens END
), 0)
FROM model_usage_reservations
WHERE tenant_id = $1 AND app_code = $2 AND period_start = $3`,
			request.TenantID, request.AppCode, periodStart).Scan(&accounted); err != nil {
			return UsageReservation{}, fmt.Errorf("read token budget usage: %w", err)
		}
		if accounted+request.ReservedTokens > request.TokenBudget {
			return UsageReservation{}, ErrTokenBudget
		}
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO model_usage_reservations (
    reservation_id, tenant_id, app_code, channel_type, binding_id, message_id, trace_id,
    period_start, reserved_tokens, status, lease_until, created_at, updated_at
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'pending',$10,$11,$11)`,
		reservation.ID, request.TenantID, request.AppCode, request.Channel, request.BindingID,
		request.MessageID, request.TraceID, periodStart, request.ReservedTokens,
		now.Add(request.LeaseTTL), now); err != nil {
		return UsageReservation{}, fmt.Errorf("insert model usage reservation: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return UsageReservation{}, fmt.Errorf("commit model usage reservation: %w", err)
	}
	return reservation, nil
}

func (g *PostgresUsageGovernor) Renew(ctx context.Context, reservation UsageReservation, ttl time.Duration) error {
	if ttl <= 0 {
		return errors.New("usage reservation lease TTL must be positive")
	}
	tx, err := dbscope.BeginTenantTransaction(ctx, g.database, reservation.TenantID)
	if err != nil {
		return fmt.Errorf("begin usage reservation renewal: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	now := g.now().UTC()
	result, err := tx.ExecContext(ctx, `
UPDATE model_usage_reservations
SET lease_until = $4, updated_at = $3
WHERE reservation_id = $1 AND tenant_id = $2 AND status = 'pending' AND lease_until > $3`,
		reservation.ID, reservation.TenantID, now, now.Add(ttl))
	if err != nil {
		return fmt.Errorf("renew usage reservation: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("inspect usage reservation renewal: %w", err)
	}
	if rows != 1 {
		return ErrUsageLeaseLost
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit usage reservation renewal: %w", err)
	}
	return nil
}

func (g *PostgresUsageGovernor) SettleKnown(ctx context.Context, reservation UsageReservation, usage SettledUsage) error {
	if usage.PromptTokens < 0 || usage.CompletionTokens < 0 || usage.TotalTokens < 0 || usage.CostMicros < 0 ||
		usage.PromptTokens+usage.CompletionTokens != usage.TotalTokens {
		return errors.New("known model usage is invalid")
	}
	return g.settle(ctx, reservation, true, usage)
}

func (g *PostgresUsageGovernor) SettleUnknown(ctx context.Context, reservation UsageReservation) error {
	return g.settle(ctx, reservation, false, SettledUsage{})
}

func (g *PostgresUsageGovernor) PurgeUsageReservationsBefore(ctx context.Context, tenantID, appCode string, before time.Time, limit int) (int64, error) {
	tenantID, appCode = strings.TrimSpace(tenantID), strings.TrimSpace(appCode)
	if tenantID == "" || appCode == "" || before.IsZero() || limit <= 0 {
		return 0, errors.New("usage reservation retention requires tenant, app, cutoff and positive limit")
	}
	tx, err := dbscope.BeginTenantTransaction(ctx, g.database, tenantID)
	if err != nil {
		return 0, fmt.Errorf("begin usage reservation retention: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	result, err := tx.ExecContext(ctx, `
WITH candidates AS (
    SELECT reservation_id
    FROM model_usage_reservations
    WHERE tenant_id = $1 AND app_code = $2 AND period_start < $3
      AND (status <> 'pending' OR lease_until <= $4)
    ORDER BY period_start, reservation_id
    LIMIT $5
    FOR UPDATE SKIP LOCKED
)
DELETE FROM model_usage_reservations AS reservation
USING candidates
WHERE reservation.reservation_id = candidates.reservation_id`,
		tenantID, appCode, before.UTC(), g.now().UTC(), limit)
	if err != nil {
		return 0, fmt.Errorf("purge model usage reservations: %w", err)
	}
	deleted, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("inspect model usage reservation retention: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit model usage reservation retention: %w", err)
	}
	return deleted, nil
}

var _ UsageReservationRetention = (*PostgresUsageGovernor)(nil)

func (g *PostgresUsageGovernor) settle(ctx context.Context, reservation UsageReservation, known bool, usage SettledUsage) error {
	if strings.TrimSpace(reservation.ID) == "" || strings.TrimSpace(reservation.TenantID) == "" {
		return errors.New("usage reservation identity is required")
	}
	tx, err := dbscope.BeginTenantTransaction(ctx, g.database, reservation.TenantID)
	if err != nil {
		return fmt.Errorf("begin usage settlement: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	status := "settled_unknown"
	var prompt, completion, total, cost any
	if known {
		status = "settled_known"
		prompt, completion, total, cost = usage.PromptTokens, usage.CompletionTokens, usage.TotalTokens, usage.CostMicros
	}
	now := g.now().UTC()
	result, err := tx.ExecContext(ctx, `
UPDATE model_usage_reservations
SET status = $3, prompt_tokens = $4, completion_tokens = $5, total_tokens = $6,
    cost_micros = $7, lease_until = NULL, updated_at = $8
WHERE reservation_id = $1 AND tenant_id = $2 AND status = 'pending'`,
		reservation.ID, reservation.TenantID, status, prompt, completion, total, cost, now)
	if err != nil {
		return fmt.Errorf("settle model usage: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("inspect model usage settlement: %w", err)
	}
	if rows != 1 {
		return ErrUsageLeaseLost
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit model usage settlement: %w", err)
	}
	return nil
}

func validateUsageReservationRequest(request UsageReservationRequest) error {
	if strings.TrimSpace(request.TenantID) == "" || strings.TrimSpace(request.AppCode) == "" ||
		strings.TrimSpace(request.Channel) == "" || strings.TrimSpace(request.BindingID) == "" || strings.TrimSpace(request.MessageID) == "" || strings.TrimSpace(request.TraceID) == "" {
		return errors.New("usage reservation identity is incomplete")
	}
	if request.MaxConcurrentRuns < 0 || request.TokenBudget < 0 || request.ReservedTokens < 0 {
		return errors.New("usage reservation limits must not be negative")
	}
	if request.TokenBudget > 0 && (request.ReservedTokens <= 0 || request.ReservedTokens > request.TokenBudget) {
		return errors.New("usage reservation tokens must be within the configured token budget")
	}
	if request.LeaseTTL <= 0 {
		return errors.New("usage reservation lease TTL must be positive")
	}
	return nil
}
