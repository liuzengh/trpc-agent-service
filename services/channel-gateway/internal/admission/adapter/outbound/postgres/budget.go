package postgresadapter

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/admission/domain"
)

// Budget is an initial admission policy, not a measured capacity commitment.
// New Inbox receipts share a durable fixed one-minute window across replicas.
type Budget struct {
	MaxPendingOutbox     int64
	MaxOldestOutboxAge   time.Duration
	MaxNewInboxPerMinute int64
}

func DefaultBudget() Budget {
	return Budget{MaxPendingOutbox: 10000, MaxOldestOutboxAge: 10 * time.Minute, MaxNewInboxPerMinute: 10000}
}
func (b Budget) validate() error {
	if b.MaxPendingOutbox < 1 || b.MaxOldestOutboxAge <= 0 || b.MaxNewInboxPerMinute < 1 {
		return errors.New("invalid admission budget")
	}
	return nil
}

type BudgetHealth struct {
	Saturated bool
	Pending   int64
	OldestAge time.Duration
	NewInbox  int64
}

// Health is an observational snapshot only. Commit enforces the same policy
// atomically even if a caller skips readiness or uses a stale probe result.
func (s *Store) Health(ctx context.Context) (BudgetHealth, error) {
	var health BudgetHealth
	var ageSeconds float64
	err := s.pool.QueryRow(ctx, `SELECT
  (SELECT count(*) FROM gateway_outbox WHERE published_at IS NULL),
  COALESCE((SELECT GREATEST(0,EXTRACT(EPOCH FROM clock_timestamp()-min(created_at))) FROM gateway_outbox WHERE published_at IS NULL),0),
  CASE WHEN window_started_at<=clock_timestamp()-interval '1 minute' THEN 0 ELSE new_events END
 FROM gateway_admission_budget WHERE singleton=true`).Scan(&health.Pending, &ageSeconds, &health.NewInbox)
	if err != nil {
		return health, err
	}
	health.OldestAge = time.Duration(ageSeconds * float64(time.Second))
	health.Saturated = health.Pending >= s.budget.MaxPendingOutbox || health.OldestAge >= s.budget.MaxOldestOutboxAge || health.NewInbox >= s.budget.MaxNewInboxPerMinute
	return health, nil
}

// chargeBudget serializes all fresh acceptance transactions through one row.
// Duplicate lookup must happen first. The counter increment rolls back with the
// Inbox/Admission/Outbox if any later write or commit fails.
func (s *Store) chargeBudget(ctx context.Context, tx pgx.Tx, createsOutbox bool) error {
	var start, now time.Time
	var count int64
	err := tx.QueryRow(ctx, `SELECT window_started_at,new_events FROM gateway_admission_budget WHERE singleton=true FOR UPDATE`).Scan(&start, &count)
	if err != nil {
		return err
	}
	if err = tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return err
	}
	if now.Sub(start) >= time.Minute {
		start = now
		count = 0
	}
	if count >= s.budget.MaxNewInboxPerMinute {
		return domain.ErrUnavailable
	}
	if createsOutbox {
		var pending int64
		var oldest *time.Time
		if err = tx.QueryRow(ctx, `SELECT count(*),min(created_at) FROM gateway_outbox WHERE published_at IS NULL`).Scan(&pending, &oldest); err != nil {
			return err
		}
		if pending >= s.budget.MaxPendingOutbox || (oldest != nil && now.Sub(*oldest) >= s.budget.MaxOldestOutboxAge) {
			return domain.ErrUnavailable
		}
	}
	_, err = tx.Exec(ctx, `UPDATE gateway_admission_budget SET window_started_at=$1,new_events=$2 WHERE singleton=true`, start, count+1)
	return err
}
