package bus

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/storage/sqlutil"
)

// ErrDuplicateIdem is returned by Append when the idempotency marker already
// exists, i.e. the inbound message was already processed and its reply (if
// any) is already recorded.
var ErrDuplicateIdem = errors.New("bus: duplicate idempotency key")

// Outbox retry policy. A publish failure is retried on an exponential schedule
// derived from the event's retries and created_at, so the schedule needs no
// extra column and survives process restarts (see outboxNextAttemptAt).
//
// The retry cap exists because the dispatcher selects pending events on every
// tick: an event that can never be published would otherwise be retried until
// the end of time — invisible, unbounded DB churn during exactly the incident
// the operator is trying to diagnose. DefaultMaxOutboxRetries with the schedule
// below means the tenth attempt happens about five minutes in, so a Redis
// restart or a failover is absorbed while a permanently broken event is parked
// as 'dead' for inspection.
const (
	DefaultMaxOutboxRetries = 10
	outboxRetryBase         = 2 * time.Second
	outboxRetryMax          = 60 * time.Second
)

// outboxBackoff returns the delay before attempt number retries+1.
func outboxBackoff(retries int) time.Duration {
	if retries <= 0 {
		return 0
	}
	d := outboxRetryBase
	for i := 1; i < retries; i++ {
		if d >= outboxRetryMax {
			return outboxRetryMax
		}
		d *= 2
	}
	if d > outboxRetryMax {
		return outboxRetryMax
	}
	return d
}

// outboxDue reports whether a pending event may be attempted now, given how old
// the database says it is and how many attempts it already had.
//
// Two rules, for two different reasons:
//
//   - A never-attempted event is always due. The first attempt has nothing to
//     back off from, so it must not be gated on a clock comparison at all. This
//     is also what heals rows written before the MySQL session time zone was
//     pinned to UTC: those carry a timestamp the current session reads as hours
//     in the future, and without this rule they would sit pending forever.
//   - Afterwards the age gates the retry. The age is measured by MySQL
//     (TIMESTAMPDIFF against NOW()) instead of by comparing created_at with this
//     process's clock, because the driver parses DATETIME as UTC while the
//     server writes CURRENT_TIMESTAMP in its own session time zone: on a UTC+8
//     server the two readings of the same row were eight hours apart, every
//     fresh event looked like it was still in the future, and the dispatcher
//     deferred every reply for the whole offset while the rest of the pipeline
//     stayed healthy. One clock, one comparison: the database supplies both.
func outboxDue(age time.Duration, retries int) bool {
	if retries <= 0 {
		return true
	}
	return age >= outboxBackoff(retries)
}

// outboxExhausted reports whether attempts publish tries have used up the
// budget, i.e. the event must be parked as 'dead'.
func outboxExhausted(attempts, maxRetries int) bool {
	if maxRetries <= 0 {
		maxRetries = DefaultMaxOutboxRetries
	}
	return attempts >= maxRetries
}

// Outbox is the MySQL-authoritative reliable-delivery log. Replies are written
// here transactionally with their inbound idempotency marker; a dispatcher
// then drains pending events to Redis Streams (at-least-once). If the process
// dies between commit and publish, the event stays pending and is re-drained.
type Outbox struct {
	db *sql.DB
	// maxRetries is the publish-attempt budget per event before it is parked
	// as 'dead'. Zero means DefaultMaxOutboxRetries.
	maxRetries int
}

// NewOutbox returns an outbox backed by the given database (tables
// outbox_events and idempotency_keys).
func NewOutbox(db *sql.DB) *Outbox {
	return &Outbox{db: db, maxRetries: DefaultMaxOutboxRetries}
}

// SetMaxRetries overrides the per-event publish-attempt budget (tests use a
// small value; zero restores the default).
func (o *Outbox) SetMaxRetries(n int) {
	if n <= 0 {
		n = DefaultMaxOutboxRetries
	}
	o.maxRetries = n
}

// Append records the reply as a pending outbox event and the inbound msgKey
// as processed, in one transaction: either both rows land or neither does.
// Returns ErrDuplicateIdem when msgKey was already recorded (the caller
// treats the message as handled).
func (o *Outbox) Append(ctx context.Context, m *Message, msgKey string) error {
	if m == nil || m.ID == "" {
		return errors.New("bus: outbox append requires a message id")
	}
	if msgKey == "" {
		return errors.New("bus: outbox append requires an idempotency key")
	}
	payload, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("bus: encode outbox payload: %w", err)
	}

	tx, err := o.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("bus: outbox begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO idempotency_keys (msg_key, tenant_id) VALUES (?, ?)`,
		msgKey, m.TenantID); err != nil {
		if sqlutil.IsDuplicate(err) {
			return ErrDuplicateIdem
		}
		return fmt.Errorf("bus: insert idempotency key: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO outbox_events (event_id, tenant_id, topic, payload) VALUES (?, ?, ?, ?)`,
		m.ID, m.TenantID, StreamOutbound, string(payload)); err != nil {
		if sqlutil.IsDuplicate(err) {
			return fmt.Errorf("bus: outbox event %q already exists", m.ID)
		}
		return fmt.Errorf("bus: insert outbox event: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("bus: outbox commit: %w", err)
	}
	return nil
}

// dispatchBatch drains a page of pending events, newest failures last: the page
// is ordered by retries then age so an event that is ready to go is not pushed
// behind ones still waiting out their backoff. A malformed payload is parked as
// dead (never publishable, keep the log moving); a publish failure bumps retries
// and leaves the event pending for the next pass, until the retry budget runs
// out and the event is parked as dead for inspection.
//
// It returns the page size and how many of those events were still in backoff,
// so the caller can tell "log drained" from "nothing was due yet".
func (o *Outbox) dispatchBatch(ctx context.Context, b Bus) (int, int, error) {
	// The age is computed by the database (see outboxDue) so both sides of the
	// due-check come from one clock.
	rows, err := o.db.QueryContext(ctx,
		`SELECT event_id, payload, retries, TIMESTAMPDIFF(SECOND, created_at, NOW())
		   FROM outbox_events WHERE status = 'pending' ORDER BY retries, created_at LIMIT 100`)
	if err != nil {
		return 0, 0, fmt.Errorf("bus: outbox select: %w", err)
	}

	type event struct {
		id      string
		payload string
		retries int
		age     time.Duration
	}
	var batch []event
	for rows.Next() {
		var e event
		var ageSeconds int64
		if err := rows.Scan(&e.id, &e.payload, &e.retries, &ageSeconds); err != nil {
			_ = rows.Close()
			return 0, 0, fmt.Errorf("bus: outbox scan: %w", err)
		}
		e.age = time.Duration(ageSeconds) * time.Second
		batch = append(batch, e)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return 0, 0, fmt.Errorf("bus: outbox rows: %w", err)
	}
	_ = rows.Close()

	deferred := 0
	for _, e := range batch {
		if !outboxDue(e.age, e.retries) {
			deferred++
			continue
		}
		var m Message
		if err := json.Unmarshal([]byte(e.payload), &m); err != nil {
			// Poison payload: it can never publish. Park it and move on; a
			// failed mark would leave it pending and re-drained forever.
			o.park(ctx, e.id, e.retries, fmt.Sprintf("malformed payload: %v", err))
			continue
		}
		if err := b.PublishOutbound(ctx, &m); err != nil {
			o.retryOrPark(ctx, e.id, e.retries, err)
			continue
		}
		if _, err := o.db.ExecContext(ctx,
			`UPDATE outbox_events SET status = 'sent' WHERE event_id = ?`, e.id); err != nil {
			return 0, 0, fmt.Errorf("bus: outbox mark sent: %w", err)
		}
	}
	return len(batch), deferred, nil
}

// retryOrPark records a failed publish: another attempt when the budget allows,
// otherwise the event is parked as dead with its reason. retries is the count
// already recorded, so this failure is attempt retries+1.
func (o *Outbox) retryOrPark(ctx context.Context, eventID string, retries int, cause error) {
	attempt := retries + 1
	if outboxExhausted(attempt, o.maxRetries) {
		slog.Error("bus: outbox event parked as dead after repeated publish failures",
			"event", eventID, "attempts", attempt, "err", cause,
			"requeue", "UPDATE outbox_events SET status='pending', retries=0 WHERE event_id='"+eventID+"'")
		o.park(ctx, eventID, attempt, cause.Error())
		return
	}
	if _, err := o.db.ExecContext(ctx,
		`UPDATE outbox_events SET retries = ? WHERE event_id = ?`, attempt, eventID); err != nil {
		slog.Warn("bus: outbox retry bump failed", "event", eventID, "err", err)
		return
	}
	slog.Warn("bus: outbox publish failed, will retry",
		"event", eventID, "attempt", attempt, "next_in", outboxBackoff(attempt), "err", cause)
}

// park moves an event out of the pending set, recording why. It is terminal:
// only an operator (or a manual requeue) brings the event back.
func (o *Outbox) park(ctx context.Context, eventID string, retries int, reason string) {
	if _, err := o.db.ExecContext(ctx,
		`UPDATE outbox_events SET status = 'dead', retries = ?, last_error = ? WHERE event_id = ?`,
		retries, truncate(reason, 255), eventID); err != nil {
		slog.Error("bus: outbox park failed, event stays pending",
			"event", eventID, "reason", reason, "err", err)
	}
}

// truncate bounds a diagnostic string to the column width.
func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max]
}

// Dispatch drains all pending outbox events to the bus. It stops when the log
// is drained or when a full page of events is still waiting out its backoff —
// the next tick picks those up, so a degraded backend cannot turn this loop
// into a busy spin.
func (o *Outbox) Dispatch(ctx context.Context, b Bus) error {
	for {
		n, deferred, err := o.dispatchBatch(ctx, b)
		if err != nil {
			return err
		}
		if n < 100 || deferred > 0 {
			return nil
		}
	}
}

// Run is the dispatcher loop: drain, sleep, repeat until ctx is done. It is
// safe to run several dispatchers across nodes; delivery is at-least-once.
//
// A failed pass is logged and retried rather than returned: the loop is the
// only thing draining the outbox, so exiting on a transient MySQL/Redis error
// would stop IM delivery for the rest of the process's life (the previous
// behavior — one blip and every reply stayed pending until a restart).
func (o *Outbox) Run(ctx context.Context, b Bus, every time.Duration) error {
	t := time.NewTicker(every)
	defer t.Stop()
	var lastErr error
	for {
		err := o.Dispatch(ctx, b)
		switch {
		case err == nil:
			lastErr = nil
		case ctx.Err() != nil:
			return nil
		case lastErr == nil || err.Error() != lastErr.Error():
			// Log every distinct failure (a repeating one is not re-logged on
			// every tick) so the operator sees the first occurrence.
			slog.Warn("bus: outbox dispatch failed, retrying", "err", err)
			lastErr = err
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}
