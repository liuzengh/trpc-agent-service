package bus

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	gosql "github.com/go-sql-driver/mysql"
)

// ErrDuplicateIdem is returned by Append when the idempotency marker already
// exists, i.e. the inbound message was already processed and its reply (if
// any) is already recorded.
var ErrDuplicateIdem = errors.New("bus: duplicate idempotency key")

// Outbox is the MySQL-authoritative reliable-delivery log. Replies are written
// here transactionally with their inbound idempotency marker; a dispatcher
// then drains pending events to Redis Streams (at-least-once). If the process
// dies between commit and publish, the event stays pending and is re-drained.
type Outbox struct {
	db *sql.DB
}

// NewOutbox returns an outbox backed by the given database (tables
// outbox_events and idempotency_keys).
func NewOutbox(db *sql.DB) *Outbox {
	return &Outbox{db: db}
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
		if isDuplicateSQL(err) {
			return ErrDuplicateIdem
		}
		return fmt.Errorf("bus: insert idempotency key: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO outbox_events (event_id, tenant_id, topic, payload) VALUES (?, ?, ?, ?)`,
		m.ID, m.TenantID, StreamOutbound, string(payload)); err != nil {
		if isDuplicateSQL(err) {
			return fmt.Errorf("bus: outbox event %q already exists", m.ID)
		}
		return fmt.Errorf("bus: insert outbox event: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("bus: outbox commit: %w", err)
	}
	return nil
}

// dispatchBatch drains at most 100 pending events. A malformed payload is
// marked sent (never publishable, keep the log moving); a publish failure
// bumps retries and leaves the event pending for the next pass.
func (o *Outbox) dispatchBatch(ctx context.Context, b Bus) (int, error) {
	rows, err := o.db.QueryContext(ctx,
		`SELECT event_id, payload FROM outbox_events WHERE status = 'pending' ORDER BY created_at LIMIT 100`)
	if err != nil {
		return 0, fmt.Errorf("bus: outbox select: %w", err)
	}

	type event struct {
		id      string
		payload string
	}
	var batch []event
	for rows.Next() {
		var e event
		if err := rows.Scan(&e.id, &e.payload); err != nil {
			_ = rows.Close()
			return 0, fmt.Errorf("bus: outbox scan: %w", err)
		}
		batch = append(batch, e)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return 0, fmt.Errorf("bus: outbox rows: %w", err)
	}
	_ = rows.Close()

	for _, e := range batch {
		var m Message
		if err := json.Unmarshal([]byte(e.payload), &m); err != nil {
			// Poison payload: it can never publish. Record and move on.
			_, _ = o.db.ExecContext(ctx,
				`UPDATE outbox_events SET status = 'sent' WHERE event_id = ?`, e.id)
			continue
		}
		if err := b.PublishOutbound(ctx, &m); err != nil {
			_, _ = o.db.ExecContext(ctx,
				`UPDATE outbox_events SET retries = retries + 1 WHERE event_id = ?`, e.id)
			continue
		}
		if _, err := o.db.ExecContext(ctx,
			`UPDATE outbox_events SET status = 'sent' WHERE event_id = ?`, e.id); err != nil {
			return 0, fmt.Errorf("bus: outbox mark sent: %w", err)
		}
	}
	return len(batch), nil
}

// Dispatch drains all pending outbox events to the bus.
func (o *Outbox) Dispatch(ctx context.Context, b Bus) error {
	for {
		n, err := o.dispatchBatch(ctx, b)
		if err != nil {
			return err
		}
		if n < 100 {
			return nil
		}
	}
}

// Run is the dispatcher loop: drain, sleep, repeat until ctx is done. It is
// safe to run several dispatchers across nodes; delivery is at-least-once.
func (o *Outbox) Run(ctx context.Context, b Bus, every time.Duration) error {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		if err := o.Dispatch(ctx, b); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}

// isDuplicateSQL reports a MySQL duplicate-entry error (1062).
func isDuplicateSQL(err error) bool {
	var my *gosql.MySQLError
	return errors.As(err, &my) && my.Number == 1062
}
