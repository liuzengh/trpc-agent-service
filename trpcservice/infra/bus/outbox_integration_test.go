//go:build integration

package bus

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/testcontainers/testcontainers-go/modules/mysql"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

const outboxSchema = `CREATE TABLE IF NOT EXISTS outbox_events (
    event_id   VARCHAR(36) NOT NULL,
    tenant_id  VARCHAR(36) NOT NULL,
    topic      VARCHAR(64) NOT NULL,
    payload    JSON        NOT NULL,
    status     ENUM('pending','sent','dead') NOT NULL DEFAULT 'pending',
    retries    INT         NOT NULL DEFAULT 0,
    last_error VARCHAR(255) NOT NULL DEFAULT '',
    created_at DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (event_id),
    KEY idx_outbox_status (status, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS idempotency_keys (
    msg_key    VARCHAR(128) NOT NULL,
    tenant_id  VARCHAR(36)  NOT NULL,
    created_at DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (msg_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`

// failingBus rejects every outbound publish, standing in for an unreachable
// Redis while the dispatcher keeps running.
type failingBus struct{ Bus }

func (failingBus) PublishOutbound(context.Context, *Message) error {
	return errors.New("redis: connection refused")
}

// okBus records published events.
type okBus struct {
	Bus
	published []*Message
}

func (b *okBus) PublishOutbound(_ context.Context, m *Message) error {
	b.published = append(b.published, m)
	return nil
}

func newOutboxDB(t *testing.T) *sql.DB {
	t.Helper()
	ctx := context.Background()
	c, err := mysql.Run(ctx, "mysql:8.0",
		mysql.WithUsername("test"), mysql.WithPassword("test"), mysql.WithDatabase("test"))
	if err != nil {
		t.Fatalf("mysql run: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })
	dsn, err := c.ConnectionString(ctx, "parseTime=true", "multiStatements=true")
	if err != nil {
		t.Fatalf("mysql dsn: %v", err)
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open mysql: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(outboxSchema); err != nil {
		t.Fatalf("create outbox schema: %v", err)
	}
	return db
}

func appendOutboxMessage(t *testing.T, o *Outbox, id, tenantID string) {
	t.Helper()
	reply := model.NewAssistantMessage("hello")
	if err := o.Append(context.Background(), &Message{
		ID: id, TenantID: tenantID, AgentID: "a-1", SessionID: "s-1",
		Channel: "admin", UserID: "u-1", Content: &reply,
	}, id+":idem"); err != nil {
		t.Fatalf("append: %v", err)
	}
}

// TestOutboxParksEventAfterRetryBudget is the regression for the unbounded
// retry loop: with a permanently failing transport the dispatcher used to bump
// retries forever and re-select the same event every tick, so a reply that
// could never be delivered was neither delivered nor reported. It must now end
// as a dead row carrying the reason.
func TestOutboxParksEventAfterRetryBudget(t *testing.T) {
	ctx := context.Background()
	db := newOutboxDB(t)
	o := NewOutbox(db)
	o.SetMaxRetries(3)
	appendOutboxMessage(t, o, "ev-park", "t-park")

	// The schedule is time-based, so drive it by backdating created_at instead
	// of sleeping through the real backoff.
	for attempt := 1; attempt <= 3; attempt++ {
		if _, err := db.ExecContext(ctx,
			`UPDATE outbox_events SET created_at = NOW() - INTERVAL 1 HOUR WHERE event_id = 'ev-park'`); err != nil {
			t.Fatalf("backdate: %v", err)
		}
		if err := o.Dispatch(ctx, failingBus{}); err != nil {
			t.Fatalf("dispatch: %v", err)
		}
	}

	var status, lastError string
	var retries int
	if err := db.QueryRowContext(ctx,
		`SELECT status, retries, last_error FROM outbox_events WHERE event_id = 'ev-park'`).
		Scan(&status, &retries, &lastError); err != nil {
		t.Fatalf("select event: %v", err)
	}
	if status != "dead" {
		t.Errorf("status = %q, want dead after the retry budget", status)
	}
	if retries != 3 {
		t.Errorf("retries = %d, want 3", retries)
	}
	if lastError == "" {
		t.Error("last_error must record why the event was parked")
	}

	// Dead rows are out of the pending set: another pass must not touch them.
	if err := o.Dispatch(ctx, failingBus{}); err != nil {
		t.Fatalf("dispatch after park: %v", err)
	}
	if err := db.QueryRowContext(ctx,
		`SELECT retries FROM outbox_events WHERE event_id = 'ev-park'`).Scan(&retries); err != nil {
		t.Fatalf("re-select: %v", err)
	}
	if retries != 3 {
		t.Errorf("retries = %d after the event was parked, want 3 (no further attempts)", retries)
	}
}

// TestOutboxBackoffDefersRetry proves the backoff is real against a database:
// a just-failed event is not retried on the next pass, and becomes due once its
// schedule elapses. Without this the retry budget would be spent in
// milliseconds during a brief outage.
func TestOutboxBackoffDefersRetry(t *testing.T) {
	ctx := context.Background()
	db := newOutboxDB(t)
	o := NewOutbox(db)
	appendOutboxMessage(t, o, "ev-backoff", "t-backoff")

	if err := o.Dispatch(ctx, failingBus{}); err != nil {
		t.Fatalf("first dispatch: %v", err)
	}
	var retries int
	if err := db.QueryRowContext(ctx,
		`SELECT retries FROM outbox_events WHERE event_id = 'ev-backoff'`).Scan(&retries); err != nil {
		t.Fatalf("select retries: %v", err)
	}
	if retries != 1 {
		t.Fatalf("retries = %d after one failure, want 1", retries)
	}

	// Still inside the backoff window: the second pass must leave it alone.
	if err := o.Dispatch(ctx, failingBus{}); err != nil {
		t.Fatalf("second dispatch: %v", err)
	}
	if err := db.QueryRowContext(ctx,
		`SELECT retries FROM outbox_events WHERE event_id = 'ev-backoff'`).Scan(&retries); err != nil {
		t.Fatalf("select retries: %v", err)
	}
	if retries != 1 {
		t.Errorf("retries = %d after a pass inside the backoff window, want 1", retries)
	}

	// Backdating created_at moves the event past its schedule.
	if _, err := db.ExecContext(ctx,
		`UPDATE outbox_events SET created_at = NOW() - INTERVAL 1 HOUR WHERE event_id = 'ev-backoff'`); err != nil {
		t.Fatalf("backdate: %v", err)
	}
	if err := o.Dispatch(ctx, failingBus{}); err != nil {
		t.Fatalf("third dispatch: %v", err)
	}
	if err := db.QueryRowContext(ctx,
		`SELECT retries FROM outbox_events WHERE event_id = 'ev-backoff'`).Scan(&retries); err != nil {
		t.Fatalf("select retries: %v", err)
	}
	if retries != 2 {
		t.Errorf("retries = %d after the schedule elapsed, want 2", retries)
	}
}

// TestOutboxDeliversOnceTransportRecovers keeps the happy path honest: the
// retry machinery must not cost a deliverable reply.
func TestOutboxDeliversOnceTransportRecovers(t *testing.T) {
	ctx := context.Background()
	db := newOutboxDB(t)
	o := NewOutbox(db)
	appendOutboxMessage(t, o, "ev-recovers", "t-recover")

	if err := o.Dispatch(ctx, failingBus{}); err != nil {
		t.Fatalf("failing dispatch: %v", err)
	}
	if _, err := db.ExecContext(ctx,
		`UPDATE outbox_events SET created_at = NOW() - INTERVAL 1 HOUR WHERE event_id = 'ev-recovers'`); err != nil {
		t.Fatalf("backdate: %v", err)
	}
	good := &okBus{}
	if err := o.Dispatch(ctx, good); err != nil {
		t.Fatalf("recovering dispatch: %v", err)
	}
	if len(good.published) != 1 || good.published[0].ID != "ev-recovers" {
		t.Fatalf("published = %+v, want the retried event", good.published)
	}
	var status string
	if err := db.QueryRowContext(ctx,
		`SELECT status FROM outbox_events WHERE event_id = 'ev-recovers'`).Scan(&status); err != nil {
		t.Fatalf("select status: %v", err)
	}
	if status != "sent" {
		t.Errorf("status = %q, want sent", status)
	}
}

// TestOutboxParksPoisonPayload covers the other terminal case: a payload that
// can never be decoded is parked as dead instead of being marked 'sent', which
// used to hide the loss.
func TestOutboxParksPoisonPayload(t *testing.T) {
	ctx := context.Background()
	db := newOutboxDB(t)
	o := NewOutbox(db)
	if _, err := db.ExecContext(ctx,
		`INSERT INTO outbox_events (event_id, tenant_id, topic, payload) VALUES ('ev-poison', 't-p', 'stream:outbound', '"not-an-object"')`); err != nil {
		t.Fatalf("insert poison: %v", err)
	}
	if err := o.Dispatch(ctx, &okBus{}); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	var status, lastError string
	if err := db.QueryRowContext(ctx,
		`SELECT status, last_error FROM outbox_events WHERE event_id = 'ev-poison'`).Scan(&status, &lastError); err != nil {
		t.Fatalf("select poison: %v", err)
	}
	if status != "dead" {
		t.Errorf("status = %q, want dead", status)
	}
	if lastError == "" {
		t.Error("last_error must explain the malformed payload")
	}
}

// TestOutboxRunSurvivesATransientDispatchError is the availability guard: the
// dispatcher loop is the only thing draining the outbox, so it must log and
// retry a failed pass instead of returning (a single blip used to stop IM
// delivery until the process was restarted).
func TestOutboxRunSurvivesATransientDispatchError(t *testing.T) {
	db := newOutboxDB(t)
	o := NewOutbox(db)
	appendOutboxMessage(t, o, "ev-run", "t-run")

	// Closing the database makes every pass fail without waiting for a real
	// outage; the loop must keep going until the context is cancelled.
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- o.Run(ctx, &okBus{}, 10*time.Millisecond) }()

	select {
	case err := <-done:
		t.Fatalf("Run returned early with %v, want it to keep retrying", err)
	case <-time.After(300 * time.Millisecond):
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run returned %v on shutdown, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop on context cancellation")
	}
}

// TestOutboxDispatchStopsWhenNothingIsDue guards the drain loop: a page full of
// events waiting out their backoff must end the pass instead of spinning.
func TestOutboxDispatchStopsWhenNothingIsDue(t *testing.T) {
	ctx := context.Background()
	db := newOutboxDB(t)
	o := NewOutbox(db)
	appendOutboxMessage(t, o, "ev-spin", "t-spin")
	if err := o.Dispatch(ctx, failingBus{}); err != nil {
		t.Fatalf("first dispatch: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- o.Dispatch(ctx, failingBus{}) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("second dispatch: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Dispatch spun on an event waiting out its backoff")
	}
}
