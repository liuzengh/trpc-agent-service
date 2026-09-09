//go:build integration

package integration

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lib/pq"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/reliable"
)

func TestPostgresFairClaimCannotOverbookDuringUncommittedRenewal(t *testing.T) {
	db, store, tenants := newFairClockFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	tenantID := tenants[0]
	if err := store.UpsertTenantQueuePolicy(ctx, reliable.TenantQueuePolicy{TenantID: tenantID, Weight: 1, MaxInflight: 1}); err != nil {
		t.Fatal(err)
	}
	for i := range 2 {
		message := &reliable.InboxMessage{
			TenantID: tenantID, ChannelType: "telegram", ChannelAccountID: "bot",
			AgentApp: "support", ExternalMessageID: fmt.Sprintf("message-%d", i),
			ConversationID: "chat", ReplyToID: "reply", UserID: "user",
			SessionID: fmt.Sprintf("session-%d", i), PayloadHash: strings.Repeat("a", 64),
			Payload: []byte(`{"content":"hello"}`), MaxAttempts: 1,
		}
		if inserted, err := store.EnqueueInbox(ctx, message); err != nil || !inserted {
			t.Fatalf("enqueue inserted=%v error=%v", inserted, err)
		}
	}
	first, err := store.ClaimInboxFair(ctx, "first-worker", 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	connector, err := pq.NewConnector(os.Getenv("TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	gate := &renewalCommitGate{ctx: ctx, entered: make(chan struct{}), release: make(chan struct{})}
	renewDB := sql.OpenDB(&renewalGateConnector{Connector: connector, gate: gate})
	t.Cleanup(func() { renewDB.Close() })
	type renewalResult struct {
		lease reliable.Lease
		err   error
	}
	result := make(chan renewalResult, 1)
	done := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(gate.release) }) }
	go func() {
		defer close(done)
		lease, err := reliable.NewPostgresStore(renewDB).RenewInbox(ctx, first.ID, first.Lease, 30*time.Second)
		result <- renewalResult{lease: lease, err: err}
	}()
	t.Cleanup(func() {
		release()
		cancel()
		<-done
	})
	select {
	case <-gate.entered:
	case early := <-result:
		t.Fatalf("renewal failed before delayed commit: %v", early.err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// The real RenewInbox UPDATE is now uncommitted. Its old committed lease
	// expires while another session is eligible; exhausted A is not a candidate.
	for {
		var expired bool
		if err := db.QueryRowContext(ctx, `SELECT clock_timestamp() > $1`, first.Lease.Until).Scan(&expired); err != nil {
			t.Fatal(err)
		}
		if expired {
			break
		}
		select {
		case <-time.After(10 * time.Millisecond):
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if claim, err := store.ClaimInboxFair(ctx, "competing-worker", time.Minute); !errors.Is(err, reliable.ErrNoWork) || claim != nil {
		t.Fatalf("claim during uncommitted renewal=%+v error=%v, want no work", claim, err)
	}
	release()
	var renewed renewalResult
	select {
	case renewed = <-result:
		if renewed.err != nil {
			t.Fatal(renewed.err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if claim, err := store.ClaimInboxFair(ctx, "competing-worker", time.Minute); !errors.Is(err, reliable.ErrNoWork) || claim != nil {
		t.Fatalf("claim after renewal=%+v error=%v, want no work", claim, err)
	}
	if _, err := store.CompleteInbox(ctx, first.ID, renewed.lease, reliable.OutboxReply{Content: "reply"}); err != nil {
		t.Fatal(err)
	}
	next, err := store.ClaimInboxFair(ctx, "competing-worker", time.Minute)
	if err != nil || next == nil || next.ID == first.ID {
		t.Fatalf("claim after completion=%+v error=%v, want next session", next, err)
	}
}

// Only the database driver's commit boundary is delayed. All renewal and
// admission statements still execute through their production Store methods.
type renewalCommitGate struct {
	ctx     context.Context
	entered chan struct{}
	release chan struct{}
}

type renewalGateConnector struct {
	driver.Connector
	gate *renewalCommitGate
}

func (c *renewalGateConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &renewalGateConn{Conn: conn, gate: c.gate}, nil
}

type renewalGateConn struct {
	driver.Conn
	gate *renewalCommitGate
}

func (c *renewalGateConn) Begin() (driver.Tx, error) {
	tx, err := c.Conn.Begin()
	if err != nil {
		return nil, err
	}
	return &renewalGateTx{Tx: tx, gate: c.gate}, nil
}

type renewalGateTx struct {
	driver.Tx
	gate *renewalCommitGate
}

func (tx *renewalGateTx) Commit() error {
	close(tx.gate.entered)
	select {
	case <-tx.gate.release:
		return tx.Tx.Commit()
	case <-tx.gate.ctx.Done():
		_ = tx.Tx.Rollback()
		return tx.gate.ctx.Err()
	}
}
