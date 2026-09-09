//go:build integration

package integration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/reliable"
)

func TestPostgresFairClockNewTenantDoesNotCatchUpHistoricalService(t *testing.T) {
	_, store, tenants := newFairClockFixture(t)
	enqueueFairClockMessages(t, store, tenants[0], 0, 20)
	for range 20 {
		completeFairClockClaim(t, store)
	}
	enqueueFairClockMessages(t, store, tenants[0], 20, 20)
	enqueueFairClockMessages(t, store, tenants[1], 0, 20)
	assertEqualFairClockShare(t, store, tenants[0], tenants[1])
}

func TestPostgresFairClockIdleTenantResumesWithoutAccumulatedCredit(t *testing.T) {
	db, store, tenants := newFairClockFixture(t)
	enqueueFairClockMessages(t, store, tenants[1], 0, 1)
	completeFairClockClaim(t, store)
	enqueueFairClockMessages(t, store, tenants[0], 0, 20)
	for range 20 {
		completeFairClockClaim(t, store)
	}
	store = reliable.NewPostgresStore(db)
	enqueueFairClockMessages(t, store, tenants[0], 20, 20)
	enqueueFairClockMessages(t, store, tenants[1], 1, 20)
	assertEqualFairClockShare(t, store, tenants[0], tenants[1])
}

func TestPostgresFairClockPreservesWeightedShare(t *testing.T) {
	_, store, tenants := newFairClockFixture(t)
	ctx := context.Background()
	if err := store.UpsertTenantQueuePolicy(ctx, reliable.TenantQueuePolicy{TenantID: tenants[0], Weight: 4}); err != nil {
		t.Fatal(err)
	}
	enqueueFairClockMessages(t, store, tenants[0], 0, 100)
	enqueueFairClockMessages(t, store, tenants[1], 0, 100)
	counts := make(map[string]int)
	for range 100 {
		counts[completeFairClockClaim(t, store).TenantID]++
	}
	if counts[tenants[0]] != 80 || counts[tenants[1]] != 20 {
		t.Fatalf("weighted service=%v, want 80:20", counts)
	}
}

func TestPostgresFairClockPolicyResetPreservesServiceDebt(t *testing.T) {
	_, store, tenants := newFairClockFixture(t)
	enqueueFairClockMessages(t, store, tenants[0], 0, 3)
	enqueueFairClockMessages(t, store, tenants[1], 0, 3)
	first := completeFairClockClaim(t, store)
	if err := store.DeleteTenantQueuePolicy(context.Background(), first.TenantID); err != nil {
		t.Fatal(err)
	}
	next := completeFairClockClaim(t, store)
	if next.TenantID == first.TenantID {
		t.Fatalf("policy reset let %s bypass an unserved equal-weight tenant", first.TenantID)
	}
}

func TestPostgresFairClockConcurrentClaimsKeepTenantInflightBound(t *testing.T) {
	db, store, tenants := newFairClockFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for _, tenantID := range tenants[:2] {
		if err := store.UpsertTenantQueuePolicy(ctx, reliable.TenantQueuePolicy{TenantID: tenantID, Weight: 1, MaxInflight: 2}); err != nil {
			t.Fatal(err)
		}
		enqueueFairClockMessages(t, store, tenantID, 0, 16)
	}
	const callers = 16
	start := make(chan struct{})
	claims := make(chan *reliable.InboxMessage, callers)
	errorsSeen := make(chan error, callers)
	var wg sync.WaitGroup
	for i := range callers {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			<-start
			// Separate Store instances use independent database transactions.
			claim, err := reliable.NewPostgresStore(db).ClaimInboxFair(ctx, fmt.Sprintf("consumer-%d", index), time.Minute)
			if errors.Is(err, reliable.ErrNoWork) {
				return
			}
			if err != nil {
				errorsSeen <- err
				return
			}
			claims <- claim
		}(i)
	}
	close(start)
	wg.Wait()
	close(claims)
	close(errorsSeen)
	for err := range errorsSeen {
		t.Errorf("concurrent fair claim: %v", err)
	}
	counts := make(map[string]int)
	seen := make(map[int64]bool)
	for claim := range claims {
		if seen[claim.ID] {
			t.Errorf("Inbox %d claimed twice under concurrent owners", claim.ID)
		}
		seen[claim.ID] = true
		counts[claim.TenantID]++
		stale := claim.Lease
		stale.Fence--
		if _, err := store.CompleteInbox(ctx, claim.ID, stale, reliable.OutboxReply{Content: "stale"}); !errors.Is(err, reliable.ErrStaleLease) {
			t.Errorf("stale completion error=%v, want ErrStaleLease", err)
		}
		if _, err := store.CompleteInbox(ctx, claim.ID, claim.Lease, reliable.OutboxReply{Content: "reply"}); err != nil {
			t.Error(err)
		}
	}
	if counts[tenants[0]] != 2 || counts[tenants[1]] != 2 {
		t.Fatalf("concurrent inflight claims=%v, want exactly 2 per active tenant", counts)
	}
}

func newFairClockFixture(t *testing.T) (*sql.DB, *reliable.PostgresStore, []string) {
	t.Helper()
	db := openDatabase(t)
	store := reliable.NewPostgresStore(db)
	if err := store.CheckFairInboxReady(context.Background()); err != nil {
		t.Fatal(err)
	}
	prefix := "fair-" + uuid.NewString()
	tenants := []string{prefix + "-a", prefix + "-b", prefix + "-idle"}
	for _, tenantID := range tenants {
		if _, err := db.Exec(`INSERT INTO tenants(id,name,status,config) VALUES($1,'fair-clock','active','{}')`, tenantID); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_, _ = db.Exec(`DELETE FROM outbox_messages WHERE tenant_id=$1`, tenantID)
			_, _ = db.Exec(`DELETE FROM inbox_messages WHERE tenant_id=$1`, tenantID)
			_, _ = db.Exec(`DELETE FROM tenants WHERE id=$1`, tenantID)
		})
		if err := store.UpsertTenantQueuePolicy(context.Background(), reliable.TenantQueuePolicy{TenantID: tenantID, Weight: 1}); err != nil {
			t.Fatal(err)
		}
	}
	return db, store, tenants
}

func enqueueFairClockMessages(t *testing.T, store reliable.Store, tenantID string, start, count int) {
	t.Helper()
	for i := start; i < start+count; i++ {
		message := &reliable.InboxMessage{
			TenantID: tenantID, ChannelType: "telegram", ChannelAccountID: "bot",
			AgentApp: "support", ExternalMessageID: fmt.Sprintf("message-%d", i),
			ConversationID: "chat", ReplyToID: "reply", UserID: "user",
			SessionID: fmt.Sprintf("session-%d", i), PayloadHash: strings.Repeat("a", 64),
			Payload: []byte(`{"content":"hello"}`),
		}
		if inserted, err := store.EnqueueInbox(context.Background(), message); err != nil || !inserted {
			t.Fatalf("enqueue inserted=%v error=%v", inserted, err)
		}
	}
}

func completeFairClockClaim(t *testing.T, store *reliable.PostgresStore) *reliable.InboxMessage {
	t.Helper()
	ctx := context.Background()
	claim, err := store.ClaimInboxFair(ctx, "consumer", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CompleteInbox(ctx, claim.ID, claim.Lease, reliable.OutboxReply{Content: "reply"}); err != nil {
		t.Fatal(err)
	}
	return claim
}

func assertEqualFairClockShare(t *testing.T, store *reliable.PostgresStore, first, second string) {
	t.Helper()
	counts := make(map[string]int)
	for range 10 {
		counts[completeFairClockClaim(t, store).TenantID]++
	}
	if counts[first] != 5 || counts[second] != 5 {
		t.Fatalf("active equal-weight tenants received %v claims, want 5 each", counts)
	}
}
