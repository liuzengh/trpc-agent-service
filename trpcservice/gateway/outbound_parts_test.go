package gateway

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/database"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
)

func TestMemoryOutboundPartsContract(t *testing.T) {
	j := NewMemoryJournal()
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(j)
	testParts(t, j)
}
func TestPostgresOutboundPartsContract(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_URL")
	if dsn == "" {
		t.Skip("isolated database not configured")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(db)
	if err := database.Migrate(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	if err := controlplane.SeedBootstrap(context.Background(), db, controlplane.DefaultBootstrapData()); err != nil {
		t.Fatal(err)
	}
	j, _ := NewPostgresJournal(db)
	testParts(t, j)
}
func testParts(t *testing.T, j Journal) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	suffix := fmt.Sprintf("parts-%x", time.Now().UnixNano())
	accepted, err := j.Accept(ctx, InboundRequest{Scope: runtimecontext.TutorialScope(), ExternalMessageID: suffix, UserID: suffix, SessionID: suffix, ChatType: "direct", Text: "test", DirectReply: "reply"})
	if err != nil {
		t.Fatal(err)
	}
	_ = accepted
	items, err := j.ClaimOutbound(ctx, "part-owner", 100, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var item OutboundItem
	for _, candidate := range items {
		if candidate.RequestID == accepted.RequestID {
			item = candidate
		}
	}
	if item.ID == "" {
		t.Fatal("outbound not claimed")
	}
	hash := strings.Repeat("a", 64)
	p, owner, err := j.BeginPart(ctx, item, "part-owner", 0, 2, hash)
	if err != nil || !owner {
		t.Fatal(err)
	}
	if _, owner, err := j.BeginPart(ctx, item, "part-owner", 0, 2, hash); err != nil || owner {
		t.Fatal("duplicate part got ownership")
	}
	if err := j.FinishPart(ctx, p, "sent", "first"); err != nil {
		t.Fatal(err)
	}
	p2, owner, err := j.BeginPart(ctx, item, "part-owner", 1, 2, hash)
	if err != nil || !owner {
		t.Fatal(err)
	}
	if err := j.FinishPart(ctx, p2, "unknown", ""); err != nil {
		t.Fatal(err)
	}
	if err := j.MarkOutboundFailed(ctx, item.ID, "part-owner", time.Now(), true, nil, item.AttemptCount); err != nil {
		t.Fatal(err)
	}
	if err := j.ReconcilePart(ctx, p2, "sent", "second", hash, "test-operator", ""); err != nil {
		t.Fatal(err)
	}
	parts, err := j.ListParts(ctx, item.TenantID, item.ID)
	if err != nil || len(parts) != 2 || parts[0].Status != "sent" || parts[1].Status != "sent" {
		t.Fatalf("parts=%+v err=%v", parts, err)
	}
	if err := j.FinishPart(ctx, p2, "pending", ""); err == nil {
		t.Fatal("late completion undid reconciliation")
	}
	if err := j.ReconcilePart(ctx, OutboundPart{TenantID: "other", OutboundID: item.ID, Index: 1, Owner: p2.Owner}, "not_sent", "", hash, "operator", ""); err == nil {
		t.Fatal("cross-tenant reconciliation accepted")
	}
}
