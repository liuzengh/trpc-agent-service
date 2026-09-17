package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

func TestPostgreSQLGovernanceContracts(t *testing.T) {
	if os.Getenv("TRPC_MIGRATION_TEST") != "1" {
		t.Skip("TRPC_MIGRATION_TEST=1 is required")
	}
	dsn := os.Getenv("TRPC_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("TRPC_POSTGRES_TEST_DSN is required")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	tenantID := "t_01ARZ3NDEKTSV4RRFFQ69G5FAT"
	_, err = db.ExecContext(ctx, `INSERT INTO tenant(tenant_id,tenant_key,display_name,monthly_token_budget,monthly_cost_budget_micros) VALUES($1,'governance-contract','Governance Contract',100,100) ON CONFLICT (tenant_id) DO NOTHING`, tenantID)
	if err != nil {
		t.Fatal(err)
	}
	store := New(db)
	now := time.Now().UTC()
	pricingValue := governance.PricingV1{SchemaVersion: 1, Currency: "USD", ConversionRef: "fx", ValidFrom: now.Add(-time.Hour), ValidUntil: now.Add(time.Hour), Prices: []governance.Price{{Provider: "deepseek", Model: "chat", ModelProfileID: "model", ModelProfileVersion: 1, InputMicrosPerMillion: 1, OutputMicrosPerMillion: 1}}}
	pricingDigest, _, _ := governance.PricingDigest(pricingValue)
	if err := store.PublishPricing(ctx, governance.PricingSnapshot{TenantID: tenantID, Version: 1, SchemaVersion: 1, Pricing: pricingValue, ContentDigest: pricingDigest, PublishedAt: now}); err != nil {
		t.Fatal(err)
	}
	policyValue := governance.PolicyV1{SchemaVersion: 1, DefaultAction: governance.ActionAllow, AllowedModels: []governance.VersionedRef{{ID: "model", Version: 1}}, InputDLP: governance.DLPDisabled, OutputDLP: governance.DLPDisabled, Budget: governance.BudgetPolicy{MaxInputTokens: 5, MaxOutputTokens: 5, MaxCostMicrosPerRun: 10}, PricingVersion: 1}
	policyDigest, _, _ := governance.PolicyDigest(policyValue)
	if err := store.PublishPolicy(ctx, governance.PolicySnapshot{TenantID: tenantID, Version: 1, SchemaVersion: 1, Policy: policyValue, ContentDigest: policyDigest, PublishedAt: now}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE policy_snapshot SET content_digest=repeat('a',64) WHERE tenant_id=$1 AND policy_version=1`, tenantID); err == nil {
		t.Fatal("published policy was mutable")
	}
	var allowed atomic.Int64
	winners := make(chan governance.Reservation, 100)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			reservation, reserveErr := store.Reserve(ctx, governance.ReserveRequest{TenantID: tenantID, RequestID: fmt.Sprintf("budget-%03d", index), ResourceID: "model", AttemptClass: "model", PolicyVersion: 1, PricingVersion: 1, MaxCostMicros: 10, MaxTokens: 10})
			if reserveErr == nil {
				allowed.Add(1)
				winners <- reservation
			}
		}(i)
	}
	wg.Wait()
	if allowed.Load() != 10 {
		t.Fatalf("concurrent reservations=%d", allowed.Load())
	}
	stable := <-winners
	settled, err := store.Settle(ctx, governance.SettleRequest{TenantID: tenantID, ReservationID: stable.ReservationID, RequestID: stable.RequestID, Stage: "model", UsageKind: "tokens", ExpectedVersion: stable.Version, Usage: governance.Usage{InputTokens: 4, OutputTokens: 3}, ActualCostMicros: 7})
	if err != nil {
		t.Fatal(err)
	}
	replay, err := store.Settle(ctx, governance.SettleRequest{TenantID: tenantID, ReservationID: stable.ReservationID, RequestID: stable.RequestID, Stage: "model", UsageKind: "tokens", ExpectedVersion: stable.Version, Usage: governance.Usage{InputTokens: 4, OutputTokens: 3}, ActualCostMicros: 7})
	if err != nil || replay != settled {
		t.Fatalf("settle replay=%#v err=%v", replay, err)
	}
	decision := governance.Decision{DecisionID: governance.StableDecisionID(tenantID, stable.RequestID, "input", 1), TenantID: tenantID, RequestID: stable.RequestID, Stage: "input", Action: governance.ActionAllow, ReasonCode: governance.ReasonAllowed, PolicyVersion: 1, ReservationID: stable.ReservationID}
	if err := store.RecordDecision(ctx, decision); err != nil {
		t.Fatal(err)
	}
	changed := decision
	changed.ReasonCode = governance.ReasonSubjectDenied
	if err := store.RecordDecision(ctx, changed); !errors.Is(err, runtime.ErrIdempotencyCollision) {
		t.Fatalf("decision collision=%v", err)
	}
}
