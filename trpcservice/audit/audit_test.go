package audit

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	servicelog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type retentionPolicies struct{ policies []TenantPolicy }

func (source retentionPolicies) ListAuditPolicies(context.Context) ([]TenantPolicy, error) {
	return append([]TenantPolicy(nil), source.policies...), nil
}

func TestAuditTenantScopeAndSecretRedaction(t *testing.T) {
	const canary = "audit-canary"
	store := NewMemoryStore(servicelog.NewRedactor(nil, []string{canary}))
	for _, tenantID := range []string{"a", "b"} {
		if err := store.Append(context.Background(), Record{TenantID: tenantID, Decision: "allow", TraceID: "trace", ErrorType: "token=" + canary, Details: map[string]any{"authorization": canary, "message": canary}}); err != nil {
			t.Fatal(err)
		}
	}
	records := store.Records("a")
	if len(records) != 1 || records[0].TenantID != "a" {
		t.Fatalf("records=%+v", records)
	}
	payload, _ := json.Marshal(records)
	if strings.Contains(string(payload), canary) {
		t.Fatalf("audit leaked secret: %s", payload)
	}
}

func TestAuditPreservesConfigAndPolicyVersions(t *testing.T) {
	store := NewMemoryStore(nil)
	record := Record{TenantID: "tenant", Decision: "allow", TraceID: "trace", ConfigVersion: 7, PolicyVersion: 7}
	if err := store.Append(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	records := store.Records("tenant")
	if len(records) != 1 || records[0].ConfigVersion != 7 || records[0].PolicyVersion != 7 {
		t.Fatalf("audit versions=%+v", records)
	}
}

func TestAuditRetentionIsTenantScoped(t *testing.T) {
	store := NewMemoryStore(nil)
	now := time.Now().UTC()
	for _, record := range []Record{
		{TenantID: "a", Decision: "allow", TraceID: "old-a", CreatedAt: now.Add(-48 * time.Hour)},
		{TenantID: "a", Decision: "allow", TraceID: "new-a", CreatedAt: now},
		{TenantID: "b", Decision: "allow", TraceID: "old-b", CreatedAt: now.Add(-48 * time.Hour)},
	} {
		if err := store.Append(context.Background(), record); err != nil {
			t.Fatal(err)
		}
	}
	deleted, err := PruneTenant(context.Background(), store, "a", tenant.AuditPolicy{RetentionDays: 1}, now)
	if err != nil || deleted != 1 || len(store.Records("a")) != 1 || len(store.Records("b")) != 1 {
		t.Fatalf("deleted=%d err=%v a=%v b=%v", deleted, err, store.Records("a"), store.Records("b"))
	}
}

func TestRetentionWorkerPrunesOnlyExpiredTenantRows(t *testing.T) {
	now := time.Date(2026, 8, 31, 0, 0, 0, 0, time.UTC)
	store := NewMemoryStore(nil)
	for _, record := range []Record{
		{TenantID: "a", Decision: "allow", TraceID: "old", CreatedAt: now.Add(-48 * time.Hour)},
		{TenantID: "a", Decision: "allow", TraceID: "new", CreatedAt: now.Add(-time.Hour)},
		{TenantID: "b", Decision: "allow", TraceID: "keep", CreatedAt: now.Add(-48 * time.Hour)},
	} {
		if err := store.Append(context.Background(), record); err != nil {
			t.Fatal(err)
		}
	}
	result := make(chan error, 1)
	var once sync.Once
	worker := &RetentionWorker{
		Store: store,
		Policies: retentionPolicies{policies: []TenantPolicy{
			{TenantID: "a", Policy: tenant.AuditPolicy{RetentionDays: 1}},
			{TenantID: "b", Policy: tenant.AuditPolicy{RetentionDays: 0}},
		}},
		Interval: time.Hour,
		Now:      func() time.Time { return now },
		OnResult: func(deleted int64, err error) {
			once.Do(func() {
				if err == nil && deleted != 1 {
					err = errors.New("unexpected deleted count")
				}
				result <- err
			})
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := worker.Start(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("retention cycle did not run")
	}
	cancel()
	if err := worker.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(store.Records("a")) != 1 || len(store.Records("b")) != 1 {
		t.Fatalf("records a=%d b=%d", len(store.Records("a")), len(store.Records("b")))
	}
}
