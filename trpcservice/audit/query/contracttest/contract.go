// Package contracttest holds the shared audit query store contract, exercised
// by both the in-memory and compliance (PostgreSQL) implementations.
package contracttest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/audit/query"
)

func event(tenant, id string, at time.Time) audit.Event {
	return audit.Event{SchemaVersion: 1, AuditID: id, TenantID: tenant, Action: "test.action",
		Decision: "recorded", OccurredAt: at}
}

type seeder func(...audit.Event)

// Suite runs the shared contract against any query.Store.
func Suite(t *testing.T, store query.Store, seed seeder) {
	t.Helper()
	ctx := context.Background()
	base := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)

	t.Run("tenant scope and time range", func(t *testing.T) {
		seed(
			event("qa-tenant-a", "a1", base),
			event("qa-tenant-a", "a2", base.Add(time.Minute)),
			event("qa-tenant-b", "b1", base.Add(time.Minute)),
			event("qa-tenant-a", "a3", base.Add(2*time.Hour)),
		)
		page, err := store.Query(ctx, query.Filter{TenantID: "qa-tenant-a", From: base.Add(-time.Hour),
			To: base.Add(90 * time.Minute), PageSize: 10})
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Events) != 2 {
			t.Fatalf("events=%d, want 2", len(page.Events))
		}
		for _, e := range page.Events {
			if e.TenantID != "qa-tenant-a" {
				t.Fatalf("leaked tenant %q", e.TenantID)
			}
		}
	})

	t.Run("pagination resumes without overlap", func(t *testing.T) {
		for i := 0; i < 5; i++ {
			seed(event("qa-tenant-c", fmt.Sprintf("c%d", i), base.Add(time.Duration(i)*time.Minute)))
		}
		first, err := store.Query(ctx, query.Filter{TenantID: "qa-tenant-c", From: base.Add(-time.Hour),
			To: base.Add(time.Hour), PageSize: 2})
		if err != nil {
			t.Fatal(err)
		}
		if len(first.Events) != 2 || first.NextCursor == "" {
			t.Fatalf("first page events=%d cursor=%q", len(first.Events), first.NextCursor)
		}
		second, err := store.Query(ctx, query.Filter{TenantID: "qa-tenant-c", From: base.Add(-time.Hour),
			To: base.Add(time.Hour), PageSize: 2, Cursor: first.NextCursor})
		if err != nil {
			t.Fatal(err)
		}
		if len(second.Events) == 0 {
			t.Fatal("second page is empty")
		}
		seen := map[string]bool{}
		for _, e := range append(first.Events, second.Events...) {
			if seen[e.AuditID] {
				t.Fatalf("duplicate event %q across pages", e.AuditID)
			}
			seen[e.AuditID] = true
		}
	})

	t.Run("cross tenant pagination has total ordering", func(t *testing.T) {
		at := base.Add(10 * time.Hour)
		seed(event("qa-cross-a", "same-id", at), event("qa-cross-b", "same-id", at))
		filter := query.Filter{CrossTenant: true, From: at.Add(-time.Minute), To: at.Add(time.Minute), PageSize: 1}
		first, err := store.Query(ctx, filter)
		if err != nil {
			t.Fatal(err)
		}
		if len(first.Events) != 1 || first.NextCursor == "" {
			t.Fatalf("first page=%+v", first)
		}
		filter.Cursor = first.NextCursor
		second, err := store.Query(ctx, filter)
		if err != nil {
			t.Fatal(err)
		}
		if len(second.Events) != 1 || second.Events[0].TenantID == first.Events[0].TenantID {
			t.Fatalf("cross-tenant cursor skipped or duplicated row: first=%+v second=%+v", first, second)
		}
	})

	t.Run("records every access", func(t *testing.T) {
		if err := store.RecordAccess(ctx, query.QueryRecord{QueryID: "q1", TenantID: "qa-tenant-a",
			Subject: "op", Decision: "allowed", FilterDigest: "f", ResultDigest: "r"}); err != nil {
			t.Fatal(err)
		}
		if err := store.RecordAccess(ctx, query.QueryRecord{QueryID: "q2", TenantID: "qa-tenant-a",
			Subject: "op", Decision: "denied", FilterDigest: "f", ResultDigest: "r"}); err != nil {
			t.Fatal(err)
		}
		if reader, ok := store.(interface{ Records() []query.QueryRecord }); ok {
			records := reader.Records()
			if len(records) != 2 || records[0].Decision != "allowed" || records[1].Decision != "denied" {
				t.Fatalf("records=%+v", records)
			}
		}
	})
}

func TestValidateFilter(t *testing.T) {
	base := time.Now().UTC()
	if err := query.ValidateFilter(query.Filter{TenantID: "t", From: base, To: base.Add(time.Hour), PageSize: 10}, 31*24*time.Hour, 200); err != nil {
		t.Fatalf("valid filter rejected: %v", err)
	}
	bad := []query.Filter{
		{TenantID: "t", From: base, To: base, PageSize: 10},                          // empty window
		{TenantID: "t", From: base, To: base.Add(40 * 24 * time.Hour), PageSize: 10}, // window too wide
		{TenantID: "t", From: base, To: base.Add(time.Hour), PageSize: 201},          // page too large
		{From: base, To: base.Add(time.Hour), PageSize: 10},                          // missing tenant, not cross
	}
	for _, f := range bad {
		if err := query.ValidateFilter(f, 31*24*time.Hour, 200); err == nil {
			t.Fatalf("invalid filter accepted: %+v", f)
		}
	}
}
