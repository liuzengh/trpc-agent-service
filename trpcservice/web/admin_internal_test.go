package web

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/testenv"
)

// An audit op with an empty tenant id falls back to the zero uuid instead of
// failing the insert, and a nil before/after yields a nil detail.
func TestAfterWriteEmptyTenantAudit(t *testing.T) {
	pool := testenv.PG(t)
	a := &AdminAPI{pool: pool, auditor: storage.NewAuditor(pool)}

	op := "internal_test_op"
	req := httptest.NewRequest("POST", "/admin/internal", nil)
	a.afterWrite(req, op, "", nil, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var n int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM audit_log
		 WHERE tenant_id = '00000000-0000-0000-0000-000000000000' AND tool_name = $1`,
		op).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("empty tenant id must audit under the zero uuid, got %d rows", n)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(),
			`DELETE FROM audit_log WHERE tenant_id = '00000000-0000-0000-0000-000000000000' AND tool_name = $1`, op)
	})
}

// changeDetail drops nil sides; with both nil the detail is nil. A row image
// that is not JSON yields an empty tenant id instead of an error.
func TestChangeDetailAndTenantIDOf(t *testing.T) {
	if got := changeDetail(nil, nil); len(got) != 0 {
		t.Fatalf("changeDetail(nil, nil) = %s, want nil", got)
	}
	before := changeDetail(nil, map[string]any{"k": "v"})
	var d struct {
		After map[string]any `json:"after"`
	}
	if err := json.Unmarshal(before, &d); err != nil || d.After["k"] != "v" {
		t.Fatalf("after-only detail malformed: %s (%v)", before, err)
	}

	if got := tenantIDOf(json.RawMessage(`{not-json`)); got != "" {
		t.Fatalf("tenantIDOf over garbage = %q, want empty", got)
	}
	if got := tenantIDOf(json.RawMessage(`{"tenant_id":"t9"}`)); got != "t9" {
		t.Fatalf("tenantIDOf = %q, want t9", got)
	}
}
