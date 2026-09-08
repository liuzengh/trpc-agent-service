//go:build integration

// Regression for the usage-metering row-loss bug: the admin /chat path stamps
// inbound messages with uuid.NewString() (36 chars), and record_id is
// VARCHAR(36). The old "messageID:dimension" RecordID scheme overflowed the
// column, truncated back to the bare message id, and collided with the
// :token row — so the INSERT IGNORE silently dropped the tool / skill /
// sandbox rows while token survived. This test drives the real worker
// metering path (buildUsageEntries + recordUsage) against real MySQL with a
// uuid message id and asserts every positive dimension lands exactly once.
package worker

import (
	"context"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/bus"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/storage"
)

func TestUsageDimensionsSurviveUUIDMessageID(t *testing.T) {
	once.Do(startContainers)
	if platform.err != nil {
		t.Skip(platform.err)
	}
	ctx := context.Background()

	db, err := storage.OpenMySQL(platform.dsn)
	if err != nil {
		t.Fatalf("open mysql: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	rec := audit.NewMySQL(db)
	t.Cleanup(func() { _ = rec.Close() })

	w := &Worker{auditor: rec}
	msgID := "550e8400-e29b-41d4-a716-446655440000" // 36 chars, as uuid.NewString produces
	if len(msgID) != 36 {
		t.Fatalf("message id must be 36 chars, got %d", len(msgID))
	}
	m := &bus.Message{ID: msgID, TenantID: "t-uuid", AgentID: "a-1", SessionID: "s-1"}

	entries := buildUsageEntries(m, "a-1", 150,
		map[string]int{"echo": 2, "execute_code": 1}, // tool x2 + sandbox x1
		[]skillUsageRef{{SkillID: "sk-1", Code: "triage", Name: "Triage", Version: 1}}, 0) // skill injected

	// Two deliveries must be idempotent (INSERT IGNORE on record_id).
	for i := 0; i < 2; i++ {
		w.recordUsage(ctx, m, "a-1", entries)
	}

	rows, err := rec.UsageRows(ctx, audit.UsageQuery{TenantID: "t-uuid", Limit: 10})
	if err != nil {
		t.Fatalf("UsageRows: %v", err)
	}
	// token + tool + sandbox + skill all present, each exactly once.
	counts := map[string]int{}
	for _, r := range rows {
		counts[r.Dimension]++
		if r.AgentID != "a-1" {
			t.Errorf("row agent = %q, want a-1", r.AgentID)
		}
	}
	for dim, want := range map[string]int{
		audit.UsageDimensionToken:   1,
		audit.UsageDimensionTool:    1,
		audit.UsageDimensionSandbox: 1,
		audit.UsageDimensionSkill:   1,
	} {
		if counts[dim] != want {
			t.Errorf("usage_records rows for dimension %q = %d, want %d (rows=%v); "+
				"this is the truncation/collision regression: a record_id >36 chars "+
				"silently dropped this dimension", dim, counts[dim], want, counts)
		}
	}
}
