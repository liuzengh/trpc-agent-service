package worker

import (
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/bus"
)

func msgWithID(id string) *bus.Message {
	return &bus.Message{ID: id, TenantID: "t1", SessionID: "s1", UserID: "u1", Channel: "wecom"}
}

// TestBuildUsageEntriesAllDimensions verifies every usage dimension triggers
// with a real production signal, keyed idempotently on messageID:dimension.
func TestBuildUsageEntriesAllDimensions(t *testing.T) {
	m := msgWithID("m1")
	entries := buildUsageEntries(m, "a1", 0,
		map[string]int{"get_current_time": 2, codeExecToolName: 1},
		[]skillUsageRef{{SkillID: "sk1", Code: "triage", Name: "Triage", Version: 1},
			{SkillID: "sk2", Code: "writer", Name: "Writer", Version: 2}},
		1,
	)
	got := map[string]float64{}
	for _, e := range entries {
		if e.RecordID != usageRecordID("m1", e.Dimension) {
			t.Errorf("RecordID %q should derive deterministically from message+dimension", e.RecordID)
		}
		if len(e.RecordID) != 32 {
			t.Errorf("RecordID %q must be 32 chars to fit the VARCHAR(36) column", e.RecordID)
		}
		got[e.Dimension] = e.Amount
	}
	want := map[string]float64{
		audit.UsageDimensionTool:     3, // 2+1 calls
		audit.UsageDimensionSandbox:  1, // code-exec call
		audit.UsageDimensionArtifact: 1,
		audit.UsageDimensionSkill:    2,
	}
	for dim, w := range want {
		if got[dim] != w {
			t.Errorf("dimension %s amount = %v, want %v", dim, got[dim], w)
		}
	}
	if _, ok := got[audit.UsageDimensionToken]; ok {
		t.Error("token record should be omitted when tokens == 0")
	}
}

// TestBuildUsageEntriesTokenOnly verifies a no-tool turn records just token.
func TestBuildUsageEntriesTokenOnly(t *testing.T) {
	m := msgWithID("m2")
	entries := buildUsageEntries(m, "a1", 120, nil, nil, 0)
	if len(entries) != 1 || entries[0].Dimension != audit.UsageDimensionToken || entries[0].Amount != 120 {
		t.Errorf("entries = %+v, want single token record of 120", entries)
	}
}

// TestBuildUsageEntriesMetaCarriesNames verifies tool/skill names reach meta.
func TestBuildUsageEntriesMetaCarriesNames(t *testing.T) {
	m := msgWithID("m3")
	entries := buildUsageEntries(m, "a1", 10,
		map[string]int{"echo": 1}, []skillUsageRef{{SkillID: "sk9", Code: "triage", Name: "Triage", Version: 1}}, 0)
	var toolEntry, skillEntry *audit.UsageEntry
	for i := range entries {
		switch entries[i].Dimension {
		case audit.UsageDimensionTool:
			toolEntry = &entries[i]
		case audit.UsageDimensionSkill:
			skillEntry = &entries[i]
		}
	}
	if toolEntry == nil || skillEntry == nil {
		t.Fatalf("tool/skill entry missing: %+v", entries)
	}
	if toolEntry.Meta["tools"] == nil || toolEntry.Meta["calls"] == nil {
		t.Error("tool entry should carry tools/calls meta")
	}
	refs, ok := skillEntry.Meta["skills"].([]skillUsageRef)
	if !ok || len(refs) != 1 || refs[0].Code != "triage" || refs[0].Name != "Triage" || refs[0].Version != 1 {
		t.Errorf("skill meta should carry skill snapshots (code/name/version), got %#v", skillEntry.Meta["skills"])
	}
}

// TestUsageRecordIDNoCollisionForUUIDMessage is a regression test for the
// silent drop of tool/skill/sandbox/artifact rows: admin-console message ids
// are 36-char UUIDs, and the old "messageID:dimension" scheme overflowed the
// VARCHAR(36) record_id column, truncated back to the bare id and collided
// with the token record, which INSERT IGNORE then silently discarded.
func TestUsageRecordIDNoCollisionForUUIDMessage(t *testing.T) {
	uuid := "550e8400-e29b-41d4-a716-446655440000" // 36 chars, as uuid.NewString produces
	if len(uuid) != 36 {
		t.Fatalf("test uuid must be 36 chars, got %d", len(uuid))
	}
	token := usageRecordID(uuid, audit.UsageDimensionToken)
	tool := usageRecordID(uuid, audit.UsageDimensionTool)
	skill := usageRecordID(uuid, audit.UsageDimensionSkill)
	if token == tool || token == skill || tool == skill {
		t.Error("dimension record ids collide for a UUID message id")
	}
	for _, id := range []string{token, tool, skill} {
		if len(id) != 32 {
			t.Errorf("record id %q must be 32 chars to fit the column", id)
		}
	}
	// Deterministic: a redelivered turn maps to the same key (idempotency).
	if usageRecordID(uuid, audit.UsageDimensionToken) != token {
		t.Error("record id must be deterministic for the same message+dimension")
	}
}
