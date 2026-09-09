package recovery

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSQLStoreRejectsInvalidOperationsBeforeQuery(t *testing.T) {
	var nilStore *SQLStore
	if _, err := nilStore.List(context.Background(), "tenant", KindInbox, []Status{StatusDLQ}, 1); err == nil {
		t.Fatal("nil store accepted list")
	}
	if _, err := nilStore.Redrive(context.Background(), "tenant", KindInbox, "id", StatusDLQ); err == nil {
		t.Fatal("nil store accepted redrive")
	}
	if _, err := nilStore.ResolveOutbox(context.Background(), "tenant", "id", StatusUncertain, StatusSent); err == nil {
		t.Fatal("nil store accepted resolution")
	}
}

func TestRecoveryTableAndStatusContracts(t *testing.T) {
	tests := []struct {
		kind      Kind
		table     string
		idColumn  string
		allowed   []Status
		forbidden Status
	}{
		{kind: KindInbox, table: "inbox_messages", idColumn: "inbox_id", allowed: []Status{StatusDLQ}, forbidden: StatusUncertain},
		{kind: KindOutbox, table: "outbox_messages", idColumn: "outbox_id", allowed: []Status{StatusDLQ, StatusUncertain}, forbidden: StatusSent},
	}
	for _, test := range tests {
		table, idColumn, allowed, err := tableFor(test.kind)
		if err != nil || table != test.table || idColumn != test.idColumn {
			t.Fatalf("kind=%s table=%s id=%s err=%v", test.kind, table, idColumn, err)
		}
		for _, status := range test.allowed {
			if !allowed[status] {
				t.Fatalf("kind=%s status=%s unexpectedly forbidden", test.kind, status)
			}
		}
		if allowed[test.forbidden] {
			t.Fatalf("kind=%s status=%s unexpectedly allowed", test.kind, test.forbidden)
		}
	}
	if _, _, _, err := tableFor(Kind("unknown")); !errors.Is(err, ErrInvalid) {
		t.Fatalf("unknown kind error=%v", err)
	}
}

func TestSQLStoreClockIsInjectable(t *testing.T) {
	want := time.Date(2026, 9, 8, 1, 2, 3, 0, time.UTC)
	if got := (&SQLStore{Now: func() time.Time { return want }}).now(); !got.Equal(want) {
		t.Fatalf("now=%s want=%s", got, want)
	}
	if (&SQLStore{}).now().IsZero() {
		t.Fatal("default clock returned zero")
	}
}
