package storage

import (
	"context"
	"errors"
	"testing"
)

func TestMemoryEventStoreAppendPendingAndDuplicate(t *testing.T) {
	store := NewMemoryEventStore()
	event := UserEvent{
		SessionID: "s1", TenantID: "tenant-a", AppID: "app-a",
		Channel: "webui", MsgID: "m1", SenderID: "u1", Text: "hi",
	}
	if err := store.AppendUserEvent(context.Background(), event); err != nil {
		t.Fatalf("AppendUserEvent() error = %v", err)
	}
	pending, err := store.PendingUserEvents(context.Background(), "s1", 0)
	if err != nil || len(pending) != 1 || pending[0].Text != "hi" {
		t.Fatalf("pending = %#v, err = %v", pending, err)
	}
	if err := store.AppendUserEvent(context.Background(), event); !errors.Is(err, ErrDuplicateEvent) {
		t.Fatalf("duplicate error = %v", err)
	}
	if err := store.MarkConsumed(context.Background(), []int64{pending[0].ID}); err != nil {
		t.Fatalf("MarkConsumed() error = %v", err)
	}
	pending, err = store.PendingUserEvents(context.Background(), "s1", 0)
	if err != nil || len(pending) != 0 {
		t.Fatalf("pending after consume = %#v, err = %v", pending, err)
	}
}
