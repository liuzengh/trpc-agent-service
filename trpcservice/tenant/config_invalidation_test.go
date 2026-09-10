package tenant

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

func TestConfigInvalidationDispatcherDeletesCommittedKeys(t *testing.T) {
	t.Parallel()

	store := storage.NewMemoryStateStore()
	cache := &invalidationCache{}
	event := recordConfigInvalidation(t, store, []string{"tenant-config:active:8:tenant-a7:support", "tenant-config:binding:8:telegram5:bot-a"})
	dispatcher, err := NewConfigInvalidationDispatcher(store, cache)
	if err != nil {
		t.Fatalf("NewConfigInvalidationDispatcher() error = %v", err)
	}

	dispatched, err := dispatcher.DispatchTenant(context.Background(), event.TenantID, 10)
	if err != nil {
		t.Fatalf("DispatchTenant() error = %v", err)
	}
	if dispatched != 1 {
		t.Fatalf("DispatchTenant() dispatched %d events, want 1", dispatched)
	}
	if got, want := cache.deleted, []string{"tenant-config:active:8:tenant-a7:support", "tenant-config:binding:8:telegram5:bot-a"}; !sameStrings(got, want) {
		t.Fatalf("deleted keys = %#v, want %#v", got, want)
	}
	if event, err := store.FindOutboxByRequestID(context.Background(), event.TenantID, "config-publish-1"); err != nil || event.DeliveredAt == nil || event.DeliveryReceipt != "redis-cache-invalidated" {
		t.Fatalf("event delivery = %#v, %v; want delivered cache invalidation receipt", event, err)
	}
}

func recordConfigInvalidation(t *testing.T, store *storage.MemoryStateStore, keys []string) storage.OutboxEvent {
	t.Helper()
	payload, err := json.Marshal(ConfigCacheInvalidation{Keys: keys})
	if err != nil {
		t.Fatalf("marshal config invalidation payload: %v", err)
	}
	event, err := store.RecordExecution(context.Background(), storage.ExecutionRecord{
		TenantID: "tenant-a", AppCode: "support", SessionKey: "tenant-a/support/web/config-publish-1",
		MessageID: "config-publish-1", Channel: "control-plane", BindingID: "control-plane", TraceID: "trace-config-publish-1",
		Action: "config_publish", Result: "committed", AuditDetail: "cache invalidation",
		OutboxType: ConfigCacheInvalidationEventType, OutboxPayload: payload, OutboxRequestID: "config-publish-1",
	})
	if err != nil {
		t.Fatalf("RecordExecution() error = %v", err)
	}
	return event
}

type invalidationCache struct {
	deleted []string
}

func (c *invalidationCache) Get(context.Context, string) (Snapshot, bool, error) {
	return Snapshot{}, false, nil
}
func (c *invalidationCache) Set(context.Context, string, Snapshot, time.Duration) error { return nil }
func (c *invalidationCache) Delete(_ context.Context, key string) error {
	c.deleted = append(c.deleted, key)
	return nil
}

func sameStrings(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for index := range got {
		if got[index] != want[index] {
			return false
		}
	}
	return true
}
