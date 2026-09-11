package tenant

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
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

func TestConfigInvalidationDispatcherFailureAndValidationBoundaries(t *testing.T) {
	t.Parallel()
	if _, err := NewConfigInvalidationDispatcher(struct{ storage.StateStore }{}, &invalidationCache{}); err == nil {
		t.Fatal("dispatcher accepted state store without delivery leases")
	}
	if _, err := NewConfigInvalidationDispatcher(storage.NewMemoryStateStore(), nil); err == nil {
		t.Fatal("dispatcher accepted nil cache")
	}
	var nilDispatcher *ConfigInvalidationDispatcher
	if _, err := nilDispatcher.DispatchTenant(context.Background(), "tenant-a", 1); err == nil {
		t.Fatal("nil DispatchTenant succeeded")
	}
	if err := nilDispatcher.Dispatch(context.Background(), storage.OutboxEvent{}); err == nil {
		t.Fatal("nil Dispatch succeeded")
	}

	store := storage.NewMemoryStateStore()
	cache := &invalidationCache{}
	dispatcher, err := NewConfigInvalidationDispatcher(store, cache)
	if err != nil {
		t.Fatal(err)
	}

	if err := dispatcher.Dispatch(context.Background(), storage.OutboxEvent{DeliveryOwner: "foreign", Type: ConfigCacheInvalidationEventType}); err == nil || !strings.Contains(err.Error(), "not claimed") {
		t.Fatalf("foreign claim error = %v", err)
	}
	if err := dispatcher.Dispatch(context.Background(), storage.OutboxEvent{DeliveryOwner: dispatcher.owner, Type: "channel_reply.web"}); err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("wrong type error = %v", err)
	}

	for _, test := range []struct {
		name    string
		payload []byte
		want    string
	}{
		{name: "malformed", payload: []byte(`{`), want: "decode configuration invalidation payload"},
		{name: "empty keys", payload: []byte(`{"keys":[]}`), want: "no cache keys"},
		{name: "blank key", payload: []byte(`{"keys":[" "]}`), want: "empty cache key"},
	} {
		t.Run(test.name, func(t *testing.T) {
			event := recordRawConfigInvalidation(t, store, "request-"+test.name, test.payload)
			claimed, claimErr := store.ClaimPendingOutboxByType(context.Background(), event.TenantID, dispatcher.owner, configInvalidationLease, 1, ConfigCacheInvalidationEventType)
			if claimErr != nil || len(claimed) != 1 {
				t.Fatalf("claim = %#v, %v", claimed, claimErr)
			}
			if err := dispatcher.Dispatch(context.Background(), claimed[0]); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Dispatch() error = %v, want %q", err, test.want)
			}
			stored, findErr := store.FindOutboxByRequestID(context.Background(), event.TenantID, "request-"+test.name)
			if findErr != nil || stored.DeliveryOwner != "" || stored.LastDeliveryError == "" || !stored.AvailableAt.After(stored.CreatedAt) {
				t.Fatalf("failed event state = %#v, %v", stored, findErr)
			}
		})
	}
}

func TestConfigInvalidationDispatcherDeduplicatesKeysAndRetriesCacheFailure(t *testing.T) {
	t.Parallel()
	store := storage.NewMemoryStateStore()
	wantErr := errors.New("redis unavailable")
	cache := &invalidationCache{deleteErr: wantErr}
	event := recordConfigInvalidation(t, store, []string{"key-a", "key-a", "key-b"})
	dispatcher, err := NewConfigInvalidationDispatcher(store, cache)
	if err != nil {
		t.Fatal(err)
	}

	dispatched, err := dispatcher.DispatchTenant(context.Background(), event.TenantID, 10)
	if dispatched != 0 || err == nil || !strings.Contains(err.Error(), "invalidate tenant configuration cache") {
		t.Fatalf("first DispatchTenant() = %d, %v", dispatched, err)
	}
	if len(cache.deleted) != 1 || cache.deleted[0] != "key-a" {
		t.Fatalf("delete attempts = %#v", cache.deleted)
	}
	stored, err := store.FindOutboxByRequestID(context.Background(), event.TenantID, "config-publish-1")
	if err != nil || stored.DeliveryOwner != "" || stored.LastDeliveryError == "" {
		t.Fatalf("retry state = %#v, %v", stored, err)
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

func recordRawConfigInvalidation(t *testing.T, store *storage.MemoryStateStore, requestID string, payload []byte) storage.OutboxEvent {
	t.Helper()
	event, err := store.RecordExecution(context.Background(), storage.ExecutionRecord{
		TenantID: "tenant-a", AppCode: "support", SessionKey: "tenant-a/support/web/" + requestID,
		MessageID: requestID, Channel: "control-plane", BindingID: "control-plane", TraceID: "trace-" + requestID,
		Action: "config_publish", Result: "committed", AuditDetail: "cache invalidation",
		OutboxType: ConfigCacheInvalidationEventType, OutboxPayload: payload, OutboxRequestID: requestID,
	})
	if err != nil {
		t.Fatalf("RecordExecution() error = %v", err)
	}
	return event
}

type invalidationCache struct {
	deleted   []string
	deleteErr error
}

func (c *invalidationCache) Get(context.Context, string) (Snapshot, bool, error) {
	return Snapshot{}, false, nil
}
func (c *invalidationCache) Set(context.Context, string, Snapshot, time.Duration) error { return nil }
func (c *invalidationCache) Delete(_ context.Context, key string) error {
	c.deleted = append(c.deleted, key)
	return c.deleteErr
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
