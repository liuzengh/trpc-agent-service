package tenant

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

const (
	// ConfigCacheInvalidationEventType is persisted in the shared outbox after
	// a configuration transaction commits. It is claimed only by the cache
	// invalidation dispatcher, never by a channel sender.
	ConfigCacheInvalidationEventType = "config.cache.invalidate"
	configInvalidationLease          = 30 * time.Second
)

// ConfigCacheInvalidation is the durable payload for one configuration
// publication. Keys include the active application key and both old and new
// binding keys whose resolution can change.
type ConfigCacheInvalidation struct {
	Keys []string `json:"keys"`
}

// ConfigInvalidationDispatcher deletes shared cache entries after the control
// plane transaction has committed. Delivery is at-least-once; Delete is
// idempotent and failures are retained in the outbox with retry backoff.
type ConfigInvalidationDispatcher struct {
	store storage.OutboxDeliveryStore
	cache Cache
	owner string
}

func NewConfigInvalidationDispatcher(store storage.StateStore, cache Cache) (*ConfigInvalidationDispatcher, error) {
	deliveryStore, ok := store.(storage.OutboxDeliveryStore)
	if !ok {
		return nil, fmt.Errorf("outbox state store must implement durable delivery leases")
	}
	if cache == nil {
		return nil, fmt.Errorf("tenant configuration cache is required")
	}
	owner, err := uuid.NewRandom()
	if err != nil {
		return nil, fmt.Errorf("generate configuration invalidation dispatcher identity: %w", err)
	}
	return &ConfigInvalidationDispatcher{store: deliveryStore, cache: cache, owner: owner.String()}, nil
}

func (d *ConfigInvalidationDispatcher) DispatchTenant(ctx context.Context, tenantID string, limit int) (int, error) {
	if d == nil {
		return 0, fmt.Errorf("configuration invalidation dispatcher is required")
	}
	events, err := d.store.ClaimPendingOutboxByType(ctx, tenantID, d.owner, configInvalidationLease, limit, ConfigCacheInvalidationEventType)
	if err != nil {
		return 0, err
	}
	dispatched := 0
	for _, event := range events {
		if err := d.Dispatch(ctx, event); err != nil {
			return dispatched, err
		}
		dispatched++
	}
	return dispatched, nil
}

func (d *ConfigInvalidationDispatcher) Dispatch(ctx context.Context, event storage.OutboxEvent) error {
	if d == nil {
		return fmt.Errorf("configuration invalidation dispatcher is required")
	}
	if event.DeliveryOwner != d.owner {
		return fmt.Errorf("configuration invalidation event is not claimed by this dispatcher")
	}
	if event.Type != ConfigCacheInvalidationEventType {
		return fmt.Errorf("unsupported configuration invalidation event type %q", event.Type)
	}

	var payload ConfigCacheInvalidation
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return d.fail(ctx, event, fmt.Errorf("decode configuration invalidation payload: %w", err))
	}
	keys := uniqueKeys(payload.Keys)
	if len(keys) == 0 {
		return d.fail(ctx, event, fmt.Errorf("configuration invalidation payload has no cache keys"))
	}
	for _, key := range keys {
		if strings.TrimSpace(key) == "" {
			return d.fail(ctx, event, fmt.Errorf("configuration invalidation payload has an empty cache key"))
		}
		if err := d.cache.Delete(ctx, key); err != nil {
			return d.fail(ctx, event, fmt.Errorf("invalidate tenant configuration cache: %w", err))
		}
	}
	if err := d.store.CompleteOutboxDelivery(ctx, event.TenantID, event.ID, d.owner, "redis-cache-invalidated"); err != nil {
		return fmt.Errorf("persist configuration cache invalidation receipt: %w", err)
	}
	return nil
}

func (d *ConfigInvalidationDispatcher) fail(ctx context.Context, event storage.OutboxEvent, cause error) error {
	if err := d.store.FailOutboxDelivery(ctx, event.TenantID, event.ID, d.owner, cause); err != nil {
		return fmt.Errorf("%v; persist configuration invalidation retry: %w", cause, err)
	}
	return cause
}
