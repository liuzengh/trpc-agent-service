// Package postgres discovers durable channel reply streams and resolves the
// provider adapter from the exact ConfigSnapshot version frozen on an event.
package postgres

import (
	"context"
	"database/sql"
	"errors"

	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/delivery"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

type Catalog struct {
	db       *sql.DB
	adapters map[string]channel.Adapter
}

func New(db *sql.DB, adapters ...channel.Adapter) (*Catalog, error) {
	if db == nil {
		return nil, runtime.ErrInvariantViolation
	}
	result := &Catalog{db: db, adapters: make(map[string]channel.Adapter)}
	for _, adapter := range adapters {
		if adapter == nil || adapter.ID() == "" {
			return nil, runtime.ErrInvariantViolation
		}
		if _, exists := result.adapters[adapter.ID()]; exists {
			return nil, runtime.ErrIdempotencyCollision
		}
		result.adapters[adapter.ID()] = adapter
	}
	if len(result.adapters) == 0 {
		return nil, runtime.ErrInvariantViolation
	}
	return result, nil
}

func (c *Catalog) ListDeliveryDestinations(ctx context.Context) ([]channel.ReplyDestination, error) {
	if c == nil || c.db == nil {
		return nil, runtime.ErrInvariantViolation
	}
	rows, err := c.db.QueryContext(ctx, `SELECT DISTINCT binding.tenant_id,binding.channel,binding.binding_id,binding.external_account_id
FROM channel_binding binding
JOIN config_snapshot snapshot USING (tenant_id,config_version)
JOIN tenant root ON root.tenant_id=binding.tenant_id
WHERE snapshot.state='published' AND root.status IN ('active','suspended') AND binding.channel IN ('feishu','wecom','webui')
ORDER BY binding.tenant_id,binding.channel,binding.binding_id,binding.external_account_id`)
	if err != nil {
		return nil, classify(ctx, err)
	}
	defer rows.Close()
	var result []channel.ReplyDestination
	for rows.Next() {
		var destination channel.ReplyDestination
		if err := rows.Scan(&destination.TenantID, &destination.Channel, &destination.ChannelBindingID, &destination.ExternalAccountID); err != nil {
			return nil, classify(ctx, err)
		}
		if _, exists := c.adapters[destination.Channel]; !exists {
			return nil, runtime.ErrCapabilityUnsupported
		}
		result = append(result, destination)
	}
	if err := rows.Err(); err != nil {
		return nil, classify(ctx, err)
	}
	return result, nil
}

func (c *Catalog) ResolveAdapter(ctx context.Context, tenantID, bindingID string) (channel.Adapter, error) {
	return nil, runtime.ErrCapabilityUnsupported
}

func (c *Catalog) ResolveVersionedAdapter(ctx context.Context, tenantID, bindingID string, configVersion int64) (channel.Adapter, error) {
	if c == nil || c.db == nil || tenantID == "" || bindingID == "" || configVersion < 1 {
		return nil, runtime.ErrTenantScope
	}
	var provider string
	err := c.db.QueryRowContext(ctx, `SELECT binding.channel FROM channel_binding binding
JOIN config_snapshot snapshot USING (tenant_id,config_version)
JOIN tenant root ON root.tenant_id=binding.tenant_id
WHERE binding.tenant_id=$1 AND binding.config_version=$2 AND binding.binding_id=$3
  AND snapshot.state='published' AND root.status IN ('active','suspended')`, tenantID, configVersion, bindingID).Scan(&provider)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, runtime.ErrNotFound
	}
	if err != nil {
		return nil, classify(ctx, err)
	}
	adapter, exists := c.adapters[provider]
	if !exists {
		return nil, runtime.ErrCapabilityUnsupported
	}
	return adapter, nil
}

func (c *Catalog) Probe(ctx context.Context) error {
	_, err := c.ListDeliveryDestinations(ctx)
	return err
}

func classify(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return runtime.ErrBackendUnavailable
}

var _ delivery.DestinationCatalog = (*Catalog)(nil)
var _ delivery.VersionedAdapterResolver = (*Catalog)(nil)
