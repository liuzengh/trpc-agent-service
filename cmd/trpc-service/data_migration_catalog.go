package main

import (
	"context"
	"fmt"
	"sort"

	"github.com/liuzengh/trpc-agent-service/trpcservice/migration"
	"github.com/liuzengh/trpc-agent-service/trpcservice/postgres"
	platformsession "github.com/liuzengh/trpc-agent-service/trpcservice/session"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

// dataMigrationSessionCatalog combines the SQL lane catalog with Redis's
// backend-owned inventory. Redis sessions may predate session_lane entirely.
type dataMigrationSessionCatalog struct {
	store    *postgres.Store
	sessions *platformsession.Router
}

func (c dataMigrationSessionCatalog) ListDataMigrationSessionKeys(
	ctx context.Context,
	record migration.Record,
) ([]session.Key, error) {
	if c.store == nil || c.sessions == nil {
		return nil, fmt.Errorf("data migration session catalog dependencies are required")
	}
	keys, err := c.store.ListDataMigrationSessionKeys(ctx, record)
	if err != nil {
		return nil, err
	}
	config, err := c.store.ResolveAppConfig(ctx, record.TenantID, record.AppID, record.SourceConfigVersion)
	if err != nil {
		return nil, fmt.Errorf("resolve data migration source config: %w", err)
	}
	if config.BackendConfig.Session.Provider != "redis" {
		return keys, nil
	}
	appName, err := (tenant.Scope{TenantID: record.TenantID, AppID: record.AppID}).Key("runner")
	if err != nil {
		return nil, fmt.Errorf("build data migration session app name: %w", err)
	}
	legacy, err := c.sessions.ListSessionKeys(ctx, worker.Execution{
		Tenant: tenant.RuntimeContext{
			TenantID:      record.TenantID,
			AppID:         record.AppID,
			ConfigVersion: record.SourceConfigVersion,
		},
		Config: config,
	})
	if err != nil {
		return nil, fmt.Errorf("list redis data migration session keys: %w", err)
	}
	merged := make(map[session.Key]struct{}, len(keys)+len(legacy))
	for _, key := range keys {
		merged[key] = struct{}{}
	}
	for _, key := range legacy {
		if key.AppName != appName {
			return nil, fmt.Errorf("redis session inventory scope mismatch")
		}
		merged[key] = struct{}{}
	}
	result := make([]session.Key, 0, len(merged))
	for key := range merged {
		result = append(result, key)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].AppName != result[j].AppName {
			return result[i].AppName < result[j].AppName
		}
		if result[i].UserID != result[j].UserID {
			return result[i].UserID < result[j].UserID
		}
		return result[i].SessionID < result[j].SessionID
	})
	return result, nil
}
