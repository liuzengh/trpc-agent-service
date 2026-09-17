package admin

import (
	"context"
	"database/sql"
	"errors"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

// PostgreSQLCatalog is intentionally a read-only, presentation-specific
// projection. Queries are tenant-scoped and never select secret material.
type PostgreSQLCatalog struct{ DB *sql.DB }

func (c PostgreSQLCatalog) GetCatalog(ctx context.Context, tenantID string) (CatalogSnapshot, error) {
	if c.DB == nil || tenantID == "" {
		return CatalogSnapshot{}, runtime.ErrCapabilityUnsupported
	}
	var result CatalogSnapshot
	err := c.DB.QueryRowContext(ctx, `SELECT tenant_id,display_name,status,version,COALESCE(active_config_version,0) FROM tenant WHERE tenant_id=$1`, tenantID).
		Scan(&result.Tenant.TenantID, &result.Tenant.DisplayName, &result.Tenant.Status, &result.Tenant.Version, &result.Tenant.ActiveConfigVersion)
	if errors.Is(err, sql.ErrNoRows) {
		return CatalogSnapshot{}, runtime.ErrNotFound
	}
	if err != nil {
		return CatalogSnapshot{}, err
	}
	apps, err := c.DB.QueryContext(ctx, `SELECT agent_app_id,agent_app_key,display_name,status,COALESCE(current_revision,0),version FROM agent_app WHERE tenant_id=$1 ORDER BY display_name,agent_app_id`, tenantID)
	if err != nil {
		return CatalogSnapshot{}, err
	}
	defer apps.Close()
	for apps.Next() {
		var item CatalogApp
		if err := apps.Scan(&item.ID, &item.Key, &item.DisplayName, &item.Status, &item.CurrentRevision, &item.Version); err != nil {
			return CatalogSnapshot{}, err
		}
		result.Apps = append(result.Apps, item)
	}
	if err := apps.Err(); err != nil {
		return CatalogSnapshot{}, err
	}
	models, err := c.DB.QueryContext(ctx, `SELECT p.model_profile_id,p.profile_key,p.display_name,p.status,COALESCE(p.current_version,0),COALESCE(r.provider,''),COALESCE(r.model_name,'') FROM model_profile p LEFT JOIN model_profile_revision r ON r.tenant_id=p.tenant_id AND r.model_profile_id=p.model_profile_id AND r.profile_version=p.current_version WHERE p.tenant_id=$1 ORDER BY p.display_name,p.model_profile_id`, tenantID)
	if err != nil {
		return CatalogSnapshot{}, err
	}
	defer models.Close()
	for models.Next() {
		var item CatalogModel
		if err := models.Scan(&item.ID, &item.Key, &item.DisplayName, &item.Status, &item.CurrentVersion, &item.Provider, &item.Model); err != nil {
			return CatalogSnapshot{}, err
		}
		result.Models = append(result.Models, item)
	}
	if err := models.Err(); err != nil {
		return CatalogSnapshot{}, err
	}
	channels, err := c.DB.QueryContext(ctx, `SELECT binding_id,channel,external_account_id,agent_app_id,config_version,created_at FROM channel_binding WHERE tenant_id=$1 ORDER BY config_version DESC,binding_id`, tenantID)
	if err != nil {
		return CatalogSnapshot{}, err
	}
	defer channels.Close()
	for channels.Next() {
		var item CatalogChannel
		if err := channels.Scan(&item.BindingID, &item.Channel, &item.Account, &item.AgentAppID, &item.ConfigVersion, &item.CreatedAt); err != nil {
			return CatalogSnapshot{}, err
		}
		result.Channels = append(result.Channels, item)
	}
	if err := channels.Err(); err != nil {
		return CatalogSnapshot{}, err
	}
	return result, nil
}

var _ Catalog = PostgreSQLCatalog{}
