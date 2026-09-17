package admin

import (
	"context"
	"time"
)

// Catalog is the read-only projection consumed by the same-origin Admin UI.
// It deliberately excludes endpoint options and secret references.
type Catalog interface {
	GetCatalog(context.Context, string) (CatalogSnapshot, error)
}

type CatalogSnapshot struct {
	Tenant   CatalogTenant    `json:"tenant"`
	Apps     []CatalogApp     `json:"apps"`
	Models   []CatalogModel   `json:"models"`
	Channels []CatalogChannel `json:"channels"`
}

type CatalogTenant struct {
	TenantID            string `json:"tenant_id"`
	DisplayName         string `json:"display_name"`
	Status              string `json:"status"`
	Version             int64  `json:"version"`
	ActiveConfigVersion int64  `json:"active_config_version"`
}
type CatalogApp struct {
	ID              string `json:"id"`
	Key             string `json:"key"`
	DisplayName     string `json:"display_name"`
	Status          string `json:"status"`
	CurrentRevision int64  `json:"current_revision,omitempty"`
	Version         int64  `json:"version"`
}
type CatalogModel struct {
	ID             string `json:"id"`
	Key            string `json:"key"`
	DisplayName    string `json:"display_name"`
	Status         string `json:"status"`
	CurrentVersion int64  `json:"current_version,omitempty"`
	Provider       string `json:"provider,omitempty"`
	Model          string `json:"model,omitempty"`
}
type CatalogChannel struct {
	BindingID     string    `json:"binding_id"`
	Channel       string    `json:"channel"`
	Account       string    `json:"external_account_id"`
	AgentAppID    string    `json:"agent_app_id"`
	ConfigVersion int64     `json:"config_version"`
	CreatedAt     time.Time `json:"created_at"`
}
