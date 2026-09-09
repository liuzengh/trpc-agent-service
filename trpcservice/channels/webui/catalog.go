package webui

import (
	"context"
	"sort"

	"github.com/Violet2314/trpc-agent-service/trpcservice/tenant"
)

// Desk is a WebUI-selectable Agent route. It never includes secret refs.
type Desk struct {
	RouteKey       string `json:"route_key"`
	BindingID      string `json:"binding_id"`
	TenantID       string `json:"tenant_id"`
	TenantName     string `json:"tenant_name"`
	AppID          string `json:"app_id"`
	AppName        string `json:"app_name"`
	SessionBackend string `json:"session_backend"`
	MemoryBackend  string `json:"memory_backend"`
}

// Catalog lists chat desks the browser dropdown can select.
type Catalog interface {
	ListDesks(context.Context) ([]Desk, error)
}

// StoreCatalog lists active webui bindings from the control-plane store.
type StoreCatalog struct {
	Store tenant.ConfigStore
}

// ListDesks implements Catalog.
func (c StoreCatalog) ListDesks(ctx context.Context) ([]Desk, error) {
	if c.Store == nil {
		return nil, nil
	}
	tenants, err := c.Store.ListTenants(ctx)
	if err != nil {
		return nil, err
	}
	names := make(map[string]string, len(tenants))
	active := make(map[string]bool, len(tenants))
	for _, item := range tenants {
		names[item.ID] = item.Name
		active[item.ID] = item.IsActive
	}
	var desks []Desk
	for _, item := range tenants {
		if !item.IsActive {
			continue
		}
		apps, err := c.Store.ListApps(ctx, item.ID)
		if err != nil {
			return nil, err
		}
		for _, app := range apps {
			bindings, err := c.Store.ListBindings(ctx, app.ID)
			if err != nil {
				return nil, err
			}
			for _, binding := range bindings {
				if binding.Channel != channelType || !binding.IsActive {
					continue
				}
				if !active[binding.TenantID] {
					continue
				}
				desks = append(desks, Desk{
					RouteKey:       binding.RouteKey,
					BindingID:      binding.ID,
					TenantID:       binding.TenantID,
					TenantName:     names[binding.TenantID],
					AppID:          app.ID,
					AppName:        app.AppName,
					SessionBackend: app.Backends.Session,
					MemoryBackend:  app.Backends.Memory,
				})
			}
		}
	}
	sort.Slice(desks, func(i, j int) bool {
		if desks[i].TenantID != desks[j].TenantID {
			return desks[i].TenantID < desks[j].TenantID
		}
		return desks[i].RouteKey < desks[j].RouteKey
	})
	return desks, nil
}
