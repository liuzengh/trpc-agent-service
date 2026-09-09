package config

import (
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// RuntimeSnapshot is an immutable tenant application configuration. Getters
// return value copies so a request cannot mutate the published configuration.
type RuntimeSnapshot struct {
	tenantID string
	appID    string
	version  tenant.ConfigVersion
	app      tenant.AgentApp
	audit    tenant.AuditPolicy
	runtime  tenant.RuntimePolicy
}

// Snapshot resolves one tenant application into an immutable runtime view.
func (file *File) Snapshot(tenantID, appID string) (RuntimeSnapshot, error) {
	if file == nil {
		return RuntimeSnapshot{}, fmt.Errorf("config: nil file")
	}
	for _, currentTenant := range file.Tenants {
		if currentTenant.ID != tenantID {
			continue
		}
		if !currentTenant.Enabled {
			return RuntimeSnapshot{}, fmt.Errorf(
				"config: tenant %q is disabled", tenantID,
			)
		}
		for _, app := range currentTenant.Apps {
			if app.ID == appID {
				if !app.Enabled {
					return RuntimeSnapshot{}, fmt.Errorf(
						"config: app %q is disabled in tenant %q", appID, tenantID,
					)
				}
				return RuntimeSnapshot{
					tenantID: currentTenant.ID,
					appID:    app.ID,
					version:  currentTenant.ConfigVersion,
					app:      app.Clone(),
					audit:    currentTenant.Audit.Clone(),
					runtime:  currentTenant.Runtime,
				}, nil
			}
		}
		return RuntimeSnapshot{}, fmt.Errorf(
			"config: app %q not found in tenant %q", appID, tenantID,
		)
	}
	return RuntimeSnapshot{}, fmt.Errorf("config: tenant %q not found", tenantID)
}

// TenantID returns the trusted tenant identifier.
func (snapshot RuntimeSnapshot) TenantID() string { return snapshot.tenantID }

// AppID returns the trusted application identifier.
func (snapshot RuntimeSnapshot) AppID() string { return snapshot.appID }

// Version returns the published tenant configuration version.
func (snapshot RuntimeSnapshot) Version() tenant.ConfigVersion { return snapshot.version }

// App returns a deep copy of the application configuration.
func (snapshot RuntimeSnapshot) App() tenant.AgentApp { return snapshot.app.Clone() }

// Audit returns a deep copy of the tenant audit policy.
func (snapshot RuntimeSnapshot) Audit() tenant.AuditPolicy {
	return snapshot.audit.Clone()
}

// Runtime returns the immutable tenant execution policy pinned to this request.
func (snapshot RuntimeSnapshot) Runtime() tenant.RuntimePolicy { return snapshot.runtime }
