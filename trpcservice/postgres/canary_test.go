package postgres

import (
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestSelectCanaryConfigVersionIsDeterministicAndScoped(t *testing.T) {
	app := tenant.AgentApp{
		TenantID:            "tenant-a",
		AppID:               "support",
		Name:                "Support",
		ActiveConfigVersion: "v1",
		CanaryConfigVersion: "v2",
		CanaryPercentage:    100,
		CanaryStatus:        tenant.CanaryEnabled,
		Status:              tenant.StatusActive,
	}
	runtime := tenant.RuntimeContext{
		TenantID:           app.TenantID,
		AppID:              app.AppID,
		SessionPrincipalID: "principal-1",
		SessionID:          "session-1",
	}
	if got := selectCanaryConfigVersion(app, runtime); got != "v2" {
		t.Fatalf("canary version = %q, want v2", got)
	}
	if got := selectCanaryConfigVersion(app, runtime); got != "v2" {
		t.Fatalf("repeated canary version = %q, want v2", got)
	}
	runtime.SessionID = ""
	if got := selectCanaryConfigVersion(app, runtime); got != "v1" {
		t.Fatalf("incomplete routing identity version = %q, want v1", got)
	}
	app.CanaryStatus = tenant.CanaryPaused
	if got := selectCanaryConfigVersion(app, runtime); got != "v1" {
		t.Fatalf("paused canary version = %q, want v1", got)
	}
}
