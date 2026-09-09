// Package config loads and bootstraps platform configuration.
package config

import (
	"context"
	"errors"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

const (
	DemoTenantID   = "demo"
	DemoAgentAppID = "echo"
	DemoRevisionID = "echo-v1"
	// DemoPrincipalID is the only chat principal the local demo credential
	// authenticates as. Sessions are keyed by it, never by a request body.
	DemoPrincipalID = "demo-user"
)

// SeedDemo creates an idempotent local configuration with no external API key.
func SeedDemo(ctx context.Context, repository tenant.Repository) error {
	if repository == nil {
		return fmt.Errorf("config: tenant repository is required")
	}
	_, err := repository.CreateTenant(ctx, tenant.Tenant{
		ID: DemoTenantID, Slug: DemoTenantID, Name: "Demo Tenant",
	})
	if err != nil && !errors.Is(err, tenant.ErrAlreadyExists) {
		return fmt.Errorf("config: create demo tenant: %w", err)
	}
	scope := tenant.TenantContext{TenantID: DemoTenantID}
	_, err = repository.CreateAgentApp(ctx, scope, tenant.AgentApp{
		ID: DemoAgentAppID, TenantID: DemoTenantID, Name: "Echo Assistant",
	})
	if err != nil && !errors.Is(err, tenant.ErrAlreadyExists) {
		return fmt.Errorf("config: create demo app: %w", err)
	}
	_, err = repository.CreateRevision(ctx, scope, tenant.AgentRevision{
		ID:         DemoRevisionID,
		TenantID:   DemoTenantID,
		AgentAppID: DemoAgentAppID,
		RevisionNo: 1,
		CreatedBy:  "system-bootstrap",
		Config: tenant.RevisionConfig{
			AgentName:   "echo-assistant",
			Description: "Deterministic bootstrap agent",
			Instruction: "Return the model response. This bootstrap runtime verifies the service path.",
			Model: tenant.ModelConfig{
				Provider: "deterministic",
				Name:     "deterministic-echo",
			},
		},
	})
	if err != nil && !errors.Is(err, tenant.ErrAlreadyExists) {
		return fmt.Errorf("config: create demo revision: %w", err)
	}
	// Publishing is the one step that is not naturally idempotent, so it is the
	// one step that has to ask first.
	//
	// Every create above tolerates ErrAlreadyExists, which used to make the
	// whole function safe to re-run: with an in-memory repository a restart
	// started from an empty store, so re-publishing echo-v1 only restated what
	// the previous boot had done. Against a persistent repository that is no
	// longer true. An operator who publishes echo-v2 and then restarts the
	// process would find the default silently moved back to echo-v1 — a
	// bootstrap helper undoing a deliberate deployment, on nothing more than a
	// process lifecycle event.
	//
	// So the app's current routing decides: seed publishes only when there is
	// none. A first boot, or a boot that resumes after a crash between creating
	// the revision and publishing it, still ends with echo-v1 serving; every
	// later boot leaves whatever is published alone.
	//
	// Concurrent boots are safe. Each one tolerates the others' creates, and
	// only reaches this point once the revision exists, so the worst case is
	// two processes publishing the same echo-v1 — the same write twice, not a
	// conflict. What this does not close is an operator publishing echo-v2 in
	// the window between the read below and the publish that follows: this
	// reads and writes in two steps, and the Repository has no
	// publish-if-unpublished primitive to make it one. That window is a few
	// milliseconds during startup, against a manual action, and closing it
	// would mean a new control-plane operation for the benefit of a bootstrap
	// helper.
	app, err := repository.GetAgentApp(ctx, scope, DemoAgentAppID)
	if err != nil {
		return fmt.Errorf("config: read demo app routing: %w", err)
	}
	if app.RoutingPolicy.DefaultRevisionID != "" {
		return nil
	}
	if _, _, err = repository.PublishRevision(
		ctx, scope, DemoAgentAppID, DemoRevisionID,
	); err != nil {
		return fmt.Errorf("config: publish demo revision: %w", err)
	}
	return nil
}
