package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	platformaudit "github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// EnableAppCanary publishes a candidate version to a deterministic percentage
// of new admissions. Authoritative backend changes are intentionally rejected;
// those require the existing data-migration path first.
func (s *Store) EnableAppCanary(
	ctx context.Context,
	tenantID, appID, version string,
	percentage int,
) (tenant.AgentApp, error) {
	if percentage <= 0 || percentage > 100 {
		return tenant.AgentApp{}, errors.New("canary percentage must be between 1 and 100")
	}
	return s.updateCanary(ctx, tenantID, appID, version, percentage, tenant.CanaryEnabled)
}

// PauseAppCanary stops new traffic to the candidate while retaining its
// target and percentage so an operator can resume without rebuilding state.
func (s *Store) PauseAppCanary(ctx context.Context, tenantID, appID string) (tenant.AgentApp, error) {
	return s.updateCanary(ctx, tenantID, appID, "", 0, tenant.CanaryPaused)
}

// DisableAppCanary removes the candidate from the application rollout.
func (s *Store) DisableAppCanary(ctx context.Context, tenantID, appID string) (tenant.AgentApp, error) {
	return s.updateCanary(ctx, tenantID, appID, "", 0, tenant.CanaryDisabled)
}

// RollbackAppCanary removes the candidate and leaves the stable version
// unchanged. Existing executions remain pinned to their admitted version.
func (s *Store) RollbackAppCanary(ctx context.Context, tenantID, appID string) (tenant.AgentApp, error) {
	return s.updateCanary(ctx, tenantID, appID, "", 0, tenant.CanaryDisabled)
}

// ApplyCanaryDecision applies an automated health decision only when the
// observed candidate is still the current candidate. The row lock and version
// predicate make stale Prometheus/Alertmanager decisions harmless.
func (s *Store) ApplyCanaryDecision(
	ctx context.Context,
	tenantID, appID, expectedVersion string,
	action tenant.CanaryAction,
	reason string,
) (tenant.AgentApp, error) {
	if err := s.validate(); err != nil {
		return tenant.AgentApp{}, err
	}
	if tenantID == "" || appID == "" || expectedVersion == "" {
		return tenant.AgentApp{}, errors.New("tenant_id, app_id, and expected canary version are required")
	}
	if action != tenant.CanaryActionPause && action != tenant.CanaryActionRollback {
		return tenant.AgentApp{}, errors.New("automated canary action must be PAUSE or ROLLBACK")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return tenant.AgentApp{}, fmt.Errorf("begin automated canary decision: %w", err)
	}
	defer func() { rollback(tx) }()
	app, err := lockAgentApp(ctx, tx, tenantID, appID)
	if err != nil {
		return tenant.AgentApp{}, fmt.Errorf("lock agent app for automated canary decision: %w", err)
	}
	if app.CanaryConfigVersion != expectedVersion || app.CanaryStatus == tenant.CanaryDisabled {
		return tenant.AgentApp{}, tenant.ErrCanaryDecisionStale
	}
	if action == tenant.CanaryActionPause && app.CanaryStatus == tenant.CanaryPaused {
		return app, nil
	}
	candidateVersion := app.CanaryConfigVersion
	eventType := platformaudit.ConfigCanaryPaused
	decision := "paused"
	if action == tenant.CanaryActionRollback {
		eventType = platformaudit.ConfigCanaryRolledBack
		decision = "rolled_back"
	}
	if action == tenant.CanaryActionPause {
		if _, err := tx.Exec(ctx, `
UPDATE platform.agent_app
SET canary_status = 'PAUSED', updated_at = clock_timestamp()
WHERE tenant_id = $1 AND app_id = $2 AND canary_config_version = $3`, tenantID, appID, expectedVersion); err != nil {
			return tenant.AgentApp{}, fmt.Errorf("pause automated app canary: %w", err)
		}
		app.CanaryStatus = tenant.CanaryPaused
	} else {
		if _, err := tx.Exec(ctx, `
UPDATE platform.agent_app
SET canary_config_version = NULL,
    canary_percentage = 0,
    canary_status = 'DISABLED',
    updated_at = clock_timestamp()
WHERE tenant_id = $1 AND app_id = $2 AND canary_config_version = $3`, tenantID, appID, expectedVersion); err != nil {
			return tenant.AgentApp{}, fmt.Errorf("rollback automated app canary: %w", err)
		}
		app.CanaryConfigVersion = ""
		app.CanaryPercentage = 0
		app.CanaryStatus = tenant.CanaryDisabled
	}
	event := controlPlaneAuditEvent(ctx, tenantID, appID, candidateVersion, eventType, decision)
	event.PolicyRuleID = "canary_metric_threshold"
	event.PolicyReason = platformaudit.SafePolicyReason(reason)
	if err := recordControlPlaneAuditTx(ctx, tx, event); err != nil {
		return tenant.AgentApp{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return tenant.AgentApp{}, fmt.Errorf("commit automated canary decision: %w", err)
	}
	return app, nil
}

// PromoteAppCanary makes the current candidate the stable version and clears
// the canary state in one transaction.
func (s *Store) PromoteAppCanary(ctx context.Context, tenantID, appID string) (tenant.AgentApp, error) {
	if err := s.validate(); err != nil {
		return tenant.AgentApp{}, err
	}
	if tenantID == "" || appID == "" {
		return tenant.AgentApp{}, errors.New("tenant_id and app_id are required")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return tenant.AgentApp{}, fmt.Errorf("begin promote app canary: %w", err)
	}
	defer func() { rollback(tx) }()
	app, err := lockAgentApp(ctx, tx, tenantID, appID)
	if err != nil {
		return tenant.AgentApp{}, fmt.Errorf("lock agent app for canary promote: %w", err)
	}
	if app.CanaryConfigVersion == "" ||
		(app.CanaryStatus != tenant.CanaryEnabled && app.CanaryStatus != tenant.CanaryPaused) {
		return tenant.AgentApp{}, errors.New("app has no active canary to promote")
	}
	target := app.CanaryConfigVersion
	if _, err := tx.Exec(ctx, `
UPDATE platform.agent_app
SET active_config_version = canary_config_version,
    canary_config_version = NULL,
    canary_percentage = 0,
    canary_status = 'DISABLED',
    updated_at = clock_timestamp()
WHERE tenant_id = $1 AND app_id = $2`, tenantID, appID); err != nil {
		return tenant.AgentApp{}, fmt.Errorf("promote app canary: %w", err)
	}
	app.ActiveConfigVersion = target
	app.CanaryConfigVersion = ""
	app.CanaryPercentage = 0
	app.CanaryStatus = tenant.CanaryDisabled
	if err := recordControlPlaneAuditTx(ctx, tx, controlPlaneAuditEvent(
		ctx, tenantID, appID, target, platformaudit.ConfigCanaryPromoted, "promoted",
	)); err != nil {
		return tenant.AgentApp{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return tenant.AgentApp{}, fmt.Errorf("commit promote app canary: %w", err)
	}
	return app, nil
}

func (s *Store) updateCanary(
	ctx context.Context,
	tenantID, appID, version string,
	percentage int,
	status tenant.CanaryStatus,
) (tenant.AgentApp, error) {
	if err := s.validate(); err != nil {
		return tenant.AgentApp{}, err
	}
	if tenantID == "" || appID == "" {
		return tenant.AgentApp{}, errors.New("tenant_id and app_id are required")
	}
	if status == tenant.CanaryEnabled {
		if version == "" || percentage <= 0 || percentage > 100 {
			return tenant.AgentApp{}, errors.New("enabled canary requires a target and percentage between 1 and 100")
		}
	} else if status != tenant.CanaryPaused && status != tenant.CanaryDisabled {
		return tenant.AgentApp{}, errors.New("canary status is invalid")
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return tenant.AgentApp{}, fmt.Errorf("begin update app canary: %w", err)
	}
	defer func() { rollback(tx) }()
	app, err := lockAgentApp(ctx, tx, tenantID, appID)
	if err != nil {
		return tenant.AgentApp{}, fmt.Errorf("lock agent app for canary: %w", err)
	}
	if status == tenant.CanaryEnabled {
		if version == app.ActiveConfigVersion {
			return tenant.AgentApp{}, errors.New("canary target must differ from active config")
		}
		activeBackend, err := storedBackendConfig(ctx, tx, tenantID, appID, app.ActiveConfigVersion)
		if err != nil {
			return tenant.AgentApp{}, err
		}
		targetBackend, err := storedBackendConfig(ctx, tx, tenantID, appID, version)
		if err != nil {
			return tenant.AgentApp{}, err
		}
		if !sameAuthoritativeBackends(activeBackend, targetBackend) {
			return tenant.AgentApp{}, errors.New("canary target changes authoritative backends; migrate the backends before enabling canary")
		}
	}

	var eventType string
	switch status {
	case tenant.CanaryEnabled:
		eventType = platformaudit.ConfigCanaryEnabled
	case tenant.CanaryPaused:
		eventType = platformaudit.ConfigCanaryPaused
	case tenant.CanaryDisabled:
		if app.CanaryConfigVersion != "" {
			eventType = platformaudit.ConfigCanaryRolledBack
		} else {
			eventType = platformaudit.ConfigCanaryDisabled
		}
	}
	if status == tenant.CanaryEnabled {
		if _, err := tx.Exec(ctx, `
UPDATE platform.agent_app
SET canary_config_version = $3,
    canary_percentage = $4,
    canary_status = 'ENABLED',
    updated_at = clock_timestamp()
WHERE tenant_id = $1 AND app_id = $2`, tenantID, appID, version, percentage); err != nil {
			return tenant.AgentApp{}, fmt.Errorf("enable app canary: %w", err)
		}
		app.CanaryConfigVersion = version
		app.CanaryPercentage = percentage
		app.CanaryStatus = tenant.CanaryEnabled
	} else if status == tenant.CanaryPaused {
		if app.CanaryConfigVersion == "" || app.CanaryPercentage <= 0 {
			return tenant.AgentApp{}, errors.New("app has no canary to pause")
		}
		if _, err := tx.Exec(ctx, `
UPDATE platform.agent_app
SET canary_status = 'PAUSED', updated_at = clock_timestamp()
WHERE tenant_id = $1 AND app_id = $2`, tenantID, appID); err != nil {
			return tenant.AgentApp{}, fmt.Errorf("pause app canary: %w", err)
		}
		app.CanaryStatus = tenant.CanaryPaused
	} else {
		if _, err := tx.Exec(ctx, `
UPDATE platform.agent_app
SET canary_config_version = NULL,
    canary_percentage = 0,
    canary_status = 'DISABLED',
    updated_at = clock_timestamp()
WHERE tenant_id = $1 AND app_id = $2`, tenantID, appID); err != nil {
			return tenant.AgentApp{}, fmt.Errorf("disable app canary: %w", err)
		}
		app.CanaryConfigVersion = ""
		app.CanaryPercentage = 0
		app.CanaryStatus = tenant.CanaryDisabled
	}
	if eventType != "" {
		auditVersion := version
		if auditVersion == "" {
			auditVersion = app.CanaryConfigVersion
		}
		if auditVersion == "" {
			auditVersion = app.ActiveConfigVersion
		}
		if err := recordControlPlaneAuditTx(ctx, tx, controlPlaneAuditEvent(
			ctx, tenantID, appID, auditVersion, eventType, string(status),
		)); err != nil {
			return tenant.AgentApp{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return tenant.AgentApp{}, fmt.Errorf("commit update app canary: %w", err)
	}
	return app, nil
}
