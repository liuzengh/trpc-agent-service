package controlplane

import (
	"context"
	"errors"
	"time"
)

type AppSettingsRepository interface {
	UpdateAppSettings(context.Context, string, string, string, string, string, int64) (AgentApp, error)
}

func (r *PostgresRepository) UpdateAppSettings(ctx context.Context, tenant, id, name, description, status string, expected int64) (AgentApp, error) {
	app, err := scanAgentApp(r.dbFor(ctx).QueryRowContext(ctx, `UPDATE agent_app SET name=$3,description=$4,status=$5,version=version+1,updated_at=now() WHERE tenant_id=$1 AND app_id=$2 AND version=$6 RETURNING app_id,tenant_id,name,description,status,stable_revision_id,rollout_policy,version,created_at,updated_at`, tenant, id, name, description, status, expected))
	if errors.Is(err, ErrNotFound) {
		err = ErrConflict
	}
	return app, err
}
func (r *MemoryRepository) UpdateAppSettings(_ context.Context, tenant, id, name, description, status string, expected int64) (AgentApp, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := scopedKey(tenant, id)
	app, ok := r.apps[key]
	if !ok {
		return app, ErrNotFound
	}
	if app.Version != expected {
		return app, ErrConflict
	}
	app.Name = name
	app.Description = description
	app.Status = status
	app.Version++
	app.UpdatedAt = time.Now().UTC()
	r.apps[key] = cloneAgentApp(app)
	return cloneAgentApp(app), nil
}
