package platform

import (
	"context"
	"errors"
)

type backendMigrationControl interface {
	beginBackendMigration(context.Context, string, string, backendSelection) error
	finishBackendMigration(context.Context, string, string, *backendSelection) error
}

func (p *SnapshotControlPlane) beginBackendMigration(ctx context.Context, tenant, id string, expected backendSelection) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.refreshLockedContext(ctx) {
		return p.persistenceErr
	}
	current, ok := p.backendSelections[tenant]
	if !ok || current != expected {
		return errors.New("migration source is not selected or already locked")
	}
	next := current
	next.MigrationID = id
	p.backendSelections[tenant] = next
	if p.persistLockedContext(ctx) {
		return nil
	}
	p.backendSelections[tenant] = current
	return p.persistenceErr
}
func (p *SnapshotControlPlane) finishBackendMigration(ctx context.Context, tenant, id string, next *backendSelection) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.refreshLockedContext(ctx) {
		return p.persistenceErr
	}
	current, ok := p.backendSelections[tenant]
	if !ok || current.MigrationID != id {
		return errors.New("migration ownership changed")
	}
	replacement := current
	replacement.MigrationID = ""
	if next != nil {
		replacement = *next
	}
	p.backendSelections[tenant] = replacement
	if p.persistLockedContext(ctx) {
		return nil
	}
	p.backendSelections[tenant] = current
	return p.persistenceErr
}
