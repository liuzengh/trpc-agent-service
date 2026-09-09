package postgres

import (
	"context"
	"errors"
	"sync"
)

var ErrMigrationNotReady = errors.New("postgres: migration readiness is blocked")

// MigrationReadiness is a startup/readiness gate backed by the real migrator.
// It is deliberately fail-closed: only an explicit successful Initialize call
// can make the gate ready, and a failed initialization never becomes ready by
// observing a later transient database response.
type MigrationReadiness struct {
	migrator Migrator
	mu       sync.RWMutex
	ready    bool
	err      error
}

func NewMigrationReadiness(migrator Migrator) *MigrationReadiness {
	return &MigrationReadiness{migrator: migrator}
}

func (g *MigrationReadiness) Initialize(ctx context.Context) error {
	if g == nil || g.migrator == nil {
		return ErrMigrationNotReady
	}
	if err := g.migrator.Up(ctx); err != nil {
		g.mu.Lock()
		g.ready, g.err = false, err
		g.mu.Unlock()
		return errors.Join(ErrMigrationNotReady, err)
	}
	g.mu.Lock()
	g.ready, g.err = true, nil
	g.mu.Unlock()
	return nil
}

func (g *MigrationReadiness) Ready(context.Context) error {
	if g == nil {
		return ErrMigrationNotReady
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	if g.ready {
		return nil
	}
	if g.err != nil {
		return errors.Join(ErrMigrationNotReady, g.err)
	}
	return ErrMigrationNotReady
}
