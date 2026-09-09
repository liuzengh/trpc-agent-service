// Package migrationruntime connects online migration coordination to the
// same operator-owned backends used by Worker and Summary Worker.
package migrationruntime

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/google/uuid"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/datamigration"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/runtimeplane"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/storage"
)

type Options struct {
	DB *sql.DB
	// GateDB is a separate bounded pool on DB's PostgreSQL authority. Both
	// pools remain caller-owned and must outlive Runtime.Close.
	GateDB            *sql.DB
	StorageProfiles   storage.BackendProfileResolver
	DataPlaneProfiles *runtimeplane.Catalog
	Owner             string
	LeaseTTL          time.Duration
	BatchSize         int
}

type Runtime struct {
	Coordinator *datamigration.LiveCoordinator
	Sessions    *SessionBackends
	DataPlane   *DataPlaneRuntime
}

func New(options Options) (*Runtime, error) {
	if options.Owner == "" {
		options.Owner = "migration-runtime-" + uuid.NewString()
	}
	if options.LeaseTTL == 0 {
		options.LeaseTTL = 2 * time.Minute
	}
	if options.BatchSize == 0 {
		options.BatchSize = 100
	}
	sessions, err := NewSessionBackends(options.DB, options.StorageProfiles, SessionBackendOptions{})
	if err != nil {
		return nil, err
	}
	runtime := &Runtime{Sessions: sessions}
	coordinator, err := datamigration.NewLiveCoordinator(datamigration.LiveOptions{
		DB: options.DB, GateDB: options.GateDB, Owner: options.Owner, LeaseTTL: options.LeaseTTL, BatchSize: options.BatchSize,
		Resolve: func(ctx context.Context, tenantID string, domain datamigration.Domain, profile string) (datamigration.LiveBackend, func(), error) {
			if domain == datamigration.DomainSession {
				return sessions.Resolve(ctx, tenantID, domain, profile)
			}
			if runtime.DataPlane == nil {
				return nil, nil, datamigration.ErrMigrationCapability
			}
			return runtime.DataPlane.ResolveBackend(ctx, tenantID, domain, profile)
		},
	})
	if err != nil {
		_ = sessions.Close()
		return nil, err
	}
	runtime.Coordinator = coordinator
	if options.DataPlaneProfiles != nil {
		runtime.DataPlane, err = NewDataPlaneRuntime(options.DB, options.DataPlaneProfiles, coordinator)
		if err != nil {
			_ = sessions.Close()
			return nil, err
		}
	}
	return runtime, nil
}

func (r *Runtime) SessionDecorator() storage.SessionServiceDecorator {
	return r.Sessions.Decorator(r.Coordinator)
}

func (r *Runtime) DataPlaneOption() runtimeplane.ProfileResolverOption {
	return runtimeplane.WithMigrationDecorators(r.DataPlane.DecorateKnowledge, r.DataPlane.DecorateArtifact)
}

func (r *Runtime) Close() error {
	if r == nil {
		return nil
	}
	var err error
	if r.DataPlane != nil {
		err = r.DataPlane.Close()
	}
	if r.Sessions != nil {
		err = errors.Join(err, r.Sessions.Close())
	}
	return err
}
