package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/persistence"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionfence"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

const (
	sqlInitAttempts   = 5
	sqlInitRetryDelay = time.Second
)

type SQLInitResult struct {
	TenantID  string
	ProfileID string
	Kind      tenant.StorageKind
}

type sqlInitBackend interface {
	Ready(context.Context) error
	Close() error
}

type sqlInitBackendFactory func(tenant.StorageProfile, string, sessionfence.Limits) (sqlInitBackend, error)

// InitializeSQLProfiles reuses SQLBackend as the single schema authority.
// readyOnly forces SkipDBInit and therefore validates without creating DDL.
func InitializeSQLProfiles(ctx context.Context, profiles []tenant.StorageProfile, resolver config.CredentialResolver, requested string, readyOnly bool) ([]SQLInitResult, error) {
	return initializeSQLProfiles(ctx, profiles, resolver, requested, readyOnly, func(profile tenant.StorageProfile, credential string, limits sessionfence.Limits) (sqlInitBackend, error) {
		return NewSQLBackend(profile, credential, limits)
	})
}

func initializeSQLProfiles(ctx context.Context, profiles []tenant.StorageProfile, resolver config.CredentialResolver, requested string, readyOnly bool, factory sqlInitBackendFactory) ([]SQLInitResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if resolver == nil || factory == nil {
		return nil, errors.New("SQL initializer dependencies are required")
	}
	if requested != "postgres" && requested != "mysql" && requested != "all" {
		return nil, errors.New("SQL kind must be postgres, mysql, or all")
	}
	results := make([]SQLInitResult, 0, len(profiles))
	for _, profile := range profiles {
		if !profile.Kind.IsSQL() || (requested != "all" && string(profile.Kind) != requested) {
			continue
		}
		credential, err := resolver.Resolve(profile.CredentialRef)
		if err != nil {
			return nil, fmt.Errorf("resolve %s tenant=%s profile=%s credential: %w", profile.Kind, profile.TenantID, profile.ID, err)
		}
		profile.SkipDBInit = readyOnly
		backend, err := factory(profile, credential, sessionfence.Limits{MaxTurnEvents: config.DefaultMaxTurnEvents, MaxTurnBytes: config.DefaultMaxTurnBytes})
		if err != nil {
			return nil, fmt.Errorf("create %s tenant=%s profile=%s: %w", profile.Kind, profile.TenantID, profile.ID, err)
		}
		readyErr := readySQLBackend(ctx, backend)
		closeErr := backend.Close()
		if readyErr != nil {
			return nil, fmt.Errorf("sql-%s tenant=%s profile=%s: %w", map[bool]string{true: "ready", false: "init"}[readyOnly], profile.TenantID, profile.ID, readyErr)
		}
		if closeErr != nil {
			return nil, fmt.Errorf("close %s tenant=%s profile=%s: %w", profile.Kind, profile.TenantID, profile.ID, closeErr)
		}
		results = append(results, SQLInitResult{TenantID: profile.TenantID, ProfileID: profile.ID, Kind: profile.Kind})
	}
	if len(results) == 0 {
		return nil, fmt.Errorf("no active %s SQL storage profile found", requested)
	}
	return results, nil
}

func readySQLBackend(ctx context.Context, backend sqlInitBackend) error {
	var err error
	for attempt := 0; attempt < sqlInitAttempts; attempt++ {
		err = backend.Ready(ctx)
		if err == nil || !errors.Is(err, persistence.ErrBackendUnavailable) || attempt == sqlInitAttempts-1 {
			return err
		}
		timer := time.NewTimer(sqlInitRetryDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return err
}
