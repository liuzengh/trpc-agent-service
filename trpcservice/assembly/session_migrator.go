package assembly

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	platformstorage "github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

const (
	sessionMigrationLeaseTTL  = 5 * time.Minute
	sessionMigrationEventCap  = 1_000_000
	sessionMigrationOwnerBase = "session-migration:"
)

// SessionMigrationManager is the control-plane surface used by the console.
// Each mutation is generation-fenced so two operators cannot advance one
// migration from the same stale state.
type SessionMigrationManager interface {
	StartSessionMigration(context.Context, string, string, string) (platformstorage.SessionMigrationStatus, error)
	AdvanceSessionMigration(context.Context, string, string, string, uint64) (platformstorage.SessionMigrationStatus, error)
	RollbackSessionMigration(context.Context, string, string, string, uint64) (platformstorage.SessionMigrationStatus, error)
}

// SessionMigrator performs online framework Session backend migration. The
// durable migration record drives routing on every replica. Backfill acquires
// the same per-Session execution lease used by Runtime, so a copied Session can
// never race a live model execution.
type SessionMigrator struct {
	store         platformstorage.SessionMigrationStore
	configs       tenant.Repository
	provider      *ManagedSessionProvider
	catalog       platformstorage.ApplicationSessionLister
	leaser        platformstorage.SessionExecutionLeaser
	leaseTTL      time.Duration
	maximumEvents int
}

func NewSessionMigrator(store platformstorage.SessionMigrationStore, configs tenant.Repository, provider *ManagedSessionProvider, catalog platformstorage.ApplicationSessionLister, leaser platformstorage.SessionExecutionLeaser) (*SessionMigrator, error) {
	if store == nil || configs == nil || provider == nil || catalog == nil || leaser == nil {
		return nil, errors.New("Session migration store, configuration repository, provider, catalog, and leaser are required")
	}
	return &SessionMigrator{
		store: store, configs: configs, provider: provider, catalog: catalog, leaser: leaser,
		leaseTTL: sessionMigrationLeaseTTL, maximumEvents: sessionMigrationEventCap,
	}, nil
}

func (m *SessionMigrator) StartSessionMigration(ctx context.Context, tenantID, appCode, targetProfileID string) (platformstorage.SessionMigrationStatus, error) {
	active, err := m.configs.GetActive(ctx, tenantID, appCode)
	if err != nil {
		return platformstorage.SessionMigrationStatus{}, fmt.Errorf("resolve active application for Session migration: %w", err)
	}
	sourceProfileID := strings.TrimSpace(active.Config.Storage.Session.ProfileID)
	targetProfileID = strings.TrimSpace(targetProfileID)
	if sourceProfileID == "" || targetProfileID == "" {
		return platformstorage.SessionMigrationStatus{}, errors.New("Session migration source and target profiles are required")
	}
	sourceConfig, err := m.provider.profiles.ResolveTenantBackend(ctx, tenantID, platformstorage.BackendDomainSession, sourceProfileID)
	if err != nil {
		return platformstorage.SessionMigrationStatus{}, fmt.Errorf("resolve Session migration source profile: %w", err)
	}
	targetConfig, err := m.provider.profiles.ResolveTenantBackend(ctx, tenantID, platformstorage.BackendDomainSession, targetProfileID)
	if err != nil {
		return platformstorage.SessionMigrationStatus{}, fmt.Errorf("resolve Session migration target profile: %w", err)
	}
	source := platformstorage.SessionBackendRef{Driver: sourceConfig.Driver, ConnectionRef: sourceConfig.ConnectionRef}.Normalize(SessionDriverPostgres)
	target := platformstorage.SessionBackendRef{Driver: targetConfig.Driver, ConnectionRef: targetConfig.ConnectionRef}.Normalize(SessionDriverPostgres)
	if source.Equal(target) {
		return platformstorage.SessionMigrationStatus{}, errors.New("Session migration target already matches the active backend")
	}
	// Construct both physical services before creating durable migration state.
	// This fails fast on missing secrets, invalid endpoints, or unsupported drivers.
	if _, err := m.provider.SessionServiceFor(ctx, active.Config, source); err != nil {
		return platformstorage.SessionMigrationStatus{}, fmt.Errorf("prepare Session migration source: %w", err)
	}
	if _, err := m.provider.SessionServiceFor(ctx, active.Config, target); err != nil {
		return platformstorage.SessionMigrationStatus{}, fmt.Errorf("prepare Session migration target: %w", err)
	}
	return m.store.CreateSessionMigration(ctx, platformstorage.SessionMigrationStatus{
		TenantID: tenantID, AppCode: appCode,
		SourceProfileID: sourceProfileID, TargetProfileID: targetProfileID,
		Source: source, Target: target,
	})
}

func (m *SessionMigrator) AdvanceSessionMigration(ctx context.Context, tenantID, appCode, migrationID string, expectedGeneration uint64) (platformstorage.SessionMigrationStatus, error) {
	status, err := m.store.GetSessionMigration(ctx, tenantID, appCode, migrationID)
	if err != nil {
		return platformstorage.SessionMigrationStatus{}, err
	}
	if status.Generation != expectedGeneration || expectedGeneration == 0 {
		return platformstorage.SessionMigrationStatus{}, platformstorage.ErrSessionMigrationConflict
	}
	active, err := m.configs.GetActive(ctx, tenantID, appCode)
	if err != nil {
		return platformstorage.SessionMigrationStatus{}, err
	}

	switch status.Phase {
	case platformstorage.SessionMigrationPrepared:
		status.Phase = platformstorage.SessionMigrationDualWrite
	case platformstorage.SessionMigrationDualWrite:
		status.BackfilledSessions, err = m.backfill(ctx, active.Config, status)
		if err == nil {
			status.Phase = platformstorage.SessionMigrationBackfill
		}
	case platformstorage.SessionMigrationBackfill:
		status.VerifiedSessions, err = m.verify(ctx, active.Config, status)
		if err == nil {
			status.Phase = platformstorage.SessionMigrationVerify
		}
	case platformstorage.SessionMigrationVerify:
		status.Phase = platformstorage.SessionMigrationCutRead
	case platformstorage.SessionMigrationCutRead:
		status.Phase = platformstorage.SessionMigrationStopOldWrite
	case platformstorage.SessionMigrationStopOldWrite:
		if err = m.publishTargetConfiguration(ctx, active.Config, status); err == nil {
			status.Phase = platformstorage.SessionMigrationDone
		}
	case platformstorage.SessionMigrationDone, platformstorage.SessionMigrationRolledBack:
		return status, nil
	default:
		return platformstorage.SessionMigrationStatus{}, fmt.Errorf("unsupported Session migration phase %q", status.Phase)
	}

	if err != nil {
		status.LastError = truncateSessionMigrationError(err.Error())
		updated, updateErr := m.store.UpdateSessionMigration(ctx, status, expectedGeneration)
		if updateErr != nil {
			return platformstorage.SessionMigrationStatus{}, errors.Join(err, updateErr)
		}
		return updated, err
	}
	status.LastError = ""
	return m.store.UpdateSessionMigration(ctx, status, expectedGeneration)
}

func (m *SessionMigrator) RollbackSessionMigration(ctx context.Context, tenantID, appCode, migrationID string, expectedGeneration uint64) (platformstorage.SessionMigrationStatus, error) {
	status, err := m.store.GetSessionMigration(ctx, tenantID, appCode, migrationID)
	if err != nil {
		return platformstorage.SessionMigrationStatus{}, err
	}
	if status.Generation != expectedGeneration || expectedGeneration == 0 {
		return platformstorage.SessionMigrationStatus{}, platformstorage.ErrSessionMigrationConflict
	}
	switch status.Phase {
	case platformstorage.SessionMigrationPrepared,
		platformstorage.SessionMigrationDualWrite,
		platformstorage.SessionMigrationBackfill,
		platformstorage.SessionMigrationVerify,
		platformstorage.SessionMigrationCutRead:
		status.Phase = platformstorage.SessionMigrationRolledBack
		status.LastError = ""
		return m.store.UpdateSessionMigration(ctx, status, expectedGeneration)
	case platformstorage.SessionMigrationRolledBack:
		return status, nil
	case platformstorage.SessionMigrationStopOldWrite, platformstorage.SessionMigrationDone:
		return platformstorage.SessionMigrationStatus{}, errors.New("Session migration cannot roll back after old-backend writes have stopped")
	default:
		return platformstorage.SessionMigrationStatus{}, fmt.Errorf("unsupported Session migration phase %q", status.Phase)
	}
}

func (m *SessionMigrator) backfill(ctx context.Context, tenantConfig config.TenantConfig, status platformstorage.SessionMigrationStatus) (int, error) {
	entries, err := m.catalog.ListApplicationSessions(ctx, status.TenantID, status.AppCode)
	if err != nil {
		return 0, fmt.Errorf("list Session migration catalog: %w", err)
	}
	source, err := m.provider.SessionServiceFor(ctx, tenantConfig, status.Source)
	if err != nil {
		return 0, err
	}
	target, err := m.provider.SessionServiceFor(ctx, tenantConfig, status.Target)
	if err != nil {
		return 0, err
	}
	for index, entry := range entries {
		if err := m.withSessionMigrationLease(ctx, status, entry, func(lease *platformstorage.SessionExecutionLease) error {
			return m.copySession(ctx, source, target, entry, lease)
		}); err != nil {
			return index, err
		}
	}
	return len(entries), nil
}

func (m *SessionMigrator) verify(ctx context.Context, tenantConfig config.TenantConfig, status platformstorage.SessionMigrationStatus) (int, error) {
	entries, err := m.catalog.ListApplicationSessions(ctx, status.TenantID, status.AppCode)
	if err != nil {
		return 0, err
	}
	source, err := m.provider.SessionServiceFor(ctx, tenantConfig, status.Source)
	if err != nil {
		return 0, err
	}
	target, err := m.provider.SessionServiceFor(ctx, tenantConfig, status.Target)
	if err != nil {
		return 0, err
	}
	for index, entry := range entries {
		if err := m.withSessionMigrationLease(ctx, status, entry, func(lease *platformstorage.SessionExecutionLease) error {
			sourceSession, err := m.readCompleteSession(ctx, source, entry)
			if err != nil {
				return err
			}
			targetSession, err := m.readCompleteSession(ctx, target, entry)
			if err != nil {
				return err
			}
			if sessionMigrationDigest(sourceSession) == sessionMigrationDigest(targetSession) {
				return nil
			}
			// A partial prior attempt is repaired once while the Session is
			// execution-locked, then checked again before read cutover.
			if err := m.copySession(ctx, source, target, entry, lease); err != nil {
				return err
			}
			targetSession, err = m.readCompleteSession(ctx, target, entry)
			if err != nil || sessionMigrationDigest(sourceSession) != sessionMigrationDigest(targetSession) {
				return fmt.Errorf("Session %q failed migration verification", entry.SessionKey)
			}
			return nil
		}); err != nil {
			return index, err
		}
	}
	return len(entries), nil
}

func (m *SessionMigrator) withSessionMigrationLease(ctx context.Context, status platformstorage.SessionMigrationStatus, entry platformstorage.Session, operation func(*platformstorage.SessionExecutionLease) error) error {
	if strings.TrimSpace(entry.SubjectID) == "" {
		return fmt.Errorf("Session %q has no framework subject identity", entry.SessionKey)
	}
	owner := sessionMigrationOwnerBase + status.ID
	lease, err := m.leaser.AcquireSessionExecutionLease(ctx, status.TenantID, entry.SessionKey, owner, m.leaseTTL)
	if err != nil {
		return fmt.Errorf("lock Session %q for migration: %w", entry.SessionKey, err)
	}
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = m.leaser.ReleaseSessionExecutionLease(releaseCtx, lease)
	}()
	return operation(&lease)
}

func (m *SessionMigrator) copySession(ctx context.Context, source, target session.Service, entry platformstorage.Session, lease *platformstorage.SessionExecutionLease) error {
	current, err := m.readCompleteSession(ctx, source, entry)
	if err != nil {
		return err
	}
	key := frameworkSessionKey(entry)
	if existing, err := target.GetSession(ctx, key); err != nil {
		return fmt.Errorf("read target Session %q: %w", entry.SessionKey, err)
	} else if existing != nil {
		if err := target.DeleteSession(ctx, key); err != nil {
			return fmt.Errorf("replace target Session %q: %w", entry.SessionKey, err)
		}
	}
	if err := syncApplicationState(ctx, source, target, key.AppName); err != nil {
		return err
	}
	if err := syncUserState(ctx, source, target, session.UserKey{AppName: key.AppName, UserID: key.UserID}); err != nil {
		return err
	}
	created, err := target.CreateSession(ctx, key, cloneSessionState(current.SnapshotState()))
	if err != nil {
		return fmt.Errorf("create target Session %q: %w", entry.SessionKey, err)
	}
	for index := range current.Events {
		if time.Until(lease.LeaseUntil) < m.leaseTTL/3 {
			renewed, err := m.leaser.RenewSessionExecutionLease(ctx, *lease, m.leaseTTL)
			if err != nil {
				return fmt.Errorf("renew Session %q migration lease: %w", entry.SessionKey, err)
			}
			*lease = renewed
		}
		eventCopy := current.Events[index]
		if err := target.AppendEvent(ctx, created, &eventCopy); err != nil {
			return fmt.Errorf("copy Session %q event %d: %w", entry.SessionKey, index, err)
		}
	}
	// Summaries are derived data. The authoritative state and complete event
	// history are migrated; the target backend rebuilds summaries naturally on
	// subsequent framework summarization instead of copying backend internals.
	return nil
}

func (m *SessionMigrator) readCompleteSession(ctx context.Context, service session.Service, entry platformstorage.Session) (*session.Session, error) {
	current, err := service.GetSession(ctx, frameworkSessionKey(entry), session.WithEventNum(m.maximumEvents))
	if err != nil {
		return nil, fmt.Errorf("read Session %q: %w", entry.SessionKey, err)
	}
	if current == nil {
		return nil, fmt.Errorf("framework Session %q does not exist in selected backend", entry.SessionKey)
	}
	if len(current.Events) >= m.maximumEvents {
		return nil, fmt.Errorf("Session %q reached migration event safety cap %d", entry.SessionKey, m.maximumEvents)
	}
	return current, nil
}

func (m *SessionMigrator) publishTargetConfiguration(ctx context.Context, active config.TenantConfig, status platformstorage.SessionMigrationStatus) error {
	if strings.TrimSpace(active.Storage.Session.ProfileID) == status.TargetProfileID {
		return nil
	}
	if strings.TrimSpace(active.Storage.Session.ProfileID) != status.SourceProfileID {
		return errors.New("active Session backend profile changed during migration")
	}
	versions, err := m.configs.ListVersions(ctx, active.TenantID, active.AppCode, 1)
	if err != nil || len(versions) == 0 {
		return fmt.Errorf("resolve next application configuration version: %w", err)
	}
	next := active
	next.ConfigVersion = versions[0].Config.ConfigVersion + 1
	next.Storage.Session = config.BackendProfileRef{ProfileID: status.TargetProfileID}
	if _, err := m.configs.Publish(ctx, next); err != nil {
		return fmt.Errorf("publish Session migration target configuration: %w", err)
	}
	return nil
}

func frameworkSessionKey(entry platformstorage.Session) session.Key {
	return session.Key{AppName: entry.TenantID + "/" + entry.AppCode, UserID: entry.SubjectID, SessionID: entry.SessionKey}
}

func syncApplicationState(ctx context.Context, source, target session.Service, appName string) error {
	sourceState, err := source.ListAppStates(ctx, appName)
	if err != nil {
		return fmt.Errorf("read source application Session state: %w", err)
	}
	targetState, err := target.ListAppStates(ctx, appName)
	if err != nil {
		return fmt.Errorf("read target application Session state: %w", err)
	}
	for key := range targetState {
		if _, exists := sourceState[key]; !exists {
			if err := target.DeleteAppState(ctx, appName, key); err != nil {
				return err
			}
		}
	}
	if len(sourceState) > 0 {
		return target.UpdateAppState(ctx, appName, cloneSessionState(sourceState))
	}
	return nil
}

func syncUserState(ctx context.Context, source, target session.Service, key session.UserKey) error {
	sourceState, err := source.ListUserStates(ctx, key)
	if err != nil {
		return fmt.Errorf("read source user Session state: %w", err)
	}
	targetState, err := target.ListUserStates(ctx, key)
	if err != nil {
		return fmt.Errorf("read target user Session state: %w", err)
	}
	for stateKey := range targetState {
		if _, exists := sourceState[stateKey]; !exists {
			if err := target.DeleteUserState(ctx, key, stateKey); err != nil {
				return err
			}
		}
	}
	if len(sourceState) > 0 {
		return target.UpdateUserState(ctx, key, cloneSessionState(sourceState))
	}
	return nil
}

func sessionMigrationDigest(current *session.Session) [sha256.Size]byte {
	if current == nil {
		return sha256.Sum256(nil)
	}
	payload, _ := json.Marshal(struct {
		AppName string
		UserID  string
		ID      string
		State   session.StateMap
		Events  any
	}{
		AppName: current.AppName, UserID: current.UserID, ID: current.ID,
		State: current.SnapshotState(), Events: current.Events,
	})
	return sha256.Sum256(payload)
}

func truncateSessionMigrationError(value string) string {
	value = strings.TrimSpace(value)
	if len(value) > 512 {
		return value[:512]
	}
	return value
}

var _ SessionMigrationManager = (*SessionMigrator)(nil)
