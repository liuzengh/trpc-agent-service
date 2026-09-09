package storage

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/session"

	"github.com/Violet2314/trpc-agent-service/trpcservice/tenant"
)

// MigrationPhase is a persisted online migration state.
type MigrationPhase string

const (
	PhasePrepared     MigrationPhase = "prepared"
	PhaseDualWrite    MigrationPhase = "dual_write"
	PhaseBackfill     MigrationPhase = "backfill"
	PhaseVerify       MigrationPhase = "verify"
	PhaseCutRead      MigrationPhase = "cut_read"
	PhaseStopOldWrite MigrationPhase = "stop_old_write"
	PhaseDone         MigrationPhase = "done"
	PhaseRolledBack   MigrationPhase = "rolled_back"
)

// ErrMigrationConflict means a new Session migration cannot start because one
// is already in flight or the source backend is not the live backend.
var ErrMigrationConflict = errors.New("migration conflict")

// MigrationStatus is the durable migration control record.
type MigrationStatus struct {
	ID          string         `json:"migration_id"`
	AppID       string         `json:"app_id"`
	FromBackend string         `json:"from_backend"`
	ToBackend   string         `json:"to_backend"`
	Phase       MigrationPhase `json:"phase"`
	Detail      map[string]any `json:"detail"`
	UpdatedAt   time.Time      `json:"updated_at"`
}

// MigrationStore persists migration state independently from process memory.
type MigrationStore interface {
	Create(context.Context, MigrationStatus) error
	Get(context.Context, string) (MigrationStatus, error)
	Update(context.Context, MigrationStatus) error
	ListActive(context.Context) ([]MigrationStatus, error)
}

// SessionCatalog enumerates all framework session keys for one app.
type SessionCatalog interface {
	ListSessionKeys(context.Context, string) ([]session.Key, error)
}

// SessionMigrationLocker serializes backfill with writes to one session.
type SessionMigrationLocker interface {
	WithSessionLock(context.Context, string, func() error) error
}

// AppResolver loads the current app version.
type AppResolver interface {
	GetCurrentApp(context.Context, string) (tenant.AgentApp, error)
}

// Migrator advances and rolls back online Session migrations.
type Migrator interface {
	Start(context.Context, string, string, string) (string, error)
	Advance(context.Context, string) (MigrationPhase, error)
	Rollback(context.Context, string) error
	Status(context.Context, string) (MigrationStatus, error)
	Resume(context.Context) error
}

// SessionMigrator implements Redis-to-MySQL online migration.
type SessionMigrator struct {
	store   MigrationStore
	apps    AppResolver
	catalog SessionCatalog
	locker  SessionMigrationLocker
	factory *BackendFactory
}

// NewSessionMigrator constructs a migration state machine.
func NewSessionMigrator(
	store MigrationStore,
	apps AppResolver,
	catalog SessionCatalog,
	locker SessionMigrationLocker,
	factory *BackendFactory,
) (*SessionMigrator, error) {
	if store == nil || apps == nil || catalog == nil || factory == nil {
		return nil, errors.New("migration store, app resolver, catalog, and backend factory are required")
	}
	if locker == nil {
		locker = noOpMigrationLocker{}
	}
	return &SessionMigrator{
		store: store, apps: apps, catalog: catalog, locker: locker, factory: factory,
	}, nil
}

// Start verifies both backends, rejects conflicting migrations, and creates
// the prepared state.
func (m *SessionMigrator) Start(
	ctx context.Context,
	appID, from, to string,
) (string, error) {
	if from != "redis" || to != "mysql" {
		return "", errors.New("only redis to mysql Session migration is supported")
	}
	app, err := m.apps.GetCurrentApp(ctx, appID)
	if err != nil {
		return "", fmt.Errorf("resolve migration app: %w", err)
	}
	if err := m.checkStartConflicts(ctx, app, from); err != nil {
		return "", err
	}
	if _, err := m.factory.SessionServiceFor(app, from); err != nil {
		return "", fmt.Errorf("prepare source backend: %w", err)
	}
	if _, err := m.factory.SessionServiceFor(app, to); err != nil {
		return "", fmt.Errorf("prepare target backend: %w", err)
	}
	id, err := migrationID()
	if err != nil {
		return "", err
	}
	status := MigrationStatus{
		ID: id, AppID: appID, FromBackend: from, ToBackend: to,
		Phase: PhasePrepared, Detail: map[string]any{}, UpdatedAt: time.Now(),
	}
	// Persist before applying the route: durable state is the source of truth
	// from which every replica resolves migration routes.
	if err := m.store.Create(ctx, status); err != nil {
		return "", fmt.Errorf("create migration: %w", err)
	}
	if err := m.applyRoute(status); err != nil {
		return "", err
	}
	return id, nil
}

// checkStartConflicts prevents a new migration from silently reverting an
// app's live routing: it rejects in-flight migrations and requires the source
// backend to match the app's effective session backend (the target of the
// latest completed migration, or the configured backend otherwise).
func (m *SessionMigrator) checkStartConflicts(
	ctx context.Context,
	app tenant.AgentApp,
	from string,
) error {
	statuses, err := m.store.ListActive(ctx)
	if err != nil {
		return fmt.Errorf("list migrations: %w", err)
	}
	effective := app.Backends.Session
	var latestDone MigrationStatus
	for _, status := range statuses {
		if status.AppID != app.ID {
			continue
		}
		if status.Phase != PhaseDone {
			return fmt.Errorf(
				"%w: app %q already has migration %q in phase %q",
				ErrMigrationConflict, app.ID, status.ID, status.Phase,
			)
		}
		if status.UpdatedAt.After(latestDone.UpdatedAt) {
			latestDone = status
		}
	}
	if latestDone.ID != "" {
		effective = latestDone.ToBackend
	}
	if from != effective {
		return fmt.Errorf(
			"%w: migration source backend %q does not match the app's effective session backend %q",
			ErrMigrationConflict, from, effective,
		)
	}
	return nil
}

// Advance performs one complete transition.
func (m *SessionMigrator) Advance(ctx context.Context, id string) (MigrationPhase, error) {
	status, err := m.store.Get(ctx, id)
	if err != nil {
		return "", err
	}
	app, err := m.apps.GetCurrentApp(ctx, status.AppID)
	if err != nil {
		return "", err
	}
	switch status.Phase {
	case PhasePrepared:
		status.Phase = PhaseDualWrite
	case PhaseDualWrite:
		count, err := m.backfill(ctx, app, status)
		if err != nil {
			return "", err
		}
		status.Detail["backfilled_sessions"] = count
		status.Phase = PhaseBackfill
	case PhaseBackfill:
		count, err := m.verify(ctx, app, status)
		if err != nil {
			return "", err
		}
		status.Detail["verified_sessions"] = count
		status.Phase = PhaseVerify
	case PhaseVerify:
		status.Phase = PhaseCutRead
	case PhaseCutRead:
		status.Phase = PhaseStopOldWrite
	case PhaseStopOldWrite:
		status.Phase = PhaseDone
	case PhaseDone, PhaseRolledBack:
		return status.Phase, nil
	default:
		return "", fmt.Errorf("unsupported migration phase %q", status.Phase)
	}
	status.UpdatedAt = time.Now()
	// Persist first: other replicas resolve routes from the migration store,
	// so the durable phase must never trail the local route.
	if err := m.store.Update(ctx, status); err != nil {
		return "", err
	}
	if err := m.applyRoute(status); err != nil {
		return "", err
	}
	return status.Phase, nil
}

// Rollback returns reads and writes to the source backend.
func (m *SessionMigrator) Rollback(ctx context.Context, id string) error {
	status, err := m.store.Get(ctx, id)
	if err != nil {
		return err
	}
	status.Phase = PhaseRolledBack
	status.UpdatedAt = time.Now()
	if err := m.store.Update(ctx, status); err != nil {
		return err
	}
	return m.applyRoute(status)
}

// Status returns the persisted migration state.
func (m *SessionMigrator) Status(ctx context.Context, id string) (MigrationStatus, error) {
	return m.store.Get(ctx, id)
}

// Resume restores routes for migrations that survived a process restart.
func (m *SessionMigrator) Resume(ctx context.Context) error {
	statuses, err := m.store.ListActive(ctx)
	if err != nil {
		return err
	}
	for _, status := range statuses {
		if err := m.applyRoute(status); err != nil {
			return fmt.Errorf("restore migration %q: %w", status.ID, err)
		}
	}
	return nil
}

func (m *SessionMigrator) applyRoute(status MigrationStatus) error {
	route, err := routeForPhase(status)
	if err != nil {
		return err
	}
	return m.factory.SetSessionRoute(status.AppID, route)
}

// routeForPhase maps a persisted migration phase to its session route. It is
// the single mapping shared by the local migrator and the durable route
// source so every replica derives the same route.
func routeForPhase(status MigrationStatus) (SessionRoute, error) {
	switch status.Phase {
	case PhasePrepared, PhaseRolledBack:
		return SessionRoute{
			Reader: status.FromBackend, Writers: []string{status.FromBackend},
		}, nil
	case PhaseDualWrite, PhaseBackfill, PhaseVerify:
		return SessionRoute{
			Reader:  status.FromBackend,
			Writers: []string{status.FromBackend, status.ToBackend},
		}, nil
	case PhaseCutRead:
		return SessionRoute{
			Reader:  status.ToBackend,
			Writers: []string{status.FromBackend, status.ToBackend},
		}, nil
	case PhaseStopOldWrite, PhaseDone:
		return SessionRoute{
			Reader: status.ToBackend, Writers: []string{status.ToBackend},
		}, nil
	default:
		return SessionRoute{}, fmt.Errorf("cannot route migration phase %q", status.Phase)
	}
}

// MigrationRouteSource adapts a MigrationStore into a factory RouteSource so
// replicas that never ran the migration still resolve its route.
type MigrationRouteSource struct {
	store MigrationStore
}

// NewMigrationRouteSource resolves session routes from durable migration
// state.
func NewMigrationRouteSource(store MigrationStore) *MigrationRouteSource {
	return &MigrationRouteSource{store: store}
}

// ActiveRoute returns the route of the app's most recently updated migration.
func (s *MigrationRouteSource) ActiveRoute(
	ctx context.Context,
	appID string,
) (SessionRoute, bool, error) {
	statuses, err := s.store.ListActive(ctx)
	if err != nil {
		return SessionRoute{}, false, err
	}
	var latest *MigrationStatus
	for index := range statuses {
		if statuses[index].AppID != appID {
			continue
		}
		if latest == nil || statuses[index].UpdatedAt.After(latest.UpdatedAt) {
			latest = &statuses[index]
		}
	}
	if latest == nil {
		return SessionRoute{}, false, nil
	}
	route, err := routeForPhase(*latest)
	if err != nil {
		return SessionRoute{}, false, err
	}
	return route, true, nil
}

func (m *SessionMigrator) backfill(
	ctx context.Context,
	app tenant.AgentApp,
	status MigrationStatus,
) (int, error) {
	keys, err := m.catalog.ListSessionKeys(ctx, app.ID)
	if err != nil {
		return 0, fmt.Errorf("list migration sessions: %w", err)
	}
	source, err := m.factory.SessionServiceFor(app, status.FromBackend)
	if err != nil {
		return 0, err
	}
	target, err := m.factory.SessionServiceFor(app, status.ToBackend)
	if err != nil {
		return 0, err
	}
	copied := 0
	for _, key := range keys {
		key := key
		if err := m.locker.WithSessionLock(ctx, key.SessionID, func() error {
			return copySession(ctx, source, target, key)
		}); err != nil {
			return copied, fmt.Errorf("backfill session %q: %w", key.SessionID, err)
		}
		copied++
	}
	return copied, nil
}

func (m *SessionMigrator) verify(
	ctx context.Context,
	app tenant.AgentApp,
	status MigrationStatus,
) (int, error) {
	keys, err := m.catalog.ListSessionKeys(ctx, app.ID)
	if err != nil {
		return 0, err
	}
	source, err := m.factory.SessionServiceFor(app, status.FromBackend)
	if err != nil {
		return 0, err
	}
	target, err := m.factory.SessionServiceFor(app, status.ToBackend)
	if err != nil {
		return 0, err
	}
	for _, key := range keys {
		sourceSession, err := source.GetSession(ctx, key)
		if err != nil {
			return 0, err
		}
		targetSession, err := target.GetSession(ctx, key)
		if err != nil {
			return 0, err
		}
		if sessionDigest(sourceSession) != sessionDigest(targetSession) {
			if err := copySession(ctx, source, target, key); err != nil {
				return 0, err
			}
			targetSession, err = target.GetSession(ctx, key)
			if err != nil || sessionDigest(sourceSession) != sessionDigest(targetSession) {
				return 0, fmt.Errorf("session %q failed migration verification", key.SessionID)
			}
		}
	}
	return len(keys), nil
}

func copySession(
	ctx context.Context,
	source session.Service,
	target session.Service,
	key session.Key,
) error {
	current, err := source.GetSession(ctx, key)
	if err != nil || current == nil {
		return err
	}
	existing, err := target.GetSession(ctx, key)
	if err != nil {
		return err
	}
	if existing != nil {
		if err := target.DeleteSession(ctx, key); err != nil {
			return err
		}
	}
	created, err := target.CreateSession(ctx, key, cloneState(current.State))
	if err != nil {
		return err
	}
	for index := range current.Events {
		eventCopy := current.Events[index]
		if err := target.AppendEvent(ctx, created, &eventCopy); err != nil {
			return err
		}
	}
	return nil
}

func sessionDigest(current *session.Session) [sha256.Size]byte {
	if current == nil {
		return sha256.Sum256(nil)
	}
	payload, _ := json.Marshal(struct {
		State  session.StateMap
		Events any
	}{State: current.State, Events: current.Events})
	return sha256.Sum256(payload)
}

func migrationID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("generate migration ID: %w", err)
	}
	return hex.EncodeToString(value[:]), nil
}

type noOpMigrationLocker struct{}

func (noOpMigrationLocker) WithSessionLock(
	_ context.Context,
	_ string,
	operation func() error,
) error {
	return operation()
}

// MySQLMigrationStore persists the migration table.
type MySQLMigrationStore struct {
	db *sql.DB
}

// NewMySQLMigrationStore constructs a migration store.
func NewMySQLMigrationStore(db *sql.DB) (*MySQLMigrationStore, error) {
	if db == nil {
		return nil, errors.New("migration database is required")
	}
	return &MySQLMigrationStore{db: db}, nil
}

func (s *MySQLMigrationStore) Create(ctx context.Context, status MigrationStatus) error {
	detail, err := json.Marshal(status.Detail)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO migration
		(migration_id, app_id, from_backend, to_backend, phase, detail_json, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		status.ID, status.AppID, status.FromBackend, status.ToBackend,
		status.Phase, detail, status.UpdatedAt,
	)
	return err
}

func (s *MySQLMigrationStore) Get(ctx context.Context, id string) (MigrationStatus, error) {
	var status MigrationStatus
	var detail []byte
	err := s.db.QueryRowContext(ctx, `SELECT migration_id, app_id, from_backend,
		to_backend, phase, detail_json, updated_at FROM migration WHERE migration_id = ?`,
		id,
	).Scan(
		&status.ID, &status.AppID, &status.FromBackend, &status.ToBackend,
		&status.Phase, &detail, &status.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return MigrationStatus{}, tenant.ErrNotFound
	}
	if err != nil {
		return MigrationStatus{}, err
	}
	if len(detail) > 0 {
		if err := json.Unmarshal(detail, &status.Detail); err != nil {
			return MigrationStatus{}, err
		}
	}
	if status.Detail == nil {
		status.Detail = map[string]any{}
	}
	return status, nil
}

func (s *MySQLMigrationStore) Update(ctx context.Context, status MigrationStatus) error {
	detail, err := json.Marshal(status.Detail)
	if err != nil {
		return err
	}
	result, err := s.db.ExecContext(ctx, `UPDATE migration SET phase = ?,
		detail_json = ?, updated_at = ? WHERE migration_id = ?`,
		status.Phase, detail, status.UpdatedAt, status.ID,
	)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return tenant.ErrNotFound
	}
	return nil
}

func (s *MySQLMigrationStore) ListActive(ctx context.Context) ([]MigrationStatus, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT migration_id, app_id, from_backend,
		to_backend, phase, detail_json, updated_at FROM migration
		WHERE phase <> ? ORDER BY updated_at`,
		PhaseRolledBack,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []MigrationStatus
	for rows.Next() {
		var status MigrationStatus
		var detail []byte
		if err := rows.Scan(
			&status.ID, &status.AppID, &status.FromBackend, &status.ToBackend,
			&status.Phase, &detail, &status.UpdatedAt,
		); err != nil {
			return nil, err
		}
		if len(detail) > 0 {
			if err := json.Unmarshal(detail, &status.Detail); err != nil {
				return nil, err
			}
		}
		if status.Detail == nil {
			status.Detail = map[string]any{}
		}
		result = append(result, status)
	}
	return result, rows.Err()
}
