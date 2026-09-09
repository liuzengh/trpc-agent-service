package platform

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

const ControlPlaneSchemaVersion = 2
const controlPlaneOperationTimeout = 5 * time.Second

type persistedVersionCreation struct {
	TenantID       string            `json:"tenant_id"`
	DeploymentID   string            `json:"deployment_id"`
	IdempotencyKey string            `json:"idempotency_key"`
	Config         string            `json:"config"`
	Version        DeploymentVersion `json:"version"`
}

type controlPlaneSnapshot struct {
	Tenants            map[string]Tenant              `json:"tenants"`
	Apps               map[string]AgentApp            `json:"agent_apps"`
	Deployments        map[string]Deployment          `json:"deployments"`
	Versions           map[string][]DeploymentVersion `json:"deployment_versions"`
	VersionCreations   []persistedVersionCreation     `json:"version_creations"`
	ChannelBindings    map[string]ChannelBinding      `json:"channel_bindings"`
	ProviderRoutes     map[string]BotRoute            `json:"provider_routes,omitempty"`
	BackendSelections  map[string]backendSelection    `json:"backend_selections,omitempty"`
	GovernancePolicies map[string]TenantPolicy        `json:"governance_policies,omitempty"`
}

type controlPlanePersistence interface {
	Load(context.Context) (controlPlaneSnapshot, int64, error)
	Save(context.Context, controlPlaneSnapshot, int64) (int64, error)
	Close() error
}

func controlPlaneOperationContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, controlPlaneOperationTimeout)
}

type sqliteControlPlanePersistence struct{ db *sql.DB }
type postgresControlPlanePersistence struct{ db *sql.DB }

// MigrateSQLiteControlPlane applies forward-only control-plane migrations.
func MigrateSQLiteControlPlane(path string) error {
	if path == "" {
		return errors.New("control plane SQLite path is required")
	}
	if path != ":memory:" {
		if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
			return fmt.Errorf("create control plane directory: %w", err)
		}
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return fmt.Errorf("open control plane: %w", err)
	}
	defer db.Close()
	ctx, cancel := controlPlaneOperationContext(context.Background())
	defer cancel()
	statements := []string{
		`CREATE TABLE IF NOT EXISTS control_plane_schema (singleton INTEGER PRIMARY KEY CHECK (singleton = 1), version INTEGER NOT NULL)`,
		`INSERT INTO control_plane_schema(singleton, version) VALUES (1, 2) ON CONFLICT(singleton) DO NOTHING`,
		`UPDATE control_plane_schema SET version = 2 WHERE singleton = 1 AND version < 2`,
		`CREATE TABLE IF NOT EXISTS control_plane_state (singleton INTEGER PRIMARY KEY CHECK (singleton = 1), revision INTEGER NOT NULL, payload BLOB NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS run_executions (tenant_id TEXT NOT NULL, session_id TEXT NOT NULL, request_id TEXT NOT NULL, input_hash TEXT NOT NULL, enqueue_order INTEGER NOT NULL, owner_id TEXT NOT NULL, fencing_token INTEGER NOT NULL, state TEXT NOT NULL, lease_expires_at TEXT NOT NULL, cancel_requested_at TEXT, terminal_type TEXT NOT NULL DEFAULT '', updated_at TEXT NOT NULL, PRIMARY KEY (tenant_id, session_id, request_id))`,
		`CREATE UNIQUE INDEX IF NOT EXISTS run_executions_one_owner_per_session ON run_executions(tenant_id, session_id) WHERE state = 'running'`,
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("migrate control plane: %w", err)
		}
	}
	return nil
}

func MigratePostgresControlPlane(dsn string) error {
	if dsn == "" {
		return errors.New("control plane PostgreSQL DSN is required")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return fmt.Errorf("open control plane: %w", err)
	}
	defer db.Close()
	ctx, cancel := controlPlaneOperationContext(context.Background())
	defer cancel()
	for _, statement := range []string{
		`CREATE TABLE IF NOT EXISTS control_plane_schema (singleton INTEGER PRIMARY KEY CHECK (singleton = 1), version INTEGER NOT NULL)`,
		`INSERT INTO control_plane_schema(singleton, version) VALUES (1, 2) ON CONFLICT(singleton) DO NOTHING`,
		`UPDATE control_plane_schema SET version = 2 WHERE singleton = 1 AND version < 2`,
		`CREATE TABLE IF NOT EXISTS control_plane_state (singleton INTEGER PRIMARY KEY CHECK (singleton = 1), revision BIGINT NOT NULL, payload BYTEA NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS session_execution_leases (tenant_id TEXT NOT NULL, session_id TEXT NOT NULL, owner_id TEXT NOT NULL, fencing_token BIGINT NOT NULL, expires_at TIMESTAMPTZ NOT NULL, PRIMARY KEY (tenant_id, session_id))`,
		`CREATE TABLE IF NOT EXISTS run_executions (tenant_id TEXT NOT NULL, session_id TEXT NOT NULL, request_id TEXT NOT NULL, input_hash TEXT NOT NULL, enqueue_order BIGSERIAL NOT NULL, owner_id TEXT NOT NULL, fencing_token BIGINT NOT NULL, state TEXT NOT NULL, lease_expires_at TIMESTAMPTZ NOT NULL, cancel_requested_at TIMESTAMPTZ, terminal_type TEXT NOT NULL DEFAULT '', updated_at TIMESTAMPTZ NOT NULL, PRIMARY KEY (tenant_id, session_id, request_id))`,
		`CREATE UNIQUE INDEX IF NOT EXISTS run_executions_one_owner_per_session ON run_executions(tenant_id, session_id) WHERE state = 'running'`,
	} {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("migrate control plane: %w", err)
		}
	}
	return nil
}

// NewSQLiteControlPlane opens an already-migrated development Control Plane
// Store. Service startup deliberately does not migrate the schema implicitly.
func NewSQLiteControlPlane(path string) (ControlPlaneStore, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open control plane: %w", err)
	}
	var version int
	ctx, cancel := controlPlaneOperationContext(context.Background())
	defer cancel()
	if err := db.QueryRowContext(ctx, `SELECT version FROM control_plane_schema WHERE singleton = 1`).Scan(&version); err != nil {
		db.Close()
		return nil, fmt.Errorf("control plane schema is not initialized: %w", err)
	}
	if version != ControlPlaneSchemaVersion {
		db.Close()
		return nil, fmt.Errorf("control plane schema version %d is incompatible with required version %d", version, ControlPlaneSchemaVersion)
	}
	platform := NewInMemoryControlPlane()
	persistence := &sqliteControlPlanePersistence{db: db}
	snapshot, revision, err := persistence.Load(ctx)
	if err != nil {
		db.Close()
		return nil, err
	}
	applyControlPlaneSnapshot(platform, snapshot)
	platform.persistence = persistence
	platform.persistenceRevision = revision
	return platform, nil
}

func NewPostgresControlPlane(dsn string) (ControlPlaneStore, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("open control plane: %w", err)
	}
	var version int
	ctx, cancel := controlPlaneOperationContext(context.Background())
	defer cancel()
	if err := db.QueryRowContext(ctx, `SELECT version FROM control_plane_schema WHERE singleton = 1`).Scan(&version); err != nil {
		db.Close()
		return nil, fmt.Errorf("control plane schema is not initialized: %w", err)
	}
	if version != ControlPlaneSchemaVersion {
		db.Close()
		return nil, fmt.Errorf("control plane schema version %d is incompatible with required version %d", version, ControlPlaneSchemaVersion)
	}
	persistence := &postgresControlPlanePersistence{db: db}
	return loadPersistentControlPlane(persistence)
}

func loadPersistentControlPlane(persistence controlPlanePersistence) (ControlPlaneStore, error) {
	platform := NewInMemoryControlPlane()
	ctx, cancel := controlPlaneOperationContext(context.Background())
	defer cancel()
	snapshot, revision, err := persistence.Load(ctx)
	if err != nil {
		persistence.Close()
		return nil, err
	}
	applyControlPlaneSnapshot(platform, snapshot)
	platform.persistence = persistence
	platform.persistenceRevision = revision
	return platform, nil
}

func (p *sqliteControlPlanePersistence) Load(ctx context.Context) (controlPlaneSnapshot, int64, error) {
	return loadControlPlaneSnapshot(ctx, p.db)
}

func loadControlPlaneSnapshot(ctx context.Context, db *sql.DB) (controlPlaneSnapshot, int64, error) {
	var payload []byte
	var revision int64
	err := db.QueryRowContext(ctx, `SELECT revision, payload FROM control_plane_state WHERE singleton = 1`).Scan(&revision, &payload)
	if errors.Is(err, sql.ErrNoRows) {
		return controlPlaneSnapshot{}, 0, nil
	}
	if err != nil {
		return controlPlaneSnapshot{}, 0, fmt.Errorf("load control plane: %w", err)
	}
	var snapshot controlPlaneSnapshot
	if err := json.Unmarshal(payload, &snapshot); err != nil {
		return controlPlaneSnapshot{}, 0, fmt.Errorf("decode control plane: %w", err)
	}
	return snapshot, revision, nil
}

func (p *sqliteControlPlanePersistence) Save(ctx context.Context, snapshot controlPlaneSnapshot, _ int64) (int64, error) {
	payload, err := json.Marshal(snapshot)
	if err != nil {
		return 0, fmt.Errorf("encode control plane: %w", err)
	}
	_, err = p.db.ExecContext(ctx, `INSERT INTO control_plane_state(singleton, revision, payload) VALUES (1, 1, ?)
		ON CONFLICT(singleton) DO UPDATE SET revision = control_plane_state.revision + 1, payload = excluded.payload`, payload)
	if err != nil {
		return 0, fmt.Errorf("save control plane: %w", err)
	}
	var revision int64
	if err := p.db.QueryRowContext(ctx, `SELECT revision FROM control_plane_state WHERE singleton = 1`).Scan(&revision); err != nil {
		return 0, fmt.Errorf("read control plane revision: %w", err)
	}
	return revision, nil
}

func (p *sqliteControlPlanePersistence) Close() error { return p.db.Close() }

func (p *postgresControlPlanePersistence) Load(ctx context.Context) (controlPlaneSnapshot, int64, error) {
	return loadControlPlaneSnapshot(ctx, p.db)
}

func (p *postgresControlPlanePersistence) Save(ctx context.Context, snapshot controlPlaneSnapshot, expectedRevision int64) (int64, error) {
	payload, err := json.Marshal(snapshot)
	if err != nil {
		return 0, fmt.Errorf("encode control plane: %w", err)
	}
	tx, err := p.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(83726104)`); err != nil {
		return 0, err
	}
	var current int64
	err = tx.QueryRowContext(ctx, `SELECT revision FROM control_plane_state WHERE singleton = 1 FOR UPDATE`).Scan(&current)
	if errors.Is(err, sql.ErrNoRows) {
		current = 0
	} else if err != nil {
		return 0, err
	}
	if current != expectedRevision {
		return 0, errors.New("control plane revision changed")
	}
	next := current + 1
	if _, err := tx.ExecContext(ctx, `INSERT INTO control_plane_state(singleton, revision, payload) VALUES (1, $1, $2)
		ON CONFLICT(singleton) DO UPDATE SET revision = excluded.revision, payload = excluded.payload`, next, payload); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return next, nil
}

func (p *postgresControlPlanePersistence) Close() error { return p.db.Close() }

func controlPlaneSnapshotFrom(platform *SnapshotControlPlane) controlPlaneSnapshot {
	snapshot := controlPlaneSnapshot{
		Tenants: platform.tenants, Apps: platform.apps, Deployments: platform.deployments,
		Versions:           platform.versions,
		ChannelBindings:    platform.channelBindings,
		ProviderRoutes:     platform.providerRoutes,
		BackendSelections:  platform.backendSelections,
		GovernancePolicies: platform.governancePolicies,
		VersionCreations:   make([]persistedVersionCreation, 0, len(platform.versionCreations)),
	}
	for key, creation := range platform.versionCreations {
		snapshot.VersionCreations = append(snapshot.VersionCreations, persistedVersionCreation{
			TenantID: key.tenantID, DeploymentID: key.deploymentID, IdempotencyKey: key.idempotencyKey,
			Config: creation.config, Version: creation.version,
		})
	}
	return snapshot
}

func applyControlPlaneSnapshot(platform *SnapshotControlPlane, snapshot controlPlaneSnapshot) {
	if snapshot.Tenants != nil {
		platform.tenants = snapshot.Tenants
		for id, tenant := range platform.tenants {
			tenant.AuditPolicy = normalizedAuditPolicy(tenant.AuditPolicy)
			platform.tenants[id] = tenant
		}
	}
	if snapshot.Apps != nil {
		platform.apps = snapshot.Apps
	}
	if snapshot.Deployments != nil {
		platform.deployments = snapshot.Deployments
	}
	if snapshot.Versions != nil {
		platform.versions = snapshot.Versions
	}
	if snapshot.ChannelBindings != nil {
		platform.channelBindings = snapshot.ChannelBindings
	}
	if snapshot.ProviderRoutes != nil {
		platform.providerRoutes = snapshot.ProviderRoutes
	}
	if snapshot.BackendSelections != nil {
		platform.backendSelections = snapshot.BackendSelections
	}
	if snapshot.GovernancePolicies != nil {
		platform.governancePolicies = snapshot.GovernancePolicies
	}
	for _, creation := range snapshot.VersionCreations {
		key := versionCreationKey{tenantID: creation.TenantID, deploymentID: creation.DeploymentID, idempotencyKey: creation.IdempotencyKey}
		platform.versionCreations[key] = versionCreation{config: creation.Config, version: creation.Version}
	}
}
