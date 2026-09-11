package configcontrol

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/cyl6/trpc-agent-service/trpcservice/config"
)

// PostgresStore is the persistent control-plane implementation. The pool is
// owned by the caller; Close is intentionally a no-op so Queue, Budget and
// ControlPlane can be migrated independently while sharing one database.
type PostgresStore struct {
	pool         *pgxpool.Pool
	heartbeatTTL time.Duration
}

func NewPostgres(pool *pgxpool.Pool) *PostgresStore {
	return &PostgresStore{pool: pool, heartbeatTTL: 30 * time.Second}
}

func NewPostgresWithTTL(pool *pgxpool.Pool, ttl time.Duration) *PostgresStore {
	store := NewPostgres(pool)
	if ttl > 0 {
		store.heartbeatTTL = ttl
	}
	return store
}

func (s *PostgresStore) Close() {}

func (s *PostgresStore) Bootstrap(ctx context.Context, tenants []config.TenantConfig, actor, reason string) error {
	if s == nil || s.pool == nil {
		return ErrControlUnavailable
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ErrControlUnavailable
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var exists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM config_tenant_state LIMIT 1)`).Scan(&exists); err != nil {
		return ErrControlUnavailable
	}
	if exists {
		return nil
	}
	if len(tenants) == 0 {
		return errors.New("config control: bootstrap requires at least one tenant")
	}
	seen := make(map[string]struct{}, len(tenants))
	for _, tenant := range tenants {
		if _, ok := seen[tenant.TenantID]; ok {
			return fmt.Errorf("config control: duplicate bootstrap tenant %s", tenant.TenantID)
		}
		seen[tenant.TenantID] = struct{}{}
		revision, err := makeRevision(RevisionInput{
			TenantID: tenant.TenantID, Revision: tenant.Version, Tenant: tenant,
			CreatedBy: actor, ChangeReason: reason,
		}, time.Now().UTC())
		if err != nil {
			return err
		}
		if err := insertRevision(ctx, tx, revision); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO config_tenant_state
				(tenant_id, active_revision, generation, updated_by)
			VALUES ($1, $2, 1, $3)
			ON CONFLICT (tenant_id) DO NOTHING`, tenant.TenantID, tenant.Version, actor); err != nil {
			return fmt.Errorf("config control: bootstrap state: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return ErrControlUnavailable
	}
	return nil
}

func (s *PostgresStore) EnsureTenant(ctx context.Context, tenant config.TenantConfig, actor, reason string) (TenantState, error) {
	if s == nil || s.pool == nil {
		return TenantState{}, ErrControlUnavailable
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return TenantState{}, ErrControlUnavailable
	}
	defer func() { _ = tx.Rollback(ctx) }()
	state, err := scanState(tx.QueryRow(ctx, `
		SELECT tenant_id, active_revision, canary_revision, rollout_percent, generation, updated_by, updated_at
		FROM config_tenant_state WHERE tenant_id=$1 FOR UPDATE`, tenant.TenantID))
	if err == nil {
		return state, nil
	}
	if !errors.Is(err, ErrTenantNotFound) {
		return TenantState{}, err
	}
	revision, err := makeRevision(RevisionInput{TenantID: tenant.TenantID, Revision: tenant.Version, Tenant: tenant, CreatedBy: actor, ChangeReason: reason}, time.Now().UTC())
	if err != nil {
		return TenantState{}, err
	}
	if err := insertRevision(ctx, tx, revision); err != nil {
		return TenantState{}, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO config_tenant_state (tenant_id, active_revision, generation, updated_by) VALUES ($1,$2,1,$3)`, tenant.TenantID, tenant.Version, actor); err != nil {
		return TenantState{}, ErrControlUnavailable
	}
	state = TenantState{TenantID: tenant.TenantID, ActiveRevision: tenant.Version, Generation: 1, UpdatedBy: actor, UpdatedAt: time.Now().UTC()}
	if err := tx.Commit(ctx); err != nil {
		return TenantState{}, ErrControlUnavailable
	}
	return state, nil
}

func (s *PostgresStore) PutRevision(ctx context.Context, input RevisionInput) (Revision, error) {
	if s == nil || s.pool == nil {
		return Revision{}, ErrControlUnavailable
	}
	revision, err := makeRevision(input, time.Now().UTC())
	if err != nil {
		return Revision{}, err
	}
	if err := insertRevision(ctx, s.pool, revision); err != nil {
		return Revision{}, err
	}
	existing, err := s.GetRevision(ctx, revision.TenantID, revision.Revision)
	if err != nil {
		return Revision{}, err
	}
	if existing.ConfigSHA256 != revision.ConfigSHA256 {
		return Revision{}, fmt.Errorf("%w: tenant %s revision %s", ErrRevisionConflict, revision.TenantID, revision.Revision)
	}
	return existing, nil
}

type execQuerier interface {
	Exec(context.Context, string, ...any) (pgconnCommandTag, error)
}

// pgconnCommandTag is satisfied by pgconn.CommandTag without exposing that
// implementation detail in the tiny insert helper's call sites.
type pgconnCommandTag interface{ RowsAffected() int64 }

type rowQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func insertRevision(ctx context.Context, q interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}, revision Revision) error {
	encoded, err := json.Marshal(revision.Tenant)
	if err != nil {
		return fmt.Errorf("config control: encode revision: %w", err)
	}
	_, err = q.Exec(ctx, `
		INSERT INTO config_tenant_revisions
			(tenant_id, revision, config, config_sha256, created_by, change_reason)
		VALUES ($1, $2, $3::jsonb, $4, $5, $6)
		ON CONFLICT (tenant_id, revision) DO NOTHING`,
		revision.TenantID, revision.Revision, encoded, revision.ConfigSHA256,
		revision.CreatedBy, revision.ChangeReason)
	if err != nil {
		return fmt.Errorf("config control: insert revision: %w", err)
	}
	return nil
}

func (s *PostgresStore) GetRevision(ctx context.Context, tenantID, revision string) (Revision, error) {
	if s == nil || s.pool == nil {
		return Revision{}, ErrControlUnavailable
	}
	var (
		result  Revision
		encoded []byte
	)
	var created time.Time
	err := s.pool.QueryRow(ctx, `
		SELECT tenant_id, revision, config, config_sha256, created_by, change_reason, created_at
		FROM config_tenant_revisions WHERE tenant_id = $1 AND revision = $2`, tenantID, revision).
		Scan(&result.TenantID, &result.Revision, &encoded, &result.ConfigSHA256, &result.CreatedBy, &result.ChangeReason, &created)
	if errors.Is(err, pgx.ErrNoRows) {
		return Revision{}, ErrRevisionNotFound
	}
	if err != nil {
		return Revision{}, ErrControlUnavailable
	}
	if err := json.Unmarshal(encoded, &result.Tenant); err != nil {
		return Revision{}, fmt.Errorf("config control: decode revision: %w", err)
	}
	result.CreatedAt = created
	return result, nil
}

func (s *PostgresStore) ListRevisions(ctx context.Context, tenantID string) ([]RevisionSummary, error) {
	if s == nil || s.pool == nil {
		return nil, ErrControlUnavailable
	}
	rows, err := s.pool.Query(ctx, `
		SELECT tenant_id, revision, config_sha256, created_by, change_reason, created_at
		FROM config_tenant_revisions WHERE tenant_id = $1 ORDER BY created_at, revision`, tenantID)
	if err != nil {
		return nil, ErrControlUnavailable
	}
	defer rows.Close()
	result := make([]RevisionSummary, 0)
	for rows.Next() {
		var item RevisionSummary
		if err := rows.Scan(&item.TenantID, &item.Revision, &item.ConfigSHA256, &item.CreatedBy, &item.ChangeReason, &item.CreatedAt); err != nil {
			return nil, ErrControlUnavailable
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, ErrControlUnavailable
	}
	if len(result) == 0 {
		return nil, ErrTenantNotFound
	}
	return result, nil
}

func (s *PostgresStore) GetState(ctx context.Context, tenantID string) (TenantState, error) {
	if s == nil || s.pool == nil {
		return TenantState{}, ErrControlUnavailable
	}
	return scanState(s.pool.QueryRow(ctx, `
		SELECT tenant_id, active_revision, canary_revision, rollout_percent, generation, updated_by, updated_at
		FROM config_tenant_state WHERE tenant_id = $1`, tenantID))
}

func scanState(row pgx.Row) (TenantState, error) {
	var state TenantState
	var canary pgtype.Text
	err := row.Scan(&state.TenantID, &state.ActiveRevision, &canary, &state.RolloutPercent, &state.Generation, &state.UpdatedBy, &state.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return TenantState{}, ErrTenantNotFound
	}
	if err != nil {
		return TenantState{}, ErrControlUnavailable
	}
	if canary.Valid {
		state.CanaryRevision = canary.String
	}
	return state, nil
}

func (s *PostgresStore) ListStates(ctx context.Context) ([]TenantState, error) {
	if s == nil || s.pool == nil {
		return nil, ErrControlUnavailable
	}
	rows, err := s.pool.Query(ctx, `
		SELECT tenant_id, active_revision, canary_revision, rollout_percent, generation, updated_by, updated_at
		FROM config_tenant_state ORDER BY tenant_id`)
	if err != nil {
		return nil, ErrControlUnavailable
	}
	defer rows.Close()
	result := make([]TenantState, 0)
	for rows.Next() {
		state, err := scanState(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, state)
	}
	if err := rows.Err(); err != nil {
		return nil, ErrControlUnavailable
	}
	return result, nil
}

func (s *PostgresStore) CreateRelease(ctx context.Context, request CreateReleaseRequest) (Release, error) {
	if s == nil || s.pool == nil {
		return Release{}, ErrControlUnavailable
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Release{}, ErrControlUnavailable
	}
	defer func() { _ = tx.Rollback(ctx) }()
	state, err := scanState(tx.QueryRow(ctx, `
		SELECT tenant_id, active_revision, canary_revision, rollout_percent, generation, updated_by, updated_at
		FROM config_tenant_state WHERE tenant_id = $1 FOR UPDATE`, request.TenantID))
	if err != nil {
		return Release{}, err
	}
	if request.ExpectedGeneration > 0 && request.ExpectedGeneration != state.Generation {
		return Release{}, fmt.Errorf("%w: expected %d, current %d", ErrGenerationConflict, request.ExpectedGeneration, state.Generation)
	}
	if request.ExpectedActive != "" && request.ExpectedActive != state.ActiveRevision {
		return Release{}, ErrGenerationConflict
	}
	if request.ExpectedCanary != "" && request.ExpectedCanary != state.CanaryRevision {
		return Release{}, ErrGenerationConflict
	}
	if request.Kind != ReleaseFull && request.Kind != ReleaseCanary && request.Kind != ReleasePromote && request.Kind != ReleaseRollback {
		return Release{}, fmt.Errorf("config control: unsupported release kind %q", request.Kind)
	}
	if request.TargetRevision == "" {
		return Release{}, errors.New("config control: target revision is required")
	}
	if request.Kind == ReleasePromote && (state.CanaryRevision == "" || request.TargetRevision != state.CanaryRevision) {
		return Release{}, ErrInvalidTransition
	}
	if request.TargetRolloutPercent < 0 || request.TargetRolloutPercent > 100 {
		return Release{}, errors.New("config control: rollout percent must be between 0 and 100")
	}
	var targetExists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM config_tenant_revisions WHERE tenant_id = $1 AND revision = $2)`, request.TenantID, request.TargetRevision).Scan(&targetExists); err != nil {
		return Release{}, ErrControlUnavailable
	}
	if !targetExists {
		return Release{}, ErrRevisionNotFound
	}
	var pending bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (
		SELECT 1 FROM config_releases WHERE tenant_id = $1 AND status IN ('preparing','active_pending_ack')
	)`, request.TenantID).Scan(&pending); err != nil {
		return Release{}, ErrControlUnavailable
	}
	if pending {
		return Release{}, ErrReleaseConflict
	}
	releaseID := uuid.NewString()
	var release Release
	var resulting pgtype.Int8
	var activated, verified, failed pgtype.Timestamptz
	err = tx.QueryRow(ctx, `
		INSERT INTO config_releases
			(release_id, tenant_id, kind, source_active_revision, source_canary_revision,
			 target_revision, target_rollout_percent, expected_generation, status,
			 requested_by, change_reason)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'preparing', $9, $10)
		RETURNING release_id, tenant_id, kind, source_active_revision, COALESCE(source_canary_revision, ''),
			target_revision, target_rollout_percent, expected_generation, resulting_generation,
			status, requested_by, change_reason, error_message, created_at, activated_at, verified_at, failed_at`,
		releaseID, request.TenantID, request.Kind, state.ActiveRevision, state.CanaryRevision,
		request.TargetRevision, request.TargetRolloutPercent, state.Generation,
		request.RequestedBy, request.ChangeReason).Scan(
		&release.ReleaseID, &release.TenantID, &release.Kind, &release.SourceActiveRevision,
		&release.SourceCanaryRevision, &release.TargetRevision, &release.TargetRolloutPercent,
		&release.ExpectedGeneration, &resulting, &release.Status,
		&release.RequestedBy, &release.ChangeReason, &release.ErrorMessage, &release.CreatedAt,
		&activated, &verified, &failed)
	if err != nil {
		return Release{}, ErrControlUnavailable
	}
	if resulting.Valid {
		release.ResultingGeneration = resulting.Int64
	}
	if activated.Valid {
		release.ActivatedAt = &activated.Time
	}
	if verified.Valid {
		release.VerifiedAt = &verified.Time
	}
	if failed.Valid {
		release.FailedAt = &failed.Time
	}
	rows, err := tx.Query(ctx, `
		SELECT node_id, boot_id FROM config_node_heartbeats
		WHERE last_seen_at >= clock_timestamp() - ($1::bigint * interval '1 microsecond')`,
		s.heartbeatTTL.Microseconds())
	if err != nil {
		return Release{}, ErrControlUnavailable
	}
	identities := make([]NodeIdentity, 0)
	for rows.Next() {
		var node NodeIdentity
		if err := rows.Scan(&node.NodeID, &node.BootID); err != nil {
			rows.Close()
			return Release{}, ErrControlUnavailable
		}
		identities = append(identities, node)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return Release{}, ErrControlUnavailable
	}
	for _, node := range identities {
		if _, err := tx.Exec(ctx, `
			INSERT INTO config_release_nodes (release_id, node_id, boot_id, status)
			VALUES ($1, $2, $3, 'pending')`, release.ReleaseID, node.NodeID, node.BootID); err != nil {
			return Release{}, ErrControlUnavailable
		}
		release.Nodes = append(release.Nodes, ReleaseNode{ReleaseID: release.ReleaseID, NodeID: node.NodeID, BootID: node.BootID, Status: NodePending, UpdatedAt: release.CreatedAt})
	}
	if err := insertReleaseEvent(ctx, tx, release, "created", release.Status, map[string]any{}); err != nil {
		return Release{}, err
	}
	if _, err := tx.Exec(ctx, `SELECT pg_notify($1, $2)`, NotifyChannel, release.ReleaseID); err != nil {
		return Release{}, ErrControlUnavailable
	}
	if err := tx.Commit(ctx); err != nil {
		return Release{}, ErrControlUnavailable
	}
	return release, nil
}

func insertReleaseEvent(ctx context.Context, q interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}, release Release, eventType string, status ReleaseStatus, details map[string]any) error {
	encoded, err := json.Marshal(details)
	if err != nil {
		return ErrControlUnavailable
	}
	_, err = q.Exec(ctx, `
		INSERT INTO config_release_events (release_id, tenant_id, event_type, status, actor, details)
		VALUES ($1, $2, $3, $4, $5, $6::jsonb)`, release.ReleaseID, release.TenantID,
		eventType, status, release.RequestedBy, encoded)
	if err != nil {
		return ErrControlUnavailable
	}
	return nil
}

func (s *PostgresStore) GetRelease(ctx context.Context, releaseID string) (Release, error) {
	if s == nil || s.pool == nil {
		return Release{}, ErrControlUnavailable
	}
	release, err := scanRelease(s.pool.QueryRow(ctx, `
		SELECT release_id, tenant_id, kind, source_active_revision, COALESCE(source_canary_revision, ''),
			target_revision, target_rollout_percent, expected_generation, resulting_generation,
			status, requested_by, change_reason, error_message, created_at, activated_at, verified_at, failed_at
		FROM config_releases WHERE release_id = $1`, releaseID))
	if errors.Is(err, pgx.ErrNoRows) || errors.Is(err, ErrReleaseNotFound) {
		return Release{}, ErrReleaseNotFound
	}
	if err != nil {
		return Release{}, err
	}
	release.Nodes, err = scanReleaseNodes(ctx, s.pool, releaseID)
	return release, err
}

func (s *PostgresStore) ListReleases(ctx context.Context, tenantID string) ([]Release, error) {
	if s == nil || s.pool == nil {
		return nil, ErrControlUnavailable
	}
	rows, err := s.pool.Query(ctx, `
		SELECT release_id, tenant_id, kind, source_active_revision, COALESCE(source_canary_revision, ''),
			target_revision, target_rollout_percent, expected_generation, resulting_generation,
			status, requested_by, change_reason, error_message, created_at, activated_at, verified_at, failed_at
		FROM config_releases WHERE tenant_id = $1 ORDER BY created_at, release_id`, tenantID)
	if err != nil {
		return nil, ErrControlUnavailable
	}
	result := make([]Release, 0)
	for rows.Next() {
		release, err := scanRelease(rows)
		if err != nil {
			rows.Close()
			return nil, ErrControlUnavailable
		}
		result = append(result, release)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, ErrControlUnavailable
	}
	for i := range result {
		result[i].Nodes, err = scanReleaseNodes(ctx, s.pool, result[i].ReleaseID)
		if err != nil {
			return nil, err
		}
	}
	if len(result) == 0 {
		return nil, ErrTenantNotFound
	}
	return result, nil
}

func (s *PostgresStore) ListPendingReleases(ctx context.Context) ([]Release, error) {
	if s == nil || s.pool == nil {
		return nil, ErrControlUnavailable
	}
	rows, err := s.pool.Query(ctx, `
		SELECT release_id, tenant_id, kind, source_active_revision, COALESCE(source_canary_revision, ''),
			target_revision, target_rollout_percent, expected_generation, resulting_generation,
			status, requested_by, change_reason, error_message, created_at, activated_at, verified_at, failed_at
		FROM config_releases WHERE status IN ('preparing','active_pending_ack') ORDER BY created_at, release_id`)
	if err != nil {
		return nil, ErrControlUnavailable
	}
	result := make([]Release, 0)
	for rows.Next() {
		release, err := scanRelease(rows)
		if err != nil {
			rows.Close()
			return nil, ErrControlUnavailable
		}
		result = append(result, release)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, ErrControlUnavailable
	}
	for i := range result {
		result[i].Nodes, err = scanReleaseNodes(ctx, s.pool, result[i].ReleaseID)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func scanRelease(row pgx.Row) (Release, error) {
	var release Release
	var resulting pgtype.Int8
	var activated, verified, failed pgtype.Timestamptz
	err := row.Scan(&release.ReleaseID, &release.TenantID, &release.Kind, &release.SourceActiveRevision,
		&release.SourceCanaryRevision, &release.TargetRevision, &release.TargetRolloutPercent,
		&release.ExpectedGeneration, &resulting, &release.Status,
		&release.RequestedBy, &release.ChangeReason, &release.ErrorMessage, &release.CreatedAt,
		&activated, &verified, &failed)
	if err != nil {
		return Release{}, err
	}
	if resulting.Valid {
		release.ResultingGeneration = resulting.Int64
	}
	if activated.Valid {
		release.ActivatedAt = &activated.Time
	}
	if verified.Valid {
		release.VerifiedAt = &verified.Time
	}
	if failed.Valid {
		release.FailedAt = &failed.Time
	}
	return release, nil
}

type rowsQuerier interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

func scanReleaseNodes(ctx context.Context, q rowsQuerier, releaseID string) ([]ReleaseNode, error) {
	rows, err := q.Query(ctx, `
		SELECT release_id, node_id, boot_id, status, loaded_revision, loaded_generation,
			error_message, prepared_at, applied_at, updated_at
		FROM config_release_nodes WHERE release_id = $1 ORDER BY node_id, boot_id`, releaseID)
	if err != nil {
		return nil, ErrControlUnavailable
	}
	defer rows.Close()
	result := make([]ReleaseNode, 0)
	for rows.Next() {
		var node ReleaseNode
		var loadedGeneration pgtype.Int8
		var prepared, applied pgtype.Timestamptz
		if err := rows.Scan(&node.ReleaseID, &node.NodeID, &node.BootID, &node.Status,
			&node.LoadedRevision, &loadedGeneration, &node.ErrorMessage,
			&prepared, &applied, &node.UpdatedAt); err != nil {
			return nil, ErrControlUnavailable
		}
		if loadedGeneration.Valid {
			node.LoadedGeneration = loadedGeneration.Int64
		}
		if prepared.Valid {
			node.PreparedAt = &prepared.Time
		}
		if applied.Valid {
			node.AppliedAt = &applied.Time
		}
		result = append(result, node)
	}
	if err := rows.Err(); err != nil {
		return nil, ErrControlUnavailable
	}
	return result, nil
}

func (s *PostgresStore) AckPrepared(ctx context.Context, releaseID string, node NodeIdentity, loadedRevision string, loadedGeneration int64, ackErr error) error {
	if s == nil || s.pool == nil {
		return ErrControlUnavailable
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ErrControlUnavailable
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var status ReleaseStatus
	if err := tx.QueryRow(ctx, `SELECT status FROM config_releases WHERE release_id = $1 FOR UPDATE`, releaseID).Scan(&status); errors.Is(err, pgx.ErrNoRows) {
		return ErrReleaseNotFound
	} else if err != nil {
		return ErrControlUnavailable
	}
	var nodeStatus NodeAckStatus
	if err := tx.QueryRow(ctx, `SELECT status FROM config_release_nodes WHERE release_id = $1 AND node_id = $2 AND boot_id = $3 FOR UPDATE`, releaseID, node.NodeID, node.BootID).Scan(&nodeStatus); errors.Is(err, pgx.ErrNoRows) {
		return ErrNodeNotRegistered
	} else if err != nil {
		return ErrControlUnavailable
	}
	if ackErr != nil {
		message := safeError(ackErr)
		if _, err := tx.Exec(ctx, `UPDATE config_release_nodes SET status='failed', error_message=$1, updated_at=clock_timestamp() WHERE release_id=$2 AND node_id=$3 AND boot_id=$4`, message, releaseID, node.NodeID, node.BootID); err != nil {
			return ErrControlUnavailable
		}
		if status != ReleaseVerified {
			if _, err := tx.Exec(ctx, `UPDATE config_releases SET status='failed', error_message=$1, failed_at=clock_timestamp() WHERE release_id=$2 AND status <> 'verified'`, message, releaseID); err != nil {
				return ErrControlUnavailable
			}
		}
		if err := insertSimpleEvent(ctx, tx, releaseID, "node_failed", ReleaseFailed, message); err != nil {
			return err
		}
		if err := notifyRelease(ctx, tx, releaseID); err != nil {
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return ErrControlUnavailable
		}
		return nil
	}
	if status != ReleasePreparing {
		if nodeStatus == NodePrepared || nodeStatus == NodeApplied {
			return nil
		}
		return ErrInvalidTransition
	}
	if nodeStatus == NodePrepared || nodeStatus == NodeApplied {
		return nil
	}
	if _, err := tx.Exec(ctx, `UPDATE config_release_nodes SET status='prepared', loaded_revision=$1, loaded_generation=$2, prepared_at=clock_timestamp(), updated_at=clock_timestamp() WHERE release_id=$3 AND node_id=$4 AND boot_id=$5`, loadedRevision, loadedGeneration, releaseID, node.NodeID, node.BootID); err != nil {
		return ErrControlUnavailable
	}
	if err := insertNodeEvent(ctx, tx, releaseID, "node_prepared", ReleasePreparing, node); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return ErrControlUnavailable
	}
	return nil
}

func insertSimpleEvent(ctx context.Context, tx pgx.Tx, releaseID, eventType string, status ReleaseStatus, message string) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO config_release_events (release_id, tenant_id, event_type, status, actor, details)
		SELECT release_id, tenant_id, $2, $3, requested_by, jsonb_build_object('error', $4::text)
		FROM config_releases WHERE release_id = $1`, releaseID, eventType, status, message)
	if err != nil {
		return ErrControlUnavailable
	}
	return nil
}

func insertNodeEvent(ctx context.Context, tx pgx.Tx, releaseID, eventType string, status ReleaseStatus, node NodeIdentity) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO config_release_events (release_id, tenant_id, event_type, status, actor, details)
		SELECT release_id, tenant_id, $2, $3, requested_by,
			jsonb_build_object('node_id', $4::text, 'boot_id', $5::text)
		FROM config_releases WHERE release_id = $1`, releaseID, eventType, status, node.NodeID, node.BootID)
	if err != nil {
		return ErrControlUnavailable
	}
	return nil
}

func notifyRelease(ctx context.Context, tx pgx.Tx, releaseID string) error {
	if _, err := tx.Exec(ctx, `SELECT pg_notify($1, $2)`, NotifyChannel, releaseID); err != nil {
		return ErrControlUnavailable
	}
	return nil
}

func (s *PostgresStore) ActivateIfReady(ctx context.Context, releaseID string) (Release, error) {
	if s == nil || s.pool == nil {
		return Release{}, ErrControlUnavailable
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Release{}, ErrControlUnavailable
	}
	defer func() { _ = tx.Rollback(ctx) }()
	release, err := scanRelease(tx.QueryRow(ctx, `
		SELECT release_id, tenant_id, kind, source_active_revision, COALESCE(source_canary_revision, ''),
			target_revision, target_rollout_percent, expected_generation, resulting_generation,
			status, requested_by, change_reason, error_message, created_at, activated_at, verified_at, failed_at
		FROM config_releases WHERE release_id = $1 FOR UPDATE`, releaseID))
	if errors.Is(err, pgx.ErrNoRows) {
		return Release{}, ErrReleaseNotFound
	}
	if err != nil {
		return Release{}, ErrControlUnavailable
	}
	if release.Status == ReleaseActivePending || release.Status == ReleaseVerified {
		if err := tx.Commit(ctx); err != nil {
			return Release{}, ErrControlUnavailable
		}
		got, getErr := s.GetRelease(ctx, releaseID)
		if getErr != nil {
			return Release{}, ErrControlUnavailable
		}
		return got, nil
	}
	if release.Status != ReleasePreparing {
		return Release{}, ErrInvalidTransition
	}
	var total, prepared int
	if err := tx.QueryRow(ctx, `SELECT count(*), count(*) FILTER (WHERE status='prepared') FROM config_release_nodes WHERE release_id=$1`, releaseID).Scan(&total, &prepared); err != nil {
		return Release{}, ErrControlUnavailable
	}
	if total == 0 || total != prepared {
		if err := tx.Commit(ctx); err != nil {
			return Release{}, ErrControlUnavailable
		}
		got, getErr := s.GetRelease(ctx, releaseID)
		if getErr != nil {
			return Release{}, ErrControlUnavailable
		}
		return got, nil
	}
	state, err := scanState(tx.QueryRow(ctx, `
		SELECT tenant_id, active_revision, canary_revision, rollout_percent, generation, updated_by, updated_at
		FROM config_tenant_state WHERE tenant_id=$1 FOR UPDATE`, release.TenantID))
	if err != nil {
		return Release{}, err
	}
	if state.Generation != release.ExpectedGeneration || state.ActiveRevision != release.SourceActiveRevision || state.CanaryRevision != release.SourceCanaryRevision {
		message := ErrGenerationConflict.Error()
		if _, err := tx.Exec(ctx, `UPDATE config_releases SET status='failed', error_message=$1, failed_at=clock_timestamp() WHERE release_id=$2`, message, releaseID); err != nil {
			return Release{}, ErrControlUnavailable
		}
		if err := insertSimpleEvent(ctx, tx, releaseID, "activation_failed", ReleaseFailed, message); err != nil {
			return Release{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return Release{}, ErrControlUnavailable
		}
		return s.GetRelease(ctx, releaseID)
	}
	var active, canary string
	rollout := 0
	switch release.Kind {
	case ReleaseCanary:
		active, canary, rollout = state.ActiveRevision, release.TargetRevision, release.TargetRolloutPercent
	case ReleasePromote, ReleaseFull, ReleaseRollback:
		active = release.TargetRevision
	default:
		return Release{}, ErrInvalidTransition
	}
	var generation int64
	if err := tx.QueryRow(ctx, `
		UPDATE config_tenant_state SET active_revision=$1, canary_revision=NULLIF($2,''), rollout_percent=$3,
			generation=generation+1, updated_by=$4, updated_at=clock_timestamp()
		WHERE tenant_id=$5 RETURNING generation`, active, canary, rollout, release.RequestedBy, release.TenantID).Scan(&generation); err != nil {
		return Release{}, ErrControlUnavailable
	}
	if _, err := tx.Exec(ctx, `UPDATE config_releases SET status='active_pending_ack', resulting_generation=$1, activated_at=clock_timestamp() WHERE release_id=$2`, generation, releaseID); err != nil {
		return Release{}, ErrControlUnavailable
	}
	release.ResultingGeneration = generation
	release.Status = ReleaseActivePending
	if err := insertSimpleEvent(ctx, tx, releaseID, "activated", ReleaseActivePending, ""); err != nil {
		return Release{}, ErrControlUnavailable
	}
	if err := notifyRelease(ctx, tx, releaseID); err != nil {
		return Release{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Release{}, ErrControlUnavailable
	}
	got, getErr := s.GetRelease(ctx, releaseID)
	if getErr != nil {
		return Release{}, ErrControlUnavailable
	}
	return got, nil
}

func (s *PostgresStore) AckApplied(ctx context.Context, releaseID string, node NodeIdentity, loadedRevision string, loadedGeneration int64, ackErr error) error {
	if s == nil || s.pool == nil {
		return ErrControlUnavailable
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ErrControlUnavailable
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var status ReleaseStatus
	var resulting pgtype.Int8
	if err := tx.QueryRow(ctx, `SELECT status, resulting_generation FROM config_releases WHERE release_id=$1 FOR UPDATE`, releaseID).Scan(&status, &resulting); errors.Is(err, pgx.ErrNoRows) {
		return ErrReleaseNotFound
	} else if err != nil {
		return ErrControlUnavailable
	}
	var nodeStatus NodeAckStatus
	if err := tx.QueryRow(ctx, `SELECT status FROM config_release_nodes WHERE release_id=$1 AND node_id=$2 AND boot_id=$3 FOR UPDATE`, releaseID, node.NodeID, node.BootID).Scan(&nodeStatus); errors.Is(err, pgx.ErrNoRows) {
		return ErrNodeNotRegistered
	} else if err != nil {
		return ErrControlUnavailable
	}
	if ackErr != nil {
		message := safeError(ackErr)
		if _, err := tx.Exec(ctx, `UPDATE config_release_nodes SET status='failed', error_message=$1, updated_at=clock_timestamp() WHERE release_id=$2 AND node_id=$3 AND boot_id=$4`, message, releaseID, node.NodeID, node.BootID); err != nil {
			return ErrControlUnavailable
		}
		if status != ReleaseVerified {
			if _, err := tx.Exec(ctx, `UPDATE config_releases SET status='failed', error_message=$1, failed_at=clock_timestamp() WHERE release_id=$2 AND status <> 'verified'`, message, releaseID); err != nil {
				return ErrControlUnavailable
			}
		}
		if err := insertSimpleEvent(ctx, tx, releaseID, "node_failed", ReleaseFailed, message); err != nil {
			return err
		}
		if err := notifyRelease(ctx, tx, releaseID); err != nil {
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return ErrControlUnavailable
		}
		return nil
	}
	if status == ReleaseVerified && nodeStatus == NodeApplied {
		return nil
	}
	if status != ReleaseActivePending || !resulting.Valid || loadedGeneration != resulting.Int64 {
		return ErrInvalidTransition
	}
	if nodeStatus == NodeApplied {
		return nil
	}
	if _, err := tx.Exec(ctx, `UPDATE config_release_nodes SET status='applied', loaded_revision=$1, loaded_generation=$2, applied_at=clock_timestamp(), updated_at=clock_timestamp() WHERE release_id=$3 AND node_id=$4 AND boot_id=$5`, loadedRevision, loadedGeneration, releaseID, node.NodeID, node.BootID); err != nil {
		return ErrControlUnavailable
	}
	if err := insertNodeEvent(ctx, tx, releaseID, "node_applied", ReleaseActivePending, node); err != nil {
		return err
	}
	var total, applied int
	if err := tx.QueryRow(ctx, `SELECT count(*), count(*) FILTER (WHERE status='applied') FROM config_release_nodes WHERE release_id=$1`, releaseID).Scan(&total, &applied); err != nil {
		return ErrControlUnavailable
	}
	if total > 0 && total == applied {
		if _, err := tx.Exec(ctx, `UPDATE config_releases SET status='verified', verified_at=clock_timestamp() WHERE release_id=$1 AND status='active_pending_ack'`, releaseID); err != nil {
			return ErrControlUnavailable
		}
		if err := insertSimpleEvent(ctx, tx, releaseID, "verified", ReleaseVerified, ""); err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return ErrControlUnavailable
	}
	return nil
}

func (s *PostgresStore) FailRelease(ctx context.Context, releaseID, message string) error {
	if s == nil || s.pool == nil {
		return ErrControlUnavailable
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ErrControlUnavailable
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var status ReleaseStatus
	if err := tx.QueryRow(ctx, `SELECT status FROM config_releases WHERE release_id=$1 FOR UPDATE`, releaseID).Scan(&status); errors.Is(err, pgx.ErrNoRows) {
		return ErrReleaseNotFound
	} else if err != nil {
		return ErrControlUnavailable
	}
	if status == ReleaseVerified {
		return ErrInvalidTransition
	}
	if _, err := tx.Exec(ctx, `UPDATE config_releases SET status='failed', error_message=$1, failed_at=clock_timestamp() WHERE release_id=$2`, message, releaseID); err != nil {
		return ErrControlUnavailable
	}
	if err := insertSimpleEvent(ctx, tx, releaseID, "failed", ReleaseFailed, message); err != nil {
		return err
	}
	if err := notifyRelease(ctx, tx, releaseID); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return ErrControlUnavailable
	}
	return nil
}

func (s *PostgresStore) Heartbeat(ctx context.Context, heartbeat NodeHeartbeat) error {
	if s == nil || s.pool == nil {
		return ErrControlUnavailable
	}
	if heartbeat.NodeID == "" || heartbeat.BootID == "" {
		return errors.New("config control: node_id and boot_id are required")
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO config_node_heartbeats
			(node_id, boot_id, last_seen_at, ready, loaded_generation, active_revision, canary_revision, error_message)
		VALUES ($1, $2, clock_timestamp(), $3, $4, COALESCE(NULLIF($5,''),''), COALESCE(NULLIF($6,''),''), COALESCE(NULLIF($7,''),''))
		ON CONFLICT (node_id, boot_id) DO UPDATE SET
			last_seen_at=clock_timestamp(), ready=EXCLUDED.ready, loaded_generation=EXCLUDED.loaded_generation,
			active_revision=EXCLUDED.active_revision, canary_revision=EXCLUDED.canary_revision,
			error_message=EXCLUDED.error_message, updated_at=clock_timestamp()`,
		heartbeat.NodeID, heartbeat.BootID, heartbeat.Ready, heartbeat.LoadedGeneration,
		heartbeat.ActiveRevision, heartbeat.CanaryRevision, heartbeat.ErrorMessage)
	if err != nil {
		return ErrControlUnavailable
	}
	return nil
}

func (s *PostgresStore) ListHeartbeats(ctx context.Context) ([]NodeHeartbeat, error) {
	if s == nil || s.pool == nil {
		return nil, ErrControlUnavailable
	}
	rows, err := s.pool.Query(ctx, `
		SELECT node_id, boot_id, last_seen_at, ready, loaded_generation, COALESCE(active_revision,''),
			COALESCE(canary_revision,''), COALESCE(error_message,''), updated_at
		FROM config_node_heartbeats
		WHERE last_seen_at >= clock_timestamp() - ($1::bigint * interval '1 microsecond')
		ORDER BY node_id, boot_id`, s.heartbeatTTL.Microseconds())
	if err != nil {
		return nil, ErrControlUnavailable
	}
	defer rows.Close()
	result := make([]NodeHeartbeat, 0)
	for rows.Next() {
		var heartbeat NodeHeartbeat
		if err := rows.Scan(&heartbeat.NodeID, &heartbeat.BootID, &heartbeat.LastSeenAt, &heartbeat.Ready, &heartbeat.LoadedGeneration, &heartbeat.ActiveRevision, &heartbeat.CanaryRevision, &heartbeat.ErrorMessage, &heartbeat.UpdatedAt); err != nil {
			return nil, ErrControlUnavailable
		}
		result = append(result, heartbeat)
	}
	if err := rows.Err(); err != nil {
		return nil, ErrControlUnavailable
	}
	return result, nil
}

// Listen keeps one pool connection subscribed to the control-plane channel.
// The caller owns retry/reconnect policy; returning on a driver or context
// error lets Controller fall back to its periodic poll and reconnect cleanly.
func (s *PostgresStore) Listen(ctx context.Context, onNotify func()) error {
	if s == nil || s.pool == nil {
		return ErrControlUnavailable
	}
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return ErrControlUnavailable
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `LISTEN `+NotifyChannel); err != nil {
		return ErrControlUnavailable
	}
	for {
		if _, err := conn.Conn().WaitForNotification(ctx); err != nil {
			return err
		}
		if onNotify != nil {
			onNotify()
		}
	}
}
