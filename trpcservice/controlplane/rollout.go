// Rollouts: session-sticky percentage release of an app's revision.
//
// When a rollout is active, a subset of actors (determined by a stable hash
// of the actor key) are routed to the candidate revision instead of the app's
// current pointer. All actors in one conversation — same actor key — always
// land on the same revision, because the hash is evaluated once per session
// and never changes for that session: once a session is created, its revision
// is fixed regardless of what happens to the rollout later.
//
// Stopping a rollout sends the next new session to the current pointer; an
// existing session already fixed to the candidate keeps it (the revision was
// committed at session creation). A RollbackToRevision moves the pointer and
// stops the rollout in one operation.
package controlplane

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
)

// Rollout is one active or stopped revision rollout for an app.
type Rollout struct {
	RolloutID   int64
	TenantID    string
	AppID       int64
	RevisionID  int64
	BasisPoints uint32 // 0..10000 — share of actors routed to the candidate
	Status      string // "active" or "stopped"
	CreatedBy   string
	CreatedAt   string // not parsed; stored as the raw DB representation
	StoppedAt   string
	StoppedBy   string
}

// ErrActiveRolloutExists is returned when a StartRollout call would create a
// second active rollout for the same app.
var ErrActiveRolloutExists = errors.New("controlplane: an active rollout already exists for this app")

// StartRollout creates a new rollout that sends basisPoints/10000 of actors
// to the candidate revision. An existing active rollout is stopped first.
func (s Scope) StartRollout(ctx context.Context, appPublicID string, revisionID int64, basisPoints int, by string) (*Rollout, error) {
	if basisPoints <= 0 || basisPoints > 10000 {
		return nil, fmt.Errorf("controlplane: basis_points %d must be between 1 and 10000", basisPoints)
	}
	if by == "" {
		return nil, errors.New("controlplane: a rollout needs a creator identity")
	}

	app, err := s.GetApp(ctx, appPublicID)
	if err != nil {
		return nil, err
	}
	// Verify the candidate revision exists and belongs to this app.
	if err := s.verifyRevisionOwnership(ctx, app.ID, revisionID); err != nil {
		return nil, err
	}

	var out Rollout
	err = s.WithTx(ctx, func(tx *TxScope) error {
		// Stop any existing active rollout silently (idempotent).
		if _, err := tx.Exec(ctx, `
			UPDATE rollouts SET status = 'stopped', stopped_at = UTC_TIMESTAMP(6), stopped_by = ?
			WHERE tenant_id = ? AND app_id = ? AND status = 'active'`,
			by, s.tenantID, app.ID); err != nil {
			return fmt.Errorf("controlplane: stop previous rollout: %w", err)
		}

		res, err := tx.Exec(ctx, `
			INSERT INTO rollouts (tenant_id, app_id, revision_id, basis_points, status, created_by)
			VALUES (?, ?, ?, ?, 'active', ?)`,
			s.tenantID, app.ID, revisionID, basisPoints, by)
		if err != nil {
			return fmt.Errorf("controlplane: insert rollout: %w", err)
		}
		id, err := res.LastInsertId()
		if err != nil {
			return err
		}
		row := tx.QueryRow(ctx, `
			SELECT rollout_id, tenant_id, app_id, revision_id, basis_points, status, created_by
			FROM rollouts WHERE rollout_id = ?`, id)
		if err := row.Scan(
			&out.RolloutID, &out.TenantID, &out.AppID, &out.RevisionID,
			&out.BasisPoints, &out.Status, &out.CreatedBy); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &out, nil
}

// StopRollout deactivates the active rollout for an app.
func (s Scope) StopRollout(ctx context.Context, appPublicID, by string) error {
	app, err := s.GetApp(ctx, appPublicID)
	if err != nil {
		return err
	}
	res, err := s.Exec(ctx, `
		UPDATE rollouts SET status = 'stopped', stopped_at = UTC_TIMESTAMP(6), stopped_by = ?
		WHERE tenant_id = ? AND app_id = ? AND status = 'active'`,
		by, s.tenantID, app.ID)
	if err != nil {
		return fmt.Errorf("controlplane: stop rollout: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}

// GetActiveRollout returns the currently active rollout, or ErrNotFound.
func (s Scope) GetActiveRollout(ctx context.Context, appPublicID string) (*Rollout, error) {
	app, err := s.GetApp(ctx, appPublicID)
	if err != nil {
		return nil, err
	}
	var r Rollout
	row, err := s.QueryRow(ctx, `
		SELECT rollout_id, tenant_id, app_id, revision_id, basis_points, status, created_by
		FROM rollouts WHERE tenant_id = ? AND app_id = ? AND status = 'active'
		LIMIT 1`, s.tenantID, app.ID)
	if err != nil {
		return nil, err
	}
	switch err := row.Scan(&r.RolloutID, &r.TenantID, &r.AppID, &r.RevisionID,
		&r.BasisPoints, &r.Status, &r.CreatedBy); {
	case errors.Is(err, sql.ErrNoRows):
		return nil, ErrNotFound
	case err != nil:
		return nil, fmt.Errorf("controlplane: get rollout: %w", err)
	}
	return &r, nil
}

// ActiveRevisionForActor returns which revision an actor should use. When no
// rollout is active the app's current_revision_id is returned. When one is
// active the actor's hash slot decides: a slot below the basis points lands
// on the candidate revision, above stays on the current pointer. The actor
// is the stable identity of a conversation (the external user id).
func (s Scope) ActiveRevisionForActor(ctx context.Context, appPublicID, actorKey string) (revisionID int64, viaRollout bool, err error) {
	app, err := s.GetApp(ctx, appPublicID)
	if err != nil {
		return 0, false, err
	}

	rollout, err := s.GetActiveRollout(ctx, appPublicID)
	if errors.Is(err, ErrNotFound) {
		return app.CurrentRevisionID.Int64, false, nil
	}
	if err != nil {
		return 0, false, err
	}

	if actorKey == "" {
		return app.CurrentRevisionID.Int64, false, nil
	}

	// Deterministic hash: SHA256(actorKey) % 10000 < basis_points → candidate.
	h := sha256.Sum256([]byte(actorKey))
	slot := binary.LittleEndian.Uint64(h[:8]) % 10000

	if slot < uint64(rollout.BasisPoints) {
		return rollout.RevisionID, true, nil
	}
	return app.CurrentRevisionID.Int64, false, nil
}

// verifyRevisionOwnership checks that revisionID belongs to the specified app.
func (s Scope) verifyRevisionOwnership(ctx context.Context, appID int64, revisionID int64) error {
	var id int64
	row, err := s.QueryRow(ctx, "SELECT revision_id FROM agent_revisions WHERE tenant_id = ? AND app_id = ? AND revision_id = ?", s.tenantID, appID, revisionID)
	if err != nil {
		return err
	}
	if err := row.Scan(&id); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("controlplane: revision %d does not belong to app %d", revisionID, appID)
		}
		return err
	}
	return nil
}
