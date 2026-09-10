package postgres

import (
	"context"
	"database/sql"
	"errors"

	"github.com/liuzengh/trpc-agent-service/trpcservice/migration/sessiondriver"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

// Replica applies a SessionImage to a target PostgreSQL binding. It always
// copies official session/postgres rows as opaque JSON. Full replicas also
// maintain a target coordination image for pre-cutover verification; SDK-only
// replicas are used for reverse repair after cutover, when the source control
// database already owns the current coordination rows.
type Replica struct {
	db                *sql.DB
	writeCoordination bool
}

func NewReplica(db *sql.DB) *Replica { return &Replica{db: db, writeCoordination: true} }

// NewSDKReplica writes framework-owned Session data without replacing the
// target's platform coordination journal. It is the only safe reverse target
// when a cut-over Worker has continued to commit fence/outbox facts centrally.
func NewSDKReplica(db *sql.DB) *Replica { return &Replica{db: db} }

func (r *Replica) ApplySessionSnapshot(ctx context.Context, in sessiondriver.ApplyRequest) (sessiondriver.ApplyResult, error) {
	if r == nil || r.db == nil {
		return sessiondriver.ApplyResult{}, runtime.ErrBackendUnavailable
	}
	if in.TenantID == "" || in.MigrationID == "" || in.MutationID == "" || in.Epoch < 1 ||
		in.Image.Head.TenantID != in.TenantID {
		return sessiondriver.ApplyResult{}, runtime.ErrTenantScope
	}
	digest, err := sessiondriver.SnapshotDigest(in.Image)
	if err != nil || digest != in.SnapshotDigest {
		return sessiondriver.ApplyResult{}, runtime.ErrInvariantViolation
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return sessiondriver.ApplyResult{}, mapError(err)
	}
	defer tx.Rollback()

	if result, found, err := existingApply(ctx, tx, in); err != nil || found {
		return result, err
	}
	if r.writeCoordination {
		currentVersion, exists, err := targetVersion(ctx, tx, in)
		if err != nil {
			return sessiondriver.ApplyResult{}, mapError(err)
		}
		if exists && currentVersion > in.Image.Head.Version {
			// A newer target image must never be overwritten. Record this mutation
			// as applied so repair can make forward progress; the source digest is
			// valid evidence for this apply attempt and Driver only accepts it when
			// target version strictly dominates the source version.
			if err := recordApply(ctx, tx, in, currentVersion); err != nil {
				return sessiondriver.ApplyResult{}, mapError(err)
			}
			if err := tx.Commit(); err != nil {
				return sessiondriver.ApplyResult{}, mapError(err)
			}
			return sessiondriver.ApplyResult{SessionVersion: currentVersion, SnapshotDigest: in.SnapshotDigest}, nil
		}
		if exists && currentVersion == in.Image.Head.Version {
			// The matching mutation receipt above is the only proof that an equal
			// version is the same image. Never overwrite an unproven equal version.
			return sessiondriver.ApplyResult{}, runtime.ErrVersionConflict
		}
		if err := replaceCoordination(ctx, tx, in.Image); err != nil {
			return sessiondriver.ApplyResult{}, mapError(err)
		}
	}
	if err := replaceSDKSession(ctx, tx, in.Image); err != nil {
		return sessiondriver.ApplyResult{}, mapError(err)
	}
	if err := recordApply(ctx, tx, in, in.Image.Head.Version); err != nil {
		return sessiondriver.ApplyResult{}, mapError(err)
	}
	if err := tx.Commit(); err != nil {
		return sessiondriver.ApplyResult{}, mapError(err)
	}
	return sessiondriver.ApplyResult{SessionVersion: in.Image.Head.Version, SnapshotDigest: in.SnapshotDigest}, nil
}

func existingApply(ctx context.Context, tx *sql.Tx, in sessiondriver.ApplyRequest) (sessiondriver.ApplyResult, bool, error) {
	var epoch, sourceVersion, targetVersion int64
	var digest string
	err := tx.QueryRowContext(ctx, `SELECT epoch,source_version,snapshot_digest,target_version
FROM public.session_sdk_migration_apply
WHERE tenant_id=$1 AND migration_id=$2 AND agent_app_id=$3 AND session_id=$4 AND mutation_id=$5`,
		in.TenantID, in.MigrationID, in.Image.Head.AgentAppID, in.Image.Head.SessionID, in.MutationID).
		Scan(&epoch, &sourceVersion, &digest, &targetVersion)
	if errors.Is(err, sql.ErrNoRows) {
		return sessiondriver.ApplyResult{}, false, nil
	}
	if err != nil {
		return sessiondriver.ApplyResult{}, false, err
	}
	if epoch != in.Epoch || sourceVersion != in.Image.Head.Version || digest != in.SnapshotDigest {
		return sessiondriver.ApplyResult{}, true, runtime.ErrIdempotencyCollision
	}
	return sessiondriver.ApplyResult{SessionVersion: targetVersion, SnapshotDigest: digest}, true, nil
}

func targetVersion(ctx context.Context, tx *sql.Tx, in sessiondriver.ApplyRequest) (int64, bool, error) {
	var version int64
	err := tx.QueryRowContext(ctx, `SELECT version FROM public.session_head
WHERE tenant_id=$1 AND agent_app_id=$2 AND session_id=$3 FOR UPDATE`,
		in.Image.Head.TenantID, in.Image.Head.AgentAppID, in.Image.Head.SessionID).Scan(&version)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	return version, err == nil, err
}

func replaceCoordination(ctx context.Context, tx *sql.Tx, image sessiondriver.SessionImage) error {
	head := image.Head
	_, err := tx.ExecContext(ctx, `INSERT INTO public.session_head(
tenant_id,agent_app_id,session_id,version,last_fence,last_session_seq,next_input_seq,last_allocated_input_seq,state_json,summary_id,created_at,updated_at)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,'{}'::jsonb,NULL,now(),now())
ON CONFLICT (tenant_id,agent_app_id,session_id) DO UPDATE SET
version=EXCLUDED.version,last_fence=EXCLUDED.last_fence,last_session_seq=EXCLUDED.last_session_seq,
next_input_seq=EXCLUDED.next_input_seq,last_allocated_input_seq=EXCLUDED.last_allocated_input_seq,
state_json='{}'::jsonb,summary_id=NULL,updated_at=now()`,
		head.TenantID, head.AgentAppID, head.SessionID, head.Version, head.LastFence, head.LastSessionSeq,
		head.NextInputSeq, image.LastAllocatedInputSeq)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM public.session_commit
WHERE tenant_id=$1 AND agent_app_id=$2 AND session_id=$3`, head.TenantID, head.AgentAppID, head.SessionID); err != nil {
		return err
	}
	for _, commit := range image.Commits {
		if _, err := tx.ExecContext(ctx, `INSERT INTO public.session_commit(
tenant_id,agent_app_id,session_id,commit_id,request_id,request_digest,input_seq,stage,outcome,fence,session_version,reply_cursor,result_ref,created_at)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,NULLIF($12,''),NULLIF($13,''),$14)`,
			head.TenantID, head.AgentAppID, head.SessionID, commit.CommitID, commit.RequestID, commit.RequestDigest,
			commit.InputSeq, commit.Stage, string(commit.Outcome), commit.Fence, commit.SessionVersion,
			commit.ReplyCursor, commit.ResultRef, commit.CreatedAt.UTC()); err != nil {
			return err
		}
	}
	return nil
}

func replaceSDKSession(ctx context.Context, tx *sql.Tx, image sessiondriver.SessionImage) error {
	appName := image.Head.TenantID + "/" + image.Head.AgentAppID
	// SDK tables use soft deletes. Retiring active rows before inserting the
	// snapshot preserves the framework's active-row partial unique indexes.
	if _, err := tx.ExecContext(ctx, `UPDATE public.session_events SET deleted_at=now(),updated_at=now()
WHERE app_name=$1 AND session_id=$2 AND deleted_at IS NULL`, appName, image.Head.SessionID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE public.session_track_events SET deleted_at=now(),updated_at=now()
WHERE app_name=$1 AND session_id=$2 AND deleted_at IS NULL`, appName, image.Head.SessionID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE public.session_summaries SET deleted_at=now()
WHERE app_name=$1 AND session_id=$2 AND deleted_at IS NULL`, appName, image.Head.SessionID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE public.session_states SET deleted_at=now(),updated_at=now()
WHERE app_name=$1 AND session_id=$2 AND deleted_at IS NULL`, appName, image.Head.SessionID); err != nil {
		return err
	}
	if image.SDK == nil {
		return nil
	}
	sdk := image.SDK
	if sdk.AppName != appName || sdk.SessionID != image.Head.SessionID {
		return runtime.ErrTenantScope
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO public.session_states(
app_name,user_id,session_id,state,created_at,updated_at,expires_at)
VALUES($1,$2,$3,$4::jsonb,$5,$6,$7)`, sdk.AppName, sdk.UserID, sdk.SessionID, string(sdk.State),
		sdk.CreatedAt.UTC(), sdk.UpdatedAt.UTC(), sdk.ExpiresAt); err != nil {
		return err
	}
	for _, event := range sdk.Events {
		if _, err := tx.ExecContext(ctx, `INSERT INTO public.session_events(
app_name,user_id,session_id,event,created_at,updated_at,expires_at)
VALUES($1,$2,$3,$4::jsonb,$5,$6,$7)`, sdk.AppName, sdk.UserID, sdk.SessionID, string(event.Event),
			event.CreatedAt.UTC(), event.UpdatedAt.UTC(), event.ExpiresAt); err != nil {
			return err
		}
	}
	for _, event := range sdk.TrackEvents {
		if _, err := tx.ExecContext(ctx, `INSERT INTO public.session_track_events(
app_name,user_id,session_id,track,event,created_at,updated_at,expires_at)
VALUES($1,$2,$3,$4,$5::jsonb,$6,$7,$8)`, sdk.AppName, sdk.UserID, sdk.SessionID, event.Track,
			string(event.Event), event.CreatedAt.UTC(), event.UpdatedAt.UTC(), event.ExpiresAt); err != nil {
			return err
		}
	}
	for _, summary := range sdk.Summaries {
		if _, err := tx.ExecContext(ctx, `INSERT INTO public.session_summaries(
app_name,user_id,session_id,filter_key,summary,updated_at,expires_at)
VALUES($1,$2,$3,$4,$5::jsonb,$6,$7)`, sdk.AppName, sdk.UserID, sdk.SessionID, summary.FilterKey,
			string(summary.Summary), summary.UpdatedAt.UTC(), summary.ExpiresAt); err != nil {
			return err
		}
	}
	return nil
}

func recordApply(ctx context.Context, tx *sql.Tx, in sessiondriver.ApplyRequest, targetVersion int64) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO public.session_sdk_migration_apply(
tenant_id,migration_id,agent_app_id,session_id,mutation_id,epoch,source_version,snapshot_digest,target_version)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`, in.TenantID, in.MigrationID, in.Image.Head.AgentAppID,
		in.Image.Head.SessionID, in.MutationID, in.Epoch, in.Image.Head.Version, in.SnapshotDigest, targetVersion)
	return err
}

var _ sessiondriver.ReplicaWriter = (*Replica)(nil)
