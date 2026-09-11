package controlplane

import (
	"context"
	"errors"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/database"
)

type RevisionSequenceRepository interface {
	CreateNextRevision(context.Context, AgentRevision) (AgentRevision, error)
}

func (r *PostgresRepository) CreateNextRevision(ctx context.Context, revision AgentRevision) (AgentRevision, error) {
	var result AgentRevision
	err := database.InTransaction(ctx, r.db, func(ctx context.Context) error {
		var id string
		if err := r.dbFor(ctx).QueryRowContext(ctx, "SELECT app_id FROM agent_app WHERE tenant_id=$1 AND app_id=$2 FOR UPDATE", revision.TenantID, revision.AppID).Scan(&id); err != nil {
			return mapNotFound("app", err)
		}
		if existing, err := r.GetRevision(ctx, revision.TenantID, revision.ID); err == nil {
			if existing.AppID != revision.AppID || existing.Checksum != RevisionChecksum(revision) {
				return ErrConflict
			}
			result = existing
			return nil
		} else if !errors.Is(err, ErrNotFound) {
			return err
		}
		if err := r.dbFor(ctx).QueryRowContext(ctx, "SELECT COALESCE(max(revision_no),0)+1 FROM agent_revision WHERE tenant_id=$1 AND app_id=$2", revision.TenantID, revision.AppID).Scan(&revision.RevisionNo); err != nil {
			return err
		}
		revision.Checksum = RevisionChecksum(revision)
		revision.CreatedAt = time.Now().UTC()
		if err := r.CreateRevision(ctx, revision); err != nil {
			return err
		}
		result = revision
		return nil
	})
	return result, err
}

func (r *MemoryRepository) CreateNextRevision(_ context.Context, revision AgentRevision) (AgentRevision, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.apps[scopedKey(revision.TenantID, revision.AppID)]; !ok {
		return AgentRevision{}, ErrNotFound
	}
	key := scopedKey(revision.TenantID, revision.ID)
	if existing, ok := r.revisions[key]; ok {
		if existing.AppID != revision.AppID || existing.Checksum != RevisionChecksum(revision) {
			return AgentRevision{}, ErrConflict
		}
		return cloneRevision(existing), nil
	}
	revision.RevisionNo = 1
	for _, old := range r.revisions {
		if old.TenantID == revision.TenantID && old.AppID == revision.AppID && old.RevisionNo >= revision.RevisionNo {
			revision.RevisionNo = old.RevisionNo + 1
		}
	}
	revision.Checksum = RevisionChecksum(revision)
	revision.CreatedAt = time.Now().UTC()
	r.revisions[key] = cloneRevision(revision)
	return revision, nil
}
