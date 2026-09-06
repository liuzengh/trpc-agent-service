package background

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
)

type KnowledgeMigrationPayload struct {
	MigrationID string `json:"migration_id"`
}

func (p *Processor) processKnowledgeMigration(ctx context.Context, job Job, revision controlplane.AgentRevision) error {
	var payload KnowledgeMigrationPayload
	if err := json.Unmarshal(job.Payload, &payload); err != nil {
		return err
	}
	scope, err := jobScope(job)
	if err != nil {
		return err
	}
	var done bool
	if job.Type == JobKnowledgeBackfill {
		done, err = p.knowledge.BackfillKnowledgeBatch(ctx, scope, revision, payload.MigrationID)
	} else {
		done, err = p.knowledge.VerifyKnowledgeBatch(ctx, scope, revision, payload.MigrationID)
	}
	if err != nil {
		return err
	}
	nextType := job.Type
	if done {
		m, err := p.control.GetBackendMigration(ctx, job.TenantID, payload.MigrationID)
		if err != nil {
			return err
		}
		mutable, ok := p.control.(controlplane.MutableRepository)
		if !ok {
			return errors.New("knowledge migration requires mutable control plane")
		}
		if job.Type == JobKnowledgeBackfill {
			if m.State == controlplane.MigrationBackfill {
				if _, err = mutable.TransitionBackendMigration(ctx, job.TenantID, m.ID, controlplane.MigrationVerify, m.Version, nil, nil); err != nil {
					return err
				}
			}
			nextType = JobKnowledgeVerify
		} else {
			_, err = mutable.TransitionBackendMigration(ctx, job.TenantID, m.ID, m.State, m.Version, nil, []byte(`{"passed":true,"source":"knowledge_sync"}`))
			return err
		}
	}
	// Bounded jobs, durable continuation before completion. Retrying this job
	// after enqueue-before-ACK resolves to the same continuation identity.
	_, err = p.jobs.Enqueue(ctx, EnqueueRequest{TenantID: job.TenantID, AppID: job.AppID, RevisionID: job.RevisionID, Type: nextType, DedupeKey: job.ID + ":next", Payload: job.Payload, TraceParent: job.TraceParent})
	return err
}
