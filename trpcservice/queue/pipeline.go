package queue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cyl6/trpc-agent-service/trpcservice/dbbinding"
	"github.com/cyl6/trpc-agent-service/trpcservice/store"
	"github.com/cyl6/trpc-agent-service/trpcservice/worker"
)

// stampDurablePipeline overwrites caller-supplied rollout metadata at the
// trust boundary. Only a Queue store that exposes both a transaction
// participant and a database identity may require the combined SQL commit.
func (d *Durable) stampDurablePipeline(task *worker.Task) error {
	if task == nil {
		return fmt.Errorf("%w: nil task", worker.ErrUnsupportedPipeline)
	}
	metadata := worker.DurablePipelineMetadata{
		SchemaVersion:    worker.DurablePipelineVersion,
		AtomicCommitMode: worker.AtomicCommitDisabled,
	}
	if task.Tenant.Data.Session.Type != "sql" {
		task.Pipeline = metadata
		return nil
	}
	_, hasCompleter := d.store.(store.InboxBatchTxCompleter)
	provider, hasIdentity := d.store.(dbbinding.Provider)
	if !hasCompleter && !hasIdentity {
		// A durable in-memory/local Store cannot join a PostgreSQL Session
		// transaction. It remains on the explicit recoverable legacy boundary.
		task.Pipeline = metadata
		return nil
	}
	if !hasCompleter || !hasIdentity || provider.DatabaseIdentity() == "" {
		return worker.ErrAtomicCommitUnavailable
	}
	metadata.AtomicCommitMode = worker.AtomicCommitRequired
	metadata.DatabaseIdentity = provider.DatabaseIdentity()
	task.Pipeline = metadata
	return nil
}

func mirrorPipelineOnInbox(rec *store.InboxRecord, task worker.Task) {
	rec.PipelineSchemaVersion = task.Pipeline.SchemaVersion
	rec.AtomicCommitMode = string(task.Pipeline.AtomicCommitMode)
	rec.DatabaseIdentity = task.Pipeline.DatabaseIdentity
}

func pipelineFromInbox(rec *store.InboxRecord) worker.DurablePipelineMetadata {
	if rec == nil {
		return worker.DurablePipelineMetadata{}
	}
	return worker.DurablePipelineMetadata{
		SchemaVersion:    rec.PipelineSchemaVersion,
		AtomicCommitMode: worker.AtomicCommitMode(rec.AtomicCommitMode),
		DatabaseIdentity: rec.DatabaseIdentity,
	}
}

// prepareAtomicParticipant validates the independently stored Inbox columns
// against its JSON Task snapshot. Current-version drift is rejected as
// corruption; explicitly newer versions are parked by the caller. Legacy rows
// intentionally receive no participant and drain through the pre-upgrade
// two-transaction replay protocol.
func (d *Durable) prepareAtomicParticipant(
	rec *store.InboxRecord,
	task *worker.Task,
	owner string,
) (*inboxTurnCommitParticipant, error) {
	if rec == nil || task == nil {
		return nil, fmt.Errorf("%w: missing Inbox or Task", worker.ErrUnsupportedPipeline)
	}
	inboxMetadata := pipelineFromInbox(rec)
	if task.Pipeline != inboxMetadata {
		return nil, fmt.Errorf("%w: %w", worker.ErrUnsupportedPipeline, worker.ErrPipelineMetadataMismatch)
	}
	if err := task.Pipeline.Validate(); err != nil {
		return nil, err
	}
	if task.Pipeline.IsLegacy() {
		return nil, nil
	}
	if task.Pipeline.AtomicCommitMode == worker.AtomicCommitDisabled {
		return nil, nil
	}
	if task.Tenant.Data.Session.Type != "sql" {
		return nil, fmt.Errorf("%w: required mode has non-SQL Session", worker.ErrUnsupportedPipeline)
	}
	completer, ok := d.store.(store.InboxBatchTxCompleter)
	if !ok {
		return nil, worker.ErrAtomicCommitUnavailable
	}
	provider, ok := d.store.(dbbinding.Provider)
	if !ok || provider.DatabaseIdentity() == "" {
		return nil, worker.ErrAtomicCommitUnavailable
	}
	if provider.DatabaseIdentity() != task.Pipeline.DatabaseIdentity {
		return nil, worker.ErrAtomicDatabaseMismatch
	}
	return &inboxTurnCommitParticipant{
		durable: d, completer: completer, record: *rec,
		owner: owner, binding: task.Binding,
	}, nil
}

func pipelineFailureCategory(err error) string {
	switch {
	case errors.Is(err, worker.ErrFuturePipelineVersion):
		return "future_pipeline_version"
	case errors.Is(err, worker.ErrAtomicDatabaseMismatch):
		return "atomic_database_mismatch"
	case errors.Is(err, worker.ErrAtomicCommitUnavailable):
		return "atomic_capability_unavailable"
	default:
		return "unsupported_pipeline"
	}
}

func pipelineFailureShouldPark(err error) bool {
	return errors.Is(err, worker.ErrFuturePipelineVersion) ||
		errors.Is(err, worker.ErrAtomicCommitUnavailable) ||
		errors.Is(err, worker.ErrAtomicDatabaseMismatch)
}

// parkIncompatiblePipeline releases the lease with a long retry delay without
// consuming the ordinary max-attempt budget. This gives a compatible replica
// or restored database binding a chance to take the record while keeping the
// same Session lane blocked in order.
func (d *Durable) parkIncompatiblePipeline(
	ctx context.Context,
	rec *store.InboxRecord,
	owner string,
	err error,
) {
	delay := d.opts.RetryMax
	if delay <= 0 {
		delay = time.Minute
	}
	d.parkInboxAfter(ctx, rec, owner, pipelineFailureCategory(err), delay, true)
}
