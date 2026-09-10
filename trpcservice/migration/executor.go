package migration

import (
	"context"
	"errors"
	"fmt"
	"net"

	platformknowledge "github.com/liuzengh/trpc-agent-service/trpcservice/knowledge"
	platformlog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

// SessionCatalog lists every logical Session in the platform scope being migrated.
// The catalog must be platform-owned rather than discovered by scanning backend keys.
type SessionCatalog interface {
	ListDataMigrationSessionKeys(context.Context, Record) ([]session.Key, error)
}

// KnowledgeCatalog lists authorized Knowledge chunks from platform SQL
// metadata. Implementations must not discover tenant ownership by scanning a
// Qdrant collection.
type KnowledgeCatalog interface {
	ListKnowledgeMigrationChunks(context.Context, Record) ([]platformknowledge.ChunkRef, error)
}

// Repository owns durable migration state transitions.
type Repository interface {
	AdvanceDataMigration(context.Context, Record, Status) error
}

// CheckpointRepository persists resumable copy/verify progress. It is kept
// separate from Repository so small in-memory test repositories can still
// exercise the state machine without pretending to be durable.
type CheckpointRepository interface {
	UpdateDataMigrationCheckpoint(context.Context, Record) error
}

// SessionCopier moves and verifies one logical Session between fixed source and
// target providers. It must preserve Event, State, and Summary data.
type SessionCopier interface {
	CopySession(context.Context, session.Key) error
	VerifySession(context.Context, session.Key) error
}

// KnowledgeCopier moves and verifies one authorized vector chunk between
// fixed Qdrant source and target collections.
type KnowledgeCopier interface {
	CopyKnowledgeChunk(context.Context, platformknowledge.ChunkRef) error
	VerifyKnowledgeChunk(context.Context, platformknowledge.ChunkRef) error
}

// Executor performs the copy and verification phases of one claimed migration.
// Draining is entered by the control plane before Run is called.
type Executor struct {
	Catalog          SessionCatalog
	KnowledgeCatalog KnowledgeCatalog
	Repository       Repository
	Copier           SessionCopier
	KnowledgeCopier  KnowledgeCopier
	// AfterCopy is an optional lifecycle hook used by controlled fault tests.
	// It runs after the durable transition to VERIFYING and before the first
	// verification item. Production callers leave it nil.
	AfterCopy func(context.Context, Record) error
	// AfterCopyItem is an optional lifecycle hook used by controlled fault
	// tests. It runs after each durable copy checkpoint. Production callers
	// leave it nil.
	AfterCopyItem func(context.Context, Record) error
}

// Run advances a DRAINING migration through COPYING and VERIFYING to SUCCEEDED.
// A data failure is recorded as FAILED. Context cancellation and lease failures
// are returned to allow another owner to safely resume the claimed record.
func (e Executor) Run(ctx context.Context, record Record) error {
	if err := record.Validate(); err != nil {
		return err
	}
	if err := e.validate(record); err != nil {
		return err
	}
	if record.Status != StatusDraining && record.Status != StatusCopying && record.Status != StatusVerifying {
		return errors.New("data migration is not runnable")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if record.EffectiveDomain() == DomainKnowledge {
		return e.runKnowledge(ctx, record)
	}

	keys, err := e.Catalog.ListDataMigrationSessionKeys(ctx, record)
	if err != nil {
		cause := fmt.Errorf("list migration sessions: %w", err)
		if isRetryableError(err) {
			return e.retry(ctx, record, "enumerate", cause)
		}
		return e.fail(ctx, record, "enumerate", cause)
	}

	if record.Status == StatusDraining {
		if err := e.initializeCheckpoint(ctx, &record, len(keys)); err != nil {
			return err
		}
		if err := e.Repository.AdvanceDataMigration(ctx, record, StatusCopying); err != nil {
			if errors.Is(err, ErrDrainDeadlineExceeded) {
				return e.fail(ctx, record, "drain", err)
			}
			return err
		}
		record.Status = StatusCopying
	}
	if record.Status == StatusCopying {
		if err := e.initializeCheckpoint(ctx, &record, len(keys)); err != nil {
			return err
		}
		start := int(record.CopyProgress)
		for index := start; index < len(keys); index++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := e.Copier.CopySession(ctx, keys[index]); err != nil {
				cause := fmt.Errorf("copy session %d: %w", index, err)
				if isRetryableError(err) {
					return e.retry(ctx, record, "copy", cause)
				}
				return e.fail(ctx, record, "copy", cause)
			}
			record.CopyProgress = int64(index + 1)
			record.SuccessCount++
			if err := e.checkpoint(ctx, record); err != nil {
				return err
			}
			if e.AfterCopyItem != nil {
				if err := e.AfterCopyItem(ctx, record); err != nil {
					return err
				}
			}
		}
		record.SuccessCount = 0
		if err := e.checkpoint(ctx, record); err != nil {
			return err
		}
		if err := e.Repository.AdvanceDataMigration(ctx, record, StatusVerifying); err != nil {
			return err
		}
		record.Status = StatusVerifying
		if e.AfterCopy != nil {
			if err := e.AfterCopy(ctx, record); err != nil {
				return err
			}
		}
	}
	if record.Status == StatusVerifying {
		if err := e.initializeCheckpoint(ctx, &record, len(keys)); err != nil {
			return err
		}
		start := int(record.VerifyProgress)
		for index := start; index < len(keys); index++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := e.Copier.VerifySession(ctx, keys[index]); err != nil {
				cause := fmt.Errorf("verify session %d: %w", index, err)
				if isRetryableError(err) {
					return e.retry(ctx, record, "verify", cause)
				}
				return e.fail(ctx, record, "verify", cause)
			}
			record.VerifyProgress = int64(index + 1)
			record.SuccessCount++
			if err := e.checkpoint(ctx, record); err != nil {
				return err
			}
			if e.AfterCopyItem != nil {
				if err := e.AfterCopyItem(ctx, record); err != nil {
					return err
				}
			}
		}
		if err := e.Repository.AdvanceDataMigration(ctx, record, StatusSucceeded); err != nil {
			return err
		}
	}
	return nil
}

func (e Executor) runKnowledge(ctx context.Context, record Record) error {
	keys, err := e.KnowledgeCatalog.ListKnowledgeMigrationChunks(ctx, record)
	if err != nil {
		cause := fmt.Errorf("list knowledge migration chunks: %w", err)
		if isRetryableError(err) {
			return e.retry(ctx, record, "enumerate", cause)
		}
		return e.fail(ctx, record, "enumerate", cause)
	}

	if record.Status == StatusDraining {
		if err := e.initializeCheckpoint(ctx, &record, len(keys)); err != nil {
			return err
		}
		if err := e.Repository.AdvanceDataMigration(ctx, record, StatusCopying); err != nil {
			if errors.Is(err, ErrDrainDeadlineExceeded) {
				return e.fail(ctx, record, "drain", err)
			}
			return err
		}
		record.Status = StatusCopying
	}
	if record.Status == StatusCopying {
		if err := e.initializeCheckpoint(ctx, &record, len(keys)); err != nil {
			return err
		}
		start := int(record.CopyProgress)
		for index := start; index < len(keys); index++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := e.KnowledgeCopier.CopyKnowledgeChunk(ctx, keys[index]); err != nil {
				cause := fmt.Errorf("copy knowledge chunk %d: %w", index, err)
				if isRetryableError(err) {
					return e.retry(ctx, record, "copy", cause)
				}
				return e.fail(ctx, record, "copy", cause)
			}
			record.CopyProgress = int64(index + 1)
			record.SuccessCount++
			if err := e.checkpoint(ctx, record); err != nil {
				return err
			}
			if e.AfterCopyItem != nil {
				if err := e.AfterCopyItem(ctx, record); err != nil {
					return err
				}
			}
		}
		record.SuccessCount = 0
		if err := e.checkpoint(ctx, record); err != nil {
			return err
		}
		if err := e.Repository.AdvanceDataMigration(ctx, record, StatusVerifying); err != nil {
			return err
		}
		record.Status = StatusVerifying
		if e.AfterCopy != nil {
			if err := e.AfterCopy(ctx, record); err != nil {
				return err
			}
		}
	}
	if record.Status == StatusVerifying {
		if err := e.initializeCheckpoint(ctx, &record, len(keys)); err != nil {
			return err
		}
		start := int(record.VerifyProgress)
		for index := start; index < len(keys); index++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := e.KnowledgeCopier.VerifyKnowledgeChunk(ctx, keys[index]); err != nil {
				cause := fmt.Errorf("verify knowledge chunk %d: %w", index, err)
				if isRetryableError(err) {
					return e.retry(ctx, record, "verify", cause)
				}
				return e.fail(ctx, record, "verify", cause)
			}
			record.VerifyProgress = int64(index + 1)
			record.SuccessCount++
			if err := e.checkpoint(ctx, record); err != nil {
				return err
			}
		}
		if err := e.Repository.AdvanceDataMigration(ctx, record, StatusSucceeded); err != nil {
			return err
		}
	}
	return nil
}

func isRetryableError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, ErrDrainIncomplete) || errors.Is(err, ErrLeaseLost) {
		return true
	}
	var retryable interface{ IsRetryable() bool }
	if errors.As(err, &retryable) && retryable.IsRetryable() {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && (netErr.Timeout() || netErr.Temporary())
}

func (e Executor) fail(ctx context.Context, record Record, stage string, cause error) error {
	if ctx.Err() != nil {
		return cause
	}
	record.LastFailureStage = stage
	record.FailureReason = platformlog.SafeError(cause)
	if err := e.checkpoint(ctx, record); err != nil {
		return errors.Join(cause, err)
	}
	if err := e.Repository.AdvanceDataMigration(ctx, record, StatusFailed); err != nil {
		return errors.Join(cause, err)
	}
	return cause
}

func (e Executor) retry(ctx context.Context, record Record, stage string, cause error) error {
	record.LastFailureStage = stage
	record.FailureReason = platformlog.SafeError(cause)
	if err := e.checkpoint(ctx, record); err != nil {
		return errors.Join(cause, err)
	}
	return cause
}

func (e Executor) initializeCheckpoint(ctx context.Context, record *Record, total int) error {
	if record == nil {
		return errors.New("data migration record is required")
	}
	if total < 0 {
		return errors.New("data migration session total is invalid")
	}
	if record.TotalSessions == 0 {
		record.TotalSessions = int64(total)
	} else if record.TotalSessions != int64(total) {
		return e.fail(ctx, *record, "enumerate", errors.New("data migration inventory changed"))
	}
	if record.CopyProgress > int64(total) || record.VerifyProgress > int64(total) {
		return e.fail(ctx, *record, "enumerate", errors.New("data migration checkpoint is beyond current session inventory"))
	}
	return e.checkpoint(ctx, *record)
}

func (e Executor) checkpoint(ctx context.Context, record Record) error {
	repository, ok := e.Repository.(CheckpointRepository)
	if !ok {
		return nil
	}
	if err := repository.UpdateDataMigrationCheckpoint(ctx, record); err != nil {
		return err
	}
	return nil
}

func (e Executor) validate(record Record) error {
	if e.Repository == nil {
		return errors.New("data migration repository is required")
	}
	if record.EffectiveDomain() == DomainKnowledge {
		if e.KnowledgeCatalog == nil || e.KnowledgeCopier == nil {
			return errors.New("knowledge migration executor dependencies are required")
		}
		return nil
	}
	if e.Catalog == nil || e.Copier == nil {
		return errors.New("session migration executor dependencies are required")
	}
	return nil
}
