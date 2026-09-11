package worker

import (
	"errors"
	"fmt"
)

// DurablePipelineVersion identifies the persisted Task/Inbox protocol. A
// missing version is a legacy record written before atomic eligibility was
// explicit and must be drained through the recoverable two-transaction path.
const DurablePipelineVersion = 2

var (
	// ErrUnsupportedPipeline marks a durable payload that this binary cannot
	// safely execute. It is a corruption/rollout signal, not transient work.
	ErrUnsupportedPipeline = errors.New("worker: unsupported durable pipeline metadata")
	// ErrFuturePipelineVersion indicates that a newer producer wrote the Task.
	// An older compatible fleet member may still process it, so callers should
	// park/retry rather than permanently dead-letter it.
	ErrFuturePipelineVersion = errors.New("worker: durable pipeline version is newer than this binary")
	// ErrPipelineMetadataMismatch means the authoritative Inbox columns and
	// embedded Task disagree. This is persisted-data corruption, not rollout.
	ErrPipelineMetadataMismatch = errors.New("worker: Inbox and Task pipeline metadata mismatch")
	// ErrAtomicDatabaseMismatch prevents a required participant from running
	// against a Session database other than the one recorded at acceptance.
	ErrAtomicDatabaseMismatch = errors.New("worker: atomic queue/session database identity mismatch")
	// ErrAtomicCommitUnavailable means a current record required the combined
	// transaction but one of the runtime capabilities was absent.
	ErrAtomicCommitUnavailable = errors.New("worker: required atomic turn participant is unavailable")
)

// AtomicCommitMode controls whether a durable record is allowed to join its
// Inbox/Outbox completion to a strict SQL Session transaction.
type AtomicCommitMode string

const (
	// AtomicCommitDisabled is used by current non-SQL/non-PostgreSQL tasks.
	AtomicCommitDisabled AtomicCommitMode = "disabled"
	// AtomicCommitRequired fails closed unless Queue and Session expose the
	// exact database identity persisted when the Inbox was accepted.
	AtomicCommitRequired AtomicCommitMode = "postgres_same_database_v1"
)

// DurablePipelineMetadata is persisted inside Task and mirrored on Inbox.
// DatabaseIdentity is an opaque, credential-free digest; it is required only
// for AtomicCommitRequired.
type DurablePipelineMetadata struct {
	SchemaVersion    int              `json:"schema_version"`
	AtomicCommitMode AtomicCommitMode `json:"atomic_commit_mode"`
	DatabaseIdentity string           `json:"database_identity,omitempty"`
}

// validateAtomicRuntime is called after acquiring the immutable tenant
// runtime and before reading or writing a strict Session turn. Legacy Tasks
// are accepted unchanged; the Durable relay never attaches a participant to
// them, so they drain through the prior two-transaction protocol.
func validateAtomicRuntime(task Task, sessionDatabaseIdentity string) error {
	if task.Pipeline.IsLegacy() {
		if task.TurnCommitParticipant != nil {
			return fmt.Errorf("%w: legacy task has a participant", ErrAtomicCommitUnavailable)
		}
		return nil
	}
	if err := task.Pipeline.Validate(); err != nil {
		return err
	}
	switch task.Pipeline.AtomicCommitMode {
	case AtomicCommitDisabled:
		if task.TurnCommitParticipant != nil {
			return fmt.Errorf("%w: disabled task has a participant", ErrAtomicCommitUnavailable)
		}
		return nil
	case AtomicCommitRequired:
		if task.Tenant.Data.Session.Type != "sql" || task.TurnCommitParticipant == nil || sessionDatabaseIdentity == "" {
			return ErrAtomicCommitUnavailable
		}
		if sessionDatabaseIdentity != task.Pipeline.DatabaseIdentity {
			return ErrAtomicDatabaseMismatch
		}
		return nil
	default:
		return ErrUnsupportedPipeline
	}
}

// IsLegacy reports a Task written before explicit atomic eligibility. Legacy
// SQL records remain recoverable but intentionally use two transactions while
// a rolling upgrade drains them.
func (m DurablePipelineMetadata) IsLegacy() bool {
	return m.SchemaVersion == 0 && m.AtomicCommitMode == "" && m.DatabaseIdentity == ""
}

// Validate rejects ambiguous or future metadata. Legacy is deliberately
// valid so a new binary can drain pre-upgrade Inbox rows without enabling the
// combined transaction retroactively.
func (m DurablePipelineMetadata) Validate() error {
	if m.IsLegacy() {
		return nil
	}
	if m.SchemaVersion > DurablePipelineVersion {
		return fmt.Errorf("%w: %w: schema version %d", ErrUnsupportedPipeline, ErrFuturePipelineVersion, m.SchemaVersion)
	}
	if m.SchemaVersion != DurablePipelineVersion {
		return fmt.Errorf("%w: schema version %d", ErrUnsupportedPipeline, m.SchemaVersion)
	}
	switch m.AtomicCommitMode {
	case AtomicCommitDisabled:
		if m.DatabaseIdentity != "" {
			return fmt.Errorf("%w: disabled mode carries a database identity", ErrUnsupportedPipeline)
		}
		return nil
	case AtomicCommitRequired:
		if m.DatabaseIdentity == "" {
			return fmt.Errorf("%w: required mode has no database identity", ErrUnsupportedPipeline)
		}
		return nil
	default:
		return fmt.Errorf("%w: mode %q", ErrUnsupportedPipeline, m.AtomicCommitMode)
	}
}
