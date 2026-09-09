// Package datamigration contains the bounded, operator-controlled data
// migration protocol used when a tenant changes Session/Memory/Knowledge or
// Artifact backends. It deliberately separates copy mechanics from the
// backend adapters so a migration cannot silently claim support for a backend
// that has no implementation.
package datamigration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
)

type Domain string

const (
	DomainSession   Domain = "session"
	DomainMemory    Domain = "memory"
	DomainSummary   Domain = "summary"
	DomainArtifact  Domain = "artifact"
	DomainKnowledge Domain = "knowledge"
)

type Phase string

const (
	PhasePrepare        Phase = "PREPARE"
	PhaseSnapshotCopy   Phase = "SNAPSHOT_COPY"
	PhaseDualWrite      Phase = "DUAL_WRITE"
	PhaseCatchUp        Phase = "CATCH_UP"
	PhaseValidate       Phase = "VALIDATE"
	PhaseReadShadow     Phase = "READ_SHADOW"
	PhaseCutover        Phase = "CUTOVER"
	PhaseRollbackWindow Phase = "ROLLBACK_WINDOW"
	PhaseComplete       Phase = "COMPLETE"
	PhaseRolledBack     Phase = "ROLLED_BACK"
)

var (
	ErrMigrationNotFound   = errors.New("data migration was not found")
	ErrMigrationLeaseHeld  = errors.New("data migration lease is held by another owner")
	ErrMigrationFence      = errors.New("data migration lease fence is stale")
	ErrMigrationPaused     = errors.New("data migration is paused")
	ErrApprovalRequired    = errors.New("data migration requires explicit operator approval")
	ErrMigrationTerminal   = errors.New("data migration is terminal")
	ErrCursorStalled       = errors.New("data migration source cursor did not advance")
	ErrInvalidMigration    = errors.New("data migration is invalid")
	ErrInvalidRecord       = errors.New("data migration record is invalid")
	ErrMigrationConflict   = errors.New("an active migration already exists for this tenant and domain")
	ErrMigrationCapability = errors.New("data migration capability is unavailable")
)

const (
	MaxMigrationErrorBytes   = 4096
	maxMigrationProfileBytes = 128
)

// Migration leases are persisted with millisecond precision by the
// PostgreSQL store.  Rejecting sub-millisecond values at every public lease
// boundary prevents a positive Go duration from being truncated to zero in
// SQL and expiring immediately.
const minMigrationLeaseTTL = time.Millisecond

var (
	migrationSensitiveFields = regexp.MustCompile(`(?i)(api[_-]?key|access[_-]?token|token|secret|password|authorization|dsn)\s*[:=]\s*([^\s,;]+)`)
	migrationCredentialURL   = regexp.MustCompile(`(?i)([a-z][a-z0-9+.-]*://[^/\s:@]+):([^@\s/]+)@`)
	migrationBearerToken     = regexp.MustCompile(`(?i)\bBearer\s+[^\s,;]+`)
)

// Job is the durable control-plane state for one tenant/domain migration.
// Payload data never lives in this record; source and target adapters own it.
type Job struct {
	ID                string
	TenantID          string
	Domain            Domain
	SourceProfile     string
	TargetProfile     string
	Phase             Phase
	Paused            bool
	Cursor            string
	SnapshotWatermark int64
	AppliedWatermark  int64
	LeaseOwner        string
	LeaseVersion      int64
	LeaseUntil        time.Time
	LastError         string
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

func (j Job) Validate() error {
	if j.ID == "" || len(j.ID) > 128 || strings.ContainsAny(j.ID, "\x00\r\n") {
		return fmt.Errorf("%w: id", ErrInvalidMigration)
	}
	if err := tenant.ValidateTenantID(j.TenantID); err != nil {
		return fmt.Errorf("%w: tenant: %v", ErrInvalidMigration, err)
	}
	if !validDomain(j.Domain) {
		return fmt.Errorf("%w: domain %q", ErrInvalidMigration, j.Domain)
	}
	if !validMigrationProfile(j.SourceProfile) || !validMigrationProfile(j.TargetProfile) ||
		j.SourceProfile == j.TargetProfile {
		return fmt.Errorf("%w: source and target profiles must be distinct, valid profile references", ErrInvalidMigration)
	}
	if !validPhase(j.Phase) {
		return fmt.Errorf("%w: phase %q", ErrInvalidMigration, j.Phase)
	}
	if j.SnapshotWatermark < 0 || j.AppliedWatermark < 0 {
		return fmt.Errorf("%w: watermark", ErrInvalidMigration)
	}
	if len(j.LastError) > MaxMigrationErrorBytes || !utf8.ValidString(j.LastError) ||
		strings.ContainsAny(j.LastError, "\x00\r\n\t") {
		return fmt.Errorf("%w: last error must be valid UTF-8, control-free and at most %d bytes", ErrInvalidMigration, MaxMigrationErrorBytes)
	}
	if sanitizeMigrationErrorText(j.LastError) != j.LastError {
		return fmt.Errorf("%w: last error must be sanitized before persistence", ErrInvalidMigration)
	}
	return nil
}

func validMigrationProfile(value string) bool {
	return value != "" && len(value) <= maxMigrationProfileBytes && utf8.ValidString(value) &&
		!strings.ContainsAny(value, "\x00\r\n\t")
}

func validDomain(domain Domain) bool {
	switch domain {
	case DomainSession, DomainMemory, DomainSummary, DomainArtifact, DomainKnowledge:
		return true
	default:
		return false
	}
}

func validPhase(phase Phase) bool {
	switch phase {
	case PhasePrepare, PhaseSnapshotCopy, PhaseDualWrite, PhaseCatchUp,
		PhaseValidate, PhaseReadShadow, PhaseCutover, PhaseRollbackWindow,
		PhaseComplete, PhaseRolledBack:
		return true
	default:
		return false
	}
}

func terminalPhase(phase Phase) bool {
	return phase == PhaseComplete || phase == PhaseRolledBack
}

func canTransition(from, to Phase) bool {
	switch from {
	case PhasePrepare:
		return to == PhaseSnapshotCopy
	case PhaseSnapshotCopy:
		return to == PhaseSnapshotCopy || to == PhaseDualWrite
	case PhaseDualWrite:
		return to == PhaseCatchUp
	case PhaseCatchUp:
		return to == PhaseCatchUp || to == PhaseValidate
	case PhaseValidate:
		return to == PhaseReadShadow
	case PhaseReadShadow:
		return to == PhaseCutover
	case PhaseCutover:
		return to == PhaseRollbackWindow
	case PhaseRollbackWindow:
		return to == PhaseComplete || to == PhaseRolledBack
	default:
		return false
	}
}

// Record is the transport-neutral unit copied by a migration adapter.
// Version must be monotonic in the source domain; Hash is verified before a
// target write so a corrupt or mis-scoped payload cannot be accepted.
type Record struct {
	Key     string
	Version int64
	Hash    string
	Payload []byte
	Deleted bool
}

const (
	// Artifact identities include the full validated app/user/session/file
	// scope in a reversible tombstone key. 4 KiB covers all public field limits
	// without allowing unbounded Redis keys or PostgreSQL index pressure.
	maxRecordKeyBytes     = 4 << 10
	maxRecordPayloadBytes = 16 << 20
)

func (r Record) Validate() error {
	if r.Key == "" || len(r.Key) > maxRecordKeyBytes || strings.ContainsAny(r.Key, "\x00\r\n") {
		return fmt.Errorf("%w: key", ErrInvalidRecord)
	}
	if r.Version < 0 || len(r.Payload) > maxRecordPayloadBytes || (r.Deleted && len(r.Payload) != 0) {
		return fmt.Errorf("%w: version or payload size", ErrInvalidRecord)
	}
	digest := sha256.Sum256(r.Payload)
	want := hex.EncodeToString(digest[:])
	if !strings.EqualFold(r.Hash, want) {
		return fmt.Errorf("%w: hash for %q", ErrInvalidRecord, r.Key)
	}
	return nil
}

type Batch struct {
	Records    []Record
	NextCursor string
	Watermark  int64
	Done       bool
}

func (b Batch) Validate(previousCursor string, previousWatermark int64) error {
	seen := make(map[string]struct{}, len(b.Records))
	maxVersion := previousWatermark
	for _, record := range b.Records {
		if err := record.Validate(); err != nil {
			return err
		}
		if _, ok := seen[record.Key]; ok {
			return fmt.Errorf("%w: duplicate key %q", ErrInvalidRecord, record.Key)
		}
		seen[record.Key] = struct{}{}
		if record.Version > maxVersion {
			maxVersion = record.Version
		}
	}
	if b.Watermark < maxVersion || b.Watermark < previousWatermark {
		return fmt.Errorf("%w: non-monotonic watermark", ErrInvalidRecord)
	}
	if !b.Done && b.NextCursor == previousCursor {
		return ErrCursorStalled
	}
	return nil
}

// Source reads a stable snapshot and then changes after the snapshot
// watermark. Implementations must scope every call by tenantID and domain.
type Source interface {
	Snapshot(context.Context, string, Domain, string, int) (Batch, error)
	Changes(context.Context, string, Domain, int64, int) (Batch, error)
}

// LeaseFence identifies the exact durable owner/version allowed to perform a
// migration side effect. Adapters must validate it atomically with their
// external write or cutover operation.
type LeaseFence struct {
	MigrationID string
	Owner       string
	Version     int64
}

type leaseFenceContextKey struct{}

func withLeaseFence(ctx context.Context, job Job) context.Context {
	return context.WithValue(ctx, leaseFenceContextKey{}, LeaseFence{
		MigrationID: job.ID, Owner: job.LeaseOwner, Version: job.LeaseVersion,
	})
}

// LeaseFenceFromContext returns the owner/version that was durably claimed
// for the current migration step.
func LeaseFenceFromContext(ctx context.Context) (LeaseFence, error) {
	if ctx == nil {
		return LeaseFence{}, ErrMigrationFence
	}
	fence, ok := ctx.Value(leaseFenceContextKey{}).(LeaseFence)
	if !ok || fence.MigrationID == "" || fence.Owner == "" || fence.Version <= 0 {
		return LeaseFence{}, ErrMigrationFence
	}
	return fence, nil
}

// Target receives idempotent version-aware upserts. An implementation must
// ignore an older version of an existing key rather than overwrite newer data,
// and must atomically validate the explicit LeaseFence at its side-effect
// boundary before writing.
type Target interface {
	Upsert(context.Context, string, Domain, LeaseFence, []Record) error
}

// Hooks represent side effects that cannot be safely inferred by a generic
// copier. Each hook receives the explicit LeaseFence and must verify it
// atomically with its external side effect. Missing hooks fail closed instead
// of pretending that dual-write, shadow validation, cutover, or rollback
// occurred.
type Hooks struct {
	Prepare         func(context.Context, Job, LeaseFence) error
	EnableDualWrite func(context.Context, Job, LeaseFence) error
	Validate        func(context.Context, Job, LeaseFence) error
	ShadowRead      func(context.Context, Job, LeaseFence) error
	Cutover         func(context.Context, Job, LeaseFence) error
	Complete        func(context.Context, Job, LeaseFence) error
	Rollback        func(context.Context, Job, LeaseFence) error
}

type Store interface {
	Create(context.Context, Job) error
	Get(context.Context, string) (Job, error)
	Claim(context.Context, string, string, time.Duration) (Job, error)
	Advance(context.Context, string, string, int64, JobPatch) (Job, error)
	Release(context.Context, string, string, int64) error
}

// LeaseRenewer is required by Executor. A migration step may call an
// external backend or operator hook, so a one-shot claim is not sufficient to
// fence a long-running side effect.
type LeaseRenewer interface {
	Renew(context.Context, string, string, int64, time.Duration) (Job, error)
}

func validateMigrationLease(owner string, ttl time.Duration) error {
	if owner == "" || len(owner) > 128 || ttl < minMigrationLeaseTTL || ttl > 24*time.Hour {
		return fmt.Errorf("%w: invalid owner or lease", ErrInvalidMigration)
	}
	return nil
}

func nonNilMigrationContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

// JobPatch is intentionally narrow. A caller cannot mutate tenant, domain or
// source/target identity after creation.
type JobPatch struct {
	Phase             *Phase
	Paused            *bool
	Cursor            *string
	SnapshotWatermark *int64
	AppliedWatermark  *int64
	LastError         *string
}

// NewJob constructs a migration in the only phase that can be safely created.
func NewJob(id, tenantID string, domain Domain, sourceProfile, targetProfile string, now time.Time) (Job, error) {
	job := Job{
		ID: id, TenantID: tenantID, Domain: domain,
		SourceProfile: sourceProfile, TargetProfile: targetProfile,
		Phase: PhasePrepare, CreatedAt: now.UTC(), UpdatedAt: now.UTC(),
	}
	if err := job.Validate(); err != nil {
		return Job{}, err
	}
	return job, nil
}

func sanitizeMigrationErrorText(text string) string {
	text = strings.ToValidUTF8(text, "�")
	text = strings.ReplaceAll(text, "\x00", "")
	text = strings.ReplaceAll(text, "\r", " ")
	text = strings.ReplaceAll(text, "\n", " ")
	text = strings.ReplaceAll(text, "\t", " ")
	text = migrationCredentialURL.ReplaceAllString(text, `$1:<redacted>@`)
	// Normalize the multi-word Bearer form before generic field redaction.
	// Otherwise `Authorization: Bearer token` would redact only `Bearer` and
	// leave the token as a separate word.
	text = migrationBearerToken.ReplaceAllString(text, "Bearer <redacted>")
	text = migrationSensitiveFields.ReplaceAllString(text, `$1=<redacted>`)
	if len(text) <= MaxMigrationErrorBytes {
		return text
	}
	limit := MaxMigrationErrorBytes - len("...")
	for limit > 0 && !utf8.ValidString(text[:limit]) {
		limit--
	}
	return text[:limit] + "..."
}

type sanitizedMigrationError struct {
	message string
	cause   error
}

func (e sanitizedMigrationError) Error() string { return e.message }
func (e sanitizedMigrationError) Unwrap() error { return e.cause }

func sanitizeMigrationError(err error) error {
	if err == nil {
		return nil
	}
	return sanitizedMigrationError{message: sanitizeMigrationErrorText(err.Error()), cause: err}
}
