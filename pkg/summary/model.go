// Package summary provides durable coordination for asynchronous session
// summaries.  It deliberately does not read or write tRPC Session events:
// callers supply a generator and a CAS-capable sink, so the platform can use
// the tenant's selected Session backend without weakening ordering guarantees.
package summary

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
)

const (
	// DefaultMaxAttempts bounds automatic retries for a summary job. Operators
	// can deliberately re-enqueue a job when an incident has been reconciled.
	DefaultMaxAttempts = 8
	MaxAttempts        = 100
	MaxFilterKeyBytes  = 512
	// MaxAgentAppIDBytes matches the VARCHAR(64) contract in the control-plane
	// and Summary migrations.
	MaxAgentAppIDBytes     = 64
	MaxAgentVersionIDBytes = 64
	MaxSessionOwnerIDBytes = 255
	MaxSessionIDBytes      = 512
	MaxJobErrorBytes       = 4096
	MaxSummaryBytes        = 1 << 20
	MaxSummaryEventIDBytes = 512
)

var (
	ErrNoWork                    = errors.New("no claimable summary job")
	ErrJobNotFound               = errors.New("summary job was not found")
	ErrStaleLease                = errors.New("summary job lease is stale or expired")
	ErrSummaryStale              = errors.New("summary candidate is older than the durable checkpoint")
	ErrSummaryConflict           = errors.New("summary candidate conflicts at the same event sequence")
	ErrInvalidJob                = errors.New("summary job is invalid")
	ErrInvalidCandidate          = errors.New("summary candidate is invalid")
	ErrAttemptLimit              = errors.New("summary job has reached its retry limit")
	ErrStoreUnavailable          = errors.New("summary store is unavailable")
	ErrSinkUnavailable           = errors.New("summary sink is unavailable")
	ErrGeneratorMisScoped        = errors.New("summary generator returned a different job scope")
	ErrGeneratorUnavailable      = errors.New("summary generator dependency is unavailable")
	ErrTranscriptIncomplete      = errors.New("summary transcript does not match the committed target")
	ErrNoSummaryContent          = errors.New("summary generator returned empty content")
	ErrSummaryScope              = errors.New("summary publication scope does not match the leased job")
	ErrSummaryReadUnavailable    = errors.New("durable summary is unavailable")
	ErrSummaryVersionConflict    = errors.New("summary target is already pinned to a different agent version")
	ErrTargetResolverUnavailable = errors.New("summary target resolver is unavailable")
	// ErrSummaryNotDue is a successful policy decision: the exact event prefix
	// is durable but has not advanced far enough to justify another model call.
	ErrSummaryNotDue = errors.New("summary generation threshold is not reached")
)

// Status is the durable state of a summary job.
type Status string

const (
	StatusPending    Status = "PENDING"
	StatusProcessing Status = "PROCESSING"
	StatusCompleted  Status = "COMPLETED"
	StatusFailed     Status = "FAILED"
)

// Key is the complete tenant-scoped identity of a summary stream.
type Key struct {
	TenantID       string
	AgentAppID     string
	SessionOwnerID string
	SessionID      string
	FilterKey      string
	// Empty identifies preserved pre-incarnation rows, never a production
	// Session generation. New production receipts carry the backend UUID.
	SessionIncarnationID string
}

func (k Key) Validate() error {
	if err := tenant.ValidateTenantID(k.TenantID); err != nil {
		return fmt.Errorf("%w: tenant: %v", ErrInvalidJob, err)
	}
	if !validScopedText(k.AgentAppID, MaxAgentAppIDBytes, false) {
		return fmt.Errorf("%w: agent app id", ErrInvalidJob)
	}
	if !validScopedText(k.SessionOwnerID, MaxSessionOwnerIDBytes, false) {
		return fmt.Errorf("%w: session owner id", ErrInvalidJob)
	}
	if !validScopedText(k.SessionID, MaxSessionIDBytes, false) {
		return fmt.Errorf("%w: session id", ErrInvalidJob)
	}
	if !validScopedText(k.FilterKey, MaxFilterKeyBytes, true) {
		return fmt.Errorf("%w: filter key", ErrInvalidJob)
	}
	if k.SessionIncarnationID != "" {
		id, err := uuid.Parse(k.SessionIncarnationID)
		if err != nil || id == uuid.Nil || id.String() != k.SessionIncarnationID {
			return fmt.Errorf("%w: session incarnation", ErrInvalidJob)
		}
	}
	return nil
}

func validScopedText(value string, maxBytes int, allowEmpty bool) bool {
	if (!allowEmpty && value == "") || len(value) > maxBytes || !utf8.ValidString(value) {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) || unicode.In(character, unicode.Cf) {
			return false
		}
	}
	return true
}

// Job is the durable coordination record.  LeaseVersion is a fencing token,
// not merely a liveness hint: every mutation must present the exact owner,
// version and unexpired lease returned by Claim.
type Job struct {
	ID int64
	Key
	// AgentVersionID pins model, prompt and generation policy for deterministic
	// retries. It deliberately is not part of Key: a newer event target may
	// coalesce onto the same summary stream while selecting a newer version.
	AgentVersionID         string
	TargetEventSequence    int64
	Status                 Status
	LeaseOwner             string
	LeaseVersion           int64
	LeaseUntil             time.Time
	Attempts               int
	MaxAttempts            int
	NextAttemptAt          time.Time
	LastError              string
	CompletedEventSequence int64
	CreatedAt              time.Time
	UpdatedAt              time.Time
	// A deferred request received during a claim belongs to the next lease.
	// Zero means no complete-transcript resolution remains outstanding.
	TargetResolutionLeaseVersion int64
}

func (j Job) Validate() error {
	if j.ID < 0 {
		return fmt.Errorf("%w: id", ErrInvalidJob)
	}
	if err := j.Key.Validate(); err != nil {
		return err
	}
	if !validScopedText(j.AgentVersionID, MaxAgentVersionIDBytes, false) {
		return fmt.Errorf("%w: agent version id", ErrInvalidJob)
	}
	if j.TargetEventSequence < 0 || j.CompletedEventSequence < 0 ||
		j.CompletedEventSequence > j.TargetEventSequence && j.Status != StatusCompleted {
		return fmt.Errorf("%w: event sequence", ErrInvalidJob)
	}
	if !validStatus(j.Status) {
		return fmt.Errorf("%w: status %q", ErrInvalidJob, j.Status)
	}
	if j.LeaseVersion < 0 || j.TargetResolutionLeaseVersion < 0 || j.Attempts < 0 || j.MaxAttempts <= 0 || j.MaxAttempts > MaxAttempts || j.Attempts > j.MaxAttempts {
		return fmt.Errorf("%w: lease or attempts", ErrInvalidJob)
	}
	if j.LeaseOwner != "" && (len(j.LeaseOwner) > 128 || !utf8.ValidString(j.LeaseOwner) ||
		strings.ContainsAny(j.LeaseOwner, "\x00\r\n")) {
		return fmt.Errorf("%w: lease owner", ErrInvalidJob)
	}
	if len(j.LastError) > MaxJobErrorBytes || !utf8.ValidString(j.LastError) ||
		strings.ContainsAny(j.LastError, "\x00") {
		return fmt.Errorf("%w: last error", ErrInvalidJob)
	}
	if j.Status == StatusProcessing && (j.LeaseOwner == "" || j.LeaseUntil.IsZero()) {
		return fmt.Errorf("%w: processing job has no lease", ErrInvalidJob)
	}
	if j.Status == StatusProcessing && j.LeaseVersion == 0 {
		return fmt.Errorf("%w: processing job has no fence version", ErrInvalidJob)
	}
	if j.Status == StatusProcessing && !j.NextAttemptAt.IsZero() {
		return fmt.Errorf("%w: processing job has retry schedule", ErrInvalidJob)
	}
	if j.Status == StatusCompleted && j.CompletedEventSequence < j.TargetEventSequence {
		return fmt.Errorf("%w: completed job is below target sequence", ErrInvalidJob)
	}
	if j.Status == StatusCompleted && j.TargetEventSequence == 0 {
		return fmt.Errorf("%w: unresolved job cannot be completed", ErrInvalidJob)
	}
	if j.Status != StatusProcessing && (!j.LeaseUntil.IsZero() || j.LeaseOwner != "") {
		return fmt.Errorf("%w: non-processing job retains lease", ErrInvalidJob)
	}
	return nil
}

func validStatus(status Status) bool {
	switch status {
	case StatusPending, StatusProcessing, StatusCompleted, StatusFailed:
		return true
	default:
		return false
	}
}

// EnqueueRequest asks the store to create or coalesce a job.  TargetEventSeq
// is read from the authoritative Session backend after Event/State commit.
type EnqueueRequest struct {
	Key
	AgentVersionID      string
	TargetEventSequence int64
	MaxAttempts         int
	Force               bool
}

func (r EnqueueRequest) Validate() error {
	if err := r.Key.Validate(); err != nil {
		return err
	}
	if !validScopedText(r.AgentVersionID, MaxAgentVersionIDBytes, false) {
		return fmt.Errorf("%w: agent version id", ErrInvalidJob)
	}
	if r.TargetEventSequence < 0 {
		return fmt.Errorf("%w: target event sequence", ErrInvalidJob)
	}
	if r.MaxAttempts == 0 {
		return nil
	}
	if r.MaxAttempts < 1 || r.MaxAttempts > MaxAttempts {
		return fmt.Errorf("%w: max attempts", ErrInvalidJob)
	}
	return nil
}

// EnqueueResult reports whether a new row was created.  Coalesced jobs are
// still authoritative: callers must use the returned target sequence.
type EnqueueResult struct {
	Job       Job
	Created   bool
	Coalesced bool
}

// Lease is the immutable ownership proof used by Processor and custom
// workers.  It is intentionally separate from Job so callers cannot mutate a
// job's identity while retaining its fence.
type Lease struct {
	Job     Job
	Owner   string
	Version int64
	Until   time.Time
}

// Candidate is generated from a stable read of the Session event stream.
// ContentSHA256 is mandatory, allowing a sink to detect corruption and
// nondeterministic same-sequence writers.
type Candidate struct {
	Key           Key
	EventSequence int64
	Content       string
	ContentSHA256 string
	// CutoffAt and LastEventID are the exact boundary of the filtered immutable
	// transcript sent to the summarizer. Runner uses both values to exclude only
	// history already represented by Content, including events that share a
	// timestamp.
	CutoffAt    time.Time
	LastEventID string
}

func (c Candidate) Validate() error {
	if err := c.Key.Validate(); err != nil {
		return fmt.Errorf("%w: scope: %v", ErrInvalidCandidate, err)
	}
	if c.EventSequence <= 0 || len(c.Content) > MaxSummaryBytes || !utf8.ValidString(c.Content) ||
		strings.ContainsRune(c.Content, '\x00') {
		return fmt.Errorf("%w: sequence or content", ErrInvalidCandidate)
	}
	want := HashContent(c.Content)
	if !strings.EqualFold(c.ContentSHA256, want) {
		return fmt.Errorf("%w: content hash", ErrInvalidCandidate)
	}
	if c.CutoffAt.IsZero() || !validScopedText(c.LastEventID, MaxSummaryEventIDBytes, false) {
		return fmt.Errorf("%w: transcript boundary", ErrInvalidCandidate)
	}
	return nil
}

// Checkpoint is the value currently visible to readers.
type Checkpoint struct {
	Key           Key
	EventSequence int64
	Content       string
	ContentSHA256 string
	CutoffAt      time.Time
	LastEventID   string
	UpdatedAt     time.Time
}

func (c Checkpoint) Validate() error {
	if err := c.Key.Validate(); err != nil {
		return fmt.Errorf("%w: scope: %v", ErrInvalidCandidate, err)
	}
	if c.EventSequence < 0 || len(c.Content) > MaxSummaryBytes || !utf8.ValidString(c.Content) ||
		strings.ContainsRune(c.Content, '\x00') {
		return fmt.Errorf("%w: checkpoint sequence or content", ErrInvalidCandidate)
	}
	if !strings.EqualFold(c.ContentSHA256, HashContent(c.Content)) {
		return fmt.Errorf("%w: checkpoint content hash", ErrInvalidCandidate)
	}
	if c.EventSequence > 0 && (c.CutoffAt.IsZero() || !validScopedText(c.LastEventID, MaxSummaryEventIDBytes, false)) {
		return fmt.Errorf("%w: checkpoint transcript boundary", ErrInvalidCandidate)
	}
	return nil
}

// PublishResult distinguishes an applied candidate from an idempotent retry.
type PublishResult struct {
	Checkpoint Checkpoint
	Applied    bool
}

// HashContent returns the lowercase SHA-256 representation used by all
// stores.  It is exported so generators cannot accidentally hash a different
// serialization.
func HashContent(content string) string {
	digest := sha256.Sum256([]byte(content))
	return hex.EncodeToString(digest[:])
}

func errorText(err error) string {
	if err == nil {
		return ""
	}
	text := strings.TrimSpace(err.Error())
	text = strings.ReplaceAll(text, "\r", " ")
	text = strings.ReplaceAll(text, "\n", " ")
	text = redactErrorText(text)
	if len(text) > MaxJobErrorBytes {
		text = text[:MaxJobErrorBytes]
		for len(text) > 0 && !utf8.ValidString(text) {
			text = text[:len(text)-1]
		}
	}
	return text
}

var (
	sensitiveErrorField = regexp.MustCompile(`(?i)(api[_-]?key|access[_-]?token|token|secret|password|authorization|dsn)\s*[:=]\s*([^\s,;]+)`)
	credentialURL       = regexp.MustCompile(`(?i)([a-z][a-z0-9+.-]*://[^/\s:@]+):([^@\s/]+)@`)
	bearerToken         = regexp.MustCompile(`(?i)\bBearer\s+[^\s,;]+`)
)

func redactErrorText(text string) string {
	text = credentialURL.ReplaceAllString(text, `$1:<redacted>@`)
	// Normalize the multi-word Bearer form before generic field redaction.
	// Otherwise `Authorization: Bearer token` would redact only `Bearer` and
	// leave the token as a separate word.
	text = bearerToken.ReplaceAllString(text, "Bearer <redacted>")
	return sensitiveErrorField.ReplaceAllString(text, `$1=<redacted>`)
}

// Store is the durable job state machine. Implementations must use a
// transaction/lock around each mutation and enforce owner+version+expiry.
type Store interface {
	Enqueue(context.Context, EnqueueRequest) (EnqueueResult, error)
	Get(context.Context, int64) (Job, error)
	Claim(context.Context, string, time.Duration) (Job, error)
	Renew(context.Context, Job, time.Duration) (Job, error)
	// ResolveTarget freezes an unresolved target (zero) under the exact job
	// lease. Replays return the already-bound positive target unchanged.
	ResolveTarget(context.Context, Job, int64) (Job, error)
	Fail(context.Context, Job, error, time.Time) (Job, error)
	Complete(context.Context, Job, int64) (Job, error)
}

// Sink is the CAS boundary for visible summaries. A successful Publish must
// make the candidate visible; an older candidate must never overwrite a newer
// checkpoint. Implementations should keep tenant scope in their primary key.
type Sink interface {
	Publish(context.Context, Candidate) (PublishResult, error)
	Get(context.Context, Key) (Checkpoint, bool, error)
}

// CheckpointReader is the read-only summary data plane used by Agent Workers
// to hydrate the framework Session object before Runner builds model history.
type CheckpointReader interface {
	Get(context.Context, Key) (Checkpoint, bool, error)
}

// FencedSink is an optional stronger seam. When implemented, publication and
// lease validation happen in one backend transaction, so a worker that loses
// its job lease cannot publish after a replacement worker has claimed it.
// Custom sinks that cannot provide this atomic boundary should document the
// weaker publication semantics and use an external fencing mechanism.
type FencedSink interface {
	Sink
	PublishFenced(context.Context, Candidate, Job) (PublishResult, error)
}

// Generator reads the authoritative Session backend and returns one summary
// candidate for the claimed job. It must not mutate the job or publish data.
type Generator interface {
	Generate(context.Context, Job) (Candidate, error)
}

// TargetResolver reads the authoritative Session backend when a Worker could
// not include an exact committed event count in its successful response.
type TargetResolver interface {
	ResolveTarget(context.Context, Job) (int64, error)
}
