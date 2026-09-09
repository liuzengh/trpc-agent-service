package task

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/vector"
)

const TaskIDPrefix = "vt-v1-"
const taskIDDomain = "trpc-agent/vector-task/v1"

type State string

const (
	StatePending    State = "pending"
	StateRunning    State = "running"
	StateRetryWait  State = "retry_wait"
	StateSucceeded  State = "succeeded"
	StateDeadLetter State = "dead_letter"
	StateStale      State = "stale"
	StateCancelled  State = "cancelled"
)

func (s State) Terminal() bool {
	return s == StateSucceeded || s == StateDeadLetter || s == StateStale || s == StateCancelled
}

type Category string

const (
	CategoryCancelled         Category = "cancelled"
	CategoryDeadline          Category = "deadline"
	CategoryInvalidRequest    Category = "invalid_request"
	CategoryStaleTask         Category = "stale_task"
	CategorySchemaMismatch    Category = "schema_mismatch"
	CategoryDimensionMismatch Category = "dimension_mismatch"
	CategoryModelMismatch     Category = "model_mismatch"
	CategoryUnavailable       Category = "unavailable"
	CategoryRetryable         Category = "retryable"
	CategoryPermanent         Category = "permanent"
	CategoryUnknown           Category = "unknown"
	CategoryLeaseLost         Category = "lease_lost"
	CategoryClosed            Category = "closed"
	CategoryFenceRejected     Category = "fence_rejected"
	CategorySourceMissing     Category = "source_missing"
)

var (
	ErrNotFound          = errors.New("vector task: not found")
	ErrConflictOwnership = errors.New("vector task: lease or state changed")
	ErrNotClaimable      = errors.New("vector task: candidate not claimable")
	ErrUnavailable       = errors.New("vector task: repository unavailable")
	ErrInvalidTask       = errors.New("vector task: invalid task")
	ErrTenantMismatch    = errors.New("vector task: tenant mismatch")
	ErrTooManyAttempts   = errors.New("vector task: attempt budget exhausted")
)

// Task contains durable task metadata only. It deliberately has no source
// content, embedding, prompt, provider response, credential or DSN.
type Task struct {
	TenantID        string
	TaskID          string
	SourceType      string
	SourceID        string
	ProjectionScope string
	DocumentID      string
	Operation       vector.VectorOperation
	SourceVersion   int64
	SourceSequence  int64
	ContentHash     string
	Model           string
	ModelVersion    string
	SchemaVersion   string
	Dimension       int
	State           State
	Attempt         int
	MaxAttempts     int
	NextAttemptAt   time.Time
	LastError       string
	LeaseOwner      string
	LeaseEpoch      storage.Epoch
	LeaseFence      uint64
	LeaseExpiresAt  time.Time
	ClaimedAt       time.Time
	CompletedAt     time.Time
	DeadLetteredAt  time.Time
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

func (t Task) Ref() (vector.VectorDocumentRef, error) {
	ref := vector.VectorDocumentRef{TenantID: t.TenantID, SourceType: t.SourceType, SourceID: t.SourceID, ProjectionScope: t.ProjectionScope, DocumentID: t.DocumentID, Operation: t.Operation, Deleted: t.Operation == vector.OperationDelete, SourceVersion: t.SourceVersion, SourceSequence: t.SourceSequence, ContentHash: t.ContentHash, Model: t.Model, ModelVersion: t.ModelVersion, Dimension: t.Dimension, SchemaVersion: t.SchemaVersion}
	if err := ref.Validate(); err != nil {
		return vector.VectorDocumentRef{}, err
	}
	return ref, nil
}

func TaskID(ref vector.VectorDocumentRef) (string, error) {
	if err := ref.Validate(); err != nil {
		return "", err
	}
	material := strings.Join([]string{taskIDDomain, ref.TenantID, ref.DocumentID, string(ref.Operation), strconv.FormatInt(ref.SourceVersion, 10), strconv.FormatInt(ref.SourceSequence, 10)}, "\x00")
	sum := sha256.Sum256([]byte(material))
	return TaskIDPrefix + hex.EncodeToString(sum[:])[:32], nil
}

type LeaseRef struct {
	Owner     string
	Epoch     storage.Epoch
	Fence     uint64
	ExpiresAt time.Time
}

func (l LeaseRef) Valid() bool {
	return strings.TrimSpace(l.Owner) == l.Owner && l.Owner != "" && l.Epoch > 0 && l.Fence > 0 && !l.ExpiresAt.IsZero()
}

type Candidate struct {
	TenantID string
	TaskID   string
}
type EnqueueOutcome struct {
	Task    Task
	Created bool
}
type HeadKey struct {
	Version   int64
	Sequence  int64
	Operation vector.VectorOperation
}

func (h HeadKey) Less(other HeadKey) bool {
	if h.Version != other.Version {
		return h.Version < other.Version
	}
	if h.Sequence != other.Sequence {
		return h.Sequence < other.Sequence
	}
	return h.Operation != other.Operation && h.Operation != vector.OperationDelete
}

type FailureKind string

const (
	FailureRetry      FailureKind = "retry"
	FailureDeadLetter FailureKind = "dead_letter"
	FailureStale      FailureKind = "stale"
	FailureRelease    FailureKind = "release"
)

type Failure struct {
	Kind        FailureKind
	Category    Category
	NextAttempt time.Time
}

func classifyVectorError(err error) (Category, FailureKind) {
	switch {
	case err == nil:
		return "", ""
	case errors.Is(err, context.Canceled):
		return CategoryCancelled, FailureRelease
	case errors.Is(err, context.DeadlineExceeded):
		return CategoryDeadline, FailureRetry
	case errors.Is(err, vector.ErrCancelled):
		return CategoryCancelled, FailureRelease
	case errors.Is(err, vector.ErrTimeout):
		return CategoryDeadline, FailureRetry
	case errors.Is(err, vector.ErrUnknown):
		return CategoryUnknown, FailureRetry
	case errors.Is(err, vector.ErrUnavailable), errors.Is(err, vector.ErrRetryable), errors.Is(err, vector.ErrConflict):
		return CategoryRetryable, FailureRetry
	case errors.Is(err, vector.ErrStale):
		return CategoryStaleTask, FailureStale
	case errors.Is(err, vector.ErrPermanent), errors.Is(err, vector.ErrNotFound):
		return CategoryPermanent, FailureDeadLetter
	case errors.Is(err, vector.ErrInvalidContext), errors.Is(err, vector.ErrInvalidTenant),
		errors.Is(err, vector.ErrInvalidDocument), errors.Is(err, vector.ErrInvalidFilter),
		errors.Is(err, vector.ErrInvalidConfig), errors.Is(err, vector.ErrDisabled),
		errors.Is(err, vector.ErrUnsupported):
		return CategoryInvalidRequest, FailureDeadLetter
	case errors.Is(err, vector.ErrInvalidDimension):
		return CategoryDimensionMismatch, FailureDeadLetter
	case errors.Is(err, vector.ErrInvalidModel):
		return CategoryModelMismatch, FailureDeadLetter
	case errors.Is(err, vector.ErrInvalidSchema):
		return CategorySchemaMismatch, FailureDeadLetter
	default:
		return CategoryUnknown, FailureRetry
	}
}

type Config struct {
	WorkerID           string
	Concurrency        int
	PollLimit          int
	PollInterval       time.Duration
	LeaseTTL           time.Duration
	LeaseRenewInterval time.Duration
	MaxAttempts        int
	RetryBackoff       time.Duration
	MaxRetryBackoff    time.Duration
	TaskTimeout        time.Duration
	ShutdownTimeout    time.Duration
}

func (c Config) withDefaults() (Config, error) {
	if strings.TrimSpace(c.WorkerID) != c.WorkerID || c.WorkerID == "" || len(c.WorkerID) > 128 {
		return Config{}, fmt.Errorf("vector task: invalid worker id")
	}
	if c.Concurrency == 0 {
		c.Concurrency = 1
	}
	if c.Concurrency < 1 || c.Concurrency > 16 {
		return Config{}, fmt.Errorf("vector task: invalid concurrency")
	}
	if c.PollLimit == 0 {
		c.PollLimit = 1
	}
	if c.PollLimit < 1 || c.PollLimit > 64 {
		return Config{}, fmt.Errorf("vector task: invalid poll limit")
	}
	if c.PollInterval == 0 {
		c.PollInterval = 250 * time.Millisecond
	}
	if c.PollInterval < 10*time.Millisecond || c.PollInterval > time.Minute {
		return Config{}, fmt.Errorf("vector task: invalid poll interval")
	}
	if c.LeaseTTL < time.Second || c.LeaseTTL > 10*time.Minute {
		return Config{}, fmt.Errorf("vector task: invalid lease ttl")
	}
	if c.LeaseRenewInterval == 0 {
		c.LeaseRenewInterval = c.LeaseTTL / 3
	}
	if c.LeaseRenewInterval <= 0 || c.LeaseRenewInterval >= c.LeaseTTL {
		return Config{}, fmt.Errorf("vector task: invalid lease renewal interval")
	}
	if c.MaxAttempts == 0 {
		c.MaxAttempts = 5
	}
	if c.MaxAttempts < 1 || c.MaxAttempts > 100 {
		return Config{}, fmt.Errorf("vector task: invalid max attempts")
	}
	if c.RetryBackoff == 0 {
		c.RetryBackoff = time.Second
	}
	if c.RetryBackoff < 0 || c.RetryBackoff > time.Hour {
		return Config{}, fmt.Errorf("vector task: invalid retry backoff")
	}
	if c.MaxRetryBackoff == 0 {
		c.MaxRetryBackoff = 30 * time.Second
	}
	if c.MaxRetryBackoff < c.RetryBackoff || c.MaxRetryBackoff > time.Hour {
		return Config{}, fmt.Errorf("vector task: invalid max retry backoff")
	}
	if c.TaskTimeout <= 0 || c.TaskTimeout > 10*time.Minute {
		return Config{}, fmt.Errorf("vector task: invalid task timeout")
	}
	if c.ShutdownTimeout == 0 {
		c.ShutdownTimeout = 10 * time.Second
	}
	if c.ShutdownTimeout <= 0 || c.ShutdownTimeout > time.Minute {
		return Config{}, fmt.Errorf("vector task: invalid shutdown timeout")
	}
	return c, nil
}
func (c Config) backoff(attempt int) time.Duration {
	d := c.RetryBackoff
	for i := 1; i < attempt && d < c.MaxRetryBackoff; i++ {
		if d > c.MaxRetryBackoff/2 {
			return c.MaxRetryBackoff
		}
		d *= 2
	}
	if d > c.MaxRetryBackoff {
		return c.MaxRetryBackoff
	}
	return d
}
