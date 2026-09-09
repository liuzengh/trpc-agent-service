// Package rebuild implements the P1-06E durable rebuild/reconciliation
// boundary: a tenant-scoped, keyset-paginated PostgreSQL Memory scan that
// enqueues vector projection tasks through the existing P1-06C transactional
// boundary, with a durable run cursor, Redis lease fencing and bounded
// drift reconciliation. Milvus remains a derived index; PostgreSQL stays the
// single source of truth.
package rebuild

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/vector"
)

var (
	ErrInvalidConfig  = errors.New("vector rebuild: invalid configuration")
	ErrInvalidRun     = errors.New("vector rebuild: invalid run")
	ErrNotFound       = errors.New("vector rebuild: run not found")
	ErrConflictOwner  = errors.New("vector rebuild: lease or phase changed")
	ErrRunExhausted   = errors.New("vector rebuild: run attempt budget exhausted")
	ErrScanBudget     = errors.New("vector rebuild: scan batch budget exhausted")
	ErrNotConverged   = errors.New("vector rebuild: reconciliation did not converge")
	ErrUnavailable    = errors.New("vector rebuild: unavailable")
	ErrTenantMismatch = errors.New("vector rebuild: tenant mismatch")
	ErrNotRunning     = errors.New("vector rebuild: run is not scanning")
)

const RunIDPrefix = "vrr-v1-"

// Phase is the durable rebuild run phase. Scanning means a valid owner holds
// the lease; scanned means the enqueue scan finished (this is NOT backend
// convergence); completed requires bounded reconciliation with zero drift.
type Phase string

const (
	PhasePending   Phase = "pending"
	PhaseScanning  Phase = "scanning"
	PhaseScanned   Phase = "scanned"
	PhaseCompleted Phase = "completed"
	PhaseFailed    Phase = "failed"
	PhaseCancelled Phase = "cancelled"
)

func (p Phase) Terminal() bool {
	return p == PhaseCompleted || p == PhaseFailed || p == PhaseCancelled
}

// Category is the low-cardinality safe error category persisted on a run.
type Category string

const (
	CategoryCancelled     Category = "cancelled"
	CategoryDeadline      Category = "deadline"
	CategoryInvalidReq    Category = "invalid_request"
	CategoryStaleTask     Category = "stale_task"
	CategoryUnavailable   Category = "unavailable"
	CategoryRetryable     Category = "retryable"
	CategoryPermanent     Category = "permanent"
	CategoryUnknown       Category = "unknown"
	CategoryLeaseLost     Category = "lease_lost"
	CategoryFenceRejected Category = "fence_rejected"
)

func validCategory(category Category) bool {
	switch category {
	case CategoryCancelled, CategoryDeadline, CategoryInvalidReq, CategoryStaleTask,
		CategoryUnavailable, CategoryRetryable, CategoryPermanent, CategoryUnknown,
		CategoryLeaseLost, CategoryFenceRejected:
		return true
	default:
		return false
	}
}

// Run is the durable rebuild run fact. It carries no content, vector, query,
// credential or raw error text.
type Run struct {
	TenantID     string
	RunID        string
	Fingerprint  string
	Mode         string
	Phase        Phase
	Cursor       string
	Attempt      int
	MaxAttempts  int
	Scanned      int64
	Enqueued     int64
	Tombstoned   int64
	LeaseOwner   string
	LeaseEpoch   storage.Epoch
	LeaseFence   uint64
	LeaseExpires time.Time
	LastError    string
	DeadlineAt   time.Time
	CreatedAt    time.Time
	UpdatedAt    time.Time
	CompletedAt  time.Time
}

// ProjectionFingerprint derives the server-owned fingerprint of a projection
// configuration. Callers cannot choose or spoof it.
func ProjectionFingerprint(config vector.ProjectionConfig) (string, error) {
	if strings.TrimSpace(config.Model) == "" || strings.TrimSpace(config.ModelVersion) == "" || strings.TrimSpace(config.SchemaVersion) == "" {
		return "", ErrInvalidConfig
	}
	if config.Dimension < 1 || config.Dimension > vector.MaxVectorDimension {
		return "", ErrInvalidConfig
	}
	material := strings.Join([]string{
		"trpc-agent/vector-rebuild-fingerprint/v1", config.Model, config.ModelVersion,
		config.SchemaVersion, strconv.Itoa(config.Dimension),
	}, "\x00")
	sum := sha256.Sum256([]byte(material))
	return hex.EncodeToString(sum[:]), nil
}

// NewRunID derives a unique server-owned run identity.
func NewRunID(tenantID, fingerprint string, now time.Time) string {
	material := strings.Join([]string{
		"trpc-agent/vector-rebuild-run/v1", tenantID, fingerprint,
		strconv.FormatInt(now.UnixNano(), 10),
	}, "\x00")
	sum := sha256.Sum256([]byte(material))
	return RunIDPrefix + hex.EncodeToString(sum[:])[:32]
}

func validRunID(value string) bool {
	if len(value) != len(RunIDPrefix)+32 || !strings.HasPrefix(value, RunIDPrefix) {
		return false
	}
	for _, r := range value[len(RunIDPrefix):] {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return false
		}
	}
	return true
}

func validHex64(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, r := range value {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return false
		}
	}
	return true
}

func validComponent(value string, max int) bool {
	if value == "" || len(value) > max {
		return false
	}
	for _, r := range value {
		if r < 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}

// Config is the server-owned rebuild configuration. Zero or out-of-bounds
// values fail closed.
type Config struct {
	Owner             string
	BatchSize         int
	MaxBatches        int64
	MaxAttempts       int
	LeaseTTL          time.Duration
	RunTimeout        time.Duration
	OperationTimeout  time.Duration
	ReconcileRounds   int
	ReconcileInterval time.Duration
	// MaxInspectPerRound bounds one inspector call and the listed window for
	// orphan observation.
	MaxInspectPerRound int
	// AllowOrphanDelete enables safe orphan deletion; it stays report-only
	// unless the identity fully re-validates server-side.
	AllowOrphanDelete bool
}

// WithDefaults validates and fills bounded defaults.
func (c Config) WithDefaults() (Config, error) {
	if !validComponent(c.Owner, 128) {
		return Config{}, ErrInvalidConfig
	}
	if c.BatchSize == 0 {
		c.BatchSize = 100
	}
	if c.BatchSize < 1 || c.BatchSize > 500 {
		return Config{}, ErrInvalidConfig
	}
	if c.MaxBatches == 0 {
		c.MaxBatches = 1000
	}
	if c.MaxBatches < 1 || c.MaxBatches > 100000 {
		return Config{}, ErrInvalidConfig
	}
	if c.MaxAttempts == 0 {
		c.MaxAttempts = 20
	}
	if c.MaxAttempts < 1 || c.MaxAttempts > 100 {
		return Config{}, ErrInvalidConfig
	}
	if c.LeaseTTL == 0 {
		c.LeaseTTL = 30 * time.Second
	}
	if c.LeaseTTL < time.Second || c.LeaseTTL > 10*time.Minute {
		return Config{}, ErrInvalidConfig
	}
	if c.RunTimeout == 0 {
		c.RunTimeout = 10 * time.Minute
	}
	if c.RunTimeout < 5*time.Second || c.RunTimeout > time.Hour {
		return Config{}, ErrInvalidConfig
	}
	if c.OperationTimeout == 0 {
		c.OperationTimeout = 10 * time.Second
	}
	if c.OperationTimeout < time.Second || c.OperationTimeout > time.Minute {
		return Config{}, ErrInvalidConfig
	}
	if c.ReconcileRounds == 0 {
		c.ReconcileRounds = 3
	}
	if c.ReconcileRounds < 1 || c.ReconcileRounds > 10 {
		return Config{}, ErrInvalidConfig
	}
	if c.ReconcileInterval == 0 {
		c.ReconcileInterval = 250 * time.Millisecond
	}
	if c.ReconcileInterval < 10*time.Millisecond || c.ReconcileInterval > time.Minute {
		return Config{}, ErrInvalidConfig
	}
	if c.MaxInspectPerRound == 0 {
		c.MaxInspectPerRound = 200
	}
	if c.MaxInspectPerRound < 1 || c.MaxInspectPerRound > 1000 {
		return Config{}, ErrInvalidConfig
	}
	return c, nil
}

// LeaseRef carries the Redis lease identity fencing every durable run
// mutation. It mirrors the P1-06C semantics.
type LeaseRef struct {
	Owner     string
	Epoch     storage.Epoch
	Fence     uint64
	ExpiresAt time.Time
}

func (l LeaseRef) valid() bool {
	return l.Owner != "" && l.Epoch > 0 && l.Fence > 0 && !l.ExpiresAt.IsZero()
}

// ReconcilePolicy is the server-owned reconciliation policy. DryRun is the
// default: drift is observed and reported, never repaired.
type ReconcilePolicy struct {
	DryRun            bool
	AllowOrphanDelete bool
}

// Class enumerates bounded reconciliation drift classes.
type Class string

const (
	ClassConsistent        Class = "consistent"
	ClassMissing           Class = "missing"
	ClassStale             Class = "stale"
	ClassTombstonedPresent Class = "tombstoned_present"
	ClassOrphan            Class = "orphan"
	ClassInvalid           Class = "invalid"
	ClassWrongProjection   Class = "wrong_projection"
)

// Report is the bounded reconciliation result. It contains only counts and
// class buckets of server-owned document identities.
type Report struct {
	DryRun     bool
	Consistent int
	Missing    int
	Stale      int
	Tombstoned int
	Orphan     int
	Invalid    int
	WrongProj  int
	Repaired   int
	Classes    map[Class][]string
}

func (r *Report) add(class Class, documentID string) {
	switch class {
	case ClassConsistent:
		r.Consistent++
	case ClassMissing:
		r.Missing++
	case ClassStale:
		r.Stale++
	case ClassTombstonedPresent:
		r.Tombstoned++
	case ClassOrphan:
		r.Orphan++
	case ClassInvalid:
		r.Invalid++
	case ClassWrongProjection:
		r.WrongProj++
	default:
		return
	}
	if r.Classes == nil {
		r.Classes = make(map[Class][]string)
	}
	r.Classes[class] = append(r.Classes[class], documentID)
}

var _ = fmt.Sprintf
var _ = tenant.TenantContext{}
