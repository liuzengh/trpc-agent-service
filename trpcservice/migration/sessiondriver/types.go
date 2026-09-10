// Package sessiondriver migrates tenant session snapshots between exact backend bindings.
package sessiondriver

import (
	"context"
	"encoding/json"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/migration"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	sessionstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/session"
)

const Domain = "session"

type MutationState string
type MutationDirection string

const (
	MutationPending  MutationState     = "pending"
	MutationApplying MutationState     = "applying"
	MutationApplied  MutationState     = "applied"
	DirectionForward MutationDirection = "forward"
	DirectionReverse MutationDirection = "reverse"
)

type Mutation struct {
	TenantID, MigrationID, MutationID string
	Epoch                             int64
	Direction                         MutationDirection
	sessionstore.SessionKey
	SourceVersion  int64
	MutationDigest string
	State          MutationState
	Attempt        int
	LeaseOwner     string
	LeaseUntil     time.Time
	NotBefore      time.Time
	LastErrorClass string
	TargetVersion  int64
	TargetDigest   string
	CreatedAt      time.Time
	AppliedAt      time.Time
	Version        int64
}

type RecordRequest struct {
	TenantID, MigrationID, MutationID string
	Epoch                             int64
	Direction                         MutationDirection
	sessionstore.SessionKey
	SourceVersion  int64
	MutationDigest string
	CreatedAt      time.Time
}

type ClaimRequest struct {
	TenantID, MigrationID, WorkerID string
	Limit                           int
	Now                             time.Time
	Lease                           time.Duration
}

type CompleteRequest struct {
	TenantID, MigrationID, MutationID, WorkerID string
	sessionstore.SessionKey
	ExpectedVersion, TargetVersion int64
	TargetDigest                   string
	At                             time.Time
}

type RetryRequest struct {
	TenantID, MigrationID, MutationID, WorkerID string
	sessionstore.SessionKey
	ExpectedVersion int64
	ErrorClass      string
	NotBefore       time.Time
	At              time.Time
}

type MutationLedger interface {
	Claim(context.Context, ClaimRequest) ([]Mutation, error)
	MarkApplied(context.Context, CompleteRequest) (Mutation, error)
	MarkRetry(context.Context, RetryRequest) (Mutation, error)
	Outstanding(context.Context, string, string) (int64, error)
}

// Recorder is used by adapters that cannot append the ledger in their local
// commit transaction. PostgreSQL commits are captured by database triggers;
// its implementation remains available to external target adapters.
type Recorder interface {
	Record(context.Context, RecordRequest) (Mutation, error)
}

type SnapshotReader interface {
	LoadSessionImage(context.Context, sessionstore.SessionKey) (SessionImage, error)
}

// SDKSessionImage is a lossless image of the session-scoped rows owned by
// trpc-agent-go/session/postgres. The migration driver deliberately treats
// their JSON payloads as opaque framework data; it does not reinterpret or
// recreate SDK event/state semantics.
type SDKSessionImage struct {
	AppName, UserID, SessionID string
	State                      json.RawMessage
	CreatedAt, UpdatedAt       time.Time
	ExpiresAt                  *time.Time
	Events                     []SDKEventRecord
	TrackEvents                []SDKTrackEventRecord
	Summaries                  []SDKSummaryRecord
}

type SDKEventRecord struct {
	Event                json.RawMessage
	CreatedAt, UpdatedAt time.Time
	ExpiresAt            *time.Time
}

type SDKTrackEventRecord struct {
	Track                string
	Event                json.RawMessage
	CreatedAt, UpdatedAt time.Time
	ExpiresAt            *time.Time
}

type SDKSummaryRecord struct {
	FilterKey string
	Summary   json.RawMessage
	UpdatedAt time.Time
	ExpiresAt *time.Time
}

type CommitRecord struct {
	CommitID, RequestID, RequestDigest, Stage string
	InputSeq, Fence                           uint64
	Outcome                                   runtime.Outcome
	SessionVersion                            int64
	ReplyCursor, ResultRef                    string
	CreatedAt                                 time.Time
}

// SessionImage contains platform coordination facts plus the opaque,
// framework-owned Session image. Messaging outbox and execution records stay
// in their own authorities.
type SessionImage struct {
	Head                  sessionstore.SessionHead
	LastAllocatedInputSeq uint64
	SDK                   *SDKSessionImage
	Commits               []CommitRecord
}

type ApplyRequest struct {
	TenantID, MigrationID, MutationID string
	Epoch                             int64
	Image                             SessionImage
	SnapshotDigest                    string
}

type ApplyResult struct {
	SessionVersion int64
	SnapshotDigest string
}

// ReplicaWriter must apply MutationID idempotently, reject the same ID with
// different epoch, session coordinates, version, or digest, and never replace
// a newer session image with an older version.
type ReplicaWriter interface {
	ApplySessionSnapshot(context.Context, ApplyRequest) (ApplyResult, error)
}

type PageRequest struct {
	TenantID, SnapshotWatermark, After string
	Limit                              int
}

type Page struct {
	Sessions       []SessionImage
	NextCheckpoint string
	Complete       bool
}

// BackfillSource returns a stable tenant-scoped order fixed by SnapshotWatermark.
type BackfillSource interface {
	PageSessions(context.Context, PageRequest) (Page, error)
}

type Fingerprint struct {
	Count, DivergenceCount int64
	Digest, Watermark      string
	SampleDigest           string
}

type Inventory interface {
	// An empty watermark asks the source to capture one. A non-empty watermark
	// must bound the same stable session-key range on every backend.
	Fingerprint(context.Context, string, string) (Fingerprint, error)
}

type Driver struct {
	Authority migration.Repository
	Ledger    MutationLedger
	Source    SnapshotReader
	Backfill  BackfillSource
	Target    ReplicaWriter
	// ReverseSource and ReverseTarget are required only for reverse mutations
	// captured after cutover. They read the target authority and repair source.
	ReverseSource                    SnapshotReader
	ReverseTarget                    ReplicaWriter
	SourceInventory, TargetInventory Inventory
}

type RepairRequest struct {
	TenantID, MigrationID, WorkerID string
	Limit                           int
	Now                             time.Time
	Lease, RetryDelay               time.Duration
}

type RepairResult struct{ Claimed, Applied, Retried int }

type BackfillRequest struct {
	TenantID, MigrationID string
	Limit                 int
	At                    time.Time
}
