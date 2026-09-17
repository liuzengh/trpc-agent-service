package memorydriver

import (
	"context"
	"time"
)

type Direction string
type MutationState string

const (
	DirectionForward Direction = "forward"
	DirectionReverse Direction = "reverse"
)

const (
	StatePending  MutationState = "pending"
	StateApplying MutationState = "applying"
	StateApplied  MutationState = "applied"
)

// UserKey is the smallest replayable Memory unit. A source user image is
// reapplied as a whole, preserving source authority when Redis has no event
// log or per-record revision.
type UserKey struct{ TenantID, AppName, UserID string }

type Mutation struct {
	TenantID, MigrationID, MutationID string
	Epoch                             int64
	Direction                         Direction
	Key                               UserKey
	SourceDigest                      string
	State                             MutationState
	Attempt                           int
	LeaseOwner                        string
	LeaseUntil, NotBefore             time.Time
	LastErrorClass, TargetDigest      string
	CreatedAt, AppliedAt              time.Time
	Version                           int64
}
type RecordRequest struct {
	TenantID, MigrationID, MutationID string
	Epoch, ConfigVersion              int64
	Direction                         Direction
	Key                               UserKey
	SourceDigest                      string
	CreatedAt                         time.Time
}
type ClaimRequest struct {
	TenantID, MigrationID, WorkerID string
	Limit                           int
	Now                             time.Time
	Lease                           time.Duration
}
type CompleteRequest struct {
	TenantID, MigrationID, MutationID, WorkerID string
	Key                                         UserKey
	ExpectedVersion                             int64
	TargetDigest                                string
	At                                          time.Time
}
type RetryRequest struct {
	TenantID, MigrationID, MutationID, WorkerID string
	Key                                         UserKey
	ExpectedVersion                             int64
	ErrorClass                                  string
	NotBefore, At                               time.Time
}
type MutationLedger interface {
	Record(context.Context, RecordRequest) (Mutation, error)
	Claim(context.Context, ClaimRequest) ([]Mutation, error)
	MarkApplied(context.Context, CompleteRequest) (Mutation, error)
	MarkRetry(context.Context, RetryRequest) (Mutation, error)
	Outstanding(context.Context, string, string) (int64, error)
}

// UserSnapshotReader reads the current authoritative image for one user.  A
// repair intentionally replays a whole user image: neither Redis nor the
// upstream Memory service exposes a portable, ordered change feed.
type UserSnapshotReader interface {
	LoadUser(context.Context, UserKey) ([]Image, string, error)
}

// UserApplier replaces one user's target image atomically.  A plain bulk
// Applier cannot express source-side deletes, so online replication must use
// this stronger port.
type UserApplier interface {
	ApplyUser(context.Context, UserKey, []Image) (string, error)
}
