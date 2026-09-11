package domain

import (
	"errors"
	"fmt"
	"math"
	"time"
)

var (
	ErrProjectionBlocked      = errors.New("route projection is quarantined")
	ErrNotInitialized         = errors.New("route projection replay is not initialized")
	ErrStreamPositionRequired = errors.New("trusted stream position is required")
	ErrProjectionStale        = errors.New("route projection source observation is stale")
	ErrProjectionApplyLag     = errors.New("route projection continuous apply lag exceeded")
	ErrHistoryGap             = errors.New("route source history is incomplete")
)

// ReplaySource is obtained from trusted JetStream StreamInfo, not from payload.
// StreamID is StreamInfo.Created in UTC RFC3339Nano: same-name recreation creates
// a new instance and requires explicit recovery, never reuse of old projections.
type ReplaySource struct {
	// ObservedAt is the local trusted StreamInfo observation time, not payload data.
	ObservedAt    time.Time
	StreamName    string
	StreamID      string
	FirstSequence uint64
	LastSequence  uint64
	MessageCount  uint64
}

func (s ReplaySource) Validate() error {
	if !identifier.MatchString(s.StreamName) || len(s.StreamID) > 128 {
		return ErrInvalidEvent
	}
	if _, err := time.Parse(time.RFC3339Nano, s.StreamID); err != nil {
		return ErrInvalidEvent
	}
	if s.FirstSequence > math.MaxInt64 || s.LastSequence > math.MaxInt64 || s.MessageCount > math.MaxInt64 {
		return ErrInvalidEvent
	}
	if s.LastSequence == 0 {
		if s.MessageCount != 0 || s.FirstSequence > 1 {
			return ErrHistoryGap
		}
		return nil
	}
	if s.FirstSequence != 1 || s.MessageCount != s.LastSequence {
		return ErrHistoryGap
	}
	return nil
}

type StreamPosition struct {
	StreamName string
	StreamID   string
	Sequence   uint64
}

func (p StreamPosition) Validate() error {
	if !identifier.MatchString(p.StreamName) || p.Sequence == 0 || p.Sequence > math.MaxInt64 {
		return ErrStreamPositionRequired
	}
	if _, err := time.Parse(time.RFC3339Nano, p.StreamID); err != nil {
		return ErrStreamPositionRequired
	}
	return nil
}

// ProjectionHealth derives initialization from a contiguous durable replay
// checkpoint. A non-empty projections table is never evidence of initialization.
const MaxProjectionObservationAge = 5 * time.Minute
const SourceObservationInterval = 30 * time.Second

// MaxProjectionApplyLag bounds a continuous known backlog, independently of
// startup completeness and source observation age. Progress only resets this
// grace period after the entire known source watermark has been applied.
const MaxProjectionApplyLag = 60 * time.Second

type ProjectionHealth struct {
	LastObservedAt     time.Time
	ApplyLagSince      *time.Time
	ApplyLagExceeded   bool
	Stale              bool
	Initialized        bool
	BlockedReason      string
	StreamName         string
	StreamID           string
	TargetSequence     uint64
	ContiguousSequence uint64
	HighestSequence    uint64
}

func (h ProjectionHealth) RequireInitialized() error {
	if h.BlockedReason != "" && h.BlockedReason != "projection_not_initialized" {
		return fmt.Errorf("%w: %s", ErrProjectionBlocked, h.BlockedReason)
	}
	if h.Stale {
		return ErrProjectionStale
	}
	if h.ApplyLagExceeded {
		return ErrProjectionApplyLag
	}
	if !h.Initialized {
		return ErrNotInitialized
	}
	return nil
}

type QuarantineReason string

const (
	QuarantineInvalidSchema     QuarantineReason = "invalid_schema"
	QuarantineRouteConflict     QuarantineReason = "route_generation_conflict"
	QuarantineHistoryGap        QuarantineReason = "source_history_gap"
	QuarantineSourceChanged     QuarantineReason = "source_instance_changed"
	QuarantineMetadata          QuarantineReason = "source_metadata_invalid"
	QuarantineSequenceConflict  QuarantineReason = "stream_sequence_conflict"
	QuarantineCorruptProjection QuarantineReason = "corrupt_projection"
	QuarantineUnboundProjection QuarantineReason = "unbound_projection"
)

func (r QuarantineReason) Validate() error {
	switch r {
	case QuarantineInvalidSchema, QuarantineRouteConflict, QuarantineHistoryGap, QuarantineSourceChanged, QuarantineMetadata, QuarantineSequenceConflict, QuarantineCorruptProjection, QuarantineUnboundProjection:
		return nil
	}
	return ErrInvalidEvent
}
