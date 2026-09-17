package gateway

import (
	"context"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type PrepareDispatchRequest struct {
	Tenant      tenant.Context
	Binding     tenant.ExecutionBinding
	RequestID   string
	SessionID   string
	UserID      string
	PayloadRef  string
	TraceParent string
}

type PreparedDispatch struct {
	Envelope       runtime.ExecutionEnvelope
	Accepted       bool
	TerminalReason string
}
type ExecutionKey struct{ TenantID, RequestID string }
type ExecutionStatus struct {
	Envelope        runtime.ExecutionEnvelope
	Outcome         runtime.Outcome
	ResultRef       string
	Version         int64
	CancelRequested bool
	CancelVersion   int64
}
type CancelRequest struct {
	TenantID, RequestID string
	ExpectedVersion     int64
	ActorID             string
	ReasonCode          string
	TraceParent         string
}
type CancelResult struct {
	Accepted      bool
	Version       int64
	CancelVersion int64
}
type ParkRequest struct {
	TenantID, RequestID string
	InputSeq            uint64
}

type ParkDisposition string

const (
	ParkedInput       ParkDisposition = "parked"
	ParkInputReady    ParkDisposition = "ready"
	ParkInputTerminal ParkDisposition = "terminal"
	ParkInputBlocked  ParkDisposition = "blocked"
)

type ParkResult struct {
	Disposition ParkDisposition
	Attempt     int
	Version     int64
	NotBefore   time.Time
	Deadline    time.Time
}

type ParkPolicy struct {
	BaseDelay   time.Duration
	MaxDelay    time.Duration
	Deadline    time.Duration
	MaxAttempts int
}

func DefaultParkPolicy() ParkPolicy {
	return ParkPolicy{BaseDelay: time.Second, MaxDelay: 5 * time.Minute, Deadline: 15 * time.Minute, MaxAttempts: 8}
}

func (p ParkPolicy) Validate() error {
	if p.BaseDelay < time.Second || p.MaxDelay < p.BaseDelay || p.Deadline < p.MaxDelay || p.MaxAttempts < 1 || p.MaxAttempts > 64 {
		return runtime.ErrInvariantViolation
	}
	return nil
}

type WakeupCandidate struct {
	Execution ExecutionStatus
	Ready     bool
	Blocked   bool
	Version   int64
}

type InputParker interface {
	ParkInput(context.Context, ParkRequest) (ParkResult, error)
}

type WakeupStore interface {
	InspectWakeup(context.Context, ExecutionKey) (WakeupCandidate, error)
	MarkWoken(context.Context, ExecutionKey, int64) error
}

type ParkedInputStore interface {
	WakeupStore
	ListActionableParkedInputs(context.Context, time.Time, int) ([]ExecutionKey, error)
}

type ExecutionReader interface {
	GetExecution(context.Context, ExecutionKey) (ExecutionStatus, error)
}

type TaskStore interface {
	PrepareDispatch(context.Context, PrepareDispatchRequest) (PreparedDispatch, error)
	ExecutionReader
	RequestCancel(context.Context, CancelRequest) (CancelResult, error)
	InputParker
}
