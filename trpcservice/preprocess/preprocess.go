// Package preprocess owns the durable, leased transition between verified
// Channel ingress and execution dispatch.
package preprocess

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
)

type State string

const (
	Pending   State = "pending"
	Running   State = "running"
	Ready     State = "ready"
	Rejected  State = "rejected"
	RetryWait State = "retry_wait"
)

type ClaimRequest struct {
	Inbox            messaging.ClaimInboxRequest
	TenantVersion    int64
	ConfigVersion    int64
	ChannelBindingID string
	UserID           string
	TraceParent      string
}

type Job struct {
	TenantID, RequestID, JobID, PayloadRef, PreparedPayloadRef, AgentAppID string
	SessionID, UserID, Channel, ChannelBindingID, TraceParent              string
	TenantVersion, ConfigVersion                                           int64
	State                                                                  State
	Attempt                                                                int
	Version                                                                int64
	LeaseOwner, RejectReason                                               string
	LeaseUntil, NotBefore, CreatedAt, UpdatedAt                            time.Time
	DispatchedAt                                                           time.Time
}

type ClaimOptions struct {
	Owner string
	Now   time.Time
	TTL   time.Duration
	Limit int
}

type Store interface {
	ClaimInboxAndSchedule(context.Context, ClaimRequest) (messaging.InboxRecord, Job, error)
	ClaimJobs(context.Context, ClaimOptions) ([]Job, error)
	FinishReady(context.Context, Job) (Job, error)
	FinishRetry(context.Context, Job, time.Time, string) (Job, error)
	FinishRejected(context.Context, Job, string) (Job, error)
	ListReadyForDispatch(context.Context, int) ([]Job, error)
	ClaimReadyForDispatch(context.Context, ClaimOptions) ([]Job, error)
	MarkDispatched(context.Context, Job, time.Time) (Job, error)
}

func StableJobID(tenantID, requestID string) (string, error) {
	if tenantID == "" || requestID == "" {
		return "", runtime.ErrInvariantViolation
	}
	sum := sha256.Sum256([]byte(tenantID + "\x00" + requestID + "\x00preprocess-v1"))
	return "pp1_" + base64.RawURLEncoding.EncodeToString(sum[:]), nil
}
