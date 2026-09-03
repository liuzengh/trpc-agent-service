// Package gateway persists inbound messages before asynchronous Agent work.
package gateway

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
	"github.com/liuzengh/trpc-agent-service/trpcservice/workqueue"
)

var (
	ErrMessageConflict = errors.New("external message ID was reused with different payload")
	ErrJournalClosed   = errors.New("inbound journal is closed")
)

// InboundRequest is the normalized message accepted by the durable Gateway.
type InboundRequest struct {
	Scope             runtimecontext.Scope
	ExternalMessageID string
	UserID            string
	SessionID         string
	ChatType          string
	Text              string
	ReplyTarget       string
}

// AcceptResult identifies the durable records created for an inbound message.
type AcceptResult struct {
	InboundID      string `json:"inbound_id"`
	RequestID      string `json:"request_id"`
	ConversationID string `json:"conversation_id"`
	RevisionID     string `json:"revision_id"`
	TurnSeq        int64  `json:"turn_seq"`
	Duplicate      bool   `json:"duplicate"`
}

// QueueOutboxItem is one claimed task waiting to be published.
type QueueOutboxItem struct {
	ID   string
	Task workqueue.AgentTask
}

// RunResult is the durable outcome written by an Agent Worker.
type RunResult struct {
	Reply        string
	AgentName    string
	FencingToken int64
	EventCount   int
}

// OutboundItem is one reply claimed for provider delivery.
type OutboundItem struct {
	ID               string
	RequestID        string
	TenantID         string
	ChannelBindingID string
	Text             string
	ReplyTarget      string
	AttemptCount     int
}

// Journal atomically creates inbound, run and queue-outbox records.
type Journal interface {
	Accept(ctx context.Context, request InboundRequest) (AcceptResult, error)
	ClaimQueueOutbox(
		ctx context.Context,
		workerID string,
		limit int,
		lease time.Duration,
	) ([]QueueOutboxItem, error)
	MarkQueueOutboxPublished(ctx context.Context, outboxID string, workerID string) error
	MarkQueueOutboxFailed(
		ctx context.Context,
		outboxID string,
		workerID string,
		retryAt time.Time,
		cause error,
	) error
	MarkRunRunning(ctx context.Context, requestID string, workerID string) error
	CompleteRun(ctx context.Context, task workqueue.AgentTask, result RunResult) error
	FailRun(ctx context.Context, requestID string, errorType string, cause error) error
	ClaimOutbound(
		ctx context.Context,
		workerID string,
		limit int,
		lease time.Duration,
	) ([]OutboundItem, error)
	MarkOutboundSent(
		ctx context.Context,
		outboundID string,
		workerID string,
		providerMessageID string,
	) error
	MarkOutboundFailed(
		ctx context.Context,
		outboundID string,
		workerID string,
		retryAt time.Time,
		terminal bool,
		cause error,
	) error
	Ready(ctx context.Context) error
	Close() error
}

func validateInbound(request *InboundRequest) error {
	if request == nil {
		return fmt.Errorf("inbound request is required")
	}
	if err := request.Scope.Validate(); err != nil {
		return fmt.Errorf("validate inbound scope: %w", err)
	}
	request.ExternalMessageID = strings.TrimSpace(request.ExternalMessageID)
	request.UserID = strings.TrimSpace(request.UserID)
	request.SessionID = strings.TrimSpace(request.SessionID)
	request.ChatType = strings.TrimSpace(request.ChatType)
	request.Text = strings.TrimSpace(request.Text)
	request.ReplyTarget = strings.TrimSpace(request.ReplyTarget)
	if request.ExternalMessageID == "" || request.UserID == "" ||
		request.SessionID == "" || request.ChatType == "" || request.Text == "" {
		return fmt.Errorf("inbound message ID, user, session, chat type and text are required")
	}
	if request.ReplyTarget == "" {
		request.ReplyTarget = request.UserID
	}
	return nil
}
