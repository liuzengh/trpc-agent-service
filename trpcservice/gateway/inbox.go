// Package gateway persists inbound messages before asynchronous Agent work.
package gateway

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
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

// Journal atomically creates inbound, run and queue-outbox records.
type Journal interface {
	Accept(ctx context.Context, request InboundRequest) (AcceptResult, error)
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
	if request.ExternalMessageID == "" || request.UserID == "" ||
		request.SessionID == "" || request.ChatType == "" || request.Text == "" {
		return fmt.Errorf("inbound message ID, user, session, chat type and text are required")
	}
	return nil
}
