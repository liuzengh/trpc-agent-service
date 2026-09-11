// Package approval persists human decisions for dangerous tool calls.
package approval

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	StatusPending  = "pending"
	StatusApproved = "approved"
	StatusDenied   = "denied"
	StatusExpired  = "expired"
)

var (
	ErrNotFound  = errors.New("tool approval not found")
	ErrForbidden = errors.New("tool approval identity mismatch")
	ErrConflict  = errors.New("tool approval decision conflict")
	ErrExpired   = errors.New("tool approval expired")
)

type Request struct {
	TenantID         string
	AppID            string
	RevisionID       string
	ChannelBindingID string
	RequestID        string
	MessageID        string
	UserID           string
	SessionID        string
	ToolCallID       string
	ToolName         string
	ArgumentsHash    string
	ResumeText       string
	ReplyTarget      string
	ExpiresAt        time.Time
}

type Record struct {
	OriginTraceParent string
	ApprovalID        string
	TenantID          string
	AppID             string
	RevisionID        string
	ChannelBindingID  string
	RequestID         string
	MessageID         string
	UserID            string
	SessionID         string
	ToolCallID        string
	ToolName          string
	ArgumentsHash     string
	ResumeText        string
	ReplyTarget       string
	Status            string
	DecisionMessageID string
	DecisionReason    string
	ExpiresAt         time.Time
	CreatedAt         time.Time
	DecidedAt         time.Time
	ResumedAt         time.Time
}

type Decision struct {
	ApprovalID        string
	TenantID          string
	ChannelBindingID  string
	UserID            string
	SessionID         string
	ExternalMessageID string
	Status            string
	Reason            string
}

type Repository interface {
	Request(ctx context.Context, request Request) (Record, error)
	ListPendingByRequest(ctx context.Context, tenantID string, requestID string) ([]Record, error)
	ListPendingBySession(ctx context.Context, tenantID, bindingID, userID, sessionID string) ([]Record, error)
	Decide(ctx context.Context, decision Decision) (Record, error)
	MarkResumed(ctx context.Context, approvalID string) error
	Ready(ctx context.Context) error
	Close() error
}

func StableID(tenantID string, requestID string, toolCallID string) string {
	digest := sha256.Sum256([]byte(tenantID + "\x00" + requestID + "\x00" + toolCallID))
	return "apr_" + hex.EncodeToString(digest[:16])
}

func ArgumentsHash(arguments []byte) string {
	digest := sha256.Sum256(arguments)
	return hex.EncodeToString(digest[:])
}

func validateRequest(request *Request) error {
	if request == nil {
		return fmt.Errorf("approval request is required")
	}
	request.TenantID = strings.TrimSpace(request.TenantID)
	request.AppID = strings.TrimSpace(request.AppID)
	request.RevisionID = strings.TrimSpace(request.RevisionID)
	request.ChannelBindingID = strings.TrimSpace(request.ChannelBindingID)
	request.RequestID = strings.TrimSpace(request.RequestID)
	request.MessageID = strings.TrimSpace(request.MessageID)
	request.UserID = strings.TrimSpace(request.UserID)
	request.SessionID = strings.TrimSpace(request.SessionID)
	request.ToolCallID = strings.TrimSpace(request.ToolCallID)
	request.ToolName = strings.TrimSpace(request.ToolName)
	request.ArgumentsHash = strings.TrimSpace(request.ArgumentsHash)
	request.ResumeText = strings.TrimSpace(request.ResumeText)
	request.ReplyTarget = strings.TrimSpace(request.ReplyTarget)
	if request.TenantID == "" || request.AppID == "" || request.RevisionID == "" ||
		request.ChannelBindingID == "" || request.RequestID == "" || request.MessageID == "" ||
		request.UserID == "" || request.SessionID == "" || request.ToolCallID == "" ||
		request.ToolName == "" || request.ArgumentsHash == "" || request.ResumeText == "" {
		return fmt.Errorf("approval request identity and resume data are required")
	}
	if request.ReplyTarget == "" {
		request.ReplyTarget = request.UserID
	}
	if request.ExpiresAt.IsZero() {
		request.ExpiresAt = time.Now().UTC().Add(15 * time.Minute)
	}
	return nil
}

func validDecisionStatus(status string) bool {
	return status == StatusApproved || status == StatusDenied
}
