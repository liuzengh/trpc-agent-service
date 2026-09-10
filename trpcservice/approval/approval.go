// Package approval defines the durable, metadata-only human approval contract
// for review-required tool calls.
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

// Status is the terminal decision state of one approval request.
type Status string

const (
	StatusPending  Status = "PENDING"
	StatusApproved Status = "APPROVED"
	StatusDenied   Status = "DENIED"
	StatusExpired  Status = "EXPIRED"
)

var (
	// ErrNotFound means an approval does not exist in the requested scope.
	ErrNotFound = errors.New("approval not found")
	// ErrAlreadyDecided means a different decision was already recorded.
	ErrAlreadyDecided = errors.New("approval was already decided")
	// ErrExpired means a pending approval passed its expiry time.
	ErrExpired = errors.New("approval has expired")
)

const DefaultTTL = 15 * time.Minute

// Request is the safe identity of a tool approval. ArgumentDigest is a
// SHA-256 digest; raw tool arguments are deliberately absent.
type Request struct {
	TenantID       string
	AppID          string
	ConfigVersion  string
	RequestID      string
	SessionID      string
	ToolName       string
	ToolCallID     string
	ArgumentDigest string
	ExpiresAt      time.Time
}

func (r Request) Validate() error {
	for name, value := range map[string]string{
		"tenant_id":       r.TenantID,
		"app_id":          r.AppID,
		"config_version":  r.ConfigVersion,
		"request_id":      r.RequestID,
		"session_id":      r.SessionID,
		"tool_name":       r.ToolName,
		"argument_digest": r.ArgumentDigest,
	} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s is required", name)
		}
	}
	if len(r.ArgumentDigest) != sha256.Size*2 {
		return errors.New("argument_digest must be a SHA-256 hex digest")
	}
	if _, err := hex.DecodeString(r.ArgumentDigest); err != nil {
		return errors.New("argument_digest must be a SHA-256 hex digest")
	}
	if r.ExpiresAt.IsZero() {
		return errors.New("expires_at is required")
	}
	return nil
}

// DigestArguments creates the only persisted representation of tool args.
func DigestArguments(args []byte) string {
	digest := sha256.Sum256(args)
	return hex.EncodeToString(digest[:])
}

// Record is the durable approval row. It contains no prompt, model output,
// raw arguments, secret, or provider credential.
type Record struct {
	ApprovalID     string     `json:"approval_id"`
	TenantID       string     `json:"tenant_id"`
	AppID          string     `json:"app_id"`
	ConfigVersion  string     `json:"config_version"`
	RequestID      string     `json:"request_id"`
	SessionID      string     `json:"session_id"`
	ToolName       string     `json:"tool_name"`
	ToolCallID     string     `json:"tool_call_id,omitempty"`
	ArgumentDigest string     `json:"argument_digest"`
	Status         Status     `json:"status"`
	ExpiresAt      time.Time  `json:"expires_at"`
	CreatedAt      time.Time  `json:"created_at"`
	DecidedAt      *time.Time `json:"decided_at,omitempty"`
}

func (r Record) Validate() error {
	if r.ApprovalID == "" {
		return errors.New("approval_id is required")
	}
	request := Request{
		TenantID: r.TenantID, AppID: r.AppID, ConfigVersion: r.ConfigVersion,
		RequestID: r.RequestID, SessionID: r.SessionID, ToolName: r.ToolName,
		ToolCallID: r.ToolCallID, ArgumentDigest: r.ArgumentDigest,
		ExpiresAt: r.ExpiresAt,
	}
	if err := request.Validate(); err != nil {
		return err
	}
	switch r.Status {
	case StatusPending, StatusApproved, StatusDenied, StatusExpired:
	default:
		return fmt.Errorf("approval status %q is invalid", r.Status)
	}
	if r.CreatedAt.IsZero() {
		return errors.New("created_at is required")
	}
	if r.Status == StatusPending && r.DecidedAt != nil {
		return errors.New("pending approval cannot have decided_at")
	}
	if r.Status != StatusPending && r.DecidedAt == nil {
		return errors.New("decided approval must have decided_at")
	}
	return nil
}

// Query selects one tenant/application approval partition.
type Query struct {
	TenantID string
	AppID    string
	Status   Status
	Limit    int
}

func (q Query) Validate() error {
	if q.TenantID == "" || q.AppID == "" {
		return errors.New("tenant_id and app_id are required")
	}
	if q.Status != "" {
		switch q.Status {
		case StatusPending, StatusApproved, StatusDenied, StatusExpired:
		default:
			return errors.New("approval status is invalid")
		}
	}
	if q.Limit < 0 || q.Limit > 1000 {
		return errors.New("approval limit is invalid")
	}
	return nil
}

// Repository is the durable approval capability consumed by Worker and Admin.
type Repository interface {
	ResolveOrCreate(context.Context, Request) (Record, error)
	List(context.Context, Query) ([]Record, error)
	Decide(context.Context, string, string, string, Status) (Record, error)
}
