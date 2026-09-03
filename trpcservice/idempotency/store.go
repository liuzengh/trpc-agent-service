// Package idempotency prevents one external message from executing an Agent
// turn more than once.
package idempotency

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

var (
	// ErrStoreClosed means the idempotency store no longer accepts work.
	ErrStoreClosed = errors.New("idempotency store is closed")
	// ErrAttemptLost means a processing record is no longer owned by this run.
	ErrAttemptLost = errors.New("idempotency attempt lost")
	// ErrRetry means the previous processing owner failed or expired, so the
	// caller should run Begin again.
	ErrRetry = errors.New("idempotency operation should be retried")
	// ErrKeyConflict means one message ID was reused for different content.
	ErrKeyConflict = errors.New("message_id was reused with different content")
)

// Key scopes a message ID to one application, user and Session.
type Key struct {
	AppName          string
	UserID           string
	SessionID        string
	MessageID        string
	ChannelBindingID string
}

// Validate rejects incomplete idempotency keys.
func (k Key) Validate() error {
	if strings.TrimSpace(k.AppName) == "" {
		return fmt.Errorf("idempotency app name is required")
	}
	if strings.TrimSpace(k.UserID) == "" {
		return fmt.Errorf("idempotency user ID is required")
	}
	if strings.TrimSpace(k.SessionID) == "" {
		return fmt.Errorf("idempotency session ID is required")
	}
	if strings.TrimSpace(k.MessageID) == "" {
		return fmt.Errorf("idempotency message ID is required")
	}
	if strings.TrimSpace(k.ChannelBindingID) == "" {
		return fmt.Errorf("idempotency channel binding ID is required")
	}
	return nil
}

// Result is the transport-neutral value cached after a successful Agent turn.
type Result struct {
	Reply      string `json:"reply"`
	RequestID  string `json:"request_id"`
	EventCount int    `json:"event_count"`
	AgentName  string `json:"agent_name"`
}

// BeginStatus describes what Begin found for one message ID.
type BeginStatus uint8

const (
	// BeginStarted means the caller owns a new processing attempt.
	BeginStarted BeginStatus = iota + 1
	// BeginProcessing means another caller currently owns the message.
	BeginProcessing
	// BeginCompleted means the cached Result can be returned immediately.
	BeginCompleted
)

// BeginResult contains either a new Attempt or a previously completed Result.
type BeginResult struct {
	Status  BeginStatus
	Attempt Attempt
	Result  Result
}

// Store owns processing records and completed result caches.
type Store interface {
	Begin(ctx context.Context, key Key, fingerprint string) (BeginResult, error)
	Wait(ctx context.Context, key Key, fingerprint string) (Result, error)
	Ready(ctx context.Context) error
	Close() error
}

// Attempt is the ownership handle for a newly started message.
type Attempt interface {
	Context() context.Context
	Complete(ctx context.Context, result Result) error
	Fail(ctx context.Context) error
}
