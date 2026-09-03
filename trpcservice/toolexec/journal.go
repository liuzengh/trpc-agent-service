// Package toolexec records tool side-effect boundaries and blocks blind replay.
package toolexec

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"
)

const (
	StatusRunning   = "running"
	StatusSucceeded = "succeeded"
	StatusFailed    = "failed"
)

var ErrReplayBlocked = errors.New("tool execution replay requires reconciliation")

type Execution struct {
	ID            string
	TenantID      string
	RequestID     string
	RevisionID    string
	ToolCallID    string
	ToolName      string
	ArgumentsHash string
	Status        string
	ResultHash    string
	ErrorType     string
	StartedAt     time.Time
	CompletedAt   time.Time
}

type StartResult struct {
	Execution Execution
	Existing  bool
}

type Journal interface {
	Start(ctx context.Context, execution Execution) (StartResult, error)
	Complete(ctx context.Context, executionID string, status string, resultHash string, errorType string) error
	Ready(ctx context.Context) error
	Close() error
}

func StableID(requestID string, toolCallID string) string {
	digest := sha256.Sum256([]byte(requestID + "\x00" + toolCallID))
	return "tool_" + hex.EncodeToString(digest[:16])
}

func Hash(value []byte) string {
	digest := sha256.Sum256(value)
	return hex.EncodeToString(digest[:])
}

func validate(execution *Execution) error {
	if execution == nil || execution.TenantID == "" || execution.RequestID == "" ||
		execution.RevisionID == "" || execution.ToolCallID == "" ||
		execution.ToolName == "" || execution.ArgumentsHash == "" {
		return fmt.Errorf("tool execution identity is required")
	}
	if execution.ID == "" {
		execution.ID = StableID(execution.RequestID, execution.ToolCallID)
	}
	return nil
}
