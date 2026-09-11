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
	StatusUnknown   = "unknown"
)

var ErrReplayBlocked = errors.New("tool execution replay requires reconciliation")
var ErrConflict = errors.New("tool execution identity or outcome conflict")
var ErrNotFound = errors.New("tool execution not found")

type Execution struct {
	ID            string    `json:"execution_id"`
	TenantID      string    `json:"tenant_id"`
	RequestID     string    `json:"request_id"`
	RevisionID    string    `json:"revision_id"`
	ToolCallID    string    `json:"tool_call_id"`
	ToolName      string    `json:"tool_name"`
	ArgumentsHash string    `json:"arguments_hash"`
	OperationID   string    `json:"operation_id,omitempty"`
	Status        string    `json:"status"`
	ResultHash    string    `json:"result_hash"`
	ErrorType     string    `json:"error_type"`
	StartedAt     time.Time `json:"started_at"`
	CompletedAt   time.Time `json:"completed_at"`
}

type StartResult struct {
	Execution Execution
	Existing  bool
}

type Journal interface {
	Start(ctx context.Context, execution Execution) (StartResult, error)
	Complete(ctx context.Context, executionID string, status string, resultHash string, errorType string) error
	ListByRequest(ctx context.Context, tenantID, requestID string) ([]Execution, error)
	Get(ctx context.Context, tenantID, executionID string) (Execution, error)
	LinkOperation(ctx context.Context, tenantID, executionID, operationID string) error
	ResolveOperation(ctx context.Context, tenantID, operationID, status, resultHash, errorType string) error
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
	if execution.ID != StableID(execution.RequestID, execution.ToolCallID) {
		return ErrConflict
	}
	return nil
}

func sameExecution(a, b Execution) bool {
	return a.TenantID == b.TenantID && a.RequestID == b.RequestID && a.RevisionID == b.RevisionID &&
		a.ToolCallID == b.ToolCallID && a.ToolName == b.ToolName && a.ArgumentsHash == b.ArgumentsHash
}

func validOutcome(status string) bool {
	return status == StatusSucceeded || status == StatusFailed || status == StatusUnknown
}
func terminal(status string) bool { return status == StatusSucceeded || status == StatusFailed }
