package toolexec

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
)

var (
	ErrOperationUnknown  = errors.New("business operation outcome is unconfirmed; query or reconcile using operation_id")
	ErrOperationRejected = errors.New("business operation was definitively rejected")
	ErrOperationConflict = errors.New("business idempotency key was reused with different input")
)

// Operation identity survives model-issued call IDs, Runner retries and new
// sessions. No raw business key, tool arguments or credentials are journaled.
type Operation struct {
	ID              string          `json:"operation_id"`
	TenantID        string          `json:"tenant_id"`
	AppID           string          `json:"app_id"`
	UserID          string          `json:"user_id"`
	ToolName        string          `json:"tool_name"`
	BusinessKeyHash string          `json:"business_key_hash"`
	InputHash       string          `json:"input_hash"`
	Status          string          `json:"status"`
	Result          json.RawMessage `json:"result,omitempty"`
	ErrorType       string          `json:"error_type,omitempty"`
	CreatedAt       time.Time       `json:"created_at"`
	UpdatedAt       time.Time       `json:"updated_at"`
}

type OperationStore interface {
	Reserve(context.Context, Operation) (Operation, bool, error)
	Get(context.Context, string, string) (Operation, error)
	List(ctx context.Context, tenantID, status, afterID string, limit int) ([]Operation, error)
	Finish(ctx context.Context, tenantID, id, status string, result json.RawMessage, errorType string) (Operation, error)
	Ready(context.Context) error
}

// Provider is code-registered, never tenant-supplied executable code. Lookup
// must report authoritative committed facts. Not-found NEVER proves rollback.
// Idempotent means the backend atomically enforces operation.ID for its whole
// retention period, including simultaneous calls and completion-before-timeout.
type OperationProvider interface {
	Name() string
	Idempotent() bool
	Execute(context.Context, Operation, json.RawMessage) (json.RawMessage, error)
	Lookup(context.Context, Operation) (result json.RawMessage, found bool, err error)
}

type OperationInput struct {
	TenantID, AppID, UserID, ToolName, BusinessKey string
	ExecutionID, RequestID                         string
	Payload                                        json.RawMessage
}

func OperationID(tenantID, appID, userID, toolName, businessKey string) string {
	data, _ := json.Marshal([]string{tenantID, appID, userID, toolName, businessKey})
	return "op_" + Hash(data)[:48]
}

func sameOperation(a, b Operation) bool {
	return a.ID == b.ID && a.TenantID == b.TenantID && a.AppID == b.AppID && a.UserID == b.UserID &&
		a.ToolName == b.ToolName && a.BusinessKeyHash == b.BusinessKeyHash && a.InputHash == b.InputHash
}

func validOperation(o Operation) bool {
	return o.ID != "" && o.TenantID != "" && o.AppID != "" && o.UserID != "" && o.ToolName != "" && o.InputHash != "" && o.BusinessKeyHash != ""
}

func cloneOperation(o Operation) Operation {
	o.Result = append(json.RawMessage(nil), o.Result...)
	return o
}

type Operations struct {
	store     OperationStore
	journal   Journal
	audit     audit.Writer
	providers map[string]OperationProvider
}

func NewOperations(store OperationStore, journal Journal, writer audit.Writer, providers ...OperationProvider) (*Operations, error) {
	if store == nil || journal == nil {
		return nil, errors.New("operation store and tool journal are required")
	}
	s := &Operations{store: store, journal: journal, audit: writer, providers: map[string]OperationProvider{}}
	for _, p := range providers {
		if p == nil || p.Name() == "" || s.providers[p.Name()] != nil {
			return nil, errors.New("invalid or duplicate operation provider")
		}
		s.providers[p.Name()] = p
	}
	return s, nil
}

func (s *Operations) Execute(ctx context.Context, input OperationInput) (Operation, error) {
	if strings.TrimSpace(input.BusinessKey) != input.BusinessKey || input.BusinessKey == "" || len(input.BusinessKey) > 128 ||
		strings.ContainsRune(input.BusinessKey, '\x00') || !json.Valid(input.Payload) {
		return Operation{}, ErrOperationRejected
	}
	canonical, err := canonicalJSON(input.Payload)
	if err != nil {
		return Operation{}, ErrOperationRejected
	}
	input.Payload = canonical
	provider := s.providers[input.ToolName]
	if provider == nil {
		return Operation{}, ErrOperationRejected
	}
	// A durable, permission-approved ToolCall must already exist.
	execution, err := s.journal.Get(ctx, input.TenantID, input.ExecutionID)
	if err != nil {
		return Operation{}, err
	}
	if execution.RequestID != input.RequestID || execution.ToolName != input.ToolName {
		return Operation{}, ErrConflict
	}
	op := Operation{
		ID:       OperationID(input.TenantID, input.AppID, input.UserID, input.ToolName, input.BusinessKey),
		TenantID: input.TenantID, AppID: input.AppID, UserID: input.UserID, ToolName: input.ToolName,
		BusinessKeyHash: Hash([]byte(input.BusinessKey)), InputHash: Hash(input.Payload),
	}
	op, fresh, err := s.store.Reserve(ctx, op)
	if err != nil {
		return Operation{}, err
	}
	if err := s.journal.LinkOperation(ctx, input.TenantID, input.ExecutionID, op.ID); err != nil {
		return op, err
	}
	if terminal(op.Status) {
		return s.finish(ctx, op, "tool_operation_replayed")
	}
	if !fresh {
		result, found, lookupErr := provider.Lookup(ctx, op)
		if lookupErr == nil && found {
			return s.recordOutcome(ctx, op, result, nil, "tool_operation_recovered")
		}
		if lookupErr != nil || !provider.Idempotent() {
			return s.recordOutcome(ctx, op, nil, ErrOperationUnknown, "tool_operation_unconfirmed")
		}
		// Only a freshly authorized invocation can repeat a backend call, and
		// only a code-registered idempotent provider with the SAME operation ID.
	}
	result, runErr := provider.Execute(ctx, op, input.Payload)
	return s.recordOutcome(ctx, op, result, runErr, "tool_operation_executed")
}

// Reconcile is read-only with respect to the business system: no retry, reset,
// fabricated status, or model call. The operator cannot provide the outcome.
func (s *Operations) Reconcile(ctx context.Context, tenantID, id string) (Operation, error) {
	op, err := s.store.Get(ctx, tenantID, id)
	if err != nil {
		return Operation{}, err
	}
	if terminal(op.Status) {
		return s.finish(ctx, op, "tool_operation_reconciled")
	}
	provider := s.providers[op.ToolName]
	if provider == nil {
		return op, ErrOperationUnknown
	}
	result, found, lookupErr := provider.Lookup(ctx, op)
	if lookupErr != nil || !found {
		return s.recordOutcome(ctx, op, nil, ErrOperationUnknown, "tool_operation_reconcile_unknown")
	}
	return s.recordOutcome(ctx, op, result, nil, "tool_operation_reconciled")
}

func (s *Operations) recordOutcome(ctx context.Context, op Operation, result json.RawMessage, runErr error, decision string) (Operation, error) {
	status, errorType := StatusSucceeded, ""
	if runErr != nil {
		status, errorType, result = StatusUnknown, "provider_unconfirmed", nil
		if errors.Is(runErr, ErrOperationRejected) {
			status, errorType = StatusFailed, "provider_rejected"
		}
	} else if !json.Valid(result) || len(result) > 32<<10 {
		status, errorType, result = StatusUnknown, "invalid_provider_receipt", nil
	} else {
		result, _ = canonicalJSON(result)
	}
	// The model request may have timed out after the backend committed. Record
	// that uncertainty/result with a bounded cleanup context, never a goroutine.
	finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer cancel()
	stored, err := s.store.Finish(finishCtx, op.TenantID, op.ID, status, result, errorType)
	if err != nil {
		return op, fmt.Errorf("%w: persistence unavailable", ErrOperationUnknown)
	}
	return s.finish(finishCtx, stored, decision)
}

func (s *Operations) finish(ctx context.Context, op Operation, decision string) (Operation, error) {
	resultHash := ""
	if len(op.Result) > 0 {
		resultHash = Hash(op.Result)
	}
	if err := s.journal.ResolveOperation(ctx, op.TenantID, op.ID, op.Status, resultHash, op.ErrorType); err != nil {
		return op, fmt.Errorf("%w: tool journal unavailable", ErrOperationUnknown)
	}
	if s.audit != nil {
		if err := s.audit.Record(ctx, audit.Event{TenantID: op.TenantID, UserID: op.UserID, ToolName: op.ToolName,
			Decision: decision, ErrorType: op.ErrorType, TraceID: audit.TraceID(ctx),
			Details: map[string]any{"operation_id": op.ID, "status": op.Status, "input_hash": op.InputHash, "result_hash": resultHash}}); err != nil {
			return op, errors.New("operation audit unavailable")
		}
	}
	switch op.Status {
	case StatusSucceeded:
		return op, nil
	case StatusFailed:
		return op, ErrOperationRejected
	default:
		return op, ErrOperationUnknown
	}
}

func (s *Operations) Get(ctx context.Context, tenantID, id string) (Operation, error) {
	return s.store.Get(ctx, tenantID, id)
}
func (s *Operations) List(ctx context.Context, tenantID, status, afterID string, limit int) ([]Operation, error) {
	if limit <= 0 || limit > 100 || (status != "" && status != StatusRunning && !validOutcome(status)) {
		return nil, ErrConflict
	}
	return s.store.List(ctx, tenantID, status, afterID, limit)
}
func (s *Operations) Ready(ctx context.Context) error { return s.store.Ready(ctx) }

func canonicalJSON(raw []byte) (json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, ErrOperationRejected
	}
	result, err := json.Marshal(value)
	return result, err
}
