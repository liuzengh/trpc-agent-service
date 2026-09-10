package governance

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	sessionstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/session"
)

type ConfirmationState string

const (
	ConfirmationPending  ConfirmationState = "pending"
	ConfirmationApproved ConfirmationState = "approved"
	ConfirmationDenied   ConfirmationState = "denied"
	ConfirmationExpired  ConfirmationState = "expired"
	ConfirmationConsumed ConfirmationState = "consumed"
)

type SuspensionRequest struct {
	ConfirmationID, TenantID, RequestID, AgentAppID, SessionID string
	InputSeq, Fence                                            uint64
	SubjectID, ChannelBindingID                                string
	Tool                                                       VersionedRef
	ToolCallID, ArgsDigest, CheckpointRef                      string
	PolicyVersion                                              int64
	Usage                                                      Usage
	ExpiresAt                                                  time.Time
}

type Confirmation struct {
	SuspensionRequest
	State      ConfirmationState
	DecisionAt time.Time
	Version    int64
}

type ConfirmationDecision struct {
	TenantID, ConfirmationID, SubjectID string
	Approve                             bool
	ExpectedVersion                     int64
	DecidedAt                           time.Time
}

type ConfirmationAction struct {
	TenantID, ConfirmationID, SubjectID, ChannelBindingID, SessionID string
	Approve                                                          bool
	ExpectedVersion                                                  int64
	DecidedAt                                                        time.Time
}

type ConfirmationActionDecider interface {
	DecideAction(context.Context, ConfirmationAction) (Confirmation, error)
}

type ConfirmationActionService struct{ Coordinator ConfirmationCoordinator }

func (s ConfirmationActionService) DecideAction(ctx context.Context, in ConfirmationAction) (Confirmation, error) {
	if s.Coordinator == nil || in.TenantID == "" || in.ConfirmationID == "" || in.SubjectID == "" ||
		in.ChannelBindingID == "" || in.SessionID == "" || in.ExpectedVersion < 1 || in.DecidedAt.IsZero() {
		return Confirmation{}, runtime.ErrInvariantViolation
	}
	confirmation, err := s.Coordinator.GetConfirmation(ctx, in.TenantID, in.ConfirmationID)
	if err != nil {
		return Confirmation{}, err
	}
	if confirmation.SubjectID != in.SubjectID || confirmation.ChannelBindingID != in.ChannelBindingID || confirmation.SessionID != in.SessionID {
		return Confirmation{}, runtime.ErrTenantScope
	}
	return s.Coordinator.Decide(ctx, ConfirmationDecision{TenantID: in.TenantID, ConfirmationID: in.ConfirmationID,
		SubjectID: in.SubjectID, Approve: in.Approve, ExpectedVersion: in.ExpectedVersion, DecidedAt: in.DecidedAt})
}

var _ ConfirmationActionDecider = ConfirmationActionService{}

type Grant struct {
	GrantID, TenantID, ConfirmationID, RequestID, SubjectID string
	Tool                                                    VersionedRef
	ToolCallID, ArgsDigest                                  string
	PolicyVersion, Version                                  int64
	ConsumedAt                                              time.Time
}

type GrantClaim struct {
	TenantID, GrantID, RequestID, SubjectID string
	Tool                                    VersionedRef
	ToolCallID, ArgsDigest                  string
	PolicyVersion, ExpectedVersion          int64
}

type ToolAttemptState string

const (
	ToolAttemptEffectUnknown ToolAttemptState = "effect_unknown"
	ToolAttemptSucceeded     ToolAttemptState = "succeeded"
	ToolAttemptFailed        ToolAttemptState = "failed"
)

type ToolAttempt struct {
	TenantID, GrantID, RequestID, ToolCallID string
	State                                    ToolAttemptState
	ResultRef                                string
}

type FinishToolAttemptRequest struct {
	TenantID, GrantID, ResultRef string
	State                        ToolAttemptState
}

// ConfirmationCoordinator owns operations whose correctness crosses the
// session and governance tables. Implementations must make each method one
// authority transaction.
type ConfirmationCoordinator interface {
	Suspend(context.Context, sessionstore.CommitTurnRequest, SuspensionRequest) (Confirmation, error)
	Decide(context.Context, ConfirmationDecision) (Confirmation, error)
	ExpireDue(context.Context, time.Time, int) ([]Confirmation, error)
	ConsumeGrant(context.Context, GrantClaim) (Grant, error)
	GetConfirmation(context.Context, string, string) (Confirmation, error)
	GetConfirmationByRequest(context.Context, string, string) (Confirmation, error)
	GetGrantByConfirmation(context.Context, string, string) (Grant, error)
	GetToolAttempt(context.Context, string, string) (ToolAttempt, error)
	FinishToolAttempt(context.Context, FinishToolAttemptRequest) (ToolAttempt, error)
}

type ConfirmationExpiryReconciler struct {
	Coordinator ConfirmationCoordinator
	Now         func() time.Time
	BatchSize   int
}

func (r ConfirmationExpiryReconciler) RunOnce(ctx context.Context) (int, error) {
	if r.Coordinator == nil {
		return 0, runtime.ErrCapabilityUnsupported
	}
	now := time.Now().UTC()
	if r.Now != nil {
		now = r.Now().UTC()
	}
	limit := r.BatchSize
	if limit <= 0 {
		limit = 100
	}
	values, err := r.Coordinator.ExpireDue(ctx, now, limit)
	return len(values), err
}

func StableConfirmationID(tenantID, requestID, toolCallID string) (string, error) {
	if tenantID == "" || requestID == "" || toolCallID == "" {
		return "", runtime.ErrInvariantViolation
	}
	return "conf_" + contentDigest([]byte(tenantID + "\x00" + requestID + "\x00" + toolCallID))[:32], nil
}

func StableGrantID(confirmationID string) (string, error) {
	if len(confirmationID) != 37 || confirmationID[:5] != "conf_" {
		return "", runtime.ErrInvariantViolation
	}
	return "grant_" + confirmationID[5:], nil
}

// CanonicalArguments rejects duplicate object keys and trailing input before
// returning stable JSON bytes and their digest.
func CanonicalArguments(data []byte) ([]byte, string, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	value, err := decodeUniqueJSON(decoder)
	if err != nil {
		return nil, "", runtime.ErrInvalidEnvelope
	}
	if token, err := decoder.Token(); err != io.EOF || token != nil {
		return nil, "", runtime.ErrInvalidEnvelope
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return nil, "", runtime.ErrInvalidEnvelope
	}
	return canonical, contentDigest(canonical), nil
}

func decodeUniqueJSON(decoder *json.Decoder) (any, error) {
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return token, nil
	}
	switch delim {
	case '{':
		result := map[string]any{}
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return nil, err
			}
			key, ok := keyToken.(string)
			if !ok {
				return nil, fmt.Errorf("object key is not a string")
			}
			if _, exists := result[key]; exists {
				return nil, fmt.Errorf("duplicate object key")
			}
			value, err := decodeUniqueJSON(decoder)
			if err != nil {
				return nil, err
			}
			result[key] = value
		}
		if end, err := decoder.Token(); err != nil || end != json.Delim('}') {
			return nil, fmt.Errorf("unterminated object")
		}
		return result, nil
	case '[':
		var result []any
		for decoder.More() {
			value, err := decodeUniqueJSON(decoder)
			if err != nil {
				return nil, err
			}
			result = append(result, value)
		}
		if end, err := decoder.Token(); err != nil || end != json.Delim(']') {
			return nil, fmt.Errorf("unterminated array")
		}
		return result, nil
	default:
		return nil, fmt.Errorf("unexpected delimiter")
	}
}
