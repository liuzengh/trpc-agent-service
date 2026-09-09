// Package governance compiles tenant revision policies into tRPC-Agent-Go run
// options.
package governance

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	agentcore "trpc.group/trpc-go/trpc-agent-go/agent"
	agenttool "trpc.group/trpc-go/trpc-agent-go/tool"
)

type ToolPolicy struct {
	AllowedTools     []string            `json:"allowed_tools"`
	DangerousTools   []string            `json:"dangerous_tools"`
	DeniedUsers      []string            `json:"denied_users"`
	ToolAllowedUsers map[string][]string `json:"tool_allowed_users,omitempty"`
	DirectOnlyTools  []string            `json:"direct_only_tools,omitempty"`
	MaxToolCalls     int                 `json:"max_tool_calls"`
	MaxRunDuration   string              `json:"max_run_duration"`
}

// ToolDecision is the security-relevant result of one tool permission check.
// Arguments are intentionally excluded so secrets from tool payloads do not
// leak into the audit trail.
type ToolDecision struct {
	Code          string
	CallsUsed     int64
	CallLimit     int
	ToolName      string
	ToolCallID    string
	ArgumentsHash string
	Action        string
	Reason        string
}

type DecisionRecorder func(context.Context, ToolDecision) error

type ApprovedToolCall struct {
	ToolName      string `json:"tool_name"`
	ArgumentsHash string `json:"arguments_hash"`
}

func ParseToolPolicy(raw json.RawMessage) (ToolPolicy, error) {
	if len(raw) == 0 {
		raw = json.RawMessage(`{}`)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var policy ToolPolicy
	if err := decoder.Decode(&policy); err != nil {
		return ToolPolicy{}, fmt.Errorf("decode tool policy: %w", err)
	}
	if policy.MaxToolCalls < 0 {
		return ToolPolicy{}, fmt.Errorf("max_tool_calls must not be negative")
	}
	if err := validateToolAccess(policy); err != nil {
		return ToolPolicy{}, err
	}
	if policy.MaxRunDuration != "" {
		duration, err := time.ParseDuration(policy.MaxRunDuration)
		if err != nil || duration <= 0 {
			return ToolPolicy{}, fmt.Errorf("max_run_duration must be a positive duration")
		}
	}
	return policy, nil
}

func RunOptions(
	policy ToolPolicy,
	userID string,
	approvedTools []string,
	recorders ...DecisionRecorder,
) []agentcore.RunOption {
	return RunOptionsWithApprovals(policy, userID, approvedTools, nil, recorders...)
}

func RunOptionsWithApprovals(
	policy ToolPolicy,
	userID string,
	approvedTools []string,
	approvedCalls []ApprovedToolCall,
	recorders ...DecisionRecorder,
) []agentcore.RunOption {
	// Legacy callers have no verified audience. Audience-restricted tools
	// therefore fail closed unless RunOptionsForCaller is used explicitly.
	return RunOptionsForCaller(policy, Caller{UserID: userID}, approvedTools, approvedCalls, recorders...)
}

func RunOptionsForCaller(policy ToolPolicy, caller Caller, approvedTools []string, approvedCalls []ApprovedToolCall, recorders ...DecisionRecorder) []agentcore.RunOption {
	policy.AllowedTools = scopeToolAccess(policy, caller)
	userID := caller.UserID
	allowed := stringSet(policy.AllowedTools)
	dangerous := stringSet(policy.DangerousTools)
	deniedUsers := stringSet(policy.DeniedUsers)
	approved := stringSet(approvedTools)
	approvedHashes := make(map[string]struct{}, len(approvedCalls))
	for _, call := range approvedCalls {
		approvedHashes[call.ToolName+"\x00"+call.ArgumentsHash] = struct{}{}
	}
	var approvalMu sync.Mutex
	consumeApproval := func(request *agenttool.PermissionRequest) bool {
		approvalMu.Lock()
		defer approvalMu.Unlock()
		if contains(approved, request.ToolName) {
			delete(approved, request.ToolName)
			return true
		}
		if approvedToolCall(approvedHashes, request) {
			digest := sha256.Sum256(request.Arguments)
			delete(approvedHashes, request.ToolName+"\x00"+hex.EncodeToString(digest[:]))
			return true
		}
		return false
	}
	options := []agentcore.RunOption{
		agentcore.WithToolFilter(func(_ context.Context, item agenttool.Tool) bool {
			return item != nil && item.Declaration() != nil &&
				contains(allowed, item.Declaration().Name)
		}),
	}
	var calls atomic.Int64
	options = append(options, agentcore.WithToolPermissionPolicyFunc(
		func(ctx context.Context, request *agenttool.PermissionRequest) (agenttool.PermissionDecision, error) {
			var decision agenttool.PermissionDecision
			var used int64
			if request != nil && !contains(deniedUsers, userID) && contains(allowed, request.ToolName) {
				used = calls.Add(1)
			}
			if request == nil {
				decision = agenttool.DenyPermission("invalid tool permission request")
			} else {
				switch {
				case contains(deniedUsers, userID):
					decision = agenttool.DenyPermission("user is not allowed to call tools")
				case !contains(allowed, request.ToolName):
					decision = agenttool.DenyPermission("tool is not allowed by the Agent revision")
				case policy.MaxToolCalls > 0 && used > int64(policy.MaxToolCalls):
					decision = agenttool.DenyPermission("tool call budget exceeded")
				case contains(dangerous, request.ToolName) &&
					!consumeApproval(request):
					decision = agenttool.AskPermission("explicit user approval is required")
				default:
					decision = agenttool.AllowPermission()
				}
			}
			code := permissionCode(decision.Reason)
			if request != nil {
				recordFeedback(ctx, ToolDecision{Code: code, ToolName: request.ToolName, CallsUsed: used, CallLimit: policy.MaxToolCalls})
			}
			for _, recorder := range recorders {
				if recorder == nil {
					continue
				}
				toolDecision := ToolDecision{
					Code: code, CallsUsed: used, CallLimit: policy.MaxToolCalls,
					Action: string(decision.Action),
					Reason: decision.Reason,
				}
				if request != nil {
					toolDecision.ToolName = request.ToolName
					toolDecision.ToolCallID = request.ToolCallID
					digest := sha256.Sum256(request.Arguments)
					toolDecision.ArgumentsHash = hex.EncodeToString(digest[:])
				}
				if err := recorder(ctx, toolDecision); err != nil {
					return decision, fmt.Errorf("record tool permission decision: %w", err)
				}
			}
			return decision, nil
		},
	))
	if policy.MaxRunDuration != "" {
		duration, _ := time.ParseDuration(policy.MaxRunDuration)
		options = append(options, agentcore.WithMaxRunDuration(duration))
	}
	return options
}

func approvedToolCall(values map[string]struct{}, request *agenttool.PermissionRequest) bool {
	if request == nil {
		return false
	}
	digest := sha256.Sum256(request.Arguments)
	_, ok := values[request.ToolName+"\x00"+hex.EncodeToString(digest[:])]
	return ok
}

func stringSet(values []string) map[string]struct{} {
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			result[value] = struct{}{}
		}
	}
	return result
}

func contains(values map[string]struct{}, value string) bool {
	_, ok := values[value]
	return ok
}
