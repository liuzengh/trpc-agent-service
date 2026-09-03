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
	"sync/atomic"
	"time"

	agentcore "trpc.group/trpc-go/trpc-agent-go/agent"
	agenttool "trpc.group/trpc-go/trpc-agent-go/tool"
)

type ToolPolicy struct {
	AllowedTools   []string `json:"allowed_tools"`
	DangerousTools []string `json:"dangerous_tools"`
	DeniedUsers    []string `json:"denied_users"`
	MaxToolCalls   int      `json:"max_tool_calls"`
	MaxRunDuration string   `json:"max_run_duration"`
}

// ToolDecision is the security-relevant result of one tool permission check.
// Arguments are intentionally excluded so secrets from tool payloads do not
// leak into the audit trail.
type ToolDecision struct {
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
	allowed := stringSet(policy.AllowedTools)
	dangerous := stringSet(policy.DangerousTools)
	deniedUsers := stringSet(policy.DeniedUsers)
	approved := stringSet(approvedTools)
	approvedHashes := make(map[string]struct{}, len(approvedCalls))
	for _, call := range approvedCalls {
		approvedHashes[call.ToolName+"\x00"+call.ArgumentsHash] = struct{}{}
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
			if request == nil {
				decision = agenttool.DenyPermission("invalid tool permission request")
			} else {
				switch {
				case contains(deniedUsers, userID):
					decision = agenttool.DenyPermission("user is not allowed to call tools")
				case !contains(allowed, request.ToolName):
					decision = agenttool.DenyPermission("tool is not allowed by the Agent revision")
				case policy.MaxToolCalls > 0 && calls.Add(1) > int64(policy.MaxToolCalls):
					decision = agenttool.DenyPermission("tool call budget exceeded")
				case contains(dangerous, request.ToolName) &&
					!contains(approved, request.ToolName) &&
					!approvedToolCall(approvedHashes, request):
					decision = agenttool.AskPermission("explicit user approval is required")
				default:
					decision = agenttool.AllowPermission()
				}
			}
			for _, recorder := range recorders {
				if recorder == nil {
					continue
				}
				toolDecision := ToolDecision{
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
