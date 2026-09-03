// Package governance compiles tenant revision policies into tRPC-Agent-Go run
// options.
package governance

import (
	"bytes"
	"context"
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
) []agentcore.RunOption {
	allowed := stringSet(policy.AllowedTools)
	dangerous := stringSet(policy.DangerousTools)
	deniedUsers := stringSet(policy.DeniedUsers)
	approved := stringSet(approvedTools)
	options := []agentcore.RunOption{
		agentcore.WithToolFilter(func(_ context.Context, item agenttool.Tool) bool {
			return item != nil && item.Declaration() != nil &&
				contains(allowed, item.Declaration().Name)
		}),
	}
	var calls atomic.Int64
	options = append(options, agentcore.WithToolPermissionPolicyFunc(
		func(_ context.Context, request *agenttool.PermissionRequest) (agenttool.PermissionDecision, error) {
			if request == nil {
				return agenttool.DenyPermission("invalid tool permission request"), nil
			}
			if contains(deniedUsers, userID) {
				return agenttool.DenyPermission("user is not allowed to call tools"), nil
			}
			if !contains(allowed, request.ToolName) {
				return agenttool.DenyPermission("tool is not allowed by the Agent revision"), nil
			}
			if policy.MaxToolCalls > 0 && calls.Add(1) > int64(policy.MaxToolCalls) {
				return agenttool.DenyPermission("tool call budget exceeded"), nil
			}
			if contains(dangerous, request.ToolName) && !contains(approved, request.ToolName) {
				return agenttool.AskPermission("explicit user approval is required"), nil
			}
			return agenttool.AllowPermission(), nil
		},
	))
	if policy.MaxRunDuration != "" {
		duration, _ := time.ParseDuration(policy.MaxRunDuration)
		options = append(options, agentcore.WithMaxRunDuration(duration))
	}
	return options
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
