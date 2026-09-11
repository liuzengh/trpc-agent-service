package governance

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"
)

// MemoryPolicy keeps automatic extraction and audience policy explicit.
// DirectOnly uses tool-based access: preload and background extraction lack
// the trusted per-request chat audience needed to enforce that boundary.
type MemoryPolicy struct {
	AutoExtract bool `json:"auto_extract"`
	EveryTurns  int  `json:"every_turns"`
	DirectOnly  bool `json:"direct_only"`
}

func ParseMemoryPolicy(raw json.RawMessage) (MemoryPolicy, error) {
	if len(raw) == 0 {
		raw = json.RawMessage(`{}`)
	}
	var policy MemoryPolicy
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&policy); err != nil {
		return policy, errors.New("invalid memory policy")
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return policy, errors.New("invalid trailing memory policy")
	}
	if policy.EveryTurns < 0 || policy.AutoExtract && policy.EveryTurns <= 0 {
		return policy, errors.New("auto memory extraction requires a positive every_turns")
	}
	if policy.DirectOnly && policy.AutoExtract {
		return policy, errors.New("direct_only memory requires explicit tools, not automatic extraction")
	}
	return policy, nil
}

func ValidateMemoryPolicy(agentConfig, memoryConfig json.RawMessage) error {
	policy, err := ParseMemoryPolicy(memoryConfig)
	if err != nil {
		return err
	}
	var agent struct {
		Preload int `json:"preload_memory"`
	}
	if json.Unmarshal(agentConfig, &agent) != nil {
		return errors.New("invalid agent memory configuration")
	}
	if policy.DirectOnly && agent.Preload > 0 {
		return errors.New("direct_only memory cannot preload into an audience-independent compiled agent")
	}
	return nil
}

// ScopeMemoryTools returns a fresh allowlist. Unknown/legacy queue audiences
// fail closed when direct_only is enabled; approvals cannot override this.
func ScopeMemoryTools(allowed []string, policy MemoryPolicy, chatType string) []string {
	result := make([]string, 0, len(allowed))
	for _, name := range allowed {
		if policy.DirectOnly && chatType != "direct" && strings.HasPrefix(name, "memory_") {
			continue
		}
		result = append(result, name)
	}
	return result
}
