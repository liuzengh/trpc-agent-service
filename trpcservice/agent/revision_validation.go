package agent

import (
	"encoding/json"
	"errors"
	"net/url"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
)

// ValidateRevisionAgentConfig performs only local parsing, never compilation,
// credential resolution, model calls or MCP discovery.
func ValidateRevisionAgentConfig(revision controlplane.AgentRevision) error {
	_, err := parseRevisionAgentConfig(revision)
	return err
}

func parseRevisionAgentConfig(revision controlplane.AgentRevision) (revisionAgentConfig, error) {
	var cfg revisionAgentConfig
	if revision.AgentType != "llm" {
		return cfg, errors.New("unsupported Agent type")
	}
	if decodeStrictJSON(revision.AgentConfig, &cfg) != nil {
		return cfg, errors.New("invalid Agent configuration: unknown field or invalid JSON")
	}
	cfg.Name = strings.TrimSpace(cfg.Name)
	cfg.Description = strings.TrimSpace(cfg.Description)
	cfg.Instruction = strings.TrimSpace(cfg.Instruction)
	if cfg.Name == "" || cfg.Instruction == "" || cfg.PreloadMemory < 0 || cfg.SummaryEveryTurns < 0 {
		return cfg, errors.New("agent requires name, instruction and nonnegative memory/summary limits")
	}
	return cfg, nil
}

// ValidateRevisionModelConfig validates the exact runtime configuration shape.
// Availability and secret values can only be checked by an execution node.
func ValidateRevisionModelConfig(raw json.RawMessage) error {
	cfg, err := parseRevisionModelConfig(raw)
	if err != nil {
		return errors.New("invalid model configuration")
	}
	source := strings.ToLower(strings.TrimSpace(cfg.Source))
	if source == "" || source == "startup_env" {
		return nil
	}
	if source != "revision" {
		return errors.New("unsupported model source")
	}
	provider := strings.ToLower(strings.TrimSpace(cfg.Provider))
	if provider != "mock" && provider != "openai" {
		return errors.New("unsupported model provider")
	}
	if provider == "openai" && strings.TrimSpace(cfg.Name) == "" {
		return errors.New("model name is required")
	}
	if (cfg.APIKeyRef == "" && cfg.APIKeyEnv == "") || (cfg.APIKeyRef != "" && cfg.APIKeyEnv != "") || (cfg.APIKeyEnv != "" && !environmentNamePattern.MatchString(cfg.APIKeyEnv)) {
		return errors.New("one valid model credential reference is required")
	}
	if cfg.BaseURL != "" {
		u, err := url.Parse(cfg.BaseURL)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return errors.New("model URL must be HTTP(S) without embedded credentials, query or fragment")
		}
	}
	return nil
}
