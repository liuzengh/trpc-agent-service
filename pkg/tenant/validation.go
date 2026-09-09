package tenant

import (
	"encoding/base64"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/modelcatalog"
)

var backendProfileIDPattern = regexp.MustCompile(`^[a-z][a-z0-9._-]{0,63}$`)

var logicalNamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

const (
	MaxTenantIDBytes        = 64
	MaxTenantNameBytes      = 255
	MaxAgentAppNameBytes    = 128
	maxModelNameBytes       = 128
	maxToolNameBytes        = 128
	maxSystemPromptBytes    = 64 << 10
	maxMetadataEntries      = 64
	maxMetadataKeyBytes     = 128
	maxMetadataValueBytes   = 4 << 10
	maxAgentsPerTenant      = 64
	maxModelsPerTenant      = 64
	maxChannelsPerTenant    = 64
	maxToolsPerAgent        = 64
	maxRuntimeNodes         = 32
	maxRuntimeEdges         = 256
	maxToolPolicyEntries    = 256
	maxChannelConfigEntries = 32
	// Governance rules are tenant-controlled input. Keep both the number of
	// rules and their compiled pattern material bounded so a valid-looking
	// configuration cannot amplify CPU and memory on every Worker request.
	maxMaskingRules           = 64
	maxContentFilters         = 64
	maxContentFilterPatterns  = 64
	maxGovernancePatternBytes = 4 << 10
	maxGovernanceReplaceBytes = 4 << 10
	maxGovernanceNameBytes    = 128
	maxGovernancePatternTotal = 256 << 10
)

const maxRedisLuaExactInteger = int64(1<<53 - 1)

var (
	telegramBotTokenPattern      = regexp.MustCompile(`^[1-9][0-9]{5,19}:[A-Za-z0-9_-]{30,128}$`)
	telegramWebhookSecretPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{32,256}$`)
	weWorkCallbackTokenPattern   = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)
	weWorkCorpIDPattern          = regexp.MustCompile(`^ww[0-9a-f]{16}$`)
	// WeCom application secrets are operator-issued and their length has varied
	// across app generations; validate the safe character set and a bounded
	// range instead of confusing them with the fixed 43-character AES key.
	weWorkCorpSecretPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{16,128}$`)
)

// ValidateTenantID applies the narrowest persisted tenant identifier contract.
func ValidateTenantID(value string) error {
	return validateLogicalName("tenant ID", value, MaxTenantIDBytes)
}

// ValidateAgentAppName applies the shared Agent/config/runtime key contract.
func ValidateAgentAppName(value string) error {
	return validateLogicalName("agent app name", value, MaxAgentAppNameBytes)
}

// SupportedAgentType validates the closed set of runtime names understood by
// the enterprise control plane. Runtime availability is checked separately by
// worker.RuntimeAgentRegistry at process composition time.
func SupportedAgentType(value string) bool {
	switch value {
	case "", AgentTypeLLM, AgentTypeChain, AgentTypeGraph, AgentTypeParallel, AgentTypeCycle:
		return true
	default:
		return false
	}
}

// ValidateAgentRuntime validates the immutable topology and its aggregate
// provider-call budget. Composite nodes deliberately share the version-bound
// model; their tools must be a subset of the parent AgentVersion capability.
func ValidateAgentRuntime(agent AgentConfig) error {
	runtimeType := agent.Type
	if runtimeType == "" {
		runtimeType = AgentTypeLLM
	}
	if runtimeType == AgentTypeLLM {
		if agent.Runtime != nil {
			return fmt.Errorf("agent %q llm runtime must not define a composite topology", agent.Name)
		}
		return nil
	}
	if agent.Runtime == nil {
		return fmt.Errorf("agent %q runtime %q requires a topology", agent.Name, runtimeType)
	}
	runtime := agent.Runtime
	if len(runtime.Nodes) == 0 || len(runtime.Nodes) > maxRuntimeNodes {
		return fmt.Errorf("agent %q runtime must define between 1 and %d nodes", agent.Name, maxRuntimeNodes)
	}

	parentTools := make(map[string]struct{}, len(agent.Tools))
	for _, name := range agent.Tools {
		parentTools[name] = struct{}{}
	}
	nodes := make(map[string]struct{}, len(runtime.Nodes))
	totalCalls := 0
	for _, node := range runtime.Nodes {
		if err := validateLogicalName("runtime node name", node.Name, MaxAgentAppNameBytes); err != nil {
			return fmt.Errorf("agent %q: %w", agent.Name, err)
		}
		if _, duplicate := nodes[node.Name]; duplicate {
			return fmt.Errorf("agent %q runtime node %q appears more than once", agent.Name, node.Name)
		}
		nodes[node.Name] = struct{}{}
		if node.MaxLLMCalls < 0 || node.MaxLLMCalls > MaxConfiguredLLMCalls {
			return fmt.Errorf("agent %q runtime node %q has an invalid max LLM call limit", agent.Name, node.Name)
		}
		if err := validatePrompt(node.SystemPrompt); err != nil {
			return fmt.Errorf("agent %q runtime node %q: %w", agent.Name, node.Name, err)
		}
		if len(node.Tools) > maxToolsPerAgent {
			return fmt.Errorf("agent %q runtime node %q exceeds %d tools", agent.Name, node.Name, maxToolsPerAgent)
		}
		seenTools := make(map[string]struct{}, len(node.Tools))
		for _, toolName := range node.Tools {
			if err := validateLogicalName("runtime node tool name", toolName, maxToolNameBytes); err != nil {
				return fmt.Errorf("agent %q runtime node %q: %w", agent.Name, node.Name, err)
			}
			if _, duplicate := seenTools[toolName]; duplicate {
				return fmt.Errorf("agent %q runtime node %q tool %q appears more than once", agent.Name, node.Name, toolName)
			}
			seenTools[toolName] = struct{}{}
			if _, allowed := parentTools[toolName]; !allowed {
				return fmt.Errorf("agent %q runtime node %q tool %q is outside the AgentVersion tool set", agent.Name, node.Name, toolName)
			}
		}
		if len(node.Tools) > 0 && node.EffectiveMaxLLMCalls() < 2 {
			return fmt.Errorf("agent %q runtime node %q needs at least two LLM calls when tools are enabled", agent.Name, node.Name)
		}
		totalCalls += node.EffectiveMaxLLMCalls()
	}

	switch runtimeType {
	case AgentTypeChain, AgentTypeParallel:
		if runtime.Entry != "" || len(runtime.Edges) != 0 || runtime.MaxIterations != 0 {
			return fmt.Errorf("agent %q runtime %q accepts ordered nodes only", agent.Name, runtimeType)
		}
	case AgentTypeCycle:
		if runtime.Entry != "" || len(runtime.Edges) != 0 {
			return fmt.Errorf("agent %q cycle runtime accepts ordered nodes and maxIterations only", agent.Name)
		}
		if runtime.MaxIterations <= 0 || runtime.MaxIterations > MaxConfiguredLLMCalls {
			return fmt.Errorf("agent %q cycle runtime maxIterations must be between 1 and %d", agent.Name, MaxConfiguredLLMCalls)
		}
		totalCalls *= runtime.MaxIterations
	case AgentTypeGraph:
		if runtime.MaxIterations != 0 {
			return fmt.Errorf("agent %q graph runtime must not define maxIterations", agent.Name)
		}
		if err := validateRuntimeGraph(agent.Name, runtime, nodes); err != nil {
			return err
		}
	default:
		return fmt.Errorf("agent %q type %q is unsupported", agent.Name, runtimeType)
	}
	if totalCalls > agent.EffectiveMaxLLMCalls() {
		return fmt.Errorf("agent %q runtime can make %d LLM calls, exceeding the AgentVersion limit %d", agent.Name, totalCalls, agent.EffectiveMaxLLMCalls())
	}
	return nil
}

func validateRuntimeGraph(agentName string, runtime *AgentRuntimeConfig, nodes map[string]struct{}) error {
	if runtime.Entry == "" {
		return fmt.Errorf("agent %q graph runtime requires an entry node", agentName)
	}
	if _, ok := nodes[runtime.Entry]; !ok {
		return fmt.Errorf("agent %q graph entry %q is not a configured node", agentName, runtime.Entry)
	}
	if len(runtime.Edges) > maxRuntimeEdges {
		return fmt.Errorf("agent %q graph runtime exceeds %d edges", agentName, maxRuntimeEdges)
	}
	adjacency := make(map[string][]string, len(nodes))
	indegree := make(map[string]int, len(nodes))
	for name := range nodes {
		indegree[name] = 0
	}
	seenEdges := make(map[string]struct{}, len(runtime.Edges))
	for _, edge := range runtime.Edges {
		if _, ok := nodes[edge.From]; !ok {
			return fmt.Errorf("agent %q graph edge references unknown source %q", agentName, edge.From)
		}
		if _, ok := nodes[edge.To]; !ok {
			return fmt.Errorf("agent %q graph edge references unknown target %q", agentName, edge.To)
		}
		if edge.From == edge.To {
			return fmt.Errorf("agent %q graph edge %q is a self-cycle", agentName, edge.From)
		}
		key := edge.From + "\x00" + edge.To
		if _, duplicate := seenEdges[key]; duplicate {
			return fmt.Errorf("agent %q graph edge %q -> %q appears more than once", agentName, edge.From, edge.To)
		}
		seenEdges[key] = struct{}{}
		adjacency[edge.From] = append(adjacency[edge.From], edge.To)
		indegree[edge.To]++
	}

	reachable := map[string]struct{}{runtime.Entry: {}}
	queue := []string{runtime.Entry}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		for _, next := range adjacency[current] {
			if _, seen := reachable[next]; seen {
				continue
			}
			reachable[next] = struct{}{}
			queue = append(queue, next)
		}
	}
	if len(reachable) != len(nodes) {
		return fmt.Errorf("agent %q graph runtime contains nodes unreachable from entry %q", agentName, runtime.Entry)
	}

	topologyQueue := make([]string, 0, len(nodes))
	for name, degree := range indegree {
		if degree == 0 {
			topologyQueue = append(topologyQueue, name)
		}
	}
	visited := 0
	for len(topologyQueue) > 0 {
		current := topologyQueue[0]
		topologyQueue = topologyQueue[1:]
		visited++
		for _, next := range adjacency[current] {
			indegree[next]--
			if indegree[next] == 0 {
				topologyQueue = append(topologyQueue, next)
			}
		}
	}
	if visited != len(nodes) {
		return fmt.Errorf("agent %q graph runtime must be acyclic; use cycle runtime for bounded loops", agentName)
	}
	return nil
}

// ValidateTenantName accepts human-readable names while rejecting database
// truncation, ambiguous surrounding whitespace and control characters.
func ValidateTenantName(value string) error {
	return validateDisplayText("tenant name", value, MaxTenantNameBytes)
}

func validateLogicalName(kind, value string, maxBytes int) error {
	if value == "" || len(value) > maxBytes || !utf8.ValidString(value) ||
		!logicalNamePattern.MatchString(value) {
		return fmt.Errorf("%s must be 1..%d bytes and use letters, digits, dot, underscore or hyphen", kind, maxBytes)
	}
	return nil
}

func validateDisplayText(kind, value string, maxBytes int) error {
	if value == "" || len(value) > maxBytes || !utf8.ValidString(value) ||
		strings.TrimSpace(value) != value {
		return fmt.Errorf("%s must be 1..%d bytes of trimmed UTF-8 text", kind, maxBytes)
	}
	for _, character := range value {
		if unicode.IsControl(character) || unicode.In(character, unicode.Cf) {
			return fmt.Errorf("%s cannot contain control characters", kind)
		}
	}
	return nil
}

func validatePrompt(value string) error {
	if len(value) > maxSystemPromptBytes || !utf8.ValidString(value) || strings.ContainsRune(value, 0) {
		return fmt.Errorf("system prompt must be valid UTF-8 without NUL and at most %d bytes", maxSystemPromptBytes)
	}
	return nil
}

func validateMetadata(kind string, values map[string]string) error {
	if len(values) > maxMetadataEntries {
		return fmt.Errorf("%s metadata exceeds %d entries", kind, maxMetadataEntries)
	}
	for key, value := range values {
		if err := validateLogicalName(kind+" metadata key", key, maxMetadataKeyBytes); err != nil {
			return err
		}
		if len(value) > maxMetadataValueBytes || !utf8.ValidString(value) || strings.ContainsRune(value, 0) {
			return fmt.Errorf("%s metadata value must be valid UTF-8 without NUL and at most %d bytes", kind, maxMetadataValueBytes)
		}
	}
	return nil
}

// ValidateAgentModel enforces execution invariants shared by tenant config,
// immutable version snapshots and Worker startup.
func ValidateAgentModel(agent AgentConfig, modelConfig ModelConfig) error {
	if err := ValidateAgentAppName(agent.Name); err != nil {
		return err
	}
	if err := validateLogicalName("default model", agent.DefaultModel, maxModelNameBytes); err != nil {
		return err
	}
	if !SupportedAgentType(agent.Type) {
		return fmt.Errorf("agent %q type %q is unsupported", agent.Name, agent.Type)
	}
	if agent.MaxLLMCalls < 0 || agent.MaxLLMCalls > MaxConfiguredLLMCalls {
		return fmt.Errorf("agent %q has an invalid max LLM call limit", agent.Name)
	}
	if err := ValidateAgentRuntime(agent); err != nil {
		return err
	}
	if len(agent.Tools) > 0 && agent.EffectiveMaxLLMCalls() < 2 {
		return fmt.Errorf("agent %q needs at least two LLM calls when tools are enabled", agent.Name)
	}
	if len(agent.Tools) > maxToolsPerAgent {
		return fmt.Errorf("agent %q exceeds %d tools", agent.Name, maxToolsPerAgent)
	}
	if err := validatePrompt(agent.SystemPrompt); err != nil {
		return fmt.Errorf("agent %q: %w", agent.Name, err)
	}
	if err := validateMetadata("agent", agent.Metadata); err != nil {
		return fmt.Errorf("agent %q: %w", agent.Name, err)
	}
	seenTools := make(map[string]struct{}, len(agent.Tools))
	for _, toolName := range agent.Tools {
		if err := validateLogicalName("tool name", toolName, maxToolNameBytes); err != nil {
			return fmt.Errorf("agent %q: %w", agent.Name, err)
		}
		if _, duplicate := seenTools[toolName]; duplicate {
			return fmt.Errorf("agent %q tool %q appears more than once", agent.Name, toolName)
		}
		seenTools[toolName] = struct{}{}
	}
	if modelConfig.Provider != "openai" {
		return fmt.Errorf("model provider and model name must identify an installed OpenAI model")
	}
	if modelConfig.APIKey != "" && modelConfig.APIKeyRef != "" {
		return fmt.Errorf("model %q cannot configure both api key and api key reference", modelConfig.ModelName)
	}
	if modelConfig.APIKeyRef != "" {
		if err := SecretRef(modelConfig.APIKeyRef).Validate(); err != nil {
			return fmt.Errorf("model API key reference is invalid")
		}
	}
	if err := validateLogicalName("model name", modelConfig.ModelName, maxModelNameBytes); err != nil {
		return err
	}
	if err := validateMetadata("model", modelConfig.Metadata); err != nil {
		return err
	}
	if modelConfig.Endpoint != "" {
		return fmt.Errorf("custom model endpoints are not installed with an SSRF-safe transport")
	}
	if modelConfig.MaxTokens < 0 || modelConfig.Temperature < 0 || modelConfig.Temperature > 2 {
		return fmt.Errorf("model %q has invalid generation limits", modelConfig.ModelName)
	}
	if profile, known := modelcatalog.Resolve(modelConfig.Provider, modelConfig.ModelName); known && modelConfig.MaxTokens > profile.MaxOutputTokens {
		return fmt.Errorf("model %q completion limit exceeds the operator-approved provider cap", modelConfig.ModelName)
	}
	if agent.DefaultModel != modelConfig.ModelName {
		return fmt.Errorf("agent default model %q does not match model %q", agent.DefaultModel, modelConfig.ModelName)
	}
	return nil
}

// ValidateAgentModelBudget proves that one invocation is fully reserved using
// the immutable operator catalog and the maximum number of provider calls.
func ValidateAgentModelBudget(agent AgentConfig, modelConfig ModelConfig, budget BudgetConfig) error {
	if err := ValidateAgentModel(agent, modelConfig); err != nil {
		return err
	}
	if budget.MaxTokensPerDay < 0 || budget.MaxTokensPerRequest < 0 ||
		budget.MaxCostPerDay < 0 || budget.MaxConcurrentSessions < 0 {
		return fmt.Errorf("budget limits cannot be negative")
	}
	if (budget.MaxTokensPerDay > 0) != (budget.MaxTokensPerRequest > 0) {
		return fmt.Errorf("daily token budget and per-request token reservation must be configured together")
	}
	if budget.MaxTokensPerRequest > budget.MaxTokensPerDay {
		return fmt.Errorf("per-request token reservation cannot exceed the daily token budget")
	}
	if budget.MaxTokensPerDay > maxRedisLuaExactInteger {
		return fmt.Errorf("daily token budget exceeds the exact integer range of the Redis ledger")
	}
	if budget.MaxTokensPerRequest == 0 {
		return nil
	}
	if modelConfig.MaxTokens <= 0 {
		return fmt.Errorf("model %q must define a positive completion limit under a token budget", modelConfig.ModelName)
	}
	profile, known := modelcatalog.Resolve(modelConfig.Provider, modelConfig.ModelName)
	if !known {
		return fmt.Errorf("model %q has no operator-approved context window for token admission", modelConfig.ModelName)
	}
	if modelConfig.MaxTokens > profile.ContextWindow {
		return fmt.Errorf("model %q completion limit exceeds its context window", modelConfig.ModelName)
	}
	required := int64(profile.ContextWindow) * int64(agent.EffectiveMaxLLMCalls())
	if required > budget.MaxTokensPerRequest {
		return fmt.Errorf("agent %q token reservation must be at least context window %d times %d LLM calls", agent.Name, profile.ContextWindow, agent.EffectiveMaxLLMCalls())
	}
	return nil
}

// ValidateProductionModelCatalog rejects model IDs that are not present in
// the immutable operator catalog. Local, zero-budget unit fixtures may use a
// placeholder model name, but an immutable version entering the publish path
// must be executable by this composition and have reviewed provider limits.
func ValidateProductionModelCatalog(agent AgentConfig, modelConfig ModelConfig) error {
	if err := ValidateAgentModel(agent, modelConfig); err != nil {
		return err
	}
	profile, ok := modelcatalog.Resolve(modelConfig.Provider, modelConfig.ModelName)
	if !ok {
		return fmt.Errorf("model %q is not present in the operator-approved model catalog", modelConfig.ModelName)
	}
	if modelConfig.MaxTokens > profile.MaxOutputTokens {
		return fmt.Errorf("model %q completion limit exceeds the operator-approved provider cap", modelConfig.ModelName)
	}
	return nil
}

// ValidatePinnedAgentModelBudget additionally proves that the immutable
// version carries the same catalog revision and context window as this binary.
func ValidatePinnedAgentModelBudget(
	agent AgentConfig,
	modelConfig ModelConfig,
	budget BudgetConfig,
	catalogRevision string,
	contextWindow int,
) error {
	if err := ValidateAgentModelBudget(agent, modelConfig, budget); err != nil {
		return err
	}
	if budget.MaxTokensPerRequest == 0 {
		return nil
	}
	profile, _ := modelcatalog.Resolve(modelConfig.Provider, modelConfig.ModelName)
	if catalogRevision != profile.Revision || contextWindow != profile.ContextWindow {
		return fmt.Errorf("version model limits do not match operator catalog revision %q", profile.Revision)
	}
	return nil
}

// ValidateConfig rejects configurations that the production composition root
// cannot execute. Accepting aspirational enum values creates delayed runtime
// failures and is worse than a precise control-plane rejection.
func ValidateConfig(config TenantConfig) error {
	if len(config.Agents) == 0 || len(config.Agents) > maxAgentsPerTenant {
		return fmt.Errorf("tenant must configure 1..%d agents", maxAgentsPerTenant)
	}
	if len(config.Models) == 0 || len(config.Models) > maxModelsPerTenant {
		return fmt.Errorf("tenant must configure 1..%d models", maxModelsPerTenant)
	}
	if len(config.Channels) > maxChannelsPerTenant {
		return fmt.Errorf("tenant exceeds %d channel bindings", maxChannelsPerTenant)
	}
	if config.ToolPolicy.Mode != "whitelist" {
		return fmt.Errorf("tool policy must use explicit whitelist mode")
	}
	if len(config.ToolPolicy.Allowed) > maxToolPolicyEntries ||
		len(config.ToolPolicy.Denied) > maxToolPolicyEntries ||
		len(config.ToolPolicy.RequireConfirmation) > maxToolPolicyEntries {
		return fmt.Errorf("tool policy exceeds %d entries per list", maxToolPolicyEntries)
	}
	if len(config.ToolPolicy.Denied) != 0 {
		return fmt.Errorf("whitelist tool policy cannot configure a denied list")
	}
	allowedTools := make(map[string]struct{}, len(config.ToolPolicy.Allowed))
	for _, toolName := range config.ToolPolicy.Allowed {
		if err := validateLogicalName("tool whitelist entry", toolName, maxToolNameBytes); err != nil {
			return err
		}
		if _, duplicate := allowedTools[toolName]; duplicate {
			return fmt.Errorf("tool %q appears more than once in the whitelist", toolName)
		}
		allowedTools[toolName] = struct{}{}
	}
	confirmationTools := make(map[string]struct{}, len(config.ToolPolicy.RequireConfirmation))
	for _, toolName := range config.ToolPolicy.RequireConfirmation {
		if err := validateLogicalName("confirmation tool", toolName, maxToolNameBytes); err != nil {
			return err
		}
		if _, allowed := allowedTools[toolName]; !allowed {
			return fmt.Errorf("confirmation tool %q is outside the whitelist", toolName)
		}
		if _, duplicate := confirmationTools[toolName]; duplicate {
			return fmt.Errorf("confirmation tool %q appears more than once", toolName)
		}
		confirmationTools[toolName] = struct{}{}
	}

	models := make(map[string]ModelConfig, len(config.Models))
	for _, modelConfig := range config.Models {
		if modelConfig.Provider != "openai" {
			return fmt.Errorf("model provider %q is not installed", modelConfig.Provider)
		}
		if err := validateLogicalName("model name", modelConfig.ModelName, maxModelNameBytes); err != nil {
			return err
		}
		if modelConfig.APIKey != "" && modelConfig.APIKeyRef != "" {
			return fmt.Errorf("model %q cannot configure both api key and api key reference", modelConfig.ModelName)
		}
		if modelConfig.APIKey == "" && modelConfig.APIKeyRef == "" {
			return fmt.Errorf("model name and API key or API key reference are required")
		}
		if modelConfig.APIKey != "" && (len(modelConfig.APIKey) > 8<<10 ||
			strings.ContainsAny(modelConfig.APIKey, "\x00\r\n")) {
			return fmt.Errorf("model API key is invalid")
		}
		if modelConfig.APIKeyRef != "" {
			if err := SecretRef(modelConfig.APIKeyRef).Validate(); err != nil {
				return fmt.Errorf("model API key reference is invalid")
			}
		}
		if err := validateMetadata("model", modelConfig.Metadata); err != nil {
			return err
		}
		if modelConfig.Endpoint != "" {
			return fmt.Errorf("custom model endpoints are not installed with an SSRF-safe transport")
		}
		if modelConfig.MaxTokens < 0 || modelConfig.Temperature < 0 || modelConfig.Temperature > 2 {
			return fmt.Errorf("model %q has invalid generation limits", modelConfig.ModelName)
		}
		if _, duplicate := models[modelConfig.ModelName]; duplicate {
			return fmt.Errorf("model %q is configured more than once", modelConfig.ModelName)
		}
		models[modelConfig.ModelName] = modelConfig
	}

	agents := make(map[string]struct{}, len(config.Agents))
	for _, agent := range config.Agents {
		if err := ValidateAgentAppName(agent.Name); err != nil {
			return err
		}
		if err := validatePrompt(agent.SystemPrompt); err != nil {
			return fmt.Errorf("agent %q: %w", agent.Name, err)
		}
		if err := validateMetadata("agent", agent.Metadata); err != nil {
			return fmt.Errorf("agent %q: %w", agent.Name, err)
		}
		if len(agent.Tools) > maxToolsPerAgent {
			return fmt.Errorf("agent %q exceeds %d tools", agent.Name, maxToolsPerAgent)
		}
		if !SupportedAgentType(agent.Type) {
			return fmt.Errorf("agent %q type %q is unsupported", agent.Name, agent.Type)
		}
		if agent.MaxLLMCalls < 0 || agent.MaxLLMCalls > MaxConfiguredLLMCalls {
			return fmt.Errorf("agent %q has an invalid max LLM call limit", agent.Name)
		}
		if len(agent.Tools) > 0 && agent.EffectiveMaxLLMCalls() < 2 {
			return fmt.Errorf("agent %q needs at least two LLM calls when tools are enabled", agent.Name)
		}
		modelConfig, exists := models[agent.DefaultModel]
		if !exists {
			return fmt.Errorf("agent %q references unknown model %q", agent.Name, agent.DefaultModel)
		}
		if err := ValidateAgentModel(agent, modelConfig); err != nil {
			return err
		}
		if _, duplicate := agents[agent.Name]; duplicate {
			return fmt.Errorf("agent %q is configured more than once", agent.Name)
		}
		for _, toolName := range agent.Tools {
			if err := validateLogicalName("tool name", toolName, maxToolNameBytes); err != nil {
				return fmt.Errorf("agent %q: %w", agent.Name, err)
			}
			if !config.ToolPolicy.IsAllowed(toolName) {
				return fmt.Errorf("agent %q tool %q is outside the tenant whitelist", agent.Name, toolName)
			}
		}
		agents[agent.Name] = struct{}{}
	}

	channelAccounts := make(map[string]struct{}, len(config.Channels))
	webhookKeys := make(map[string]struct{}, len(config.Channels))
	for _, binding := range config.Channels {
		if binding.Type != "wework" && binding.Type != "telegram" && binding.Type != "wecom_bot" {
			return fmt.Errorf("channel type %q is not installed", binding.Type)
		}
		if _, exists := agents[binding.AgentApp]; !exists {
			return fmt.Errorf("channel %q references unknown agent app %q", binding.Type, binding.AgentApp)
		}
		if binding.AccountID != "" {
			if err := validateLogicalName("channel account ID", binding.AccountID, 128); err != nil {
				return err
			}
		}
		if binding.WebhookKey != "" {
			if err := validateLogicalName("channel webhook key", binding.WebhookKey, 128); err != nil {
				return err
			}
		}
		if len(binding.Config) > maxChannelConfigEntries {
			return fmt.Errorf("channel %q exceeds %d configuration entries", binding.Type, maxChannelConfigEntries)
		}
		if err := validateChannelSecretReferences(binding); err != nil {
			return err
		}
		for key, value := range binding.Config {
			if err := validateLogicalName("channel configuration key", key, 128); err != nil {
				return err
			}
			// Channel configuration is persisted alongside credentials. Keep the
			// surface closed to keys consumed by installed adapters; arbitrary keys
			// could otherwise persist an unencrypted api_key/access_token.
			switch key {
			case "account_id", "corp_id", "encoding_aes_key":
			default:
				return fmt.Errorf("channel configuration key %q is not operator-approved", key)
			}
			if len(value) > 4<<10 || !utf8.ValidString(value) || strings.ContainsRune(value, 0) {
				return fmt.Errorf("channel configuration value must be valid UTF-8 without NUL and at most 4096 bytes")
			}
		}
		if binding.Type == "telegram" {
			if binding.TokenRef == "" && !IsValidTelegramBotToken(binding.Token) {
				return fmt.Errorf("telegram bot token has invalid format")
			}
			if binding.SecretRef == "" && !IsValidTelegramWebhookSecret(binding.Secret) {
				return fmt.Errorf("telegram webhook secret has invalid format")
			}
		}
		if binding.Type == "wework" {
			if err := validateWeWorkBinding(binding); err != nil {
				return err
			}
		}
		if binding.Type == "wecom_bot" {
			if err := ValidateWeComBotBinding(binding); err != nil {
				return err
			}
		}
		if err := validateChannelAccessPolicy(binding.AccessPolicy); err != nil {
			return fmt.Errorf("channel %q access policy: %w", binding.Type, err)
		}

		resolved := binding
		accountID := resolved.EnsureAccountID()
		// AccountID may be derived from Config["account_id"] or AppID. Validate
		// the resolved identity as well as the explicit field: this value becomes
		// part of the durable Inbox key and the Session routing namespace.
		if err := validateLogicalName("channel account ID", accountID, 128); err != nil {
			return err
		}
		accountKey := binding.Type + "\x00" + accountID
		if _, duplicate := channelAccounts[accountKey]; duplicate {
			return fmt.Errorf("channel account %q is configured more than once for %s", resolved.AccountID, binding.Type)
		}
		channelAccounts[accountKey] = struct{}{}
		if webhookKey := binding.EffectiveWebhookKey(); webhookKey != "" {
			if _, duplicate := webhookKeys[webhookKey]; duplicate {
				return fmt.Errorf("channel webhook key is configured more than once")
			}
			webhookKeys[webhookKey] = struct{}{}
		}
	}

	if err := validateBackend(
		"session",
		config.Storage.SessionBackend,
		config.Storage.SessionProfile,
		config.Storage.SessionConfig,
	); err != nil {
		return err
	}
	if err := validateBackend(
		"memory",
		config.Storage.MemoryBackend,
		config.Storage.MemoryProfile,
		config.Storage.MemoryConfig,
	); err != nil {
		return err
	}
	if err := validateDataPlaneBackend(
		"knowledge", config.Storage.KnowledgeBackend, config.Storage.KnowledgeProfile, "qdrant",
	); err != nil {
		return err
	}
	if err := validateDataPlaneBackend(
		"artifact", config.Storage.ArtifactBackend, config.Storage.ArtifactProfile, "s3",
	); err != nil {
		return err
	}
	if config.Budget.MaxTokensPerDay < 0 || config.Budget.MaxTokensPerRequest < 0 ||
		config.Budget.MaxCostPerDay < 0 || config.Budget.MaxConcurrentSessions < 0 {
		return fmt.Errorf("budget limits cannot be negative")
	}
	if (config.Budget.MaxTokensPerDay > 0) != (config.Budget.MaxTokensPerRequest > 0) {
		return fmt.Errorf("daily token budget and per-request token reservation must be configured together")
	}
	if config.Budget.MaxTokensPerRequest > config.Budget.MaxTokensPerDay {
		return fmt.Errorf("per-request token reservation cannot exceed the daily token budget")
	}
	if config.Budget.MaxTokensPerDay > maxRedisLuaExactInteger {
		return fmt.Errorf("daily token budget exceeds the exact integer range of the Redis ledger")
	}
	if config.Budget.MaxTokensPerRequest > 0 {
		for _, agent := range config.Agents {
			modelConfig := models[agent.DefaultModel]
			if err := ValidateAgentModelBudget(agent, modelConfig, config.Budget); err != nil {
				return err
			}
		}
	}
	if config.Budget.MaxCostPerDay > 0 {
		return fmt.Errorf("cost budget requires provider-reported usage and an operator price catalog, which are not installed")
	}
	if len(config.Budget.AlertThresholds) > 0 {
		return fmt.Errorf("tenant budget alert thresholds are not installed; use platform SLO alerts")
	}
	if config.Governance.AuditLevel != "" && config.Governance.AuditLevel != "basic" && config.Governance.AuditLevel != "detailed" {
		return fmt.Errorf("audit level must be basic or detailed")
	}
	if len(config.Governance.DataMasking) > maxMaskingRules {
		return fmt.Errorf("data masking rules exceed %d", maxMaskingRules)
	}
	if len(config.Governance.ContentFilters) > maxContentFilters {
		return fmt.Errorf("content filters exceed %d", maxContentFilters)
	}
	governancePatternBytes := 0
	for _, rule := range config.Governance.DataMasking {
		switch rule.Type {
		case "credit_card", "email", "phone", "ssn", "api_key", "custom":
		default:
			return fmt.Errorf("unsupported data masking rule %q", rule.Type)
		}
		if len(rule.Pattern) > maxGovernancePatternBytes ||
			!utf8.ValidString(rule.Pattern) || strings.ContainsRune(rule.Pattern, 0) {
			return fmt.Errorf("data masking pattern exceeds %d bytes or is invalid", maxGovernancePatternBytes)
		}
		if len(rule.Replace) > maxGovernanceReplaceBytes ||
			!utf8.ValidString(rule.Replace) || strings.ContainsRune(rule.Replace, 0) {
			return fmt.Errorf("data masking replacement exceeds %d bytes or is invalid", maxGovernanceReplaceBytes)
		}
		governancePatternBytes += len(rule.Pattern) + len(rule.Replace)
		if governancePatternBytes > maxGovernancePatternTotal {
			return fmt.Errorf("governance pattern material exceeds %d bytes", maxGovernancePatternTotal)
		}
		if rule.Type == "custom" {
			if rule.Pattern == "" {
				return fmt.Errorf("custom masking rule requires a pattern")
			}
			if _, err := regexp.Compile(rule.Pattern); err != nil {
				return fmt.Errorf("custom masking rule has invalid pattern: %w", err)
			}
		}
	}
	for _, filter := range config.Governance.ContentFilters {
		if err := validateDisplayText("content filter name", filter.Name, maxGovernanceNameBytes); err != nil || len(filter.Patterns) == 0 {
			if err != nil {
				return err
			}
			return fmt.Errorf("content filter name and patterns are required")
		}
		if len(filter.Patterns) > maxContentFilterPatterns {
			return fmt.Errorf("content filter %q exceeds %d patterns", filter.Name, maxContentFilterPatterns)
		}
		if filter.Type != "keyword" && filter.Type != "regex" {
			return fmt.Errorf("unsupported content filter type %q", filter.Type)
		}
		if filter.Action != "block" && filter.Action != "warn" && filter.Action != "log" {
			return fmt.Errorf("unsupported content filter action %q", filter.Action)
		}
		for _, pattern := range filter.Patterns {
			if pattern == "" || len(pattern) > maxGovernancePatternBytes ||
				!utf8.ValidString(pattern) || strings.ContainsRune(pattern, 0) {
				return fmt.Errorf("content filter %q has an invalid pattern", filter.Name)
			}
			governancePatternBytes += len(pattern)
			if governancePatternBytes > maxGovernancePatternTotal {
				return fmt.Errorf("governance pattern material exceeds %d bytes", maxGovernancePatternTotal)
			}
			if filter.Type == "regex" {
				if _, err := regexp.Compile(pattern); err != nil {
					return fmt.Errorf("content filter %q has invalid regex: %w", filter.Name, err)
				}
			}
		}
	}
	return nil
}

func validateChannelAccessPolicy(policy ChannelAccessPolicy) error {
	if !policy.AllowDirectMessages && !policy.AllowGroupMessages {
		return fmt.Errorf("at least one message scope must be explicitly enabled")
	}
	if len(policy.AllowedUsers) == 0 {
		return fmt.Errorf("at least one provider user must be allowed")
	}
	if policy.AllowGroupMessages && len(policy.AllowedGroups) == 0 {
		return fmt.Errorf("group messages require an explicit group allowlist")
	}
	if !policy.AllowGroupMessages && len(policy.AllowedGroups) != 0 {
		return fmt.Errorf("group allowlist requires group messages to be enabled")
	}
	if err := validateIdentityAllowlist("user", policy.AllowedUsers); err != nil {
		return err
	}
	if err := validateIdentityAllowlist("group", policy.AllowedGroups); err != nil {
		return err
	}
	return nil
}

func validateIdentityAllowlist(kind string, values []string) error {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value == "" || value == "*" || len(value) > 512 || !utf8.ValidString(value) ||
			strings.ContainsAny(value, "\x00\r\n") {
			return fmt.Errorf("%s allowlist contains an invalid identifier", kind)
		}
		if _, duplicate := seen[value]; duplicate {
			return fmt.Errorf("%s allowlist contains a duplicate identifier", kind)
		}
		seen[value] = struct{}{}
	}
	return nil
}

func validateWeWorkBinding(binding ChannelBinding) error {
	if binding.TokenRef == "" && !IsValidWeWorkCallbackToken(binding.Token) {
		return fmt.Errorf("WeWork callback token has invalid format")
	}
	encodedKey := binding.EncodingAESKey
	if encodedKey == "" && binding.EncodingAESKeyRef == "" {
		encodedKey = binding.Config["encoding_aes_key"]
	}
	if binding.EncodingAESKeyRef == "" {
		key, err := base64.StdEncoding.DecodeString(encodedKey + "=")
		if err != nil || len(key) != 32 {
			return fmt.Errorf("WeWork encoding AES key must be a valid 43-character key")
		}
	}
	if !IsValidWeWorkCorpID(binding.Config["corp_id"]) {
		return fmt.Errorf("WeWork corp_id has invalid format")
	}
	if binding.SecretRef == "" && !IsValidWeWorkCorpSecret(binding.Secret) {
		return fmt.Errorf("WeWork corp secret has invalid format")
	}
	if !IsValidWeWorkAgentID(binding.AppID) {
		return fmt.Errorf("WeWork agent ID has invalid format")
	}
	return nil
}

func validateChannelSecretReferences(binding ChannelBinding) error {
	for _, field := range []struct {
		name  string
		value string
		ref   string
	}{
		{name: "channel token", value: binding.Token, ref: binding.TokenRef},
		{name: "channel secret", value: binding.Secret, ref: binding.SecretRef},
		{name: "channel encoding AES key", value: binding.EncodingAESKey, ref: binding.EncodingAESKeyRef},
	} {
		if field.value != "" && field.ref != "" {
			return fmt.Errorf("%s cannot configure both an inline value and a secret reference", field.name)
		}
		if field.ref != "" {
			if err := SecretRef(field.ref).Validate(); err != nil {
				return fmt.Errorf("%s reference is invalid", field.name)
			}
		}
	}
	if binding.EncodingAESKeyRef != "" && binding.Config["encoding_aes_key"] != "" {
		return fmt.Errorf("channel encoding AES key cannot configure both a secret reference and config value")
	}
	return nil
}

// IsValidTelegramBotToken validates the documented Telegram credential shape.
func IsValidTelegramBotToken(value string) bool {
	return telegramBotTokenPattern.MatchString(value)
}

// IsValidTelegramWebhookSecret applies the platform's 32-character security
// floor in addition to Telegram's secret-token character set and upper bound.
func IsValidTelegramWebhookSecret(value string) bool {
	return telegramWebhookSecretPattern.MatchString(value)
}

// IsValidWeWorkCallbackToken validates a WeWork callback verification token.
func IsValidWeWorkCallbackToken(value string) bool {
	return weWorkCallbackTokenPattern.MatchString(value)
}

// IsValidWeWorkCorpID validates a WeWork enterprise identifier.
func IsValidWeWorkCorpID(value string) bool {
	return weWorkCorpIDPattern.MatchString(value)
}

// IsValidWeWorkCorpSecret validates a WeWork application secret.
func IsValidWeWorkCorpSecret(value string) bool {
	return weWorkCorpSecretPattern.MatchString(value)
}

// IsValidWeWorkAgentID validates a positive numeric WeWork application ID.
func IsValidWeWorkAgentID(value string) bool {
	parsed, err := strconv.ParseUint(value, 10, 32)
	return err == nil && parsed > 0
}

func validateBackend(domain, backend, profile string, config map[string]string) error {
	switch backend {
	case "", "inmemory":
		if profile != "" {
			return fmt.Errorf("%s in-memory backend cannot reference an operator profile", domain)
		}
	case "redis", "postgres":
		if !backendProfileIDPattern.MatchString(profile) {
			return fmt.Errorf("%s %s backend requires a valid operator profile", domain, backend)
		}
	default:
		return fmt.Errorf("%s backend %q is not installed", domain, backend)
	}

	for key, value := range config {
		switch domain + ":" + key {
		case "session:session_ttl":
			ttl, err := time.ParseDuration(value)
			if err != nil || ttl <= 0 || ttl > 365*24*time.Hour {
				return fmt.Errorf("session_ttl must be between one nanosecond and 365 days")
			}
		case "memory:memory_limit":
			limit, err := strconv.Atoi(value)
			if err != nil || limit <= 0 || limit > 1_000_000 {
				return fmt.Errorf("memory_limit must be between 1 and 1000000")
			}
		default:
			return fmt.Errorf("%s backend configuration key %q is not tenant-configurable", domain, key)
		}
	}
	return nil
}

func validateDataPlaneBackend(domain, backend, profile, installed string) error {
	if backend == "" {
		if profile != "" {
			return fmt.Errorf("%s profile requires an enabled backend", domain)
		}
		return nil
	}
	if backend != installed {
		return fmt.Errorf("%s backend %q is not installed", domain, backend)
	}
	if !backendProfileIDPattern.MatchString(profile) {
		return fmt.Errorf("%s %s backend requires a valid operator profile", domain, backend)
	}
	return nil
}

// ValidateDistributedStorage rejects process-local state for the horizontally
// scaled production path. In-memory services remain available to unit tests
// and explicit local composition, but Admin and Worker fail closed on them.
func ValidateDistributedStorage(config StorageConfig) error {
	if config.SessionBackend == "" || config.SessionBackend == "inmemory" {
		return fmt.Errorf("production session backend must be redis or postgres")
	}
	if config.MemoryBackend == "" || config.MemoryBackend == "inmemory" {
		return fmt.Errorf("production memory backend must be redis or postgres")
	}
	if err := validateDataPlaneBackend("knowledge", config.KnowledgeBackend, config.KnowledgeProfile, "qdrant"); err != nil {
		return err
	}
	if err := validateDataPlaneBackend("artifact", config.ArtifactBackend, config.ArtifactProfile, "s3"); err != nil {
		return err
	}
	return nil
}

func configFromTenant(value *Tenant) TenantConfig {
	return TenantConfig{
		Agents:     value.Agents,
		Models:     value.Models,
		ToolPolicy: value.ToolPolicy,
		Channels:   value.Channels,
		Storage:    value.Storage,
		Governance: value.Governance,
		Budget:     value.Budget,
	}
}
