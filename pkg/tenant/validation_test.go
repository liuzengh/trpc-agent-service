package tenant

import (
	"strings"
	"testing"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/modelcatalog"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

func validConfig() TenantConfig {
	return TenantConfig{
		Agents:     []AgentConfig{{Name: "support", Type: "llm", DefaultModel: "gpt-4", MaxLLMCalls: 2, Tools: []string{"current_time"}}},
		Models:     []ModelConfig{{Provider: "openai", ModelName: "gpt-4", APIKey: "secret", MaxTokens: 1_000}},
		ToolPolicy: ToolPolicy{Mode: "whitelist", Allowed: []string{"current_time"}},
		Channels: []ChannelBinding{{
			Type: "telegram", AgentApp: "support",
			Token: "123456789:" + strings.Repeat("A", 35), Secret: strings.Repeat("s", 32),
			AccessPolicy: ChannelAccessPolicy{
				AllowDirectMessages: true,
				AllowedUsers:        []string{"42"},
			},
		}},
		Storage: StorageConfig{SessionBackend: "inmemory", MemoryBackend: "inmemory"},
	}
}

func TestValidateConfigRejectsUnsafeChannelAccessPolicy(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*ChannelAccessPolicy)
	}{
		{"default deny without an explicit channel mode", func(*ChannelAccessPolicy) {}},
		{"direct messages without users", func(p *ChannelAccessPolicy) { p.AllowDirectMessages = true }},
		{"groups without group allowlist", func(p *ChannelAccessPolicy) {
			p.AllowGroupMessages = true
			p.AllowedUsers = []string{"42"}
		}},
		{"group IDs configured while groups disabled", func(p *ChannelAccessPolicy) {
			p.AllowDirectMessages = true
			p.AllowedUsers = []string{"42"}
			p.AllowedGroups = []string{"-1001"}
		}},
		{"wildcard user", func(p *ChannelAccessPolicy) {
			p.AllowDirectMessages = true
			p.AllowedUsers = []string{"*"}
		}},
		{"duplicate user", func(p *ChannelAccessPolicy) {
			p.AllowDirectMessages = true
			p.AllowedUsers = []string{"42", "42"}
		}},
		{"control character in group", func(p *ChannelAccessPolicy) {
			p.AllowGroupMessages = true
			p.AllowedUsers = []string{"42"}
			p.AllowedGroups = []string{"-1001\n"}
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := validConfig()
			config.Channels[0].AccessPolicy = ChannelAccessPolicy{}
			test.mutate(&config.Channels[0].AccessPolicy)
			if err := ValidateConfig(config); err == nil {
				t.Fatal("unsafe IM access policy was accepted")
			}
		})
	}

	config := validConfig()
	config.Channels[0].AccessPolicy = ChannelAccessPolicy{
		AllowDirectMessages: true,
		AllowGroupMessages:  true,
		AllowedUsers:        []string{"42", "84"},
		AllowedGroups:       []string{"-1001"},
	}
	if err := ValidateConfig(config); err != nil {
		t.Fatalf("explicit channel access policy rejected: %v", err)
	}
}

func TestValidateConfigRejectsUnapprovedChannelConfigKeys(t *testing.T) {
	for _, key := range []string{"api_key", "access_token", "password", "custom_runtime_option"} {
		t.Run(key, func(t *testing.T) {
			config := validConfig()
			config.Channels[0].Config = map[string]string{key: "secret-or-unsupported"}
			if err := ValidateConfig(config); err == nil {
				t.Fatalf("channel config key %q was accepted", key)
			}
		})
	}
}

func TestValidateConfigRejectsAspirationalRuntimeValues(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*TenantConfig)
	}{
		{"implicit allow-all tool policy", func(c *TenantConfig) { c.ToolPolicy.Mode = "" }},
		{"unsupported agent type", func(c *TenantConfig) { c.Agents[0].Type = "wasm" }},
		{"unsupported model provider", func(c *TenantConfig) { c.Models[0].Provider = "anthropic" }},
		{"unknown agent model", func(c *TenantConfig) { c.Agents[0].DefaultModel = "missing" }},
		{"unbound channel", func(c *TenantConfig) { c.Channels[0].AgentApp = "missing" }},
		{"duplicate channel account", func(c *TenantConfig) { c.Channels = append(c.Channels, c.Channels[0]) }},
		{"missing operator backend profile", func(c *TenantConfig) { c.Storage.SessionBackend = "postgres" }},
		{"duplicate tool whitelist entry", func(c *TenantConfig) { c.ToolPolicy.Allowed = []string{"current_time", "current_time"} }},
		{"unknown content action", func(c *TenantConfig) {
			c.Governance.ContentFilters = []ContentFilter{{Name: "bad", Type: "keyword", Patterns: []string{"x"}, Action: "allow"}}
		}},
		{"invalid content regex", func(c *TenantConfig) {
			c.Governance.ContentFilters = []ContentFilter{{Name: "bad", Type: "regex", Patterns: []string{"["}, Action: "block"}}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := validConfig()
			test.mutate(&config)
			if err := ValidateConfig(config); err == nil {
				t.Fatal("invalid configuration was accepted")
			}
		})
	}
}

func TestValidateConfigAcceptsKnownRuntimeTypeForFactoryResolution(t *testing.T) {
	for _, runtimeType := range []string{AgentTypeLLM, AgentTypeChain, AgentTypeGraph, AgentTypeParallel, AgentTypeCycle} {
		t.Run(runtimeType, func(t *testing.T) {
			config := validConfig()
			config.Agents[0].Type = runtimeType
			switch runtimeType {
			case AgentTypeChain, AgentTypeParallel:
				config.Agents[0].Runtime = &AgentRuntimeConfig{Nodes: []AgentRuntimeNode{{Name: "step"}}}
			case AgentTypeGraph:
				config.Agents[0].Runtime = &AgentRuntimeConfig{Nodes: []AgentRuntimeNode{{Name: "step"}}, Entry: "step"}
			case AgentTypeCycle:
				config.Agents[0].Runtime = &AgentRuntimeConfig{Nodes: []AgentRuntimeNode{{Name: "step"}}, MaxIterations: 2}
			}
			if err := ValidateConfig(config); err != nil {
				t.Fatalf("known runtime type %q rejected: %v", runtimeType, err)
			}
		})
	}
}

func TestValidateAgentRuntimeRejectsUnsafeCompositeTopologies(t *testing.T) {
	base := AgentConfig{Name: "support", DefaultModel: "gpt-4", MaxLLMCalls: 4, Tools: []string{"search"}}
	tests := []struct {
		name   string
		mutate func(*AgentConfig)
	}{
		{"missing topology", func(a *AgentConfig) { a.Type = AgentTypeChain }},
		{"llm with ignored topology", func(a *AgentConfig) {
			a.Type = AgentTypeLLM
			a.Runtime = &AgentRuntimeConfig{Nodes: []AgentRuntimeNode{{Name: "step"}}}
		}},
		{"node tool outside parent", func(a *AgentConfig) {
			a.Type = AgentTypeChain
			a.Runtime = &AgentRuntimeConfig{Nodes: []AgentRuntimeNode{{Name: "step", MaxLLMCalls: 2, Tools: []string{"delete"}}}}
		}},
		{"aggregate calls exceed parent", func(a *AgentConfig) {
			a.Type = AgentTypeParallel
			a.Runtime = &AgentRuntimeConfig{Nodes: []AgentRuntimeNode{{Name: "left", MaxLLMCalls: 3}, {Name: "right", MaxLLMCalls: 2}}}
		}},
		{"unbounded cycle", func(a *AgentConfig) {
			a.Type = AgentTypeCycle
			a.Runtime = &AgentRuntimeConfig{Nodes: []AgentRuntimeNode{{Name: "step"}}}
		}},
		{"graph unknown edge", func(a *AgentConfig) {
			a.Type = AgentTypeGraph
			a.Runtime = &AgentRuntimeConfig{Nodes: []AgentRuntimeNode{{Name: "start"}}, Entry: "start", Edges: []AgentRuntimeEdge{{From: "start", To: "missing"}}}
		}},
		{"graph unreachable node", func(a *AgentConfig) {
			a.Type = AgentTypeGraph
			a.Runtime = &AgentRuntimeConfig{Nodes: []AgentRuntimeNode{{Name: "start"}, {Name: "orphan"}}, Entry: "start"}
		}},
		{"graph cycle", func(a *AgentConfig) {
			a.Type = AgentTypeGraph
			a.Runtime = &AgentRuntimeConfig{
				Nodes: []AgentRuntimeNode{{Name: "first"}, {Name: "second"}}, Entry: "first",
				Edges: []AgentRuntimeEdge{{From: "first", To: "second"}, {From: "second", To: "first"}},
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			agent := base
			test.mutate(&agent)
			if err := ValidateAgentRuntime(agent); err == nil {
				t.Fatal("unsafe runtime topology was accepted")
			}
		})
	}
}

func TestValidateConfigRequiresOperatorOwnedBackendProfiles(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*TenantConfig)
	}{
		{
			name: "tenant supplied PostgreSQL DSN",
			mutate: func(c *TenantConfig) {
				c.Storage.SessionBackend = "postgres"
				c.Storage.SessionProfile = "shared-postgres"
				c.Storage.SessionConfig = map[string]string{"dsn": "postgres://tenant:secret@internal/app"}
			},
		},
		{
			name: "tenant supplied Redis URL",
			mutate: func(c *TenantConfig) {
				c.Storage.MemoryBackend = "redis"
				c.Storage.MemoryProfile = "shared-redis"
				c.Storage.MemoryConfig = map[string]string{"url": "redis://:secret@internal:6379/0"}
			},
		},
		{
			name: "invalid profile identifier",
			mutate: func(c *TenantConfig) {
				c.Storage.SessionBackend = "postgres"
				c.Storage.SessionProfile = "../../control-plane"
			},
		},
		{
			name: "profile on process local backend",
			mutate: func(c *TenantConfig) {
				c.Storage.SessionProfile = "shared-postgres"
			},
		},
		{
			name: "unknown session tuning key",
			mutate: func(c *TenantConfig) {
				c.Storage.SessionConfig = map[string]string{"pool_size": "1000"}
			},
		},
		{
			name: "invalid session TTL",
			mutate: func(c *TenantConfig) {
				c.Storage.SessionConfig = map[string]string{"session_ttl": "forever"}
			},
		},
		{
			name: "excessive session TTL",
			mutate: func(c *TenantConfig) {
				c.Storage.SessionConfig = map[string]string{"session_ttl": (366 * 24 * time.Hour).String()}
			},
		},
		{
			name: "invalid memory limit",
			mutate: func(c *TenantConfig) {
				c.Storage.MemoryConfig = map[string]string{"memory_limit": "0"}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := validConfig()
			test.mutate(&config)
			if err := ValidateConfig(config); err == nil {
				t.Fatal("unsafe storage configuration was accepted")
			}
		})
	}

	config := validConfig()
	config.Storage.SessionBackend = "postgres"
	config.Storage.SessionProfile = "shared-postgres"
	config.Storage.SessionConfig = map[string]string{"session_ttl": "24h"}
	config.Storage.MemoryBackend = "redis"
	config.Storage.MemoryProfile = "shared-redis"
	config.Storage.MemoryConfig = map[string]string{"memory_limit": "1000"}
	if err := ValidateConfig(config); err != nil {
		t.Fatalf("operator-profile storage rejected: %v", err)
	}
}

func TestValidateConfigAcceptsOperatorOwnedKnowledgeAndArtifactProfiles(t *testing.T) {
	config := validConfig()
	config.Storage.KnowledgeBackend = "qdrant"
	config.Storage.KnowledgeProfile = "knowledge-primary"
	config.Storage.ArtifactBackend = "s3"
	config.Storage.ArtifactProfile = "artifacts-primary"
	if err := ValidateConfig(config); err != nil {
		t.Fatalf("operator-owned data-plane profiles rejected: %v", err)
	}
}

func TestValidateConfigRejectsIncompleteKnowledgeAndArtifactProfiles(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*StorageConfig)
	}{
		{"knowledge backend without profile", func(s *StorageConfig) { s.KnowledgeBackend = "qdrant" }},
		{"knowledge profile without backend", func(s *StorageConfig) { s.KnowledgeProfile = "knowledge-primary" }},
		{"unknown knowledge backend", func(s *StorageConfig) {
			s.KnowledgeBackend, s.KnowledgeProfile = "elasticsearch", "knowledge-primary"
		}},
		{"artifact backend without profile", func(s *StorageConfig) { s.ArtifactBackend = "s3" }},
		{"artifact profile without backend", func(s *StorageConfig) { s.ArtifactProfile = "artifacts-primary" }},
		{"unknown artifact backend", func(s *StorageConfig) {
			s.ArtifactBackend, s.ArtifactProfile = "filesystem", "artifacts-primary"
		}},
		{"unsafe knowledge profile", func(s *StorageConfig) {
			s.KnowledgeBackend, s.KnowledgeProfile = "qdrant", "../../knowledge"
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := validConfig()
			test.mutate(&config.Storage)
			if err := ValidateConfig(config); err == nil {
				t.Fatal("incomplete data-plane configuration was accepted")
			}
		})
	}
}

func TestValidateConfigEnforcesInstalledWeWorkContract(t *testing.T) {
	validWeWork := func() TenantConfig {
		config := validConfig()
		config.Channels = []ChannelBinding{{
			Type:           "wework",
			AgentApp:       "support",
			Token:          "CallbackToken123",
			Secret:         strings.Repeat("S", 43),
			EncodingAESKey: strings.Repeat("A", 43),
			AppID:          "1000002",
			Config:         map[string]string{"corp_id": "ww0123456789abcdef"},
			AccessPolicy: ChannelAccessPolicy{
				AllowDirectMessages: true,
				AllowedUsers:        []string{"wework-user-1"},
			},
		}}
		return config
	}
	if err := ValidateConfig(validWeWork()); err != nil {
		t.Fatalf("executable WeWork configuration rejected: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*ChannelBinding)
	}{
		{"missing callback token", func(b *ChannelBinding) { b.Token = "" }},
		{"missing AES key", func(b *ChannelBinding) { b.EncodingAESKey = "" }},
		{"invalid AES key", func(b *ChannelBinding) { b.EncodingAESKey = "short" }},
		{"missing corp ID", func(b *ChannelBinding) { delete(b.Config, "corp_id") }},
		{"missing delivery secret", func(b *ChannelBinding) { b.Secret = "" }},
		{"missing agent ID", func(b *ChannelBinding) { b.AppID = "" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := validWeWork()
			test.mutate(&config.Channels[0])
			if err := ValidateConfig(config); err == nil {
				t.Fatal("non-executable WeWork configuration was accepted")
			}
		})
	}
}

func TestValidateConfigRejectsMalformedChannelCredentials(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*TenantConfig)
	}{
		{"Telegram bot token without separator", func(c *TenantConfig) { c.Channels[0].Token = strings.Repeat("A", 40) }},
		{"Telegram bot token with control character", func(c *TenantConfig) { c.Channels[0].Token += "\n" }},
		{"Telegram webhook secret below security minimum", func(c *TenantConfig) { c.Channels[0].Secret = strings.Repeat("s", 31) }},
		{"Telegram webhook secret with space", func(c *TenantConfig) { c.Channels[0].Secret = "not a secret token" }},
		{"WeWork callback token with control character", func(c *TenantConfig) {
			c.Channels = validWeWorkChannels()
			c.Channels[0].Token += "\r"
		}},
		{"WeWork corp secret with invalid length", func(c *TenantConfig) {
			c.Channels = validWeWorkChannels()
			c.Channels[0].Secret = "short"
		}},
		{"WeWork corp ID with invalid format", func(c *TenantConfig) {
			c.Channels = validWeWorkChannels()
			c.Channels[0].Config["corp_id"] = "internal-host"
		}},
		{"WeWork agent ID with invalid format", func(c *TenantConfig) {
			c.Channels = validWeWorkChannels()
			c.Channels[0].AppID = "agent-1"
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := validConfig()
			test.mutate(&config)
			if err := ValidateConfig(config); err == nil {
				t.Fatal("malformed channel credential was accepted")
			}
		})
	}
}

func TestTelegramWebhookSecretEnterpriseLengthBoundary(t *testing.T) {
	if IsValidTelegramWebhookSecret(strings.Repeat("s", 31)) {
		t.Fatal("31-character Telegram webhook secret was accepted")
	}
	if !IsValidTelegramWebhookSecret(strings.Repeat("s", 32)) {
		t.Fatal("32-character Telegram webhook secret was rejected")
	}
	if !IsValidTelegramWebhookSecret(strings.Repeat("s", 256)) {
		t.Fatal("256-character Telegram webhook secret was rejected")
	}
	if IsValidTelegramWebhookSecret(strings.Repeat("s", 257)) {
		t.Fatal("257-character Telegram webhook secret was accepted")
	}
}

func validWeWorkChannels() []ChannelBinding {
	return []ChannelBinding{{
		Type:           "wework",
		AgentApp:       "support",
		Token:          "CallbackToken123",
		Secret:         strings.Repeat("S", 43),
		EncodingAESKey: strings.Repeat("A", 43),
		AppID:          "1000002",
		Config:         map[string]string{"corp_id": "ww0123456789abcdef"},
		AccessPolicy: ChannelAccessPolicy{
			AllowDirectMessages: true,
			AllowedUsers:        []string{"wework-user-1"},
		},
	}}
}

func TestValidateConfigAcceptsExecutableConfiguration(t *testing.T) {
	if err := ValidateConfig(validConfig()); err != nil {
		t.Fatal(err)
	}
}

func TestValidateDistributedStorageRejectsProcessLocalState(t *testing.T) {
	if err := ValidateDistributedStorage(StorageConfig{SessionBackend: "inmemory", MemoryBackend: "postgres"}); err == nil {
		t.Fatal("in-memory session backend was accepted for multi-node execution")
	}
	if err := ValidateDistributedStorage(StorageConfig{SessionBackend: "postgres", MemoryBackend: "inmemory"}); err == nil {
		t.Fatal("in-memory memory backend was accepted for multi-node execution")
	}
	if err := ValidateDistributedStorage(StorageConfig{SessionBackend: "redis", MemoryBackend: "postgres"}); err != nil {
		t.Fatalf("distributed storage rejected: %v", err)
	}
}

func TestValidateConfigRequiresBoundedTokenReservation(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*TenantConfig)
	}{
		{"daily budget without request ceiling", func(c *TenantConfig) { c.Budget.MaxTokensPerDay = 20_000 }},
		{"request ceiling without daily budget", func(c *TenantConfig) { c.Budget.MaxTokensPerRequest = 16_384 }},
		{"request ceiling exceeds daily budget", func(c *TenantConfig) {
			c.Budget.MaxTokensPerDay = 1_000
			c.Budget.MaxTokensPerRequest = 1_001
		}},
		{"daily budget exceeds exact Redis Lua integer range", func(c *TenantConfig) {
			c.Budget.MaxTokensPerDay = 1 << 53
			c.Budget.MaxTokensPerRequest = 1_000
			c.Models[0].MaxTokens = 1_000
		}},
		{"reservation below context and call bound", func(c *TenantConfig) {
			c.Budget.MaxTokensPerDay = 20_000
			c.Budget.MaxTokensPerRequest = 16_000
		}},
		{"model completion ceiling exceeds context", func(c *TenantConfig) {
			c.Budget.MaxTokensPerDay = 20_000
			c.Budget.MaxTokensPerRequest = 16_384
			c.Models[0].MaxTokens = 9_000
		}},
		{"unbounded model under hard budget", func(c *TenantConfig) {
			c.Budget.MaxTokensPerDay = 20_000
			c.Budget.MaxTokensPerRequest = 16_384
			c.Models[0].MaxTokens = 0
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := validConfig()
			test.mutate(&config)
			if err := ValidateConfig(config); err == nil {
				t.Fatal("unbounded token budget was accepted")
			}
		})
	}

	config := validConfig()
	config.Budget.MaxTokensPerDay = 20_000
	config.Budget.MaxTokensPerRequest = 16_384
	config.Models[0].MaxTokens = 1_000
	if err := ValidateConfig(config); err != nil {
		t.Fatalf("bounded token reservation rejected: %v", err)
	}
}

func TestValidateConfigBudgetDoesNotTrustMutableFrameworkRegistry(t *testing.T) {
	model.RegisterModelContextWindow("gpt-4", 2_000)
	t.Cleanup(func() { model.RegisterModelContextWindow("gpt-4", 8_192) })

	config := validConfig()
	config.Budget.MaxTokensPerDay = 20_000
	config.Budget.MaxTokensPerRequest = 4_000
	config.Models[0].MaxTokens = 1_000
	if err := ValidateConfig(config); err == nil {
		t.Fatal("mutable framework registry weakened the operator token bound")
	}
}

func TestValidatePinnedAgentModelBudgetRequiresOperatorCatalogBinding(t *testing.T) {
	agent := AgentConfig{Name: "support", Type: "llm", DefaultModel: "gpt-4", MaxLLMCalls: 2}
	modelConfig := ModelConfig{Provider: "openai", ModelName: "gpt-4", MaxTokens: 1_000}
	budget := BudgetConfig{MaxTokensPerDay: 20_000, MaxTokensPerRequest: 16_384}

	if err := ValidatePinnedAgentModelBudget(agent, modelConfig, budget, "", 0); err == nil {
		t.Fatal("hard budget accepted an unpinned model limit")
	}
	if err := ValidatePinnedAgentModelBudget(agent, modelConfig, budget, modelcatalog.Revision, 8_192); err != nil {
		t.Fatalf("operator-pinned model limit rejected: %v", err)
	}
}

func TestValidateProductionModelCatalogRejectsUnknownModel(t *testing.T) {
	agent := AgentConfig{Name: "support", Type: "llm", DefaultModel: "not-reviewed", MaxLLMCalls: 1}
	model := ModelConfig{Provider: "openai", ModelName: "not-reviewed", MaxTokens: 256}
	if err := ValidateProductionModelCatalog(agent, model); err == nil {
		t.Fatal("unknown model accepted by production catalog validation")
	}
	known := ModelConfig{Provider: "openai", ModelName: "gpt-4o-mini", MaxTokens: 256}
	agent.DefaultModel = known.ModelName
	if err := ValidateProductionModelCatalog(agent, known); err != nil {
		t.Fatalf("catalogued model rejected: %v", err)
	}
}

func TestValidateAgentModelRejectsProviderOutputLimit(t *testing.T) {
	agent := AgentConfig{Name: "support", Type: "llm", DefaultModel: "gpt-4o-mini", MaxLLMCalls: 1}
	modelConfig := ModelConfig{Provider: "openai", ModelName: "gpt-4o-mini", MaxTokens: 16_385}
	if err := ValidateAgentModel(agent, modelConfig); err == nil {
		t.Fatal("model completion limit above the provider cap was accepted")
	}
}

func TestValidateConfigRejectsUnboundedOrAmbiguousResourceNames(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*TenantConfig)
	}{
		{"agent control character", func(c *TenantConfig) { c.Agents[0].Name = "support\nadmin" }},
		{"agent whitespace", func(c *TenantConfig) { c.Agents[0].Name = "support app" }},
		{"agent non slug", func(c *TenantConfig) { c.Agents[0].Name = "客服" }},
		{"agent too long", func(c *TenantConfig) { c.Agents[0].Name = strings.Repeat("a", MaxAgentAppNameBytes+1) }},
		{"model control character", func(c *TenantConfig) {
			c.Models[0].ModelName = "gpt-4\t"
			c.Agents[0].DefaultModel = c.Models[0].ModelName
		}},
		{"oversized system prompt", func(c *TenantConfig) { c.Agents[0].SystemPrompt = strings.Repeat("p", maxSystemPromptBytes+1) }},
		{"oversized agent metadata", func(c *TenantConfig) {
			c.Agents[0].Metadata = map[string]string{"key": strings.Repeat("v", maxMetadataValueBytes+1)}
		}},
		{"webhook key control character", func(c *TenantConfig) { c.Channels[0].WebhookKey = "route\x1bkey" }},
		{"derived channel account ID too long", func(c *TenantConfig) {
			c.Channels[0].Config = map[string]string{"account_id": strings.Repeat("a", 129)}
		}},
		{"derived channel account ID control character", func(c *TenantConfig) {
			c.Channels[0].Config = map[string]string{"account_id": "bot\nadmin"}
		}},
		{"confirmation outside whitelist", func(c *TenantConfig) { c.ToolPolicy.RequireConfirmation = []string{"missing"} }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := validConfig()
			test.mutate(&config)
			if err := ValidateConfig(config); err == nil {
				t.Fatal("invalid bounded resource was accepted")
			}
		})
	}
}

func TestValidateConfigBoundsGovernanceRuleMaterial(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*TenantConfig)
	}{
		{
			name: "too many masking rules",
			mutate: func(c *TenantConfig) {
				c.Governance.DataMasking = make([]MaskingRule, maxMaskingRules+1)
				for i := range c.Governance.DataMasking {
					c.Governance.DataMasking[i] = MaskingRule{Type: "email"}
				}
			},
		},
		{
			name: "oversized custom pattern",
			mutate: func(c *TenantConfig) {
				c.Governance.DataMasking = []MaskingRule{{Type: "custom", Pattern: strings.Repeat("a", maxGovernancePatternBytes+1)}}
			},
		},
		{
			name: "too many content patterns",
			mutate: func(c *TenantConfig) {
				patterns := make([]string, maxContentFilterPatterns+1)
				for i := range patterns {
					patterns[i] = "keyword"
				}
				c.Governance.ContentFilters = []ContentFilter{{Name: "content", Type: "keyword", Patterns: patterns, Action: "block"}}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := validConfig()
			test.mutate(&config)
			if err := ValidateConfig(config); err == nil {
				t.Fatal("unbounded governance policy was accepted")
			}
		})
	}
}

func TestTenantIdentityAndDisplayNameContracts(t *testing.T) {
	for _, value := range []string{"tenant-a", strings.Repeat("t", MaxTenantIDBytes)} {
		if err := ValidateTenantID(value); err != nil {
			t.Fatalf("valid tenant ID %q rejected: %v", value, err)
		}
	}
	for _, value := range []string{"tenant a", "tenant\nadmin", "租户", strings.Repeat("t", MaxTenantIDBytes+1)} {
		if err := ValidateTenantID(value); err == nil {
			t.Fatalf("invalid tenant ID %q accepted", value)
		}
	}
	if err := ValidateTenantName("企业租户 A"); err != nil {
		t.Fatalf("human-readable tenant name rejected: %v", err)
	}
	for _, value := range []string{" leading", "trailing ", "bad\nname", "bad\u202ename", strings.Repeat("n", MaxTenantNameBytes+1)} {
		if err := ValidateTenantName(value); err == nil {
			t.Fatalf("invalid tenant name %q accepted", value)
		}
	}
}
