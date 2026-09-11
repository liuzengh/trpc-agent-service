package config

import (
	"strings"
	"testing"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/model"
	agenttool "trpc.group/trpc-go/trpc-agent-go/tool"
)

var testArtifactDrivers = []string{"postgres", "s3", "cos"}

func mustTestModelCatalog(t *testing.T, providers []ModelProviderConfig) *ModelCatalog {
	t.Helper()
	catalog, err := NewModelCatalog(providers)
	if err != nil {
		t.Fatalf("NewModelCatalog() error = %v", err)
	}
	return catalog
}

func TestConfigRejectsUnsafeManagedModelProvider(t *testing.T) {
	base := Config{
		Service: ServiceConfig{
			ListenAddress: ":8080", RequestTimeout: Duration{Duration: time.Second},
			MaxInboundBytes: 1024, AllowedSecretRefs: []string{"env:MODEL_KEY"},
		},
		ModelProviders: []ModelProviderConfig{{
			ID: "primary", BaseURL: "http://models.example/v1", APIKeyRef: "env:MODEL_KEY", Models: []ModelPricingConfig{{Name: "support"}},
		}},
	}
	if err := base.Validate(); err == nil {
		t.Fatal("Validate() error = nil, want non-HTTPS provider rejected")
	}
}

func TestConfigRejectsNegativeCachedPromptPrice(t *testing.T) {
	negative := int64(-1)
	cfg := Config{
		Service: ServiceConfig{
			ListenAddress: ":8080", RequestTimeout: Duration{Duration: time.Second},
			MaxInboundBytes: 1024, AllowedSecretRefs: []string{"env:MODEL_KEY"},
		},
		ModelProviders: []ModelProviderConfig{{
			ID: "primary", BaseURL: "https://models.example/v1", APIKeyRef: "env:MODEL_KEY",
			Models: []ModelPricingConfig{{Name: "support", CachedPromptCostMicrosPerMillionTokens: &negative}},
		}},
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() error = nil, want negative cached prompt price rejected")
	}
}

func TestPlatformPolicyValidatorAcceptsCurrentTenantPolicy(t *testing.T) {
	validator, err := NewPlatformPolicyValidator(
		mustTestModelCatalog(t, []ModelProviderConfig{{
			ID: "primary", BaseURL: "https://models.example/v1", APIKeyRef: "env:MODEL_KEY", Models: []ModelPricingConfig{{Name: "support"}},
		}}),
		[]string{"env:MODEL_KEY", "env:BOT_CONFIG", "env:S3_CONFIG"},
		[]string{"env:BOT_CONFIG"},
		nil,
		nil,
		testArtifactDrivers,
	)
	if err != nil {
		t.Fatalf("NewPlatformPolicyValidator() error = %v", err)
	}
	tenantConfig := TenantConfig{
		TenantID: "acme", AppCode: "support", Status: AgentActive, ConfigVersion: 1,
		Model: ModelConfig{ProviderID: "primary", Name: "support"},
		Storage: StoragePolicy{
			Session: BackendProfileRef{ProfileID: "platform-postgres"}, Memory: BackendProfileRef{ProfileID: "platform-postgres"},
			Knowledge: BackendProfileRef{ProfileID: "platform-pgvector"}, Artifact: BackendProfileRef{ProfileID: "artifact-s3"},
		},
		Channels: []ChannelBinding{{Type: ChannelTelegram, BindingID: "bot-a", CredentialRef: "env:BOT_CONFIG"}},
	}
	if err := validator.Validate(tenantConfig); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	tenantConfig.Storage.Artifact = BackendProfileRef{ProfileID: "invalid/profile"}
	if err := validator.Validate(tenantConfig); err == nil {
		t.Fatal("Validate() error = nil, want invalid backend profile ID rejected")
	}
}

func TestPlatformPolicyValidatorRejectsUnknownModelToolAndCredential(t *testing.T) {
	validator, err := NewPlatformPolicyValidator(
		mustTestModelCatalog(t, []ModelProviderConfig{{
			ID: "primary", BaseURL: "https://models.example/v1", APIKeyRef: "env:MODEL_KEY", Models: []ModelPricingConfig{{Name: "support"}},
		}}),
		[]string{"env:MODEL_KEY", "env:BOT_CONFIG"},
		[]string{"env:BOT_CONFIG"},
		nil,
		[]string{"query_order", "platform.save_artifact"},
		testArtifactDrivers,
	)
	if err != nil {
		t.Fatalf("NewPlatformPolicyValidator() error = %v", err)
	}
	base := TenantConfig{
		TenantID: "acme", AppCode: "support", Status: AgentActive, ConfigVersion: 1,
		Model:    ModelConfig{ProviderID: "primary", Name: "support"},
		Channels: []ChannelBinding{{Type: ChannelTelegram, BindingID: "bot-a", CredentialRef: "env:BOT_CONFIG"}},
	}

	unknownModel := base
	unknownModel.Model.ProviderID = "missing"
	if err := validator.Validate(unknownModel); err == nil {
		t.Fatal("Validate() error = nil, want unknown provider rejected")
	}

	unknownTool := base
	unknownTool.Tools.Allowed = []string{"platform.shell"}
	if err := validator.Validate(unknownTool); err == nil {
		t.Fatal("Validate() error = nil, want unknown tool rejected")
	}

	confirmationOutsideAllowList := base
	confirmationOutsideAllowList.Tools = ToolPolicy{
		Allowed:             []string{"query_order"},
		RequireConfirmation: []string{"platform.save_artifact"},
	}
	if err := validator.Validate(confirmationOutsideAllowList); err == nil {
		t.Fatal("Validate() error = nil, want confirmation tool outside allow-list rejected")
	}

	unmanagedCredential := base
	unmanagedCredential.Channels[0].CredentialRef = "env:OTHER_BOT"
	if err := validator.Validate(unmanagedCredential); err == nil {
		t.Fatal("Validate() error = nil, want unmanaged credential rejected")
	}
}

func TestPlatformPolicyValidatorAcceptsTenantHTTPAndMCPTools(t *testing.T) {
	validator, err := NewPlatformPolicyValidator(
		mustTestModelCatalog(t, nil),
		[]string{"env:TOOL_TOKEN"}, nil, []string{"env:TOOL_TOKEN"}, []string{"duckduckgo_search"}, testArtifactDrivers,
	)
	if err != nil {
		t.Fatal(err)
	}
	cfg := TenantConfig{
		TenantID: "acme", AppCode: "assistant", Status: AgentDisabled, ConfigVersion: 1,
		Tools: ToolPolicy{
			Allowed: []string{"duckduckgo_search", "query_order", "crm_find_customer"},
			HTTP: []HTTPToolConfig{{
				Name: "query_order", Description: "查询订单", URL: "https://tools.example.com/query", CredentialRef: "env:TOOL_TOKEN",
			}},
			MCP: []MCPToolConfig{{
				Name: "crm", Transport: "streamable", URL: "https://mcp.example.com/mcp", CredentialRef: "env:TOOL_TOKEN",
			}},
		},
	}
	if err := validator.Validate(cfg); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	cfg.Tools.MCP[0].Transport = "stdio"
	if err := validator.Validate(cfg); err == nil {
		t.Fatal("Validate() error = nil, want stdio MCP rejected")
	}
}

func TestPlatformPolicyValidatorRejectsToolCredentialOutsideToolCatalogAndNameCollision(t *testing.T) {
	validator, err := NewPlatformPolicyValidator(
		mustTestModelCatalog(t, nil),
		[]string{"env:MODEL_KEY", "env:TOOL_TOKEN"}, nil, []string{"env:TOOL_TOKEN"}, []string{"duckduckgo_search"}, testArtifactDrivers,
	)
	if err != nil {
		t.Fatal(err)
	}
	base := TenantConfig{
		TenantID: "acme", AppCode: "assistant", Status: AgentDisabled, ConfigVersion: 1,
		Tools: ToolPolicy{
			Allowed: []string{"query_order"},
			HTTP:    []HTTPToolConfig{{Name: "query_order", URL: "https://tools.example.com/query", CredentialRef: "env:MODEL_KEY"}},
		},
	}
	if err := validator.Validate(base); err == nil {
		t.Fatal("Validate() error = nil, want model credential rejected for custom tool")
	}

	base.Tools = ToolPolicy{
		Allowed: []string{"duckduckgo_search"},
		HTTP:    []HTTPToolConfig{{Name: "duckduckgo_search", URL: "https://tools.example.com/search"}},
	}
	if err := validator.Validate(base); err == nil {
		t.Fatal("Validate() error = nil, want built-in/custom tool name collision rejected")
	}
}

func TestPlatformPolicyValidatorRequiresManagedCredentialWhenConfigured(t *testing.T) {
	validator, err := NewPlatformPolicyValidator(mustTestModelCatalog(t, nil), []string{"env:BOT_CONFIG"}, []string{"env:BOT_CONFIG"}, nil, nil, testArtifactDrivers)
	if err != nil {
		t.Fatalf("NewPlatformPolicyValidator() error = %v", err)
	}
	tenantConfig := TenantConfig{
		TenantID: "acme", AppCode: "support", Status: AgentDisabled, ConfigVersion: 1,
		Channels: []ChannelBinding{{Type: ChannelTelegram, BindingID: "bot-a"}},
	}
	if err := validator.Validate(tenantConfig); err == nil {
		t.Fatal("Validate() error = nil, want missing managed credential rejected")
	}
}

func TestPlatformPolicyValidatorAcceptsHunyuanAndFailoverAndGeneration(t *testing.T) {
	temp := 0.7
	topP := 0.9
	maxTokens := 2048
	effort := "high"
	thinking := true
	thinkingTokens := 1024

	validator, err := NewPlatformPolicyValidator(
		mustTestModelCatalog(t, []ModelProviderConfig{
			{
				ID: "openai-main", Type: ModelProviderOpenAI, BaseURL: "https://api.openai.com/v1", APIKeyRef: "env:OPENAI_KEY",
				Models: []ModelPricingConfig{{
					Name: "reasoning-model",
					Capabilities: &ModelCapabilities{
						ReasoningEfforts: []string{"low", "medium", "high"},
						ThinkingToggle:   true,
						ThinkingBudget:   true,
					},
				}, {Name: "gpt-4o-mini"}},
			},
			{
				ID: "hunyuan-main", Type: ModelProviderHunyuan, SecretIDRef: "env:HUNYUAN_SID", SecretKeyRef: "env:HUNYUAN_SKEY",
				Models: []ModelPricingConfig{{Name: "hunyuan-pro"}},
			},
		}),
		[]string{"env:OPENAI_KEY", "env:HUNYUAN_SID", "env:HUNYUAN_SKEY"},
		nil,
		nil,
		nil,
		testArtifactDrivers,
	)
	if err != nil {
		t.Fatalf("NewPlatformPolicyValidator() error = %v", err)
	}

	valid := TenantConfig{
		TenantID: "acme", AppCode: "bot", Status: AgentActive, ConfigVersion: 1,
		Model: ModelConfig{
			ProviderID: "openai-main",
			Name:       "reasoning-model",
			FailoverCandidates: []ModelCandidate{
				{ProviderID: "hunyuan-main", Name: "hunyuan-pro"},
			},
			Generation: &model.GenerationConfig{
				Temperature:     &temp,
				TopP:            &topP,
				MaxTokens:       &maxTokens,
				ReasoningEffort: &effort,
				ThinkingEnabled: &thinking,
				ThinkingTokens:  &thinkingTokens,
			},
		},
	}
	if err := validator.Validate(valid); err != nil {
		t.Fatalf("Validate(valid) error = %v", err)
	}

	// Invalid candidate model
	badCandidate := valid
	badCandidate.Model.FailoverCandidates = []ModelCandidate{{ProviderID: "openai-main", Name: "unknown-model"}}
	if err := validator.Validate(badCandidate); err == nil {
		t.Fatal("Validate() error = nil, want unknown candidate model rejected")
	}

	// Invalid temperature
	badTemp := -0.5
	badGen := valid
	badGen.Model.Generation = &model.GenerationConfig{Temperature: &badTemp}
	if err := validator.Validate(badGen); err == nil {
		t.Fatal("Validate() error = nil, want invalid temperature rejected")
	}

	// Invalid reasoning effort
	badEffort := "invalid-effort"
	badEffortCfg := valid
	badEffortCfg.Model.Generation = &model.GenerationConfig{ReasoningEffort: &badEffort}
	if err := validator.Validate(badEffortCfg); err == nil {
		t.Fatal("Validate() error = nil, want invalid reasoning_effort rejected")
	}
}

func TestPlatformPolicyValidatorAcceptsFrameworkHuggingFaceProvider(t *testing.T) {
	modelName := "meta-llama/Llama-3.1-8B-Instruct"
	catalog := mustTestModelCatalog(t, []ModelProviderConfig{{
		ID: "hf-main", Type: ModelProviderHuggingFace, APIKeyRef: "env:HF_KEY",
		Models: []ModelPricingConfig{{Name: modelName}},
	}})
	validator, err := NewPlatformPolicyValidator(catalog, []string{"env:HF_KEY"}, nil, nil, nil, testArtifactDrivers)
	if err != nil {
		t.Fatalf("NewPlatformPolicyValidator() error = %v", err)
	}
	if err := validator.Validate(TenantConfig{
		TenantID: "acme", AppCode: "hf", Status: AgentActive, ConfigVersion: 1,
		Model: ModelConfig{ProviderID: "hf-main", Name: modelName},
	}); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestPlatformPolicyValidatorRejectsEmptyOpaqueModelID(t *testing.T) {
	_, err := NewPlatformPolicyValidator(
		mustTestModelCatalog(t, []ModelProviderConfig{{
			ID: "hf-main", Type: ModelProviderHuggingFace, APIKeyRef: "env:HF_KEY",
			Models: []ModelPricingConfig{{Name: "  "}},
		}}),
		[]string{"env:HF_KEY"}, nil, nil, nil, testArtifactDrivers,
	)
	if err == nil {
		t.Fatal("NewPlatformPolicyValidator() error = nil, want empty model id rejected")
	}
}

func TestPlatformPolicyValidatorRejectsTrimEquivalentDuplicateModelIDs(t *testing.T) {
	_, err := NewPlatformPolicyValidator(
		mustTestModelCatalog(t, []ModelProviderConfig{{
			ID: "hf-main", Type: ModelProviderHuggingFace, APIKeyRef: "env:HF_KEY",
			Models: []ModelPricingConfig{{Name: "org/model"}, {Name: " org/model "}},
		}}),
		[]string{"env:HF_KEY"}, nil, nil, nil, testArtifactDrivers,
	)
	if err == nil {
		t.Fatal("NewPlatformPolicyValidator() error = nil, want duplicate normalized model ids rejected")
	}
}

func TestPlatformPolicyValidatorRejectsReasoningControlsNotSupportedBySelectedModel(t *testing.T) {
	effort := "high"
	thinking := true
	budget := 1024
	validator, err := NewPlatformPolicyValidator(
		mustTestModelCatalog(t, []ModelProviderConfig{{
			ID: "primary", Type: ModelProviderOpenAI, BaseURL: "https://models.example/v1", APIKeyRef: "env:MODEL_KEY",
			Models: []ModelPricingConfig{{Name: "plain-model"}},
		}}),
		[]string{"env:MODEL_KEY"}, nil, nil, nil, testArtifactDrivers,
	)
	if err != nil {
		t.Fatalf("NewPlatformPolicyValidator() error = %v", err)
	}
	for name, generation := range map[string]*model.GenerationConfig{
		"effort": {ReasoningEffort: &effort},
		"toggle": {ThinkingEnabled: &thinking},
		"budget": {ThinkingTokens: &budget},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := TenantConfig{
				TenantID: "acme", AppCode: "bot", Status: AgentActive, ConfigVersion: 1,
				Model: ModelConfig{ProviderID: "primary", Name: "plain-model", Generation: generation},
			}
			if err := validator.Validate(cfg); err == nil {
				t.Fatal("Validate() error = nil, want unsupported reasoning control rejected")
			}
		})
	}
}

func TestPlatformPolicyValidatorUsesManagedDynamicModels(t *testing.T) {
	catalog := mustTestModelCatalog(t, []ModelProviderConfig{
		{ID: "provider-empty", BaseURL: "https://api.example.com/v1", APIKeyRef: "env:KEY"},
		{ID: "provider-static", BaseURL: "https://api.example.com/v1", APIKeyRef: "env:KEY", Models: []ModelPricingConfig{{Name: "static-1"}}},
	})
	validator, err := NewPlatformPolicyValidator(
		catalog,
		[]string{"env:KEY"},
		nil,
		nil,
		nil,
		testArtifactDrivers,
	)
	if err != nil {
		t.Fatalf("NewPlatformPolicyValidator error = %v", err)
	}

	dynamicCfg := TenantConfig{
		TenantID: "tenant-1", AppCode: "bot-1", Status: AgentActive, ConfigVersion: 1,
		Model: ModelConfig{ProviderID: "provider-empty", Name: "discovered-0"},
	}
	if err := validator.Validate(dynamicCfg); err == nil {
		t.Fatal("provider without a discovered catalog must reject arbitrary model names")
	}
	catalog.ReplaceDiscoveredModels("provider-empty", []string{"discovered-0"})
	if err := validator.Validate(dynamicCfg); err != nil {
		t.Fatalf("discovered model should be accepted, got %v", err)
	}
	catalog.RemoveDiscoveredModel("provider-empty", "discovered-0")
	if err := validator.Validate(dynamicCfg); err == nil {
		t.Fatal("removed dynamic model must no longer be accepted")
	}

	staticCfg := TenantConfig{
		TenantID: "tenant-1", AppCode: "bot-2", Status: AgentActive, ConfigVersion: 1,
		Model: ModelConfig{ProviderID: "provider-static", Name: "discovered-1"},
	}
	if err := validator.Validate(staticCfg); err == nil {
		t.Fatal("expected rejection of unregistered model on static provider, got nil")
	}

	catalog.ReplaceDiscoveredModels("provider-static", []string{"discovered-1"})
	if err := validator.Validate(staticCfg); err != nil {
		t.Fatalf("after discovery, model should be accepted, got %v", err)
	}
}

func newSupportPolicyValidator(t *testing.T) *PlatformPolicyValidator {
	t.Helper()
	catalog := mustTestModelCatalog(t, []ModelProviderConfig{{
		ID: "support-models", Type: ModelProviderOpenAI, BaseURL: "https://models.example.test/v1", APIKeyRef: "env:MODEL_KEY",
		Models: []ModelPricingConfig{{
			Name: "support-primary",
			Capabilities: &ModelCapabilities{
				ReasoningEfforts: []string{"low", "high"}, ThinkingToggle: true, ThinkingBudget: true,
			},
		}, {Name: "support-backup"}},
	}})
	validator, err := NewPlatformPolicyValidator(
		catalog,
		[]string{"env:MODEL_KEY", "env:CHANNEL_KEY", "env:TOOL_KEY"},
		[]string{"env:CHANNEL_KEY"},
		[]string{"env:TOOL_KEY"},
		[]string{"search_knowledge"},
		testArtifactDrivers,
	)
	if err != nil {
		t.Fatalf("NewPlatformPolicyValidator() error = %v", err)
	}
	return validator
}

func validSupportPolicyConfig() TenantConfig {
	return TenantConfig{
		TenantID: "support", AppCode: "assistant", Status: AgentActive, ConfigVersion: 1,
		Model:    ModelConfig{ProviderID: "support-models", Name: "support-primary"},
		Channels: []ChannelBinding{{Type: ChannelTelegram, BindingID: "support-main", CredentialRef: "env:CHANNEL_KEY"}},
		Tools:    ToolPolicy{Allowed: []string{"search_knowledge"}},
	}
}

func TestNewPlatformPolicyValidatorRejectsInvalidCatalogInputs(t *testing.T) {
	t.Parallel()
	if _, err := NewPlatformPolicyValidator(nil, nil, nil, nil, nil, nil); err == nil {
		t.Fatal("nil model catalog error = nil")
	}
	emptyCatalog := mustTestModelCatalog(t, nil)
	if _, err := NewPlatformPolicyValidator(emptyCatalog, nil, nil, nil, []string{" "}, nil); err == nil {
		t.Fatal("empty platform tool name error = nil")
	}
	if _, err := NewPlatformPolicyValidator(emptyCatalog, []string{"not-env"}, nil, nil, nil, nil); err == nil {
		t.Fatal("invalid allowed secret error = nil")
	}
	if _, err := NewPlatformPolicyValidator(emptyCatalog, []string{"env:KEY"}, []string{"env:MISSING"}, nil, nil, nil); err == nil {
		t.Fatal("unmanaged channel secret error = nil")
	}
	if _, err := NewPlatformPolicyValidator(emptyCatalog, []string{"env:KEY"}, nil, []string{"env:MISSING"}, nil, nil); err == nil {
		t.Fatal("unmanaged tool secret error = nil")
	}
}

func TestPlatformPolicyValidatorRejectsInvalidApplicationPolicy(t *testing.T) {
	t.Parallel()
	validator := newSupportPolicyValidator(t)
	var nilValidator *PlatformPolicyValidator
	if err := nilValidator.Validate(validSupportPolicyConfig()); err == nil {
		t.Fatal("nil validator error = nil")
	}

	tests := []struct {
		name string
		edit func(*TenantConfig)
		want string
	}{
		{"active model required", func(c *TenantConfig) { c.Model = ModelConfig{} }, "model is required"},
		{"unknown model", func(c *TenantConfig) { c.Model.Name = "missing" }, "not managed"},
		{"unknown failover provider", func(c *TenantConfig) {
			c.Model.FailoverCandidates = []ModelCandidate{{ProviderID: "missing", Name: "support-backup"}}
		}, "provider_id"},
		{"unknown failover model", func(c *TenantConfig) {
			c.Model.FailoverCandidates = []ModelCandidate{{ProviderID: "support-models", Name: "missing"}}
		}, "not managed"},
		{"missing channel credential", func(c *TenantConfig) { c.Channels[0].CredentialRef = "" }, "credential_ref is required"},
		{"unmanaged channel credential", func(c *TenantConfig) { c.Channels[0].CredentialRef = "env:OTHER" }, "not managed"},
		{"unknown allowed tool", func(c *TenantConfig) { c.Tools.Allowed = []string{"missing_tool"} }, "not registered"},
		{"invalid allowed tool", func(c *TenantConfig) { c.Tools.Allowed = []string{"bad tool"} }, "invalid tool name"},
		{"duplicate allowed tool", func(c *TenantConfig) { c.Tools.Allowed = []string{"search_knowledge", "search_knowledge"} }, "duplicate tool"},
		{"invalid confirmation tool", func(c *TenantConfig) {
			c.Tools.RequireConfirmation = []string{"bad tool"}
		}, "invalid tool name"},
		{"confirmation outside allowed", func(c *TenantConfig) {
			c.Tools.RequireConfirmation = []string{"missing_tool"}
		}, "unallowed tool"},
		{"roles outside allowed", func(c *TenantConfig) {
			c.Tools.AllowedRoles = map[string][]string{"missing_tool": {"member"}}
		}, "allowed_roles references unallowed"},
		{"empty roles", func(c *TenantConfig) {
			c.Tools.AllowedRoles = map[string][]string{"search_knowledge": {}}
		}, "must not be empty"},
		{"invalid role", func(c *TenantConfig) {
			c.Tools.AllowedRoles = map[string][]string{"search_knowledge": {"owner"}}
		}, "unsupported role"},
		{"negative governance", func(c *TenantConfig) { c.Governance.MaxToolCalls = -1 }, "must not be negative"},
		{"token reservation required", func(c *TenantConfig) { c.Governance.TokenBudgetPerHour = 1000 }, "token_reservation must be positive"},
		{"token reservation exceeds budget", func(c *TenantConfig) {
			c.Governance.TokenBudgetPerHour = 1000
			c.Governance.TokenReservation = 1001
		}, "must not exceed"},
		{"negative retention", func(c *TenantConfig) { c.Audit.RetentionDays = -1 }, "retention_days"},
		{"invalid profile id", func(c *TenantConfig) { c.Storage.Session.ProfileID = "bad/profile" }, "profile_id"},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			configuration := validSupportPolicyConfig()
			configuration.Channels = append([]ChannelBinding(nil), configuration.Channels...)
			tt.edit(&configuration)
			if err := validator.Validate(configuration); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Validate() error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestPlatformPolicyValidatorGenerationBoundaries(t *testing.T) {
	t.Parallel()
	validator := newSupportPolicyValidator(t)
	temperatureLow := -0.1
	temperatureHigh := 2.1
	topPLow := -0.1
	topPHigh := 1.1
	zero := 0
	badEffort := "medium"

	tests := []struct {
		name       string
		generation *model.GenerationConfig
		want       string
	}{
		{"temperature low", &model.GenerationConfig{Temperature: &temperatureLow}, "temperature"},
		{"temperature high", &model.GenerationConfig{Temperature: &temperatureHigh}, "temperature"},
		{"top p low", &model.GenerationConfig{TopP: &topPLow}, "top_p"},
		{"top p high", &model.GenerationConfig{TopP: &topPHigh}, "top_p"},
		{"max tokens", &model.GenerationConfig{MaxTokens: &zero}, "max_tokens"},
		{"thinking tokens", &model.GenerationConfig{ThinkingTokens: &zero}, "thinking_tokens"},
		{"reasoning effort", &model.GenerationConfig{ReasoningEffort: &badEffort}, "reasoning_effort"},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			configuration := validSupportPolicyConfig()
			configuration.Model.Generation = tt.generation
			if err := validator.Validate(configuration); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Validate() error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestPlatformPolicyValidatorCustomToolValidationMatrix(t *testing.T) {
	t.Parallel()
	validator := newSupportPolicyValidator(t)
	base := validSupportPolicyConfig()
	base.Status = AgentDisabled
	base.Model = ModelConfig{}
	base.Channels = nil

	tests := []struct {
		name string
		tool ToolPolicy
		want string
	}{
		{
			name: "http invalid name",
			tool: ToolPolicy{Allowed: []string{"bad tool"}, HTTP: []HTTPToolConfig{{Name: "bad tool", URL: "https://tools.example.test/query"}}},
			want: "invalid tool name",
		},
		{
			name: "http duplicate definition",
			tool: ToolPolicy{Allowed: []string{"query_order"}, HTTP: []HTTPToolConfig{{Name: "query_order", URL: "https://tools.example.test/a"}, {Name: "query_order", URL: "https://tools.example.test/b"}}},
			want: "duplicated",
		},
		{
			name: "http insecure url",
			tool: ToolPolicy{Allowed: []string{"query_order"}, HTTP: []HTTPToolConfig{{Name: "query_order", URL: "http://tools.example.test/query"}}},
			want: "HTTPS URL"},
		{
			name: "http url credentials",
			tool: ToolPolicy{Allowed: []string{"query_order"}, HTTP: []HTTPToolConfig{{Name: "query_order", URL: "https://user:pass@tools.example.test/query"}}},
			want: "without credentials"},
		{
			name: "http url fragment",
			tool: ToolPolicy{Allowed: []string{"query_order"}, HTTP: []HTTPToolConfig{{Name: "query_order", URL: "https://tools.example.test/query#fragment"}}},
			want: "without credentials"},
		{
			name: "http credential scheme",
			tool: ToolPolicy{Allowed: []string{"query_order"}, HTTP: []HTTPToolConfig{{Name: "query_order", URL: "https://tools.example.test/query", CredentialRef: "plain"}}},
			want: "env: reference"},
		{
			name: "http unmanaged credential",
			tool: ToolPolicy{Allowed: []string{"query_order"}, HTTP: []HTTPToolConfig{{Name: "query_order", URL: "https://tools.example.test/query", CredentialRef: "env:OTHER"}}},
			want: "not managed"},
		{
			name: "http schema",
			tool: ToolPolicy{Allowed: []string{"query_order"}, HTTP: []HTTPToolConfig{{Name: "query_order", URL: "https://tools.example.test/query", InputSchema: &agenttool.Schema{Type: "string"}}}},
			want: "input_schema"},
		{
			name: "mcp invalid name",
			tool: ToolPolicy{Allowed: []string{"bad_tool_search"}, MCP: []MCPToolConfig{{Name: "bad tool", URL: "https://mcp.example.test/mcp"}}},
			want: "invalid"},
		{
			name: "mcp duplicate definition",
			tool: ToolPolicy{Allowed: []string{"search_lookup"}, MCP: []MCPToolConfig{{Name: "search", URL: "https://mcp.example.test/a"}, {Name: "search", URL: "https://mcp.example.test/b"}}},
			want: "duplicated"},
		{
			name: "mcp transport",
			tool: ToolPolicy{Allowed: []string{"search_lookup"}, MCP: []MCPToolConfig{{Name: "search", Transport: "stdio", URL: "https://mcp.example.test/mcp"}}},
			want: "transport"},
		{
			name: "mcp url",
			tool: ToolPolicy{Allowed: []string{"search_lookup"}, MCP: []MCPToolConfig{{Name: "search", URL: "http://mcp.example.test/mcp"}}},
			want: "HTTPS URL"},
		{
			name: "mcp credential",
			tool: ToolPolicy{Allowed: []string{"search_lookup"}, MCP: []MCPToolConfig{{Name: "search", URL: "https://mcp.example.test/mcp", CredentialRef: "env:OTHER"}}},
			want: "not managed"},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			configuration := base
			configuration.Tools = tt.tool
			if err := validator.Validate(configuration); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Validate() error = %v, want %q", err, tt.want)
			}
		})
	}

	validHTTP := base
	validHTTP.Tools = ToolPolicy{Allowed: []string{"query_order"}, HTTP: []HTTPToolConfig{{Name: "query_order", URL: "https://tools.example.test/query", CredentialRef: "env:TOOL_KEY", InputSchema: &agenttool.Schema{Type: "object"}}}}
	if err := validator.Validate(validHTTP); err != nil {
		t.Fatalf("Validate(valid HTTP tool) error = %v", err)
	}
	validMCP := base
	validMCP.Tools = ToolPolicy{Allowed: []string{"search_lookup"}, MCP: []MCPToolConfig{{Name: "search", URL: "https://mcp.example.test/mcp", CredentialRef: "env:TOOL_KEY"}}}
	if err := validator.Validate(validMCP); err != nil {
		t.Fatalf("Validate(valid MCP tool) error = %v", err)
	}
}

func TestConfigModelProviderValidationMatrix(t *testing.T) {
	t.Parallel()
	base := validServiceConfigForTest()
	base.Service.AllowedSecretRefs = []string{"env:MODEL_KEY", "env:SECRET_ID", "env:SECRET_KEY"}

	tests := []struct {
		name     string
		provider ModelProviderConfig
		want     string
	}{
		{"missing id", ModelProviderConfig{BaseURL: "https://models.example.test/v1", APIKeyRef: "env:MODEL_KEY"}, ".id"},
		{"id separator", ModelProviderConfig{ID: "bad/id", BaseURL: "https://models.example.test/v1", APIKeyRef: "env:MODEL_KEY"}, "reserved separator"},
		{"unsupported type", ModelProviderConfig{ID: "support", Type: "custom"}, "unsupported"},
		{"openai missing url", ModelProviderConfig{ID: "support", APIKeyRef: "env:MODEL_KEY"}, "absolute URL"},
		{"insecure remote url", ModelProviderConfig{ID: "support", BaseURL: "http://models.example.test/v1", APIKeyRef: "env:MODEL_KEY"}, "HTTPS"},
		{"url query", ModelProviderConfig{ID: "support", BaseURL: "https://models.example.test/v1?debug=1", APIKeyRef: "env:MODEL_KEY"}, "must not include"},
		{"unmanaged key", ModelProviderConfig{ID: "support", BaseURL: "https://models.example.test/v1", APIKeyRef: "env:OTHER"}, "api_key_ref"},
		{"hunyuan secret id", ModelProviderConfig{ID: "support", Type: ModelProviderHunyuan, SecretIDRef: "env:OTHER", SecretKeyRef: "env:SECRET_KEY"}, "secret_id_ref"},
		{"hunyuan secret key", ModelProviderConfig{ID: "support", Type: ModelProviderHunyuan, SecretIDRef: "env:SECRET_ID", SecretKeyRef: "env:OTHER"}, "secret_key_ref"},
		{"empty model", ModelProviderConfig{ID: "support", BaseURL: "https://models.example.test/v1", APIKeyRef: "env:MODEL_KEY", Models: []ModelPricingConfig{{Name: " "}}}, ".name is required"},
		{"duplicate model", ModelProviderConfig{ID: "support", BaseURL: "https://models.example.test/v1", APIKeyRef: "env:MODEL_KEY", Models: []ModelPricingConfig{{Name: "model-a"}, {Name: " model-a "}}}, "duplicated"},
		{"negative prompt price", ModelProviderConfig{ID: "support", BaseURL: "https://models.example.test/v1", APIKeyRef: "env:MODEL_KEY", Models: []ModelPricingConfig{{Name: "model-a", PromptCostMicrosPerMillionTokens: -1}}}, "cost rates"},
		{"invalid reasoning capability", ModelProviderConfig{ID: "support", BaseURL: "https://models.example.test/v1", APIKeyRef: "env:MODEL_KEY", Models: []ModelPricingConfig{{Name: "model-a", Capabilities: &ModelCapabilities{ReasoningEfforts: []string{"extreme"}}}}}, "unsupported value"},
		{"duplicate reasoning capability", ModelProviderConfig{ID: "support", BaseURL: "https://models.example.test/v1", APIKeyRef: "env:MODEL_KEY", Models: []ModelPricingConfig{{Name: "model-a", Capabilities: &ModelCapabilities{ReasoningEfforts: []string{"HIGH", " high "}}}}}, "duplicate value"},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			configuration := base
			configuration.ModelProviders = []ModelProviderConfig{tt.provider}
			if err := configuration.Validate(); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Validate() error = %v, want %q", err, tt.want)
			}
		})
	}

	loopback := base
	loopback.ModelProviders = []ModelProviderConfig{{ID: "support", BaseURL: "http://127.0.0.1:9999/v1", APIKeyRef: "env:MODEL_KEY"}}
	if err := loopback.Validate(); err != nil {
		t.Fatalf("loopback HTTP provider should be valid for testing: %v", err)
	}
}
