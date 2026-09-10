package config

import (
	"testing"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/model"
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
