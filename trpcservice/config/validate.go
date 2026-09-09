package config

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

var (
	toolNamePattern = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)
	sessionBackends = backendSet(
		tenant.BackendInMemory,
		tenant.BackendRedis,
		tenant.BackendMySQL,
		tenant.BackendPostgres,
		tenant.BackendSQLite,
		tenant.BackendMongoDB,
	)
	memoryBackends = backendSet(
		tenant.BackendInMemory,
		tenant.BackendRedis,
		tenant.BackendMySQL,
		tenant.BackendPostgres,
		tenant.BackendSQLite,
		tenant.BackendExternal,
	)
	artifactBackends = backendSet(
		tenant.BackendInMemory,
		tenant.BackendPostgres,
		tenant.BackendLocal,
		tenant.BackendS3,
		tenant.BackendCOS,
	)
	knowledgeBackends = backendSet(
		tenant.BackendInMemory,
		tenant.BackendPostgres,
		tenant.BackendQdrant,
		tenant.BackendMilvus,
		tenant.BackendElasticsearch,
	)
	auditBackends = backendSet(
		tenant.BackendInMemory,
		tenant.BackendMySQL,
		tenant.BackendPostgres,
		tenant.BackendSQLite,
		tenant.BackendExternal,
	)
)

// Validate checks all configuration invariants without resolving secrets.
func (file *File) Validate() error {
	if file == nil {
		return errors.New("config: nil file")
	}
	if file.SchemaVersion != CurrentSchemaVersion {
		return fmt.Errorf(
			"config: schema_version must be %d", CurrentSchemaVersion,
		)
	}
	if len(file.Tenants) == 0 {
		return errors.New("config: at least one tenant is required")
	}

	tenantIDs := make(map[string]struct{}, len(file.Tenants))
	for tenantIndex := range file.Tenants {
		bindingIDs := make(map[string]struct{})
		current := &file.Tenants[tenantIndex]
		path := fmt.Sprintf("tenants[%d]", tenantIndex)
		if err := validateID(path+".tenant_id", current.ID); err != nil {
			return err
		}
		if _, exists := tenantIDs[current.ID]; exists {
			return fmt.Errorf("config: duplicate tenant_id %q", current.ID)
		}
		tenantIDs[current.ID] = struct{}{}
		if strings.TrimSpace(current.Name) == "" {
			return fmt.Errorf("config: %s.name is required", path)
		}
		if current.ConfigVersion == 0 {
			return fmt.Errorf("config: %s.config_version must be positive", path)
		}
		if err := validateAudit(path+".audit", current.Audit); err != nil {
			return err
		}
		if current.Runtime.MaxConcurrentRuns < 0 || current.Runtime.MaxConcurrentRuns > 256 {
			return fmt.Errorf("config: %s.runtime.max_concurrent_runs must be between 1 and 256 when configured", path)
		}
		if len(current.Apps) == 0 {
			return fmt.Errorf("config: %s.apps must not be empty", path)
		}

		appIDs := make(map[string]struct{}, len(current.Apps))
		for appIndex := range current.Apps {
			app := &current.Apps[appIndex]
			appPath := fmt.Sprintf("%s.apps[%d]", path, appIndex)
			if err := validateID(appPath+".app_id", app.ID); err != nil {
				return err
			}
			if _, exists := appIDs[app.ID]; exists {
				return fmt.Errorf(
					"config: duplicate app_id %q in tenant %q", app.ID, current.ID,
				)
			}
			appIDs[app.ID] = struct{}{}
			if err := validateApp(appPath, app, bindingIDs); err != nil {
				return err
			}
		}
	}
	return nil
}

// ValidateProduction applies controls that cannot be relaxed for a local
// config file: production credentials must come from the configured external
// secret manager, never process env or arbitrary mounted paths.
func (file *File) ValidateProduction() error {
	if err := file.Validate(); err != nil {
		return err
	}
	for ti := range file.Tenants {
		for ai := range file.Tenants[ti].Apps {
			configuredTenant := file.Tenants[ti]
			app := file.Tenants[ti].Apps[ai]
			check := func(path string, ref tenant.SecretRef) error {
				if ref.IsZero() {
					return nil
				}
				if ref.Provider == tenant.SecretProviderEnv || ref.Provider == tenant.SecretProviderFile {
					return fmt.Errorf("config: %s must use vault or kms in production", path)
				}
				expectedPrefix := configuredTenant.ID + "/" + app.ID + "/"
				if !strings.HasPrefix(ref.Key, expectedPrefix) {
					return fmt.Errorf("config: %s must use the tenant/app secret namespace", path)
				}
				remainder := strings.TrimPrefix(ref.Key, expectedPrefix)
				if remainder == "" || strings.Contains(remainder, "\\") || strings.Contains(remainder, "//") {
					return fmt.Errorf("config: %s has an invalid secret namespace", path)
				}
				for _, segment := range strings.Split(remainder, "/") {
					if segment == "" || segment == "." || segment == ".." {
						return fmt.Errorf("config: %s has an invalid secret namespace", path)
					}
				}
				return nil
			}
			base := fmt.Sprintf("tenants[%d].apps[%d]", ti, ai)
			refs := []struct {
				path string
				ref  tenant.SecretRef
			}{
				{base + ".model.api_key", app.Model.APIKey},
				{base + ".knowledge.embedding.api_key", app.Knowledge.Embedding.APIKey},
				{base + ".storage.session.credential", app.Storage.Session.Credential},
				{base + ".storage.memory.credential", app.Storage.Memory.Credential},
				{base + ".storage.summary.credential", app.Storage.Summary.Credential},
				{base + ".storage.artifact.credential", app.Storage.Artifact.Credential},
				{base + ".storage.knowledge.credential", app.Storage.Knowledge.Credential},
				{base + ".storage.audit.credential", app.Storage.Audit.Credential},
			}
			for _, item := range refs {
				if err := check(item.path, item.ref); err != nil {
					return err
				}
			}
			storageRoutes := []struct {
				path  string
				route tenant.BackendConfig
			}{
				{base + ".storage.session", app.Storage.Session},
				{base + ".storage.memory", app.Storage.Memory},
				{base + ".storage.summary", app.Storage.Summary},
				{base + ".storage.artifact", app.Storage.Artifact},
				{base + ".storage.knowledge", app.Storage.Knowledge},
				{base + ".storage.audit", app.Storage.Audit},
			}
			for _, item := range storageRoutes {
				if item.route.MigrationTarget != nil {
					if err := check(item.path+".migration_target.credential", item.route.MigrationTarget.Credential); err != nil {
						return err
					}
				}
			}
			for i, binding := range app.Channels {
				for name, ref := range map[string]tenant.SecretRef{"token": binding.Token, "secret": binding.Secret, "encryption_key": binding.EncryptionKey} {
					if err := check(fmt.Sprintf("%s.channels[%d].%s", base, i, name), ref); err != nil {
						return err
					}
				}
			}
			for i, mcp := range app.MCPServers {
				if err := check(fmt.Sprintf("%s.mcp_servers[%d].credential", base, i), mcp.Credential); err != nil {
					return err
				}
			}
			for i, business := range app.BusinessTools {
				if err := check(fmt.Sprintf("%s.business_tools[%d].credential", base, i), business.Credential); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func validateApp(
	path string,
	app *tenant.AgentApp,
	bindingIDs map[string]struct{},
) error {
	if strings.TrimSpace(app.Name) == "" {
		return fmt.Errorf("config: %s.name is required", path)
	}
	if strings.TrimSpace(app.Config.Instruction) == "" {
		return fmt.Errorf("config: %s.config.instruction is required", path)
	}
	if err := validateWorkflow(path+".workflow", app.Workflow); err != nil {
		return err
	}
	if err := validateModel(path+".model", app.Model); err != nil {
		return err
	}
	if err := validateTools(path+".tools", app.Tools); err != nil {
		return err
	}
	if app.Tools.MonthlyCostBudgetCents > 0 {
		if strings.TrimSpace(app.Model.Pricing.Version) == "" {
			return fmt.Errorf("config: %s.model.pricing.version is required when monthly cost budget is enabled", path)
		}
		if app.Model.Pricing.InputMicrosPerMillion <= 0 || app.Model.Pricing.OutputMicrosPerMillion <= 0 {
			return fmt.Errorf("config: %s.model.pricing input and output rates must be positive when monthly cost budget is enabled", path)
		}
		if app.Model.MaxTokens <= 0 {
			return fmt.Errorf("config: %s.model.max_tokens must be positive when monthly cost budget is enabled", path)
		}
	}
	if err := validateToolIntegrations(path, app); err != nil {
		return err
	}
	if len(app.Channels) == 0 {
		return fmt.Errorf("config: %s.channels must not be empty", path)
	}
	for index := range app.Channels {
		channelPath := fmt.Sprintf("%s.channels[%d]", path, index)
		binding := app.Channels[index]
		if err := validateChannel(channelPath, binding); err != nil {
			return err
		}
		if _, exists := bindingIDs[binding.ID]; exists {
			return fmt.Errorf("config: duplicate binding_id %q", binding.ID)
		}
		bindingIDs[binding.ID] = struct{}{}
	}
	if err := validateBackend(
		path+".storage.session", app.Storage.Session, sessionBackends,
	); err != nil {
		return err
	}
	if err := validateBackend(
		path+".storage.memory", app.Storage.Memory, memoryBackends,
	); err != nil {
		return err
	}
	if err := validateBackend(
		path+".storage.summary", app.Storage.Summary, sessionBackends,
	); err != nil {
		return err
	}
	if err := validateBackend(
		path+".storage.artifact", app.Storage.Artifact, artifactBackends,
	); err != nil {
		return err
	}
	if err := validateBackend(
		path+".storage.knowledge", app.Storage.Knowledge, knowledgeBackends,
	); err != nil {
		return err
	}
	if err := validateKnowledge(path+".knowledge", app.Knowledge, app.Storage.Knowledge); err != nil {
		return err
	}
	if app.Knowledge.Enabled && !containsString(app.Tools.Allow, "knowledge_search") {
		return fmt.Errorf("config: %s.tools.allow must include knowledge_search when knowledge is enabled", path)
	}
	if !app.Knowledge.Enabled && containsString(app.Tools.Allow, "knowledge_search") {
		return fmt.Errorf("config: %s.tools.allow must not include knowledge_search when knowledge is disabled", path)
	}
	return validateBackend(path+".storage.audit", app.Storage.Audit, auditBackends)
}

func validateWorkflow(path string, workflow tenant.WorkflowConfig) error {
	kind := workflow.Type
	if kind == "" || kind == tenant.WorkflowLLM {
		if len(workflow.Nodes) != 0 || len(workflow.Edges) != 0 || workflow.Aggregator != nil || workflow.Entry != "" || workflow.Finish != "" || workflow.MaxIterations != 0 || workflow.MaxConcurrency != 0 {
			return fmt.Errorf("config: %s single llm workflow must not declare composed fields", path)
		}
		return nil
	}
	if kind != tenant.WorkflowChain && kind != tenant.WorkflowParallel && kind != tenant.WorkflowCycle && kind != tenant.WorkflowGraph {
		return fmt.Errorf("config: %s.type is unsupported", path)
	}
	if len(workflow.Nodes) == 0 || len(workflow.Nodes) > 32 {
		return fmt.Errorf("config: %s.nodes must contain between 1 and 32 nodes", path)
	}
	nodes := make(map[string]struct{}, len(workflow.Nodes))
	for index, node := range workflow.Nodes {
		nodePath := fmt.Sprintf("%s.nodes[%d]", path, index)
		if err := validateID(nodePath+".id", node.ID); err != nil {
			return err
		}
		if strings.TrimSpace(node.Instruction) == "" {
			return fmt.Errorf("config: %s.instruction is required", nodePath)
		}
		if _, exists := nodes[node.ID]; exists {
			return fmt.Errorf("config: duplicate workflow node %q", node.ID)
		}
		nodes[node.ID] = struct{}{}
	}
	if workflow.MaxConcurrency < 0 || workflow.MaxConcurrency > 32 {
		return fmt.Errorf("config: %s.max_concurrency must be between 1 and 32 when set", path)
	}
	switch kind {
	case tenant.WorkflowParallel:
		if len(workflow.Edges) != 0 || workflow.Entry != "" || workflow.Finish != "" || workflow.MaxIterations != 0 || workflow.MaxConcurrency != 0 {
			return fmt.Errorf("config: %s contains fields not valid for parallel", path)
		}
		if workflow.Aggregator == nil || strings.TrimSpace(workflow.Aggregator.ID) == "" || strings.TrimSpace(workflow.Aggregator.Instruction) == "" {
			return fmt.Errorf("config: %s parallel workflow requires an aggregator", path)
		}
		if _, exists := nodes[workflow.Aggregator.ID]; exists {
			return fmt.Errorf("config: %s aggregator ID conflicts with a branch node", path)
		}
	case tenant.WorkflowCycle:
		if len(workflow.Edges) != 0 || workflow.Aggregator != nil || workflow.Entry != "" || workflow.Finish != "" || workflow.MaxConcurrency != 0 {
			return fmt.Errorf("config: %s contains fields not valid for cycle", path)
		}
		if workflow.MaxIterations <= 0 || workflow.MaxIterations > 32 {
			return fmt.Errorf("config: %s.max_iterations must be between 1 and 32", path)
		}
	case tenant.WorkflowGraph:
		if _, ok := nodes[workflow.Entry]; !ok {
			return fmt.Errorf("config: %s.entry must name a workflow node", path)
		}
		if _, ok := nodes[workflow.Finish]; !ok {
			return fmt.Errorf("config: %s.finish must name a workflow node", path)
		}
		if len(workflow.Edges) == 0 {
			return fmt.Errorf("config: %s.edges must not be empty", path)
		}
		for index, edge := range workflow.Edges {
			if _, ok := nodes[edge.From]; !ok {
				return fmt.Errorf("config: %s.edges[%d].from is unknown", path, index)
			}
			if _, ok := nodes[edge.To]; !ok {
				return fmt.Errorf("config: %s.edges[%d].to is unknown", path, index)
			}
			if edge.From == edge.To {
				return fmt.Errorf("config: %s.edges[%d] self-cycle is not allowed", path, index)
			}
		}
	default:
		if len(workflow.Edges) != 0 || workflow.Aggregator != nil || workflow.Entry != "" || workflow.Finish != "" || workflow.MaxIterations != 0 {
			return fmt.Errorf("config: %s contains fields not valid for %s", path, kind)
		}
	}
	return nil
}

func validateToolIntegrations(path string, app *tenant.AgentApp) error {
	if len(app.MCPServers) > 16 {
		return fmt.Errorf("config: %s.mcp_servers exceeds 16 entries", path)
	}
	if len(app.BusinessTools) > 32 {
		return fmt.Errorf("config: %s.business_tools exceeds 32 entries", path)
	}
	serverIDs := make(map[string]struct{}, len(app.MCPServers))
	exposedNames := map[string]struct{}{
		"echo":             {},
		"calculator":       {},
		"knowledge_search": {},
	}
	for index, server := range app.MCPServers {
		serverPath := fmt.Sprintf("%s.mcp_servers[%d]", path, index)
		if err := validateToolName(serverPath+".server_id", server.ID); err != nil {
			return err
		}
		if _, exists := serverIDs[server.ID]; exists {
			return fmt.Errorf("config: duplicate MCP server_id %q", server.ID)
		}
		serverIDs[server.ID] = struct{}{}
		if len(server.AllowedTools) > 64 || (server.Enabled && len(server.AllowedTools) == 0) {
			return fmt.Errorf("config: %s.allowed_tools must contain between 1 and 64 tools", serverPath)
		}
		remoteNames := make(map[string]struct{}, len(server.AllowedTools))
		for toolIndex, remoteName := range server.AllowedTools {
			if err := validateToolName(fmt.Sprintf("%s.allowed_tools[%d]", serverPath, toolIndex), remoteName); err != nil {
				return err
			}
			if _, exists := remoteNames[remoteName]; exists {
				return fmt.Errorf("config: duplicate MCP tool %q in server %q", remoteName, server.ID)
			}
			remoteNames[remoteName] = struct{}{}
			exposed := "mcp__" + server.ID + "__" + remoteName
			if len(exposed) > 64 {
				return fmt.Errorf("config: exposed MCP tool name %q exceeds 64 bytes", exposed)
			}
			if _, exists := exposedNames[exposed]; exists {
				return fmt.Errorf("config: duplicate exposed tool %q", exposed)
			}
			exposedNames[exposed] = struct{}{}
			if server.Enabled && !containsString(app.Tools.Allow, exposed) {
				return fmt.Errorf("config: %s.tools.allow must include %q", path, exposed)
			}
			if !server.Enabled && containsString(app.Tools.Allow, exposed) {
				return fmt.Errorf("config: %s.tools.allow must not include disabled tool %q", path, exposed)
			}
		}
		if !server.Enabled {
			continue
		}
		if !server.Idempotent {
			return fmt.Errorf("config: %s.idempotent must be true; non-idempotent remote tools must use the business HTTP adapter", serverPath)
		}
		if err := validateHTTPSURL(serverPath+".endpoint", server.Endpoint); err != nil {
			return err
		}
		if server.TimeoutSeconds < 0 || server.TimeoutSeconds > 30 {
			return fmt.Errorf("config: %s.timeout_seconds must be between 1 and 30 when set", serverPath)
		}
		if err := validateMCPAuth(serverPath, server); err != nil {
			return err
		}
	}
	for index, business := range app.BusinessTools {
		toolPath := fmt.Sprintf("%s.business_tools[%d]", path, index)
		if err := validateToolName(toolPath+".name", business.Name); err != nil {
			return err
		}
		if _, exists := exposedNames[business.Name]; exists {
			return fmt.Errorf("config: duplicate exposed tool %q", business.Name)
		}
		exposedNames[business.Name] = struct{}{}
		if !business.Enabled {
			if containsString(app.Tools.Allow, business.Name) {
				return fmt.Errorf("config: %s.tools.allow must not include disabled tool %q", path, business.Name)
			}
			continue
		}
		if strings.TrimSpace(business.Description) == "" || len(business.Description) > 512 {
			return fmt.Errorf("config: %s.description must contain between 1 and 512 bytes", toolPath)
		}
		if err := validateHTTPSURL(toolPath+".endpoint", business.Endpoint); err != nil {
			return err
		}
		if err := validateSecretRef(toolPath+".credential", business.Credential, true); err != nil {
			return err
		}
		if business.TimeoutSeconds < 0 || business.TimeoutSeconds > 30 {
			return fmt.Errorf("config: %s.timeout_seconds must be between 1 and 30 when set", toolPath)
		}
		if !containsString(app.Tools.Allow, business.Name) {
			return fmt.Errorf("config: %s.tools.allow must include %q", path, business.Name)
		}
	}
	return nil
}

func validateMCPAuth(path string, server tenant.MCPServer) error {
	if server.Credential.IsZero() {
		if server.CredentialHeader != "" || server.CredentialScheme != "" {
			return fmt.Errorf("config: %s credential header/scheme require a credential SecretRef", path)
		}
		return nil
	}
	if err := validateSecretRef(path+".credential", server.Credential, true); err != nil {
		return err
	}
	header := strings.ToLower(strings.TrimSpace(server.CredentialHeader))
	if header == "" {
		header = "authorization"
	}
	if header != "authorization" && header != "x-api-key" {
		return fmt.Errorf("config: %s.credential_header must be authorization or x-api-key", path)
	}
	scheme := strings.TrimSpace(server.CredentialScheme)
	if header == "authorization" && scheme != "" && scheme != "Bearer" {
		return fmt.Errorf("config: %s.credential_scheme must be Bearer for authorization", path)
	}
	if header == "x-api-key" && scheme != "" {
		return fmt.Errorf("config: %s.credential_scheme must be empty for x-api-key", path)
	}
	return nil
}

func validateToolName(path, value string) error {
	if value == "" || len(value) > 64 || !toolNamePattern.MatchString(value) {
		return fmt.Errorf("config: %s must match [A-Za-z0-9_-]+ and not exceed 64 bytes", path)
	}
	return nil
}

func validateHTTPSURL(path, value string) error {
	if err := validateHTTPURL(path, value, true); err != nil {
		return err
	}
	parsed, _ := url.Parse(value)
	if parsed == nil || parsed.Scheme != "https" || parsed.Host == "" {
		return fmt.Errorf("config: %s must be an HTTPS URL with a host", path)
	}
	if len(value) > 2048 {
		return fmt.Errorf("config: %s exceeds 2048 bytes", path)
	}
	return nil
}

func containsString(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}

func validateKnowledge(path string, policy tenant.KnowledgePolicy, backend tenant.BackendConfig) error {
	if !policy.Enabled {
		return nil
	}
	if policy.Embedding.Provider != "openai-compatible" {
		return fmt.Errorf("config: %s.embedding.provider must be openai-compatible", path)
	}
	if strings.TrimSpace(policy.Embedding.Model) == "" {
		return fmt.Errorf("config: %s.embedding.model is required", path)
	}
	if policy.Embedding.Dimensions <= 0 || policy.Embedding.Dimensions > 65536 {
		return fmt.Errorf("config: %s.embedding.dimensions must be between 1 and 65536", path)
	}
	if err := validateHTTPURL(path+".embedding.base_url", policy.Embedding.BaseURL, true); err != nil {
		return err
	}
	if err := validateSecretRef(path+".embedding.api_key", policy.Embedding.APIKey, true); err != nil {
		return err
	}
	if policy.MaxResults < 0 || policy.MaxResults > 100 {
		return fmt.Errorf("config: %s.max_results must be between 0 and 100", path)
	}
	if policy.MinScore < 0 || policy.MinScore > 1 {
		return fmt.Errorf("config: %s.min_score must be between 0 and 1", path)
	}
	if backend.Type != tenant.BackendPostgres && backend.Type != tenant.BackendQdrant {
		return fmt.Errorf("config: %s requires a postgres or qdrant storage backend", path)
	}
	if strings.TrimSpace(backend.Namespace) == "" {
		return fmt.Errorf("config: %s storage namespace is required", path)
	}
	return nil
}

func validateModel(path string, model tenant.ModelProfile) error {
	if strings.TrimSpace(model.Provider) == "" {
		return fmt.Errorf("config: %s.provider is required", path)
	}
	if strings.TrimSpace(model.Name) == "" {
		return fmt.Errorf("config: %s.name is required", path)
	}
	switch model.Provider {
	case "mock", "deepseek", "openai", "openai-compatible":
	default:
		return fmt.Errorf("config: %s.provider is unsupported", path)
	}
	if model.Provider != "mock" {
		if err := validateSecretRef(path+".api_key", model.APIKey, true); err != nil {
			return err
		}
	} else if err := validateSecretRef(path+".api_key", model.APIKey, false); err != nil {
		return err
	}
	if model.Temperature != nil && (*model.Temperature < 0 || *model.Temperature > 2) {
		return fmt.Errorf("config: %s.temperature must be between 0 and 2", path)
	}
	if model.MaxTokens < 0 {
		return fmt.Errorf("config: %s.max_tokens must not be negative", path)
	}
	if model.Pricing.InputMicrosPerMillion < 0 || model.Pricing.OutputMicrosPerMillion < 0 {
		return fmt.Errorf("config: %s.pricing rates must not be negative", path)
	}
	if (model.Pricing.InputMicrosPerMillion > 0 || model.Pricing.OutputMicrosPerMillion > 0) && strings.TrimSpace(model.Pricing.Version) == "" {
		return fmt.Errorf("config: %s.pricing.version is required when pricing rates are configured", path)
	}
	if model.Provider == "openai-compatible" && strings.TrimSpace(model.BaseURL) == "" {
		return fmt.Errorf("config: %s.base_url is required for openai-compatible provider", path)
	}
	return validateHTTPURL(path+".base_url", model.BaseURL, false)
}

func validateTools(path string, policy tenant.ToolPolicy) error {
	if policy.RequestTokenBudget < 0 || policy.MonthlyCostBudgetCents < 0 {
		return fmt.Errorf("config: %s budgets must not be negative", path)
	}
	allow, err := uniqueStrings(path+".allow", policy.Allow)
	if err != nil {
		return err
	}
	deny, err := uniqueStrings(path+".deny", policy.Deny)
	if err != nil {
		return err
	}
	approval, err := uniqueStrings(path+".require_approval", policy.RequireApproval)
	if err != nil {
		return err
	}
	for name := range allow {
		if _, exists := deny[name]; exists {
			return fmt.Errorf("config: tool %q is both allowed and denied", name)
		}
	}
	for name := range approval {
		if _, exists := deny[name]; exists {
			return fmt.Errorf(
				"config: tool %q requires approval but is denied", name,
			)
		}
	}
	return nil
}

func validateChannel(path string, binding tenant.ChannelBinding) error {
	if err := validateID(path+".binding_id", binding.ID); err != nil {
		return err
	}
	if strings.TrimSpace(binding.ProviderAccountID) == "" {
		return fmt.Errorf("config: %s.provider_account_id is required", path)
	}
	if binding.ReplyFormat != "" && binding.ReplyFormat != "text" && binding.ReplyFormat != "card" {
		return fmt.Errorf("config: %s.reply_format must be text or card", path)
	}
	if binding.ReplyFormat == "card" && binding.Type != tenant.ChannelTypeFeishu {
		return fmt.Errorf("config: %s.reply_format card is only supported by Feishu", path)
	}
	if _, err := uniqueStrings(path+".allowed_users", binding.AllowedUsers); err != nil {
		return err
	}
	if _, err := uniqueStrings(path+".allowed_chats", binding.AllowedChats); err != nil {
		return err
	}
	switch binding.Type {
	case tenant.ChannelTypeHTTP:
		if err := validateSecretRef(path+".token", binding.Token, false); err != nil {
			return err
		}
		if err := validateSecretRef(path+".secret", binding.Secret, false); err != nil {
			return err
		}
	case tenant.ChannelTypeWeCom:
		if strings.TrimSpace(binding.ProviderAppID) == "" {
			return fmt.Errorf("config: %s.provider_app_id is required", path)
		}
		if agentID, err := strconv.ParseInt(binding.ProviderAppID, 10, 64); err != nil || agentID <= 0 {
			return fmt.Errorf("config: %s.provider_app_id must be a positive integer", path)
		}
		if err := validateSecretRef(path+".token", binding.Token, binding.Enabled); err != nil {
			return err
		}
		if err := validateSecretRef(path+".secret", binding.Secret, binding.Enabled); err != nil {
			return err
		}
		if err := validateSecretRef(path+".encryption_key", binding.EncryptionKey, binding.Enabled); err != nil {
			return err
		}
	case tenant.ChannelTypeFeishu:
		if err := validateSecretRef(path+".token", binding.Token, binding.Enabled); err != nil {
			return err
		}
		if err := validateSecretRef(path+".secret", binding.Secret, binding.Enabled); err != nil {
			return err
		}
		if err := validateSecretRef(path+".encryption_key", binding.EncryptionKey, false); err != nil {
			return err
		}
	default:
		return fmt.Errorf("config: %s.type is unsupported", path)
	}
	return validateHTTPURL(path+".webhook_url", binding.WebhookURL, false)
}

func validateBackend(
	path string,
	backend tenant.BackendConfig,
	allowed map[tenant.BackendType]struct{},
) error {
	if backend.MigrationTarget != nil {
		if backend.MigrationTarget.MigrationTarget != nil {
			return fmt.Errorf("config: %s.migration_target cannot contain another migration target", path)
		}
		if err := validateBackend(path+".migration_target", *backend.MigrationTarget, allowed); err != nil {
			return err
		}
		if sameBackend(backend, *backend.MigrationTarget) {
			return fmt.Errorf("config: %s.migration_target must differ from the primary backend", path)
		}
	}
	if _, exists := allowed[backend.Type]; !exists {
		return fmt.Errorf(
			"config: %s.type %q is unsupported", path, backend.Type,
		)
	}
	if err := validateSecretRef(path+".credential", backend.Credential, false); err != nil {
		return err
	}
	if strings.TrimSpace(backend.Namespace) != backend.Namespace {
		return fmt.Errorf("config: %s.namespace must be trimmed", path)
	}
	if backend.Endpoint == "" {
		return nil
	}
	if strings.Contains(backend.Endpoint, "#") {
		return fmt.Errorf(
			"config: %s.endpoint must not contain fragments; use SecretRef", path,
		)
	}
	parsed, err := url.Parse(backend.Endpoint)
	if err != nil {
		return fmt.Errorf("config: %s.endpoint is invalid", path)
	}
	if parsed.User != nil {
		return fmt.Errorf(
			"config: %s.endpoint must not contain credentials; use SecretRef", path,
		)
	}
	if parsed.RawQuery != "" {
		return fmt.Errorf(
			"config: %s.endpoint must not contain query parameters; use SecretRef", path,
		)
	}
	if parsed.Fragment != "" || parsed.RawFragment != "" {
		return fmt.Errorf(
			"config: %s.endpoint must not contain fragments; use SecretRef", path,
		)
	}
	return nil
}

func sameBackend(left, right tenant.BackendConfig) bool {
	return left.Type == right.Type && left.Endpoint == right.Endpoint &&
		left.Namespace == right.Namespace && left.Credential.Provider == right.Credential.Provider && left.Credential.Key == right.Credential.Key
}

func validateSecretRef(path string, ref tenant.SecretRef, required bool) error {
	if ref.IsZero() {
		if required {
			return fmt.Errorf("config: %s is required", path)
		}
		return nil
	}
	if ref.Provider == "" || strings.TrimSpace(ref.Key) == "" {
		return fmt.Errorf("config: %s must include provider and key", path)
	}
	switch ref.Provider {
	case tenant.SecretProviderEnv,
		tenant.SecretProviderFile,
		tenant.SecretProviderVault,
		tenant.SecretProviderKMS:
	default:
		return fmt.Errorf("config: %s provider is unsupported", path)
	}
	return nil
}

func validateAudit(path string, policy tenant.AuditPolicy) error {
	if policy.Enabled && policy.RetentionDays <= 0 {
		return fmt.Errorf(
			"config: %s.retention_days must be positive when enabled", path,
		)
	}
	if policy.RetentionDays < 0 {
		return fmt.Errorf("config: %s.retention_days must not be negative", path)
	}
	_, err := uniqueStrings(path+".redact_fields", policy.RedactFields)
	return err
}

func validateID(path, value string) error {
	if value == "" || strings.TrimSpace(value) != value {
		return fmt.Errorf("config: %s must be non-empty and trimmed", path)
	}
	if len(value) > 128 {
		return fmt.Errorf("config: %s exceeds 128 bytes", path)
	}
	return nil
}

func validateHTTPURL(path, value string, required bool) error {
	if value == "" {
		if required {
			return fmt.Errorf("config: %s is required", path)
		}
		return nil
	}
	if strings.Contains(value, "#") {
		return fmt.Errorf(
			"config: %s must not contain fragments; use SecretRef", path,
		)
	}
	parsed, err := url.ParseRequestURI(value)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return fmt.Errorf("config: %s must be an HTTP(S) URL", path)
	}
	if parsed.User != nil {
		return fmt.Errorf("config: %s must not contain credentials", path)
	}
	if parsed.RawQuery != "" {
		return fmt.Errorf(
			"config: %s must not contain query parameters; use SecretRef", path,
		)
	}
	if parsed.Fragment != "" || parsed.RawFragment != "" {
		return fmt.Errorf(
			"config: %s must not contain fragments; use SecretRef", path,
		)
	}
	return nil
}

func uniqueStrings(path string, values []string) (map[string]struct{}, error) {
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value == "" || strings.TrimSpace(value) != value {
			return nil, fmt.Errorf("config: %s entries must be non-empty and trimmed", path)
		}
		if _, exists := result[value]; exists {
			return nil, fmt.Errorf("config: %s contains duplicate %q", path, value)
		}
		result[value] = struct{}{}
	}
	return result, nil
}

func backendSet(values ...tenant.BackendType) map[tenant.BackendType]struct{} {
	result := make(map[tenant.BackendType]struct{}, len(values))
	for _, value := range values {
		result[value] = struct{}{}
	}
	return result
}
