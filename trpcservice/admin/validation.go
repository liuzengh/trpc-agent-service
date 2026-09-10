package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	agentruntime "github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	platformskill "github.com/liuzengh/trpc-agent-service/trpcservice/skill"
	platformstorage "github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	platformtool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
)

// ValidationIssue contains fixed, safe messages. Never echo parser errors,
// supplied field values, URLs, secret references or credential contents.
type ValidationIssue struct {
	Code       string `json:"code"`
	Field      string `json:"field"`
	Severity   string `json:"severity"`
	Message    string `json:"message"`
	Suggestion string `json:"suggestion"`
}

// DependencyCheck is a point-in-time observation, not a connectivity probe.
// Admin-only nodes must not claim that their own dependencies describe Workers.
type DependencyCheck struct {
	Component  string    `json:"component"`
	State      string    `json:"state"` // ready, unavailable, unknown
	Source     string    `json:"source"`
	ObservedAt time.Time `json:"observed_at,omitempty"`
}

type ValidationReport struct {
	CheckID       string            `json:"check_id"`
	Valid         bool              `json:"valid"`
	RuntimeStatus string            `json:"runtime_status"`
	CheckedAt     time.Time         `json:"checked_at"`
	Issues        []ValidationIssue `json:"issues"`
	Dependencies  []DependencyCheck `json:"dependencies"`
}

type ValidationError struct{ Report ValidationReport }

func (e *ValidationError) Error() string { return "Agent configuration validation failed" }
func (e *ValidationError) Unwrap() error { return ErrInvalid }
func (e *ValidationError) Is(target error) bool {
	if target == secret.ErrForbidden {
		for _, issue := range e.Report.Issues {
			if issue.Code == "secret_not_authorized" {
				return true
			}
		}
	}
	return target == ErrInvalid
}

// WithDependencyObservations installs a read-only snapshot source. It must not
// start services, resolve secrets, invoke models or perform active probes.
func (s *Service) WithDependencyObservations(read func() []DependencyCheck) *Service {
	s.dependencyObservations = read
	return s
}

func (r *ValidationReport) add(code, field, severity, message, suggestion string) {
	r.Issues = append(r.Issues, ValidationIssue{code, field, severity, message, suggestion})
	if severity == "error" {
		r.Valid = false
	}
}

// ValidateRevision is the common, non-mutating preflight for create, publish,
// canary and future drafts/debugging. It reads control-plane metadata only.
// A valid report does NOT imply that a real model call has passed.
func (s *Service) ValidateRevision(ctx context.Context, revision controlplane.AgentRevision) (ValidationReport, error) {
	r := ValidationReport{CheckID: "check-" + uuid.NewString(), Valid: true, RuntimeStatus: "unknown", CheckedAt: time.Now().UTC(), Issues: []ValidationIssue{}, Dependencies: []DependencyCheck{}}
	if !identifierPattern.MatchString(revision.TenantID) || !identifierPattern.MatchString(revision.AppID) {
		return r, invalidf("tenant and app identities are required")
	}
	app, err := s.repository.GetAgentApp(ctx, revision.TenantID, revision.AppID)
	if err != nil {
		return r, err
	}
	if app.TenantID != revision.TenantID || app.ID != revision.AppID {
		return r, controlplane.ErrNotFound
	}
	fields := []struct {
		name string
		raw  *json.RawMessage
	}{
		{"agent_config", &revision.AgentConfig}, {"model_config", &revision.ModelConfig},
		{"tool_policy", &revision.ToolPolicy}, {"knowledge_config", &revision.KnowledgeConfig},
		{"memory_config", &revision.MemoryConfig}, {"guardrail_config", &revision.GuardrailConfig},
	}
	for _, field := range fields {
		if normalizeJSON(field.raw) != nil || len(*field.raw) == 0 || (*field.raw)[0] != '{' {
			r.add("invalid_config_object", field.name, "error", "配置必须是 JSON 对象。", "检查该配置的 JSON 类型和语法。")
		}
	}
	if !r.Valid {
		return r, nil
	}
	if agentruntime.ValidateRevisionAgentConfig(revision) != nil {
		r.add("invalid_agent_config", "agent_config", "error", "Agent 配置无效。", "使用 llm 类型，填写名称与指令，并检查记忆和摘要参数。")
	}
	if agentruntime.ValidateRevisionModelConfig(revision.ModelConfig) != nil {
		r.add("invalid_model_config", "model_config", "error", "模型配置无效。", "检查来源、供应商、模型名、密钥引用、服务地址和调用限额。")
	}
	if governance.ValidateMemoryPolicy(revision.AgentConfig, revision.MemoryConfig) != nil {
		r.add("invalid_memory_config", "memory_config", "error", "记忆策略无效或与 Agent 配置冲突。", "核对记忆作用域、预加载和自动写入设置。")
	}
	if platformstorage.ValidateRevisionKnowledgeConfig(revision.KnowledgeConfig) != nil {
		r.add("invalid_knowledge_config", "knowledge_config", "error", "知识库配置无效。", "检查 Embedding 供应商、模型、向量维度和检索参数。")
	}
	if _, err := governance.BuildModelCallbacks(revision.GuardrailConfig); err != nil {
		r.add("invalid_guardrail_config", "guardrail_config", "error", "治理策略无效。", "检查脱敏和输入输出限制配置。")
	}
	s.validateCredentialGrants(ctx, revision, &r)
	servers, serverErr := platformtool.ParseMCPServers(revision.AgentConfig)
	if serverErr != nil {
		r.add("invalid_mcp_config", "agent_config.mcp_servers", "error", "MCP 配置无效。", "检查部署者授权的服务器引用和工具声明。")
	} else {
		for i, server := range servers {
			if s.authorizeSecret(ctx, revision.TenantID, secret.MCPServer, server.CredentialRef) != nil {
				r.add("secret_not_authorized", fmt.Sprintf("agent_config.mcp_servers[%d].credential_ref", i), "error", "MCP 凭据引用未获租户授权。", "请部署者检查对应租户和 mcp_server 用途的授权。")
			}
		}
	}
	policy, policyErr := governance.ParseToolPolicy(revision.ToolPolicy)
	refs, refsErr := platformskill.ParseRefs(revision.AgentConfig)
	needsSandbox := len(refs) > 0 && slices.Contains(policy.AllowedTools, "skill_run")
	if policyErr != nil {
		r.add("invalid_tool_policy", "tool_policy", "error", "工具权限配置无效。", "检查工具名单、用户范围、次数和执行时长。")
	} else {
		if _, err := s.skills.Validate(revision.TenantID, revision.AgentConfig, policy.AllowedTools); err != nil {
			r.add("skill_not_available", "agent_config.skills", "error", "Skill 未获授权、版本校验不符或缺少必需工具。", "重新选择已授权 Skill，并确认 skill_load / skill_run 权限。")
		}
		if serverErr == nil && refsErr == nil {
			if _, err := s.tools.Resolve(platformskill.LocalTools(refs, platformtool.MCPLocalTools(servers, policy.AllowedTools))); err != nil {
				r.add("tool_not_registered", "tool_policy.allowed_tools", "error", "工具名单中有未注册的工具。", "选择平台已注册并允许使用的工具。")
			}
		}
		if needsSandbox && !slices.Contains(policy.AllowedTools, "skill_load") {
			r.add("skill_load_not_allowed", "tool_policy.allowed_tools", "error", "执行 Skill 需要先允许加载其说明。", "明确将 skill_load 加入工具白名单后重新检查。")
		}
		if needsSandbox && policy.MaxToolCalls > 0 && policy.MaxToolCalls < 2 {
			r.add("skill_tool_budget_too_low", "tool_policy.max_tool_calls", "error", "加载并执行 Skill 至少需要两次工具调用，当前上限不足。", "明确将上限改为至少 2（测试建议 4）后重新检查；平台不会自动提高预算。")
		}
		if len(policy.AllowedTools) > 0 && policy.MaxToolCalls == 0 {
			r.add("tool_budget_unlimited", "tool_policy.max_tool_calls", "warning", "当前没有设置每次执行的工具调用次数上限。", "0 表示不限次数，不是禁用工具；建议设置明确的正数上限。")
		}
	}
	if err := s.validateBackendMetadata(ctx, revision, &r); err != nil {
		return r, err
	}
	s.appendDependencyObservations(ctx, &r, revision, needsSandbox)
	return r, nil
}

func (s *Service) requireValidRevision(ctx context.Context, revision controlplane.AgentRevision) error {
	r, err := s.ValidateRevision(ctx, revision)
	if err != nil {
		return err
	}
	if !r.Valid {
		return &ValidationError{Report: r}
	}
	return nil
}

func (s *Service) validateCredentialGrants(ctx context.Context, revision controlplane.AgentRevision, r *ValidationReport) {
	var cfg struct {
		Source       string `json:"source"`
		Ref          string `json:"api_key_ref"`
		Env          string `json:"api_key_env"`
		ConnectionID string `json:"connection_id"`
	}
	_ = json.Unmarshal(revision.ModelConfig, &cfg)
	if strings.EqualFold(strings.TrimSpace(cfg.Source), "connection") {
		if err := s.models.ValidateReference(ctx, revision.TenantID, cfg.ConnectionID); err != nil {
			r.add("model_connection_unavailable", "model_config.connection_id", "error", "模型连接不存在、属于其他租户、地址未允许或部署未启用加密模型存储。", "在当前租户选择有效连接，并检查执行节点的地址允许列表；请勿复制其他租户的连接 ID。")
		}
	}
	if strings.EqualFold(strings.TrimSpace(cfg.Source), "revision") || cfg.Ref != "" || cfg.Env != "" {
		ref := cfg.Ref
		if cfg.Env != "" {
			ref = "env://" + cfg.Env
		}
		if (cfg.Ref != "" && cfg.Env != "") || s.authorizeSecret(ctx, revision.TenantID, secret.Model, ref) != nil {
			r.add("secret_not_authorized", "model_config.api_key_ref", "error", "模型凭据引用未获授权或存在冲突。", "请部署者检查 tenant/model/reference 精确授权，只使用一种密钥引用。")
		}
	}
	var knowledge struct {
		Embedding struct {
			Provider string `json:"provider"`
			Ref      string `json:"secret_ref"`
		} `json:"embedding"`
	}
	_ = json.Unmarshal(revision.KnowledgeConfig, &knowledge)
	if strings.EqualFold(strings.TrimSpace(knowledge.Embedding.Provider), "openai") || knowledge.Embedding.Ref != "" {
		if s.authorizeSecret(ctx, revision.TenantID, secret.Embedding, knowledge.Embedding.Ref) != nil {
			r.add("secret_not_authorized", "knowledge_config.embedding.secret_ref", "error", "Embedding 凭据未获租户授权。", "请部署者检查 embedding 用途的精确授权。")
		}
	}
}

func (s *Service) validateBackendMetadata(ctx context.Context, revision controlplane.AgentRevision, r *ValidationReport) error {
	bindings, err := s.repository.ListBackendBindings(ctx, revision.TenantID, revision.AppID)
	if err != nil {
		return err
	}
	selected := map[string]controlplane.BackendBinding{}
	for _, binding := range bindings {
		if binding.TenantID != revision.TenantID || (binding.AppID != "" && binding.AppID != revision.AppID) || binding.MigrationState != "active" {
			continue
		}
		prior, found := selected[binding.ResourceType]
		if !found || (binding.AppID != "" && prior.AppID == "") {
			selected[binding.ResourceType] = binding
		}
	}
	var knowledge struct {
		Enabled   bool `json:"enabled"`
		Embedding struct {
			Dimensions int `json:"dimensions"`
		} `json:"embedding"`
	}
	_ = json.Unmarshal(revision.KnowledgeConfig, &knowledge)
	if knowledge.Enabled {
		binding, ok := selected["knowledge"]
		if !ok {
			r.add("knowledge_backend_missing", "knowledge_config", "error", "没有可用的知识库后端绑定。", "先注册并启用当前租户或应用的知识库后端。")
		} else {
			var cfg struct {
				Dimensions int `json:"dimensions"`
			}
			if json.Unmarshal(binding.Config, &cfg) != nil || (cfg.Dimensions > 0 && cfg.Dimensions != knowledge.Embedding.Dimensions) {
				r.add("knowledge_dimensions_mismatch", "knowledge_config.embedding.dimensions", "error", "Embedding 维度与知识库后端配置不匹配。", "使用相同维度；变更已有向量库时走迁移流程。")
			}
		}
	}
	for _, resource := range []string{"session", "memory", "knowledge", "artifact"} {
		binding, ok := selected[resource]
		if !ok || (resource == "knowledge" && !knowledge.Enabled) {
			continue
		}
		if platformstorage.ValidateBackendBindingConfig(binding) != nil {
			r.add("invalid_backend_config", "backends."+resource, "error", "后端类型或配置无效。", "核对后端类型、地址/凭据引用、维度、桶名称与数值限制。")
		}
		if binding.SecretRef != "" && s.authorizeSecret(ctx, revision.TenantID, resource, binding.SecretRef) != nil {
			r.add("secret_not_authorized", "backends."+resource, "error", "数据后端凭据未获租户授权。", "请部署者核对后端用途和精确引用授权。")
		}
	}
	return nil
}

func (s *Service) appendDependencyObservations(ctx context.Context, r *ValidationReport, revision controlplane.AgentRevision, needsSandbox bool) {
	var observed []DependencyCheck
	if s.dependencyObservations != nil {
		observed = s.dependencyObservations()
	}
	if shared, _ := s.workerDependencies(ctx, revision.TenantID, revision.AppID); len(shared) > 0 {
		observed = shared
	}
	r.CheckedAt = time.Now().UTC()
	components := []string{"worker", "model", "session"}
	if needsSandbox {
		components = append(components, "sandbox")
	}
	known, unavailable := true, false
	for _, component := range components {
		check := DependencyCheck{Component: component, State: "unknown", Source: "not_observed"}
		for _, entry := range observed {
			age := r.CheckedAt.Sub(entry.ObservedAt)
			if entry.Component == component && (entry.Source == "local_worker" || entry.Source == "shared_worker_record") && age >= 0 && age <= 30*time.Second && (entry.State == "ready" || entry.State == "unavailable") {
				check = entry
				break
			}
		}
		r.Dependencies = append(r.Dependencies, check)
		known = known && check.State != "unknown"
		if check.State == "unavailable" {
			unavailable = true
			r.add(component+"_unavailable", "dependencies."+component, "error", "执行节点报告依赖当前不可用。", "由部署者恢复该依赖后重新检查；本次检查不会启动服务。")
		}
	}
	if unavailable {
		r.RuntimeStatus = "unavailable"
	} else if known {
		r.RuntimeStatus = "ready"
	}
	if !known {
		r.add("runtime_not_verified", "dependencies", "warning", "部分执行依赖尚未观测，配置有效不等于真实调用成功。", "在已授权的执行节点完成就绪检查；模型生成测试需显式发起。")
	}
}

var _ error = (*ValidationError)(nil)
