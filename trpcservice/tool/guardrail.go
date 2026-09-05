package tool

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type Invocation struct {
	TenantContext tenant.TenantContext
	Agent         agent.AgentSpec
	ToolName      string
	Definition    Definition
	Arguments     map[string]any
}

type GuardrailResult struct {
	Category    DecisionCategory
	Content     string
	InputBytes  int
	OutputBytes int
	Fingerprint string
}

type Guardrail interface {
	Before(context.Context, Invocation) GuardrailResult
	After(context.Context, Invocation, agent.ToolResult) GuardrailResult
}

type GuardrailConfig struct {
	MaxInputBytes  int
	MaxOutputBytes int
	Redactor       audit.Redactor
}

type GuardrailPipeline struct {
	maxInputBytes  int
	maxOutputBytes int
	redactor       audit.Redactor
}

func NewGuardrailPipeline(config GuardrailConfig) (*GuardrailPipeline, error) {
	if config.MaxInputBytes <= 0 {
		config.MaxInputBytes = 32 * 1024
	}
	if config.MaxOutputBytes <= 0 {
		config.MaxOutputBytes = 64 * 1024
	}
	if config.Redactor.MaxBytes <= 0 {
		config.Redactor = audit.NewRedactor(config.MaxOutputBytes)
	}
	return &GuardrailPipeline{maxInputBytes: config.MaxInputBytes, maxOutputBytes: config.MaxOutputBytes, redactor: config.Redactor}, nil
}

func (g *GuardrailPipeline) Before(ctx context.Context, invocation Invocation) GuardrailResult {
	if ctx == nil {
		return GuardrailResult{Category: CategoryInvalidContext}
	}
	if err := ctx.Err(); err != nil {
		return GuardrailResult{Category: categoryForContext(err)}
	}
	if err := validateInvocation(invocation); err != nil {
		return GuardrailResult{Category: CategoryInvalidContext}
	}
	encoded, err := json.Marshal(invocation.Arguments)
	if err != nil {
		return GuardrailResult{Category: CategoryInvalidInput}
	}
	maxBytes := g.maxInputBytes
	if invocation.Definition.MaxInputByte > 0 && invocation.Definition.MaxInputByte < maxBytes {
		maxBytes = invocation.Definition.MaxInputByte
	}
	result := GuardrailResult{InputBytes: len(encoded), Fingerprint: audit.Fingerprint(string(encoded))}
	if len(encoded) > maxBytes {
		result.Category = CategoryOversized
		return result
	}
	if sensitiveArguments(invocation.Arguments) {
		result.Category = CategoryInvalidInput
		return result
	}
	if err := validateSchemaValue(invocation.Arguments, invocation.Definition.InputSchema); err != nil {
		result.Category = CategoryInvalidInput
		return result
	}
	if unsafeCapability(invocation) {
		result.Category = CategoryDeny
		return result
	}
	result.Category = CategoryAllow
	return result
}

func (g *GuardrailPipeline) After(ctx context.Context, invocation Invocation, result agent.ToolResult) GuardrailResult {
	if ctx == nil {
		return GuardrailResult{Category: CategoryInvalidContext}
	}
	if err := ctx.Err(); err != nil {
		return GuardrailResult{Category: categoryForContext(err)}
	}
	if err := validateInvocation(invocation); err != nil {
		return GuardrailResult{Category: CategoryInvalidContext}
	}
	outputBytes := len(result.Content)
	maxBytes := g.maxOutputBytes
	if invocation.Definition.MaxOutputByte > 0 && invocation.Definition.MaxOutputByte < maxBytes {
		maxBytes = invocation.Definition.MaxOutputByte
	}
	if outputBytes > maxBytes {
		return GuardrailResult{Category: CategoryOversized, OutputBytes: outputBytes}
	}
	if result.IsError {
		return GuardrailResult{Category: CategoryToolFailure, OutputBytes: outputBytes}
	}
	safe := g.redactor.RedactString(result.Content)
	guarded := GuardrailResult{Category: CategoryAllow, Content: safe, OutputBytes: outputBytes, Fingerprint: audit.Fingerprint(result.Content)}
	if safe != result.Content {
		guarded.Category = CategoryRedacted
	}
	return guarded
}

var errInvalidArgument = errors.New("invalid tool arguments")

func validateInvocation(invocation Invocation) error {
	if invocation.ToolName == "" || invocation.Definition.Name != invocation.ToolName || !invocation.Definition.Enabled || invocation.Definition.Version < 1 {
		return errInvalidInvocation
	}
	tc := invocation.TenantContext
	if err := tc.Validate(); err != nil {
		return err
	}
	if invocation.Agent.TenantID != tc.TenantID || invocation.Agent.AgentAppID != tc.AgentAppID || invocation.Agent.Version != tc.ConfigVersion {
		return errInvalidInvocation
	}
	declared := false
	for _, spec := range invocation.Agent.Tools {
		if spec.Name != invocation.ToolName {
			continue
		}
		declared = canonicalVersion(spec.Version) == invocation.Definition.Version && (spec.Capability == "" || spec.Capability == invocation.Definition.Capability)
		break
	}
	if !declared {
		return errInvalidInvocation
	}
	return invocation.Definition.validate()
}

var errInvalidInvocation = &GovernanceError{Category: CategoryInvalidContext, Code: "invalid_invocation"}

func sensitiveArguments(arguments map[string]any) bool {
	return sensitiveValue("", arguments)
}

func sensitiveValue(key string, value any) bool {
	if key != "" && audit.IsSensitiveKey(key) {
		return true
	}
	switch typed := value.(type) {
	case map[string]any:
		for name, child := range typed {
			if sensitiveValue(name, child) {
				return true
			}
		}
	case []any:
		for _, child := range typed {
			if sensitiveValue("", child) {
				return true
			}
		}
	case string:
		return audit.RedactString(typed) != typed
	}
	return false
}

func unsafeCapability(invocation Invocation) bool {
	capability := strings.ToLower(strings.TrimSpace(invocation.Definition.Capability))
	switch capability {
	case "shell", "code", "execute", "admin":
		return true
	case "filesystem", "file":
		return containsPathTraversal(invocation.Arguments)
	case "network":
		return !validNetworkArguments(invocation.Arguments, invocation.Definition.AllowedHosts)
	default:
		return false
	}
}

func containsPathTraversal(value any) bool {
	switch typed := value.(type) {
	case map[string]any:
		for _, child := range typed {
			if containsPathTraversal(child) {
				return true
			}
		}
	case []any:
		for _, child := range typed {
			if containsPathTraversal(child) {
				return true
			}
		}
	case string:
		return strings.Contains(typed, "../") || strings.Contains(typed, `..\\`) || strings.HasPrefix(typed, "/")
	}
	return false
}

func validNetworkArguments(value any, allowedHosts []string) bool {
	if len(allowedHosts) == 0 {
		return false
	}
	foundURL := false
	var inspect func(any) bool
	inspect = func(current any) bool {
		switch typed := current.(type) {
		case map[string]any:
			for _, child := range typed {
				if !inspect(child) {
					return false
				}
			}
		case []any:
			for _, child := range typed {
				if !inspect(child) {
					return false
				}
			}
		case string:
			if !strings.Contains(typed, "://") {
				return true
			}
			parsed, err := url.Parse(typed)
			if err != nil || parsed.User != nil || parsed.Hostname() == "" {
				return false
			}
			foundURL = true
			for _, allowed := range allowedHosts {
				if strings.EqualFold(parsed.Hostname(), allowed) {
					return true
				}
			}
			return false
		}
		return true
	}
	return inspect(value) && foundURL
}

func validateSchemaValue(value any, schema map[string]any) error {
	if len(schema) == 0 {
		return nil
	}
	if schemaType, _ := schema["type"].(string); schemaType != "" {
		if !matchesSchemaType(value, schemaType) {
			return errInvalidArgument
		}
	}
	if maxLength, ok := integerSchema(schema["maxLength"]); ok {
		if stringValue, ok := value.(string); !ok || len(stringValue) > maxLength {
			return errInvalidArgument
		}
	}
	if maxItems, ok := integerSchema(schema["maxItems"]); ok {
		items, ok := value.([]any)
		if !ok || len(items) > maxItems {
			return errInvalidArgument
		}
	}
	if object, ok := value.(map[string]any); ok {
		if required, ok := stringList(schema["required"]); ok {
			for _, name := range required {
				if _, present := object[name]; !present {
					return errInvalidArgument
				}
			}
		}
		properties, _ := schema["properties"].(map[string]any)
		additional, _ := schema["additionalProperties"].(bool)
		for name, child := range object {
			property, present := properties[name].(map[string]any)
			if !present {
				if additional == false && schema["additionalProperties"] != nil {
					return errInvalidArgument
				}
				continue
			}
			if err := validateSchemaValue(child, property); err != nil {
				return err
			}
		}
	}
	return nil
}

func matchesSchemaType(value any, schemaType string) bool {
	switch schemaType {
	case "object":
		_, ok := value.(map[string]any)
		return ok
	case "array":
		_, ok := value.([]any)
		return ok
	case "string":
		_, ok := value.(string)
		return ok
	case "boolean":
		_, ok := value.(bool)
		return ok
	case "number":
		switch value.(type) {
		case int, int64, float64, float32:
			return true
		default:
			return false
		}
	case "integer":
		switch typed := value.(type) {
		case int, int64:
			return true
		case float64:
			return typed == float64(int64(typed))
		default:
			return false
		}
	default:
		return false
	}
}

func integerSchema(value any) (int, bool) {
	switch typed := value.(type) {
	case int:
		return typed, typed >= 0
	case int64:
		return int(typed), typed >= 0 && int64(int(typed)) == typed
	case float64:
		return int(typed), typed >= 0 && typed == float64(int(typed))
	default:
		return 0, false
	}
}

func stringList(value any) ([]string, bool) {
	items, ok := value.([]any)
	if !ok {
		return nil, false
	}
	result := make([]string, 0, len(items))
	for _, item := range items {
		name, ok := item.(string)
		if !ok {
			return nil, false
		}
		result = append(result, name)
	}
	return result, true
}

var _ Guardrail = (*GuardrailPipeline)(nil)
var _ = tenant.ChannelLark
