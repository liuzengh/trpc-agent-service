//
// Tencent is pleased to support the open source community by making trpc-agent-go available.
//
// Copyright (C) 2025 Tencent.  All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.

package governance

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
)

var (
	// ErrToolNotAllowed indicates the tool is not allowed by policy.
	ErrToolNotAllowed = fmt.Errorf("tool not allowed by policy")
	// ErrToolConfirmationRequired indicates the tool requires user confirmation.
	ErrToolConfirmationRequired = fmt.Errorf("tool requires user confirmation")
	// ErrBudgetExceeded indicates the tenant has exceeded budget limits.
	ErrBudgetExceeded = fmt.Errorf("budget limit exceeded")
	// ErrUnsafeToolOutput indicates masking cannot safely process a tool result.
	ErrUnsafeToolOutput = fmt.Errorf("tool output cannot be safely masked")

	creditCardPattern = regexp.MustCompile(`\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?(\d{4})`)
	emailPattern      = regexp.MustCompile(`([a-zA-Z])[a-zA-Z0-9._-]*@([a-zA-Z0-9.-]+)`)
	phonePattern      = regexp.MustCompile(`(\+\d{1,3}[-\s]?)\d{3}([-\s]?\d{4})`)
	ssnPattern        = regexp.MustCompile(`\d{3}-\d{2}-(\d{4})`)
	apiKeyPattern     = regexp.MustCompile(`(sk-|pk-|api_)[a-zA-Z0-9]{8,}([a-zA-Z0-9]{3})`)
)

type compiledMaskingRule struct {
	rule tenant.MaskingRule
	re   *regexp.Regexp
}

// GovernanceFilter implements tenant-level governance policies.
type GovernanceFilter struct {
	tenant       *tenant.Tenant
	maskingRules []compiledMaskingRule
	policyErr    error
}

// NewGovernanceFilter creates a new governance filter.
func NewGovernanceFilter(t *tenant.Tenant) *GovernanceFilter {
	f := &GovernanceFilter{tenant: t}
	if t == nil {
		return f
	}
	f.maskingRules = make([]compiledMaskingRule, 0, len(t.Governance.DataMasking))
	for _, rule := range t.Governance.DataMasking {
		compiled, err := compileMaskingRule(rule)
		if err != nil {
			// Tenant validation rejects this before production composition. Keep
			// the runtime boundary fail-closed for tests or custom callers that
			// bypass the control plane instead of silently disabling masking.
			f.policyErr = fmt.Errorf("compile data masking rule: %w", err)
			break
		}
		f.maskingRules = append(f.maskingRules, compiled)
	}
	return f
}

// BeforeToolInvocation validates tool invocation before execution.
func (f *GovernanceFilter) BeforeToolInvocation(ctx context.Context, toolName string, input map[string]interface{}) error {
	if f == nil || f.tenant == nil {
		return fmt.Errorf("governance tenant policy is not configured")
	}
	// Check tool whitelist/blacklist
	if !f.tenant.ToolPolicy.IsAllowed(toolName) {
		return fmt.Errorf("%w: %s", ErrToolNotAllowed, toolName)
	}

	// Check if confirmation required
	if f.tenant.ToolPolicy.RequiresConfirmation(toolName) {
		// Approval is fail-closed until a one-time approval capability is
		// explicitly injected into the invocation context.
		return ErrToolConfirmationRequired
	}

	// Check budget limits (simplified)
	// In production, this would check actual token/cost usage

	return nil
}

// AfterToolInvocation processes tool output after execution.
func (f *GovernanceFilter) AfterToolInvocation(ctx context.Context, toolName string, output interface{}, err error) (interface{}, error) {
	// A failed tool may still return a partial result. Mask that result before
	// propagating the original error; otherwise a provider/adapter that returns
	// (sensitivePayload, err) can bypass the normal output redaction path.
	if output == nil && err != nil {
		return output, err
	}

	// Apply data masking to both successful and partial/error results.
	masked, maskErr := f.maskSensitiveData(ctx, output, maxMaskingBytes)
	if maskErr != nil {
		return nil, maskErr
	}

	return masked, err
}

// maskSensitiveData applies masking rules to output.
func (f *GovernanceFilter) maskSensitiveData(ctx context.Context, data interface{}, limit int) (interface{}, error) {
	if f == nil || f.tenant == nil {
		return data, nil
	}
	if f.policyErr != nil {
		return nil, fmt.Errorf("%w: %v", ErrUnsafeToolOutput, f.policyErr)
	}
	if len(f.maskingRules) == 0 {
		return data, nil
	}
	if err := checkMaskingInput(ctx, data, limit); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrUnsafeToolOutput, err)
	}
	normalized, size, err := normalizeJSONValue(data, limit)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrUnsafeToolOutput, err)
	}
	budget := maskingBudget{limit: limit, used: size}
	masked, err := f.maskValue(ctx, normalized, &budget, 0)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrUnsafeToolOutput, err)
	}
	return masked, nil
}

func normalizeJSONValue(value interface{}, limit int) (interface{}, int, error) {
	// Always round-trip through encoding/json. Besides normalizing structs and
	// typed collections, this creates a detached value and lets the standard
	// encoder reject cycles and unsupported values before recursive masking.
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, 0, err
	}
	if len(encoded) > limit {
		return nil, 0, fmt.Errorf("masking input exceeds %d bytes", limit)
	}
	var normalized interface{}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if err := decoder.Decode(&normalized); err != nil {
		return nil, 0, err
	}
	size, err := maskingJSONSize(normalized, limit, 0)
	return normalized, size, err
}

func (f *GovernanceFilter) maskValue(ctx context.Context, value interface{}, budget *maskingBudget, depth int) (interface{}, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if depth > maxMaskingDepth {
		return nil, fmt.Errorf("masking nesting exceeds %d levels", maxMaskingDepth)
	}
	switch typed := value.(type) {
	case string:
		masked := typed
		size, err := maskingJSONStringSize(masked, budget.limit)
		if err != nil {
			return nil, err
		}
		for _, rule := range f.maskingRules {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			available := budget.limit - budget.used + size
			masked, err = replaceMaskingString(ctx, rule.re, masked, maskingReplacement(rule.rule), available-2, maxMaskingMatchBytes)
			if err != nil {
				return nil, err
			}
			nextSize, err := maskingJSONStringSize(masked, available)
			if err != nil {
				return nil, err
			}
			budget.used += nextSize - size
			size = nextSize
		}
		return masked, nil
	case map[string]interface{}:
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			masked, err := f.maskValue(ctx, typed[key], budget, depth+1)
			if err != nil {
				return nil, err
			}
			typed[key] = masked
		}
		return typed, nil
	case []interface{}:
		for index := range typed {
			masked, err := f.maskValue(ctx, typed[index], budget, depth+1)
			if err != nil {
				return nil, err
			}
			typed[index] = masked
		}
		return typed, nil
	default:
		return value, nil
	}
}

// applyMaskingRule applies a single masking rule.
func applyMaskingRule(text string, rule tenant.MaskingRule) string {
	compiled, err := compileMaskingRule(rule)
	if err != nil {
		return text
	}
	masked, err := replaceMaskingString(context.Background(), compiled.re, text, maskingReplacement(rule), maxMaskingBytes, maxMaskingMatchBytes)
	if err != nil {
		return ""
	}
	return masked
}

func compileMaskingRule(rule tenant.MaskingRule) (compiledMaskingRule, error) {
	var re *regexp.Regexp
	switch rule.Type {
	case "credit_card":
		// Mask credit card numbers: 1234-5678-9012-3456 -> ****-****-****-3456.
		re = creditCardPattern
	case "email":
		// Mask email: user@example.com -> u***@example.com.
		re = emailPattern
	case "phone":
		// Mask phone: +1-555-1234 -> +1-***-1234.
		re = phonePattern
	case "ssn":
		// Mask SSN: 123-45-6789 -> ***-**-6789.
		re = ssnPattern
	case "api_key":
		// Mask API keys: sk-abc123def456 -> sk-***456.
		re = apiKeyPattern
	case "custom":
		if rule.Pattern == "" {
			return compiledMaskingRule{}, fmt.Errorf("custom masking rule requires a pattern")
		}
		var err error
		re, err = regexp.Compile(rule.Pattern)
		if err != nil {
			return compiledMaskingRule{}, err
		}
	default:
		return compiledMaskingRule{}, fmt.Errorf("unsupported masking rule type %q", rule.Type)
	}
	return compiledMaskingRule{rule: rule, re: re}, nil
}

func maskingReplacement(rule tenant.MaskingRule) string {
	switch rule.Type {
	case "credit_card":
		return "****-****-****-$1"
	case "email":
		return "$1***@$2"
	case "phone":
		return "$1***$2"
	case "ssn":
		return "***-**-$1"
	case "api_key":
		return "$1***$2"
	default:
		return rule.Replace
	}
}

// ContentFilter filters content based on governance policies.
type ContentFilter struct {
	filters   []compiledContentFilter
	policyErr error
}

type compiledContentFilter struct {
	filter   tenant.ContentFilter
	keywords []string
	regexps  []*regexp.Regexp
}

// NewContentFilter creates a new content filter.
func NewContentFilter(filters []tenant.ContentFilter) *ContentFilter {
	result := &ContentFilter{filters: make([]compiledContentFilter, 0, len(filters))}
	for _, filter := range filters {
		compiled := compiledContentFilter{filter: filter}
		switch filter.Type {
		case "keyword":
			compiled.keywords = make([]string, 0, len(filter.Patterns))
			for _, pattern := range filter.Patterns {
				compiled.keywords = append(compiled.keywords, strings.ToLower(pattern))
			}
		case "regex":
			compiled.regexps = make([]*regexp.Regexp, 0, len(filter.Patterns))
			for _, pattern := range filter.Patterns {
				re, err := regexp.Compile(pattern)
				if err != nil {
					result.policyErr = fmt.Errorf("compile content filter %q: %w", filter.Name, err)
					return result
				}
				compiled.regexps = append(compiled.regexps, re)
			}
		default:
			result.policyErr = fmt.Errorf("unsupported content filter type %q", filter.Type)
			return result
		}
		if filter.Action != "block" && filter.Action != "warn" && filter.Action != "log" {
			result.policyErr = fmt.Errorf("unsupported content filter action %q", filter.Action)
			return result
		}
		result.filters = append(result.filters, compiled)
	}
	return result
}

// FilterContent checks content against filters and returns action.
func (f *ContentFilter) FilterContent(content string) (action string, matched bool) {
	if f == nil {
		return "", false
	}
	if f.policyErr != nil {
		// A malformed policy must never turn into an implicit allow. Worker
		// treats this as a block and records the policy failure.
		return "block", true
	}
	bestPriority := 0
	for _, filter := range f.filters {
		if f.matchFilter(content, filter) {
			priority := contentActionPriority(filter.filter.Action)
			if priority > bestPriority {
				action, matched, bestPriority = filter.filter.Action, true, priority
			}
		}
	}
	return action, matched
}

func contentActionPriority(action string) int {
	switch action {
	case "log":
		return 1
	case "warn":
		return 2
	case "block":
		return 3
	default:
		// Unknown actions are rejected by Worker and therefore must not be
		// hidden behind a lower-priority recognized action.
		return 4
	}
}

// matchFilter checks if content matches a filter.
func (f *ContentFilter) matchFilter(content string, filter compiledContentFilter) bool {
	switch filter.filter.Type {
	case "keyword":
		// Simple keyword matching
		lowerContent := strings.ToLower(content)
		for _, pattern := range filter.keywords {
			if strings.Contains(lowerContent, pattern) {
				return true
			}
		}

	case "regex":
		// Regex pattern matching
		for _, re := range filter.regexps {
			if re.MatchString(content) {
				return true
			}
		}
	}

	return false
}
