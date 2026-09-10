// Package guardrail contains the platform's small, deterministic content
// safety boundary. It never returns the matched input or output text.
package guardrail

import (
	"errors"
	"regexp"
	"strings"

	"trpc.group/trpc-go/trpc-agent-go/model"
)

var ErrInputBlocked = errors.New("input blocked by guardrail")

const (
	RuleCredential       = "input.credential"
	RuleSensitiveField   = "input.sensitive_field"
	RuleHighRiskRequest  = "input.high_risk_request"
	RuleOutputCredential = "output.credential"
	RuleOutputHighRisk   = "output.high_risk"
)

type Decision struct {
	RuleID   string
	Reason   string
	Text     string
	Redacted bool
	Blocked  bool
}

type InputBlockedError struct{ RuleID string }

func (e InputBlockedError) Error() string {
	if e.RuleID == "" {
		return ErrInputBlocked.Error()
	}
	return ErrInputBlocked.Error() + ": " + e.RuleID
}

func (e InputBlockedError) Unwrap() error { return ErrInputBlocked }

var (
	credentialPatterns = []*regexp.Regexp{
		regexp.MustCompile(`(?i)\b(?:sk|rk)-[A-Za-z0-9_-]{16,}\b`),
		regexp.MustCompile(`\b(?:AKIA|ASIA)[A-Z0-9]{16}\b`),
		regexp.MustCompile(`\b(?:ghp|github_pat|xox[baprs])-[-A-Za-z0-9_]{12,}\b`),
		regexp.MustCompile(`\bAIza[0-9A-Za-z_-]{20,}\b`),
		regexp.MustCompile(`(?i)-----BEGIN [A-Z ]*(?:PRIVATE KEY|CERTIFICATE)-----`),
		regexp.MustCompile(`(?i)\b(?:bearer|basic)\s+[A-Za-z0-9._~+/=-]{12,}`),
		regexp.MustCompile(`(?i)\b(?:eyJ[A-Za-z0-9_-]+\.){2}[A-Za-z0-9_-]+\b`),
	}
	sensitiveFieldPattern = regexp.MustCompile(`(?i)\b(?:api[_ -]?key|access[_ -]?key|client[_ -]?secret|password|passwd|secret|token|authorization|credential|private[_ -]?key)\s*[:=]`)
	highRiskInputPattern  = regexp.MustCompile(`(?i)\b(?:steal|exfiltrat|dump|harvest|bypass|disable)\b.{0,48}\b(?:credential|password|secret|token|security|auth|detection)\b|\b(?:ransomware|credential stuffing|reverse shell|keylogger|deploy malware)\b`)
	highRiskOutputPattern = regexp.MustCompile(`(?i)\b(?:ransomware|credential stuffing|reverse shell|keylogger|deploy malware)\b|-----BEGIN [A-Z ]*PRIVATE KEY-----`)
	redactionPatterns     = []*regexp.Regexp{
		regexp.MustCompile(`(?i)(\b(?:api[_ -]?key|access[_ -]?key|client[_ -]?secret|password|passwd|secret|token|authorization|credential)\b\s*[:=]\s*)("[^"]*"|'[^']*'|[^\s,;]+)`),
		regexp.MustCompile(`(?i)\b(?:bearer|basic)\s+[A-Za-z0-9._~+/=-]{12,}`),
		regexp.MustCompile(`\b(?:sk|rk)-[A-Za-z0-9_-]{16,}\b`),
		regexp.MustCompile(`\b(?:AKIA|ASIA)[A-Z0-9]{16}\b`),
		regexp.MustCompile(`\b(?:ghp|github_pat|xox[baprs])-[-A-Za-z0-9_]{12,}\b`),
		regexp.MustCompile(`\bAIza[0-9A-Za-z_-]{20,}\b`),
		regexp.MustCompile(`(?i)-----BEGIN [A-Z ]*(?:PRIVATE KEY|CERTIFICATE)-----[\s\S]*?-----END [A-Z ]*(?:PRIVATE KEY|CERTIFICATE)-----`),
	}
)

// CheckInput rejects credentials and a deliberately small set of clearly
// dangerous requests before a model is invoked.
func CheckInput(text string) Decision {
	if text == "" {
		return Decision{}
	}
	for _, pattern := range credentialPatterns {
		if pattern.MatchString(text) {
			return Decision{RuleID: RuleCredential, Reason: "credential_detected", Blocked: true}
		}
	}
	if sensitiveFieldPattern.MatchString(text) {
		return Decision{RuleID: RuleSensitiveField, Reason: "sensitive_field_detected", Blocked: true}
	}
	if highRiskInputPattern.MatchString(text) {
		return Decision{RuleID: RuleHighRiskRequest, Reason: "high_risk_request", Blocked: true}
	}
	return Decision{}
}

// CheckInputMessage applies the input policy to every textual and raw content
// field that can reach a model. This includes hydrated attachment bytes; a
// caller must not be able to hide a credential in a content part.
func CheckInputMessage(message model.Message) Decision {
	decision := CheckInput(message.Content)
	decision = mergeDecision(decision, CheckInput(message.ReasoningContent))
	for _, part := range message.ContentParts {
		if part.Text != nil {
			decision = mergeDecision(decision, CheckInput(*part.Text))
		}
		if part.ContentRef != nil {
			decision = mergeDecision(decision, CheckInput(part.ContentRef.OriginalName))
		}
		if part.File != nil {
			decision = mergeDecision(decision, CheckInput(part.File.Name))
			decision = mergeDecision(decision, CheckInput(part.File.URL))
			decision = mergeDecision(decision, CheckInput(part.File.FileID))
			decision = mergeDecision(decision, CheckInput(string(part.File.Data)))
		}
		if part.Image != nil {
			decision = mergeDecision(decision, CheckInput(part.Image.URL))
			decision = mergeDecision(decision, CheckInput(string(part.Image.Data)))
		}
		if part.Audio != nil {
			decision = mergeDecision(decision, CheckInput(part.Audio.URL))
			decision = mergeDecision(decision, CheckInput(string(part.Audio.Data)))
		}
		if part.Video != nil {
			decision = mergeDecision(decision, CheckInput(part.Video.URL))
			decision = mergeDecision(decision, CheckInput(string(part.Video.Data)))
		}
	}
	for _, call := range message.ToolCalls {
		decision = mergeDecision(decision, CheckInput(string(call.Function.Arguments)))
	}
	return decision
}

// CheckInputRequest applies the input policy to every message in a model
// request, including content that was hydrated from an artifact reference.
func CheckInputRequest(request model.Request) Decision {
	decision := Decision{}
	for _, message := range request.Messages {
		decision = mergeDecision(decision, CheckInputMessage(message))
	}
	return decision
}

// SanitizeOutput redacts credentials and blocks clearly dangerous output.
// Blocked output is replaced by a safe fixed message at the reply boundary.
func SanitizeOutput(text string) Decision {
	if text == "" {
		return Decision{Text: text}
	}
	if highRiskOutputPattern.MatchString(text) {
		return Decision{RuleID: RuleOutputHighRisk, Reason: "high_risk_output", Text: "回复已被安全策略拦截。", Blocked: true}
	}
	redacted := text
	for _, pattern := range redactionPatterns {
		if pattern == redactionPatterns[0] {
			redacted = pattern.ReplaceAllString(redacted, `${1}[REDACTED]`)
		} else {
			redacted = pattern.ReplaceAllString(redacted, "[REDACTED]")
		}
	}
	if redacted != text {
		return Decision{RuleID: RuleOutputCredential, Reason: "credential_redacted", Text: redacted, Redacted: true}
	}
	return Decision{Text: text}
}

// SafeReason maps arbitrary detector details to a bounded stable category.
func SafeReason(reason string) string {
	reason = strings.TrimSpace(strings.ToLower(reason))
	switch reason {
	case "credential_detected", "sensitive_field_detected", "high_risk_request", "high_risk_output", "credential_redacted":
		return reason
	default:
		return "policy_blocked"
	}
}
