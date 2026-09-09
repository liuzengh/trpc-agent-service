package audit

import (
	"crypto/sha256"
	"encoding/hex"
	"regexp"
	"strings"
	"unicode/utf8"
)

const (
	RedactedValue       = "[REDACTED]"
	DefaultRedactBytes  = 64 * 1024
	DefaultFingerprintN = 16
)

// Redactor is the single bounded redaction implementation used by security
// boundaries. It is deliberately conservative: an uncertain value is
// replaced instead of being emitted to a log, audit record, or response.
type Redactor struct {
	MaxBytes int
	Secrets  []string
}

func NewRedactor(maxBytes int) Redactor {
	if maxBytes <= 0 {
		maxBytes = DefaultRedactBytes
	}
	return Redactor{MaxBytes: maxBytes}
}

func NewRedactorWithSecrets(maxBytes int, secrets []string) Redactor {
	redactor := NewRedactor(maxBytes)
	for _, secret := range secrets {
		if secret != "" && len(secret) <= redactor.MaxBytes {
			redactor.Secrets = append(redactor.Secrets, secret)
		}
	}
	return redactor
}

var defaultRedactor = NewRedactor(DefaultRedactBytes)

// RedactString removes common credential-bearing fragments and bounds the
// result. It does not claim to be a complete DLP or content moderation engine.
func (r Redactor) RedactString(input string) string {
	if r.MaxBytes <= 0 {
		r = defaultRedactor
	}
	value := input
	for _, secret := range r.Secrets {
		if secret != "" {
			value = strings.ReplaceAll(value, secret, RedactedValue)
		}
	}
	for _, pattern := range redactionPatterns {
		value = pattern.ReplaceAllString(value, "$1"+RedactedValue)
	}
	if sensitiveKey(value) {
		return RedactedValue
	}
	return boundString(value, r.MaxBytes)
}

func RedactString(input string) string { return defaultRedactor.RedactString(input) }

func RedactMetadata(input map[string]string) map[string]string {
	output := make(map[string]string, len(input))
	for key, value := range input {
		if sensitiveKey(key) || containsSensitiveFragment(value) {
			output[key] = RedactedValue
			continue
		}
		output[key] = defaultRedactor.RedactString(value)
	}
	return output
}

// Fingerprint returns a stable, non-reversible reference for bounded audit
// metadata. The domain separator prevents reuse as a raw content hash.
func Fingerprint(value string) string {
	digest := sha256.Sum256([]byte("trpc-agent/audit/v1|" + value))
	return "sha256:" + hex.EncodeToString(digest[:])[:DefaultFingerprintN]
}

// IsSensitiveKey is exported for guardrails that need the same classification
// as audit storage. Callers should still treat unknown fields conservatively.
func IsSensitiveKey(value string) bool { return sensitiveKey(value) }

func sensitiveKey(value string) bool {
	normalized := strings.ToLower(strings.ReplaceAll(strings.ReplaceAll(strings.TrimSpace(value), "-", "_"), " ", "_"))
	for _, candidate := range []string{
		"authorization", "api_key", "apikey", "access_token", "bot_token", "secret", "token", "password", "cookie", "private_key",
		"system_prompt", "prompt", "history", "webhook_body", "provider_request", "provider_response", "tool_input", "tool_output",
		"database_error", "redis_error", "object_error", "lease_token", "approval_token", "budget_token", "object_key", "vector_metadata",
	} {
		if normalized == candidate || strings.Contains(normalized, candidate) {
			return true
		}
	}
	return false
}

func containsSensitiveFragment(value string) bool {
	for _, pattern := range redactionPatterns {
		if pattern.MatchString(value) {
			return true
		}
	}
	return strings.Contains(strings.ToLower(value), "-----begin ")
}

func boundString(value string, maxBytes int) string {
	if maxBytes <= 0 || len(value) <= maxBytes {
		return value
	}
	marker := "...[TRUNCATED]"
	if maxBytes <= len(marker) {
		return marker[:maxBytes]
	}
	value = value[:maxBytes-len(marker)] + marker
	for !utf8.ValidString(value) {
		value = value[:len(value)-len(marker)-1] + marker
	}
	return value
}

var redactionPatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)(bearer\s+)[A-Za-z0-9._~+/=-]+`),
	regexp.MustCompile(`(?i)((?:authorization|api[_-]?key|access[_-]?token|bot[_-]?token|password|secret|lease[_-]?token|approval[_-]?token|budget[_-]?token)\s*[:=]\s*)[^\s,;]+`),
	regexp.MustCompile(`(?i)((?:postgres(?:ql)?|mysql|redis)://)[^\s/@]+(?::[^\s/@]*)?@`),
}
