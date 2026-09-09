package log

import "regexp"

const redactedValue = "${REDACTED}"

// Redactor removes secrets before logs, traces, and audit persistence.
type Redactor interface {
	Redact(string) string
}

// PatternRedactor combines built-in secret patterns with caller patterns.
type PatternRedactor struct {
	patterns []*regexp.Regexp
}

// NewRedactor compiles a shared redactor. Invalid custom patterns are ignored
// so a bad tenant rule cannot disable built-in protection.
func NewRedactor(customPatterns ...string) *PatternRedactor {
	patterns := []*regexp.Regexp{
		regexp.MustCompile(`(?i)\b(api[_-]?key|token|password|secret)\b\s*[:=]\s*[^\s,;]+`),
		regexp.MustCompile(`(?i)\bBearer\s+[A-Za-z0-9._~+/=-]+`),
		regexp.MustCompile(`\bsk-[A-Za-z0-9_-]{8,}\b`),
	}
	for _, expression := range customPatterns {
		if pattern, err := regexp.Compile(expression); err == nil {
			patterns = append(patterns, pattern)
		}
	}
	return &PatternRedactor{patterns: patterns}
}

// Redact implements Redactor.
func (r *PatternRedactor) Redact(value string) string {
	for _, pattern := range r.patterns {
		value = pattern.ReplaceAllStringFunc(value, func(string) string {
			return redactedValue
		})
	}
	return value
}
