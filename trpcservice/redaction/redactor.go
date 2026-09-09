// Package redaction provides the process-wide, fail-safe secret redactor.
package redaction

import (
	"errors"
	"regexp"
	"strings"
)

const Replacement = "[REDACTED]"

var systemPatterns = []string{
	`(?i)(authorization\s*[:=]\s*(?:bearer\s+)?)[^\s,;]+`,
	`(?i)((?:cookie|token|secret|password|passwd|api[_-]?key)\s*[:=]\s*)(?:"[^"]*"|'[^']*'|[^\s,;]+)`,
	`(?i)(https?://[^\s/@:]+:)[^\s/@]+@`,
	`(?i)((?:redis|rediss|postgres|postgresql|mysql)://[^\s/@:]+:)[^\s/@]+@`,
	`(?i)(\b(?:bot|telegram)[_-]?token\s*[:=]\s*)[^\s,;]+`,
	`(?i)(\b(?:dsn|connection[_-]?string)\s*[:=]\s*)(?:"[^"]*"|'[^']*'|[^\s,;]+)`,
	`(?i)(https?://[^\s]+(?:token|secret|signature|sig)=[^\s&]+)`,
}

// SystemPatterns returns a copy of the rules that are always enabled.
func SystemPatterns() []string { return append([]string(nil), systemPatterns...) }

type Redactor struct{ patterns []*regexp.Regexp }

// New compiles system rules and tenant additions. Tenant patterns are limited
// by the control-plane model before this function is called, but the limits
// are repeated here so this package remains safe when used independently.
func New(extra []string) (Redactor, error) {
	if len(extra) > 32 {
		return Redactor{}, errors.New("too many redaction patterns")
	}
	patterns := make([]*regexp.Regexp, 0, len(systemPatterns)+len(extra))
	for _, raw := range append(SystemPatterns(), extra...) {
		if raw == "" || len(raw) > 256 {
			return Redactor{}, errors.New("invalid redaction pattern length")
		}
		compiled, err := regexp.Compile(raw)
		if err != nil {
			return Redactor{}, err
		}
		patterns = append(patterns, compiled)
	}
	return Redactor{patterns: patterns}, nil
}

func MustNew(extra []string) Redactor {
	r, err := New(extra)
	if err != nil {
		panic(err)
	}
	return r
}

func (r Redactor) Redact(value string) string {
	for _, pattern := range r.patterns {
		value = pattern.ReplaceAllStringFunc(value, func(string) string { return Replacement })
	}
	return value
}

func (r Redactor) RedactBytes(value []byte) []byte {
	return []byte(r.Redact(string(value)))
}

func (r Redactor) Empty() bool { return len(r.patterns) == 0 }

// Default is useful for logging paths where construction cannot fail.
var Default = MustNew(nil)

func NormalizePatterns(patterns []string) []string {
	seen := make(map[string]struct{}, len(patterns))
	out := make([]string, 0, len(patterns))
	for _, pattern := range patterns {
		pattern = strings.TrimSpace(pattern)
		if pattern == "" {
			continue
		}
		if _, ok := seen[pattern]; ok {
			continue
		}
		seen[pattern] = struct{}{}
		out = append(out, pattern)
	}
	return out
}
