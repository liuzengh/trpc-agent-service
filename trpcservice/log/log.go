// Package log configures log levels and redaction for secrets.
package log

import (
	"regexp"
	"strings"
	"unicode"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

const safeErrorMaxLength = 512

var (
	toolArgumentPattern  = regexp.MustCompile(`(?i)(\btool[_ -]?(args?|arguments?)\b\s*[:=]\s*)[^\r\n;]+`)
	secretValuePattern   = regexp.MustCompile(`(?i)(\b(api[_-]?key|access[_-]?key|client[_-]?secret|secret([_-]?key)?|password|passwd|session[_-]?token|access[_-]?token|refresh[_-]?token|token|authorization|credential|dsn|connection[_-]?string)\b\s*[:=]\s*)("[^"]*"|'[^']*'|\{[^}]*\}|\[[^]]*\]|[^\s,;]+)`)
	authorizationPattern = regexp.MustCompile(`(?i)\b(bearer|basic)\s+[^\s,;]+`)
	urlCredentialPattern = regexp.MustCompile(`(?i)(://[^/\s:@]+:)[^@/\s]+(@)`)
	openAIKeyPattern     = regexp.MustCompile(`\bsk-[A-Za-z0-9_-]{16,}\b`)
	awsKeyPattern        = regexp.MustCompile(`\b(AKIA|ASIA)[A-Z0-9]{16}\b`)
	emailPattern         = regexp.MustCompile(`(?i)\b[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}\b`)
	phonePattern         = regexp.MustCompile(`(?:\+?86[- ]?)?1[3-9]\d{9}\b`)
)

// RoutingFields returns the allowlisted tenant routing fields safe for logs.
// It excludes user principals, messages, credentials, and tool arguments.
func RoutingFields(tc tenant.RuntimeContext) map[string]string {
	fields := map[string]string{
		"tenant_id":      tc.TenantID,
		"app_id":         tc.AppID,
		"config_version": tc.ConfigVersion,
		"session_id":     tc.SessionID,
		"trace_id":       tc.TraceID,
	}
	if tc.Channel != "" {
		fields["channel"] = tc.Channel
	}
	if tc.BindingID != "" {
		fields["binding_id"] = tc.BindingID
	}
	return fields
}

// SafeError returns bounded error text with common credentials, authorization
// values, URLs with passwords, and tool arguments redacted. It is intended for
// logs, traces, dead-letter records, and persisted failure summaries. Callers
// should retain the original error separately when they need errors.Is or
// errors.As semantics.
func SafeError(err error) string {
	if err == nil {
		return ""
	}
	value := strings.Map(func(r rune) rune {
		switch {
		case r == '\n' || r == '\r' || r == '\t':
			return ' '
		case unicode.IsControl(r):
			return -1
		default:
			return r
		}
	}, err.Error())
	value = strings.Join(strings.Fields(value), " ")
	value = toolArgumentPattern.ReplaceAllString(value, `${1}[REDACTED]`)
	value = authorizationPattern.ReplaceAllString(value, `[REDACTED]`)
	value = secretValuePattern.ReplaceAllString(value, `${1}[REDACTED]`)
	value = urlCredentialPattern.ReplaceAllString(value, `${1}[REDACTED]${2}`)
	value = openAIKeyPattern.ReplaceAllString(value, `[REDACTED]`)
	value = awsKeyPattern.ReplaceAllString(value, `[REDACTED]`)
	value = emailPattern.ReplaceAllString(value, `[REDACTED]`)
	value = phonePattern.ReplaceAllString(value, `[REDACTED]`)
	if len([]rune(value)) > safeErrorMaxLength {
		value = string([]rune(value)[:safeErrorMaxLength])
	}
	return value
}
