package idempotency

import "regexp"

var inboxCredentialPattern = regexp.MustCompile(`(?i)((?:"?(?:authorization|api[_-]?key|token|secret|password)"?)\s*[:=]\s*(?:"?bearer\s+)?["']?)[^\s,;}"']+`)

func sanitizeError(value string) string {
	return inboxCredentialPattern.ReplaceAllString(value, "[REDACTED]")
}
