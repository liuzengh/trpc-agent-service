package audit

import "strings"

func RedactMetadata(input map[string]string) map[string]string {
	output := make(map[string]string, len(input))
	for key, value := range input {
		if sensitiveKey(key) || sensitiveKey(value) {
			output[key] = "[REDACTED]"
		} else {
			output[key] = value
		}
	}
	return output
}

func sensitiveKey(value string) bool {
	normalized := strings.ToLower(strings.ReplaceAll(strings.ReplaceAll(strings.TrimSpace(value), "-", "_"), " ", "_"))
	for _, candidate := range []string{"authorization", "api_key", "apikey", "secret", "token", "password", "cookie", "system_prompt", "prompt", "private_key"} {
		if normalized == candidate || strings.Contains(normalized, candidate) {
			return true
		}
	}
	return false
}
