// Package privacy enforces tenant text policies at model and IM boundaries.
package privacy

import (
	"errors"
	"github.com/cyl6/trpc-agent-service/trpcservice/config"
	"regexp"
	"sort"
	"strings"
)

var emailPattern = regexp.MustCompile(`(?i)[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}`)
var phonePattern = regexp.MustCompile(`(?:\+?86[- ]?)?1[3-9][0-9]{9}`)
var credentialPattern = regexp.MustCompile(`(?i)(api[_-]?key|token|secret|password)\s*[:=]\s*[^\s,;]+`)

func cleanPII(text string) string {
	text = emailPattern.ReplaceAllString(text, "[REDACTED]")
	var result strings.Builder
	start := 0
	for _, match := range phonePattern.FindAllStringIndex(text, -1) {
		digit := func(b byte) bool { return b >= '0' && b <= '9' }
		if (match[0] > 0 && digit(text[match[0]-1])) || (match[1] < len(text) && digit(text[match[1]])) {
			continue
		}
		result.WriteString(text[start:match[0]])
		result.WriteString("[REDACTED]")
		start = match[1]
	}
	result.WriteString(text[start:])
	return credentialPattern.ReplaceAllString(result.String(), "[REDACTED]")
}

var ErrBlocked = errors.New("privacy policy blocked content")
var ErrUnavailable = errors.New("privacy policy secret resolution unavailable")

// Apply detects email, bounded Chinese mobile numbers and credential fields, plus exact
// resolved secret values. It never logs content, matches, or secret references.
// Empty/off preserves backward compatibility for stored tenant revisions.
func Apply(mode, text string, tenant config.TenantConfig) (string, bool, error) {
	if mode == "" || mode == "off" {
		return text, false, nil
	}
	if mode != "redact" && mode != "block" {
		return "", false, ErrBlocked
	}
	var secrets []string
	// Migration credentials and disabled channels are not runtime secrets.
	tenant.Channels = append([]config.ChannelConfig(nil), tenant.Channels...)
	for i := range tenant.Channels {
		if !tenant.Channels[i].Enabled {
			tenant.Channels[i] = config.ChannelConfig{}
		}
	}
	for _, backend := range []*config.BackendConfig{&tenant.Data.Session, &tenant.Data.Memory, &tenant.Data.Summary, &tenant.Data.Artifact, &tenant.Data.Knowledge, &tenant.Data.AuditLog} {
		backend.MigrationDSNEnv = ""
		if backend.Type == "disabled" {
			*backend = config.BackendConfig{}
		}
	}
	for _, ref := range tenant.SecretEnvNames() {
		value, err := config.Secret(ref)
		if err != nil {
			if strings.HasPrefix(ref, "secret://") {
				return "", false, ErrUnavailable
			}
			// An unset local env has no resident secret to redact. Required backend
			// credentials are still validated by their normal runtime constructors.
			continue
		}
		if value != "" {
			secrets = append(secrets, value)
		}
	}
	sort.Slice(secrets, func(i, j int) bool { return len(secrets[i]) > len(secrets[j]) })
	clean := text
	for _, secret := range secrets {
		clean = strings.ReplaceAll(clean, secret, "[REDACTED]")
	}
	clean = cleanPII(clean)
	changed := clean != text
	if changed && mode == "block" {
		return "", true, ErrBlocked
	}
	return clean, changed, nil
}

// Framework model errors cross an Event boundary as text, losing Go wrapping.
// Recognize only our stable marker, never an arbitrary provider error body.
func IsBlocked(err error) bool {
	return err != nil && (errors.Is(err, ErrBlocked) || strings.Contains(err.Error(), ErrBlocked.Error()))
}
