package config

import (
	"errors"
	"os"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
)

func LoadSecretGrantsFromEnv() ([]secret.Grant, error) {
	var grants []secret.Grant
	if raw := strings.TrimSpace(os.Getenv("TRPC_AGENT_SECRET_GRANTS_JSON")); raw != "" {
		if err := decodeSecurityConfig(raw, &grants); err != nil {
			return nil, errors.New("invalid TRPC_AGENT_SECRET_GRANTS_JSON")
		}
	}
	if _, err := secret.NewEnvStore(grants); err != nil {
		return nil, err
	}
	return grants, nil
}

// SecretGrants narrows deployment grants to this process's responsibilities.
// This supplements, not replaces, per-role OS/container credential injection.
func (r Roles) SecretGrants(grants []secret.Grant) []secret.Grant {
	var result []secret.Grant
	for _, grant := range grants {
		allowed := false
		switch grant.Purpose {
		case secret.TelegramWebhook, secret.WeComCallback, secret.WeComAES, secret.WeComMCPRead:
			allowed = r.Gateway
		case secret.TelegramBot, secret.WeComApp, secret.WeComMCPSend:
			allowed = r.Sender
		case secret.Model:
			allowed = r.Worker || r.Jobs
		case secret.MCPServer, secret.TelegramMedia:
			allowed = r.Worker
		case secret.Session, secret.Memory, secret.Artifact, secret.Knowledge, secret.Embedding:
			allowed = r.Worker || r.Jobs || r.Admin
		}
		if allowed {
			result = append(result, grant)
		}
	}
	return result
}
