package session

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"
)

type SessionKey struct {
	TenantID        string
	Channel         string
	BindingID       string
	Scope           string
	MessageThreadID string
}

func NewSessionKey(tenantID, channel, bindingID, scope, threadID string) (SessionKey, error) {
	for name, value := range map[string]string{"tenant_id": tenantID, "channel": channel, "binding_id": bindingID, "scope": scope} {
		if err := validateID(value); err != nil {
			return SessionKey{}, fmt.Errorf("%w: %s: %v", ErrInvalidArgument, name, err)
		}
	}
	if !validScope(scope) {
		return SessionKey{}, fmt.Errorf("%w: scope must start with user: or group", ErrInvalidArgument)
	}
	if threadID != "" {
		if !strings.HasPrefix(scope, "group:") {
			return SessionKey{}, fmt.Errorf("%w: message_thread_id requires group scope", ErrInvalidArgument)
		}
		if err := validateID(threadID); err != nil {
			return SessionKey{}, fmt.Errorf("%w: message_thread_id: %v", ErrInvalidArgument, err)
		}
	}
	return SessionKey{TenantID: tenantID, Channel: channel, BindingID: bindingID, Scope: scope, MessageThreadID: threadID}, nil
}

func (k SessionKey) ID() string {
	sum := sha256.Sum256([]byte("v1|" + k.TenantID + "|" + k.Channel + "|" + k.BindingID + "|" + k.Scope + "|" + k.MessageThreadID))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

func (k SessionKey) String() string {
	return "v1:" + k.ID()
}

// CompatibleIDs returns the new ID first and the legacy ID used by existing channels second.
func (k SessionKey) CompatibleIDs() []string {
	legacyScope := strings.TrimPrefix(k.Scope, "user:")
	legacyChat := ""
	legacyUser := legacyScope
	if strings.HasPrefix(k.Scope, "group:") {
		legacyChat = strings.TrimPrefix(k.Scope, "group:")
		legacyUser = ""
	}
	return []string{k.ID(), LegacySessionID(k.TenantID, k.Channel, legacyUser, legacyChat)}
}

func UserScope(id string) string { return "user:" + id }

func GroupScope(id string) string { return "group:" + id }

func validScope(value string) bool {
	if strings.HasPrefix(value, "user:") {
		return validID(strings.TrimPrefix(value, "user:"))
	}
	if strings.HasPrefix(value, "group:") {
		return validID(strings.TrimPrefix(value, "group:"))
	}
	return false
}

func validID(value string) bool {
	return validateID(value) == nil
}

func LegacySessionID(tenantID, channel, userID, chatID string) string {
	scope := chatID
	if scope == "" {
		scope = userID
	}
	sum := sha256.Sum256([]byte(tenantID + "|" + channel + "|" + scope))
	return hex.EncodeToString(sum[:])
}
