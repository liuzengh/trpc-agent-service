// Package identity derives tenant-scoped internal user and session IDs from
// verified provider identities without persisting raw external IDs as keys.
package identity

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"strings"

	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
)

type Result struct {
	UserID, SessionID string
}

type Mapper struct{ Secrets secrets.Provider }

func (m Mapper) Map(ctx context.Context, binding channel.VerifiedBinding, event channel.ProviderEvent) (Result, error) {
	if m.Secrets == nil || binding.TenantID == "" || binding.ChannelBindingID == "" || binding.Channel == "" ||
		binding.ExternalAccountID == "" || binding.TenantVersion < 1 || binding.BindingVersion < 1 ||
		binding.IdentitySecretRef.Ref == "" || binding.IdentitySecretRef.Version < 1 ||
		binding.SessionSecretRef.Ref == "" || binding.SessionSecretRef.Version < 1 ||
		event.Channel != binding.Channel || event.ExternalAccountID != binding.ExternalAccountID || event.ExternalUserID == "" {
		return Result{}, runtime.ErrInvariantViolation
	}
	identityKey, err := m.resolve(ctx, binding, secrets.PurposeTenantIdentity, binding.IdentitySecretRef)
	if err != nil {
		return Result{}, err
	}
	defer zero(identityKey)
	sessionKey, err := m.resolve(ctx, binding, secrets.PurposeTenantSession, binding.SessionSecretRef)
	if err != nil {
		return Result{}, err
	}
	defer zero(sessionKey)

	userID := derive("u1_", identityKey, "identity-v1", event.Channel, event.ExternalAccountID, event.ExternalUserID)
	var sessionID string
	switch strings.ToLower(event.ConversationType) {
	case "dm", "p2p":
		sessionID = derive("s1_", sessionKey, "session-v1", event.Channel, event.ExternalAccountID, "dm", event.ExternalUserID)
	case "group":
		if event.ExternalChatID == "" {
			return Result{}, runtime.ErrInvariantViolation
		}
		sessionID = derive("s1_", sessionKey, "session-v1", event.Channel, event.ExternalAccountID, "group", event.ExternalChatID)
	default:
		return Result{}, runtime.ErrInvariantViolation
	}
	return Result{UserID: userID, SessionID: sessionID}, nil
}

func (m Mapper) resolve(ctx context.Context, binding channel.VerifiedBinding, purpose secrets.Purpose, ref secrets.SecretRef) ([]byte, error) {
	value, err := m.Secrets.Resolve(ctx, secrets.Scope{
		TenantID: binding.TenantID, Subject: binding.TenantID, Purpose: purpose,
		ResourceID: binding.TenantID, ResourceVersion: ref.Version,
	}, ref)
	if err != nil {
		return nil, err
	}
	if value.Version != ref.Version || len(value.Bytes) == 0 {
		return nil, runtime.ErrVersionMismatch
	}
	return append([]byte(nil), value.Bytes...), nil
}

func derive(prefix string, key []byte, fields ...string) string {
	mac := hmac.New(sha256.New, key)
	var length [8]byte
	for _, field := range fields {
		binary.BigEndian.PutUint64(length[:], uint64(len(field)))
		_, _ = mac.Write(length[:])
		_, _ = mac.Write([]byte(field))
	}
	return prefix + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func zero(value []byte) {
	for index := range value {
		value[index] = 0
	}
}
