package tenant

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strconv"
)

const (
	IdentityStatusActive   = "active"
	IdentityStatusDisabled = "disabled"
	IdentityScopePrivate   = "private"
	IdentityScopeGroup     = "group"
	IdentityScopeTopic     = "topic"
)

var (
	ErrIdentityInvalid        = errors.New("external identity is invalid")
	ErrIdentityNotFound       = errors.New("external identity is not mapped")
	ErrIdentityConflict       = errors.New("external identity mapping conflicts")
	ErrIdentityDisabled       = errors.New("external identity is disabled")
	ErrIdentityMismatch       = errors.New("external identity does not match tenant binding")
	ErrCallerIdentityOverride = errors.New("caller identity override is not allowed")
)

type Identity struct {
	TenantID         string
	ID               string
	Channel          string
	BindingID        string
	ExternalUserID   string
	InternalUserID   string
	Status           string
	Scope            string
	ExternalChat     string
	ExternalThreadID string
	Version          int64
}

type IdentityResolution struct {
	Identity Identity
	Scope    string
}

type IdentityRepository interface {
	ResolveIdentity(context.Context, TenantContext) (Identity, error)
}

type IdentityResolver interface {
	ResolveIdentity(context.Context, TenantContext) (IdentityResolution, error)
}

type RepositoryIdentityResolver struct{ Repository IdentityRepository }

func (r RepositoryIdentityResolver) ResolveIdentity(ctx context.Context, tc TenantContext) (IdentityResolution, error) {
	candidate, scope, err := identityCandidate(tc)
	if err != nil {
		return IdentityResolution{}, err
	}
	if r.Repository == nil {
		return IdentityResolution{Identity: candidate, Scope: scope}, nil
	}
	identity, err := r.Repository.ResolveIdentity(ctx, tc)
	if err != nil {
		return IdentityResolution{}, err
	}
	if identity.TenantID != candidate.TenantID || identity.Channel != candidate.Channel || identity.BindingID != candidate.BindingID || identity.ExternalUserID != candidate.ExternalUserID {
		return IdentityResolution{}, ErrIdentityMismatch
	}
	if identity.Scope != candidate.Scope || identity.ExternalChat != candidate.ExternalChat || identity.ExternalThreadID != candidate.ExternalThreadID {
		return IdentityResolution{}, ErrIdentityConflict
	}
	if identity.Status == IdentityStatusDisabled {
		return IdentityResolution{}, ErrIdentityDisabled
	}
	if err := identity.Validate(); err != nil {
		return IdentityResolution{}, ErrIdentityInvalid
	}
	return IdentityResolution{Identity: identity, Scope: scope}, nil
}

func DeriveIdentity(tc TenantContext) (Identity, string, error) {
	return identityCandidate(tc)
}
func identityCandidate(tc TenantContext) (Identity, string, error) {
	if err := tc.Validate(); err != nil || !validID(tc.TenantID) || !validID(tc.BindingID) || (tc.Channel != ChannelLark && tc.Channel != ChannelTelegram) || !validOpaqueID(tc.ExternalUser, 256) {
		return Identity{}, "", ErrIdentityInvalid
	}
	scope, err := conversationScope(tc)
	if err != nil {
		return Identity{}, "", err
	}
	identityID, internalID := stableIdentityIDs(tc.TenantID, tc.Channel, tc.BindingID, tc.ExternalUser)
	return Identity{TenantID: tc.TenantID, ID: identityID, Channel: tc.Channel, BindingID: tc.BindingID, ExternalUserID: tc.ExternalUser, InternalUserID: internalID, Status: IdentityStatusActive, Version: 1, Scope: scope, ExternalChat: tc.ExternalChat, ExternalThreadID: tc.ExternalThreadID}, scope, nil
}

func (i Identity) Validate() error {
	if !validOpaqueID(i.ExternalUserID, 256) || !validID(i.InternalUserID) || i.Scope == "" || len(i.Scope) > 512 || (i.ExternalChat != "" && !validOpaqueID(i.ExternalChat, 256)) || (i.ExternalThreadID != "" && !validOpaqueID(i.ExternalThreadID, 256)) {
		return ErrIdentityInvalid
	}
	if i.Status != IdentityStatusActive && i.Status != IdentityStatusDisabled {
		return ErrIdentityInvalid
	}
	if i.Version < 1 {
		return ErrIdentityInvalid
	}
	return nil
}

func conversationScope(tc TenantContext) (string, error) {
	if tc.Channel == ChannelTelegram {
		if !validOpaqueID(tc.ExternalChat, 256) {
			return "", ErrIdentityInvalid
		}
		switch tc.ExternalChatType {
		case "", "private":
			if tc.ExternalThreadID != "" {
				return "", ErrIdentityInvalid
			}
			return IdentityScopePrivate + ":" + tc.ExternalChat, nil
		case "group", "supergroup":
			if tc.ExternalThreadID != "" {
				parsed, err := strconv.ParseInt(tc.ExternalThreadID, 10, 64)
				if err != nil || parsed <= 0 || strconv.FormatInt(parsed, 10) != tc.ExternalThreadID {
					return "", ErrIdentityInvalid
				}
				return IdentityScopeTopic + ":" + tc.ExternalChat + ":" + tc.ExternalThreadID, nil
			}
			return IdentityScopeGroup + ":" + tc.ExternalChat, nil
		default:
			return "", ErrIdentityInvalid
		}
	}
	if tc.ExternalThreadID != "" || tc.ExternalChat == "" && tc.ExternalUser == "" {
		return "", ErrIdentityInvalid
	}
	if tc.ExternalChat != "" {
		return IdentityScopeGroup + ":" + tc.ExternalChat, nil
	}
	return IdentityScopePrivate + ":" + tc.ExternalUser, nil
}

func stableIdentityIDs(tenantID, channel, bindingID, externalUser string) (string, string) {
	digest := sha256.Sum256([]byte("identity|" + tenantID + "|" + channel + "|" + bindingID + "|" + externalUser))
	hexValue := hex.EncodeToString(digest[:])
	return "identity-" + hexValue[:32], "user-" + hexValue[32:64]
}

func SessionScope(tc TenantContext) (string, error) {
	_, scope, err := identityCandidate(tc)
	return scope, err
}

func (i IdentityResolution) Validate() error {
	if err := i.Identity.Validate(); err != nil || i.Scope == "" || len(i.Scope) > 512 {
		return ErrIdentityInvalid
	}
	return nil
}
