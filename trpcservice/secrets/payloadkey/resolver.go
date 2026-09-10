// Package payloadkey adapts ScopedSecretProvider to durable messaging payload
// encryption without exposing an unscoped secret lookup to storage code.
package payloadkey

import (
	"context"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
)

const ResourceID = "messaging-payload"

type Resolver struct {
	Provider secrets.Provider
	Ref      string
}

func New(provider secrets.Provider, ref string) (*Resolver, error) {
	if provider == nil || strings.TrimSpace(ref) != ref || ref == "" || strings.ContainsRune(ref, 0) {
		return nil, runtime.ErrInvariantViolation
	}
	return &Resolver{Provider: provider, Ref: ref}, nil
}

func (r *Resolver) ResolvePayloadKey(ctx context.Context, tenantID string, version int64) (messaging.PayloadCipherKey, error) {
	if r == nil || r.Provider == nil || r.Ref == "" {
		return messaging.PayloadCipherKey{}, runtime.ErrCapabilityUnsupported
	}
	scope := Scope(tenantID, version)
	ref := secrets.SecretRef{Ref: r.Ref, Version: version}
	if err := secrets.ValidateRequest(scope, ref); err != nil {
		return messaging.PayloadCipherKey{}, err
	}
	value, err := r.Provider.Resolve(ctx, scope, ref)
	if err != nil {
		clear(value.Bytes)
		return messaging.PayloadCipherKey{}, err
	}
	if value.Version != version {
		clear(value.Bytes)
		return messaging.PayloadCipherKey{}, runtime.ErrVersionMismatch
	}
	if len(value.Bytes) != 32 {
		clear(value.Bytes)
		return messaging.PayloadCipherKey{}, runtime.ErrCapabilityUnsupported
	}
	return messaging.PayloadCipherKey{Bytes: value.Bytes, Version: value.Version}, nil
}

func Scope(tenantID string, version int64) secrets.Scope {
	return secrets.Scope{TenantID: tenantID, Subject: tenantID, Purpose: secrets.PurposePayloadEncrypt,
		ResourceID: ResourceID, ResourceVersion: version}
}

var _ messaging.PayloadKeyResolver = (*Resolver)(nil)
