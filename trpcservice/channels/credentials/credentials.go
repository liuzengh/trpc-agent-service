// Package credentials resolves immutable, tenant-scoped channel_send secrets.
package credentials

import (
	"context"

	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
)

type Binding struct {
	TenantID, Channel, ChannelBindingID, ExternalAccountID string
	ConfigVersion                                          int64
	SecretRef                                              secrets.SecretRef
}

type Locator interface {
	ResolveSendBinding(context.Context, channel.ReplyDestination) (Binding, error)
}

type Resolver struct {
	Locator Locator
	Secrets secrets.Provider
}

func (r Resolver) Resolve(ctx context.Context, destination channel.ReplyDestination) (secrets.SecretValue, error) {
	if r.Locator == nil || r.Secrets == nil || !validDestination(destination) {
		return secrets.SecretValue{}, runtime.ErrInvariantViolation
	}
	binding, err := r.Locator.ResolveSendBinding(ctx, destination)
	if err != nil {
		return secrets.SecretValue{}, err
	}
	if binding.TenantID != destination.TenantID || binding.Channel != destination.Channel ||
		binding.ChannelBindingID != destination.ChannelBindingID || binding.ExternalAccountID != destination.ExternalAccountID ||
		binding.ConfigVersion != destination.ConfigVersion || binding.SecretRef.Ref == "" || binding.SecretRef.Version < 1 {
		return secrets.SecretValue{}, runtime.ErrTenantScope
	}
	value, err := r.Secrets.Resolve(ctx, secrets.Scope{TenantID: binding.TenantID, Subject: binding.ChannelBindingID,
		Purpose: secrets.PurposeChannelSend, ResourceID: binding.ChannelBindingID, ResourceVersion: binding.ConfigVersion}, binding.SecretRef)
	if err != nil {
		return secrets.SecretValue{}, err
	}
	if value.Version != binding.SecretRef.Version || len(value.Bytes) == 0 {
		clear(value.Bytes)
		return secrets.SecretValue{}, runtime.ErrVersionMismatch
	}
	return value, nil
}

func validDestination(value channel.ReplyDestination) bool {
	return value.TenantID != "" && value.Channel != "" && value.ChannelBindingID != "" && value.ExternalAccountID != "" && value.ConfigVersion >= 1
}
