package credentials_test

import (
	"context"
	"errors"
	"testing"

	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/credentials"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
)

type locatorFunc func(context.Context, channel.ReplyDestination) (credentials.Binding, error)

func (f locatorFunc) ResolveSendBinding(ctx context.Context, destination channel.ReplyDestination) (credentials.Binding, error) {
	return f(ctx, destination)
}

type providerFunc func(context.Context, secrets.Scope, secrets.SecretRef) (secrets.SecretValue, error)

func (f providerFunc) Resolve(ctx context.Context, scope secrets.Scope, ref secrets.SecretRef) (secrets.SecretValue, error) {
	return f(ctx, scope, ref)
}

func TestResolverUsesFrozenConfigAndChannelSendScope(t *testing.T) {
	destination := channel.ReplyDestination{TenantID: "tenant", Channel: "feishu", ChannelBindingID: "binding", ExternalAccountID: "app", ConfigVersion: 7}
	resolver := credentials.Resolver{Locator: locatorFunc(func(_ context.Context, got channel.ReplyDestination) (credentials.Binding, error) {
		if got != destination {
			t.Fatalf("destination=%#v", got)
		}
		return credentials.Binding{TenantID: "tenant", Channel: "feishu", ChannelBindingID: "binding", ExternalAccountID: "app", ConfigVersion: 7,
			SecretRef: secrets.SecretRef{Ref: "send", Version: 3}}, nil
	}), Secrets: providerFunc(func(_ context.Context, scope secrets.Scope, ref secrets.SecretRef) (secrets.SecretValue, error) {
		if scope.Purpose != secrets.PurposeChannelSend || scope.ResourceVersion != 7 || scope.Subject != "binding" || ref.Ref != "send" || ref.Version != 3 {
			t.Fatalf("scope=%#v ref=%#v", scope, ref)
		}
		return secrets.SecretValue{Bytes: []byte("credential"), Version: 3}, nil
	})}
	value, err := resolver.Resolve(context.Background(), destination)
	if err != nil || string(value.Bytes) != "credential" {
		t.Fatalf("value=%q err=%v", value.Bytes, err)
	}
}

func TestResolverRejectsLocatorCoordinateMismatch(t *testing.T) {
	destination := channel.ReplyDestination{TenantID: "tenant", Channel: "wecom", ChannelBindingID: "binding", ExternalAccountID: "corp", ConfigVersion: 2}
	resolver := credentials.Resolver{Locator: locatorFunc(func(context.Context, channel.ReplyDestination) (credentials.Binding, error) {
		return credentials.Binding{TenantID: "tenant", Channel: "wecom", ChannelBindingID: "binding", ExternalAccountID: "other", ConfigVersion: 2,
			SecretRef: secrets.SecretRef{Ref: "send", Version: 1}}, nil
	}), Secrets: providerFunc(func(context.Context, secrets.Scope, secrets.SecretRef) (secrets.SecretValue, error) {
		t.Fatal("secret provider must not run")
		return secrets.SecretValue{}, nil
	})}
	if _, err := resolver.Resolve(context.Background(), destination); !errors.Is(err, runtime.ErrTenantScope) {
		t.Fatalf("err=%v", err)
	}
}
