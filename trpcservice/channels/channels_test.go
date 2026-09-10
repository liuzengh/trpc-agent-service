package channels_test

import (
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestBindingValidateAcceptsLongConnectionCredentials(t *testing.T) {
	for _, tc := range []struct {
		name    string
		channel channels.Channel
		account string
		secret  string
	}{
		{name: "wecom", channel: channels.ChannelWeCom, account: "bot-id", secret: "bot-secret"},
		{name: "feishu", channel: channels.ChannelFeishu, account: "app-id", secret: "app-secret"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			binding := validBinding()
			binding.Channel = tc.channel
			binding.ExternalAccount = tc.account
			binding.Secret = tenant.SecretRef{Name: tc.secret, Version: "v1"}
			binding.PublicRouteID = ""
			if err := binding.Validate(); err != nil {
				t.Fatalf("validate binding: %v", err)
			}
			if scope := binding.Scope(); scope.TenantID != "tenant-a" || scope.AppID != "support" {
				t.Fatalf("scope = %#v, want tenant-a/support", scope)
			}
		})
	}
}

func TestBindingValidateRequiresProviderSecret(t *testing.T) {
	for _, channel := range []channels.Channel{channels.ChannelWeCom, channels.ChannelFeishu} {
		binding := validBinding()
		binding.Channel = channel
		binding.Secret = tenant.SecretRef{}
		if err := binding.Validate(); err == nil {
			t.Fatalf("validate %s binding succeeded without provider secret", channel)
		}
	}
}

func TestBindingValidateRejectsIncompleteConfig(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*channels.Binding)
	}{
		{name: "tenant", mutate: func(b *channels.Binding) { b.TenantID = "" }},
		{name: "app", mutate: func(b *channels.Binding) { b.AppID = "" }},
		{name: "binding", mutate: func(b *channels.Binding) { b.BindingID = "" }},
		{name: "channel", mutate: func(b *channels.Binding) { b.Channel = channels.Channel("slack") }},
		{name: "external account", mutate: func(b *channels.Binding) { b.ExternalAccount = "" }},
		{name: "secret", mutate: func(b *channels.Binding) { b.Secret = tenant.SecretRef{} }},
		{name: "status", mutate: func(b *channels.Binding) { b.Status = channels.BindingStatus("DELETED") }},
		{name: "invalid route", mutate: func(b *channels.Binding) { b.PublicRouteID = "bad route" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			binding := validBinding()
			tt.mutate(&binding)
			if err := binding.Validate(); err == nil {
				t.Fatal("validate binding succeeded with invalid config")
			}
		})
	}
}

func TestBindingValidateAllowsLongConnectionWithoutPublicRoute(t *testing.T) {
	binding := validBinding()
	binding.PublicRouteID = ""
	if err := binding.Validate(); err != nil {
		t.Fatalf("validate long connection binding: %v", err)
	}
}

func TestChannelValidateProvisionableRejectsMissingIngress(t *testing.T) {
	if err := channels.ChannelWeChatCustomer.Validate(); err != nil {
		t.Fatalf("legacy channel should remain readable: %v", err)
	}
	if err := channels.ChannelWeChatCustomer.ValidateProvisionable(); err == nil {
		t.Fatal("channel without an ingress was accepted for provisioning")
	}
	for _, channel := range []channels.Channel{channels.ChannelWeCom, channels.ChannelFeishu} {
		if err := channel.ValidateProvisionable(); err != nil {
			t.Fatalf("provisionable channel %s was rejected: %v", channel, err)
		}
	}
}

func TestNewPublicRouteIDIsURLSafeAndUnpredictable(t *testing.T) {
	first, err := channels.NewPublicRouteID()
	if err != nil {
		t.Fatalf("generate first public route: %v", err)
	}
	second, err := channels.NewPublicRouteID()
	if err != nil {
		t.Fatalf("generate second public route: %v", err)
	}
	if first == second {
		t.Fatal("generated public routes are duplicated")
	}
	for _, route := range []string{first, second} {
		if err := channels.ValidatePublicRouteID(route); err != nil {
			t.Fatalf("validate generated public route %q: %v", route, err)
		}
	}
}

func validBinding() channels.Binding {
	return channels.Binding{
		TenantID:        "tenant-a",
		AppID:           "support",
		BindingID:       "binding-1",
		Channel:         channels.ChannelWeCom,
		ExternalAccount: "bot-id",
		Secret:          tenant.SecretRef{Name: "bot-secret", Version: "v1"},
		PublicRouteID:   "route-binding-1",
		BindingRevision: 1,
		Status:          channels.BindingActive,
	}
}
