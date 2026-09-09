package tenant

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func p104Binding(channel string, enabled bool) ChannelBinding {
	return ChannelBinding{TenantID: "tenant-p104", ID: "binding-" + channel, Channel: channel, ExternalAppID: "provider-" + channel, SecretRef: "env://P104_SYNTHETIC_SECRET", VerifyTokenRef: "env://P104_SYNTHETIC_VERIFY", Enabled: enabled}
}

func p104Context(channel, chat, thread string) TenantContext {
	return TenantContext{TenantID: "tenant-p104", AgentAppID: "agent-p104", BindingID: "binding-" + channel, Channel: channel, ExternalUser: "external-user", ExternalChat: chat, ExternalChatType: "supergroup", ExternalThreadID: thread, SessionID: "session-p104", RequestID: "request-p104", MessageID: "message-p104", TraceID: "trace-p104", ConfigVersion: 1, BackendPolicy: BackendPolicy{Session: "postgres", Memory: "postgres", Vector: "none", Object: "none"}}
}

func TestP104BindingContract(t *testing.T) {
	for _, channel := range []string{ChannelLark, ChannelTelegram} {
		if err := p104Binding(channel, true).Validate(); err != nil {
			t.Fatalf("valid %s binding rejected: %v", channel, err)
		}
	}
	invalid := []ChannelBinding{
		func() ChannelBinding { b := p104Binding(ChannelTelegram, true); b.Channel = "wecom"; return b }(),
		func() ChannelBinding { b := p104Binding(ChannelTelegram, true); b.TenantID = ""; return b }(),
		func() ChannelBinding { b := p104Binding(ChannelTelegram, true); b.ID = ""; return b }(),
		func() ChannelBinding { b := p104Binding(ChannelTelegram, false); b.Status = "unknown"; return b }(),
		func() ChannelBinding {
			b := p104Binding(ChannelTelegram, true)
			b.ExternalTargetType = BindingTargetChat
			return b
		}(),
	}
	for _, binding := range invalid {
		if err := binding.Validate(); err == nil {
			t.Errorf("invalid binding accepted: %+v", binding)
		}
	}
	if err := p104Binding(ChannelTelegram, false).IsUsableAt(time.Now()); !errors.Is(err, ErrBindingDisabled) {
		t.Fatalf("disabled binding error = %v", err)
	}
	expired := p104Binding(ChannelTelegram, false)
	expired.Status = BindingStatusExpired
	expired.ExpiresAt = time.Now().Add(-time.Minute)
	if err := expired.IsUsableAt(time.Now()); !errors.Is(err, ErrBindingExpired) {
		t.Fatalf("expired binding error = %v", err)
	}
}

func TestP104LifecycleAndSecretRef(t *testing.T) {
	binding := p104Binding(ChannelLark, false)
	binding.SecretRef = ""
	if _, err := binding.Enable(time.Now()); !errors.Is(err, ErrBindingSecretRequired) {
		t.Fatalf("enable without secret error = %v", err)
	}
	binding = p104Binding(ChannelLark, true).Canonical()
	disabled, err := binding.Disable(time.Now())
	if err != nil || disabled.Enabled || disabled.Status != BindingStatusDisabled || disabled.Version != 2 {
		t.Fatalf("disable result = %+v err=%v", disabled, err)
	}
	enabled, err := disabled.Enable(time.Now())
	if err != nil || !enabled.Enabled || enabled.Status != BindingStatusActive || enabled.Version != 3 {
		t.Fatalf("enable result = %+v err=%v", enabled, err)
	}
	for _, test := range []struct {
		ref  string
		want error
	}{{"", ErrSecretRefEmpty}, {"plain-secret", ErrSecretRefPlaintext}, {"vault://prod/key", ErrSecretRefUnsupportedScheme}, {"env://bad-name", ErrSecretRefMalformed}, {"env://", ErrSecretRefMalformed}} {
		if err := ValidateSecretRef(test.ref); !errors.Is(err, test.want) {
			t.Errorf("ref %q error=%v want=%v", test.ref, err, test.want)
		}
	}
	t.Setenv("P104_SYNTHETIC_SECRET", "synthetic-secret-value")
	value, err := (EnvironmentSecretResolver{}).Resolve(context.Background(), "env://P104_SYNTHETIC_SECRET")
	if err != nil || value != "synthetic-secret-value" {
		t.Fatalf("secret value=%q err=%v", value, err)
	}
	missing := "env://P104_MISSING_SECRET"
	if _, err := (EnvironmentSecretResolver{}).Resolve(context.Background(), missing); !errors.Is(err, ErrSecretValueMissing) || strings.Contains(err.Error(), missing) {
		t.Fatalf("secret error leaked reference: %v", err)
	}
}

func TestP104IdentityAndTopicScope(t *testing.T) {
	private := p104Context(ChannelTelegram, "100", "")
	private.ExternalChatType = "private"
	group := p104Context(ChannelTelegram, "-100", "")
	topic := p104Context(ChannelTelegram, "-100", "77")
	for _, test := range []struct {
		ctx  TenantContext
		want string
	}{{private, "private:100"}, {group, "group:-100"}, {topic, "topic:-100:77"}} {
		got, err := SessionScope(test.ctx)
		if err != nil || got != test.want {
			t.Errorf("scope=%q err=%v want=%q", got, err, test.want)
		}
	}
	one, _, err := DeriveIdentity(private)
	if err != nil {
		t.Fatal(err)
	}
	other := private
	other.TenantID = "tenant-other"
	two, _, err := DeriveIdentity(other)
	if err != nil || one.ID == two.ID || one.InternalUserID == two.InternalUserID {
		t.Fatalf("tenant identity collision: one=%+v two=%+v err=%v", one, two, err)
	}
}

func TestP104ResolverRejectsCallerTenantOverride(t *testing.T) {
	registry := NewMemoryRegistry()
	value := Tenant{ID: "tenant-p104", Name: "P104", Status: StatusActive, ConfigVersion: 1, DefaultAgentID: "agent-p104", Backend: BackendPolicy{Session: "postgres", Memory: "postgres", Vector: "none", Object: "none"}}
	if err := registry.PutTenant(value); err != nil {
		t.Fatal(err)
	}
	if err := registry.PutAgent(AgentApp{TenantID: value.ID, ID: value.DefaultAgentID, Name: "agent", Version: 1, Status: "active"}); err != nil {
		t.Fatal(err)
	}
	if err := registry.PutBinding(p104Binding(ChannelTelegram, true)); err != nil {
		t.Fatal(err)
	}
	request := ResolveRequest{TenantID: value.ID, Channel: ChannelTelegram, ExternalAppID: "provider-telegram", ExternalUser: "u", ExternalChat: "100", ExternalChatType: "private", RequestID: "r", MessageID: "m", TraceID: "t"}
	if _, err := (RegistryResolver{Registry: registry}).Resolve(context.Background(), request); !errors.Is(err, ErrCallerTenantOverride) {
		t.Fatalf("override error=%v", err)
	}
}

func TestP104ResolverFailsClosedOnSecretResolution(t *testing.T) {
	registry := NewMemoryRegistry()
	tenantValue := Tenant{ID: "tenant-secret", Name: "secret", Status: StatusActive, ConfigVersion: 1, DefaultAgentID: "agent-secret", Backend: BackendPolicy{Session: "memory", Memory: "memory", Vector: "none", Object: "none"}}
	if err := registry.PutTenant(tenantValue); err != nil {
		t.Fatal(err)
	}
	if err := registry.PutAgent(AgentApp{TenantID: tenantValue.ID, ID: tenantValue.DefaultAgentID, Name: "agent", Version: 1, Status: "active"}); err != nil {
		t.Fatal(err)
	}
	if err := registry.PutBinding(ChannelBinding{TenantID: tenantValue.ID, ID: "binding-secret", Channel: ChannelTelegram, ExternalAppID: "provider-secret", SecretRef: "env://P104_MISSING_SECRET", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	resolver := RegistryResolver{Registry: registry, SecretResolver: EnvironmentSecretResolver{}}
	_, err := resolver.Resolve(context.Background(), ResolveRequest{Channel: ChannelTelegram, ExternalAppID: "provider-secret", ExternalUser: "user-secret", ExternalChat: "100", ExternalChatType: "private", RequestID: "request-secret", MessageID: "message-secret", TraceID: "trace-secret"})
	if !errors.Is(err, ErrBindingNotFound) || strings.Contains(err.Error(), "P104_MISSING_SECRET") {
		t.Fatalf("missing secret resolution error=%v", err)
	}
}

func TestP104SecretSchemeIsValidatedButUnresolved(t *testing.T) {
	if err := ValidateSecretRef("secret://prod/lark"); err != nil {
		t.Fatalf("secret reference validation failed: %v", err)
	}
	if _, err := (EnvironmentSecretResolver{}).Resolve(context.Background(), "secret://prod/lark"); !errors.Is(err, ErrSecretRefUnsupportedScheme) {
		t.Fatalf("secret reference unexpectedly resolved: %v", err)
	}
}
