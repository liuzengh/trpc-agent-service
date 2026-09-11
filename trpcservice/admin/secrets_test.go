package admin

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/wecommcp"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
)

func TestAdminRejectsUnassignedSecretsBeforeSaving(t *testing.T) {
	ctx := context.Background()
	data := controlplane.DefaultBootstrapData()
	repository := controlplane.NewMemoryRepository(data)
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(repository)
	service, _ := New(repository)
	store, _ := secret.NewEnvStore([]secret.Grant{
		{TenantID: "another-tenant", Purpose: secret.Model, Reference: "env://OTHER_KEY"},
		{TenantID: "tutorial-tenant", Purpose: secret.Memory, Reference: "env://DB_URL"},
	})
	service.WithSecretAuthorizer(store)
	revision := data.Revisions[0]
	revision.ID, revision.RevisionNo = "unauthorized-revision", 2
	revision.ModelConfig = json.RawMessage(`{"source":"revision","provider":"openai","name":"example","api_key_env":"OTHER_KEY"}`)
	if _, err := service.CreateRevision(ctx, revision); !errors.Is(err, secret.ErrForbidden) {
		t.Fatalf("revision: %v", err)
	}
	if _, err := repository.GetRevision(ctx, revision.TenantID, revision.ID); !errors.Is(err, controlplane.ErrNotFound) {
		t.Fatal("denied revision persisted")
	}
	revision.ModelConfig = json.RawMessage(`{"source":"revision","provider":"openai","name":"example","api_key_ref":"env://DB_URL"}`)
	if _, err := service.CreateRevision(ctx, revision); !errors.Is(err, secret.ErrForbidden) {
		t.Fatalf("purpose: %v", err)
	}
	backend := controlplane.BackendBinding{ID: "unauthorized-backend", TenantID: "tutorial-tenant", AppID: "tutorial-app", ResourceType: "session", BackendType: "postgres", Config: json.RawMessage(`{}`), SecretRef: "env://DB_URL"}
	if _, err := service.CreateBackendBinding(ctx, backend); !errors.Is(err, secret.ErrForbidden) {
		t.Fatalf("backend: %v", err)
	}
	bindings, _ := repository.ListBackendBindings(ctx, "tutorial-tenant", "tutorial-app")
	for _, b := range bindings {
		if b.ID == backend.ID {
			t.Fatal("denied backend persisted")
		}
	}
	backend.ID, backend.ResourceType = "allowed-backend", "memory"
	if _, err := service.CreateBackendBinding(ctx, backend); err != nil {
		t.Fatalf("authorized metadata must not need credential values: %v", err)
	}
}

func TestAdminWeComMCPRequiresBothGrantsAndStrictConfig(t *testing.T) {
	ctx := context.Background()
	repo := controlplane.NewMemoryRepository(controlplane.DefaultBootstrapData())
	t.Cleanup(func() { _ = repo.Close() })
	service, _ := New(repo)
	cfg := wecommcp.BindingConfig{AllowedChatIDs: []string{"group"}, AllowedUserIDs: []string{"human"}, MentionPrefix: "@bot", Timezone: "UTC", StartAt: "2026-09-06T00:00:00Z", DedupeMode: "fingerprint-v1"}
	raw, _ := json.Marshal(cfg)
	b := controlplane.ChannelBinding{ID: "mcp-binding", TenantID: "tutorial-tenant", AppID: "tutorial-app", AccountID: "mcp-bot", CallbackKey: "mcp-route", ChannelType: wecommcp.ChannelType, SecretRef: "env://MCP", Config: raw}
	readGrant := secret.Grant{TenantID: b.TenantID, Purpose: secret.WeComMCPRead, Reference: b.SecretRef}
	sendGrant := secret.Grant{TenantID: b.TenantID, Purpose: secret.WeComMCPSend, Reference: b.SecretRef}
	readOnly, _ := secret.NewEnvStore([]secret.Grant{readGrant})
	service.WithSecretAuthorizer(readOnly)
	if _, err := service.CreateChannelBinding(ctx, b); !errors.Is(err, secret.ErrForbidden) {
		t.Fatal("one-purpose grant allowed creation")
	}
	both, _ := secret.NewEnvStore([]secret.Grant{readGrant, sendGrant})
	service.WithSecretAuthorizer(both)
	created, err := service.CreateChannelBinding(ctx, b)
	if err != nil {
		t.Fatal(err)
	}
	service.WithSecretAuthorizer(secret.EnvStore{})
	if _, err := service.UpdateChannelBinding(ctx, b.TenantID, b.ID, json.RawMessage(`{"mcp_url":"credential-canary"}`), controlplane.StatusDisabled, created.Version); err == nil {
		t.Fatal("disabled binding bypassed config shape validation")
	}
	if _, err := service.UpdateChannelBinding(ctx, b.TenantID, b.ID, raw, controlplane.StatusDisabled, created.Version); err != nil {
		t.Fatal("cannot disable after credential revocation")
	}
}

func TestAdminChannelSecretUpdateIsAuthorized(t *testing.T) {
	ctx := context.Background()
	data := controlplane.DefaultBootstrapData()
	binding := controlplane.ChannelBinding{ID: "telegram-binding", TenantID: "tutorial-tenant", AppID: "tutorial-app", AccountID: "bot-account", ChannelType: "telegram", CallbackKey: "bot-callback", SecretRef: "env://BOT_KEY", Config: json.RawMessage(`{"bot_token_ref":"env://BOT_KEY","webhook_secret_ref":"env://WEBHOOK_KEY"}`)}
	repository := controlplane.NewMemoryRepository(data)
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(repository)
	service, _ := New(repository)
	if _, err := service.CreateChannelBinding(ctx, binding); !errors.Is(err, secret.ErrForbidden) {
		t.Fatalf("default authorize: %v", err)
	}
	store, _ := secret.NewEnvStore([]secret.Grant{
		{TenantID: binding.TenantID, Purpose: secret.TelegramBot, Reference: "env://BOT_KEY"},
		{TenantID: binding.TenantID, Purpose: secret.TelegramWebhook, Reference: "env://WEBHOOK_KEY"},
	})
	service.WithSecretAuthorizer(store)
	created, err := service.CreateChannelBinding(ctx, binding)
	if err != nil {
		t.Fatal(err)
	}
	badConfig := json.RawMessage(`{"bot_token_ref":"env://OTHER_KEY","webhook_secret_ref":"env://WEBHOOK_KEY"}`)
	if _, err := service.UpdateChannelBinding(ctx, binding.TenantID, binding.ID, badConfig, controlplane.StatusActive, created.Version); !errors.Is(err, secret.ErrForbidden) {
		t.Fatalf("update: %v", err)
	}
	current, _ := repository.GetChannelBinding(ctx, binding.TenantID, binding.ID)
	if current.Version != created.Version || string(current.Config) != string(created.Config) {
		t.Fatal("denied update mutated binding")
	}
	service.WithSecretAuthorizer(secret.EnvStore{})
	if _, err := service.UpdateChannelBinding(ctx, binding.TenantID, binding.ID, current.Config, controlplane.StatusDisabled, current.Version); err != nil {
		t.Fatalf("disable after revocation: %v", err)
	}
}
