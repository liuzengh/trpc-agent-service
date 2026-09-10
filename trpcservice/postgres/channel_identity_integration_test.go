//go:build integration

package postgres_test

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	platformpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/postgres"
	platformsecret "github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestIdentityMapperPersistsScopedPrincipals(t *testing.T) {
	pool := openIntegrationPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, err := platformpostgres.New(pool)
	if err != nil {
		t.Fatalf("new postgres store: %v", err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	scope := tenant.Scope{TenantID: fmt.Sprintf("im04-%d", time.Now().UnixNano()), AppID: "support"}
	binding := seedIdentityMappingScope(t, ctx, store, scope, "wecom-binding", channels.ChannelWeCom)
	targetProtector := newIntegrationTargetProtector(t, "v1")
	mapper, err := platformpostgres.NewIdentityMapper(store, integrationExternalIDHasher{}, targetProtector, []string{"v1"})
	if err != nil {
		t.Fatalf("new identity mapper: %v", err)
	}

	direct, err := mapper.Map(ctx, platformpostgres.IdentityMappingRequest{
		Scope:                scope,
		BindingID:            binding.BindingID,
		Channel:              binding.Channel,
		Kind:                 channels.ConversationDirect,
		ExternalSenderID:     "user-1",
		ProviderSenderTarget: "user-target-1",
	})
	if err != nil {
		t.Fatalf("map direct message: %v", err)
	}
	directAgain, err := mapper.Map(ctx, platformpostgres.IdentityMappingRequest{
		Scope:                scope,
		BindingID:            binding.BindingID,
		Channel:              binding.Channel,
		Kind:                 channels.ConversationDirect,
		ExternalSenderID:     " user-1 ",
		ProviderSenderTarget: "user-target-1",
	})
	if err != nil {
		t.Fatalf("map direct message again: %v", err)
	}
	if direct.Identity.UserID != directAgain.Identity.UserID || direct.SessionPrincipalID != direct.Identity.UserID {
		t.Fatalf("direct mapping is unstable: %#v %#v", direct, directAgain)
	}
	if direct.Conversation != nil {
		t.Fatal("direct mapping created a conversation")
	}
	openedTarget, err := targetProtector.Open(ctx, channels.TargetContext{
		Scope:            scope,
		BindingID:        binding.BindingID,
		Channel:          binding.Channel,
		EntityType:       channels.TargetEntityIdentity,
		InternalEntityID: direct.Identity.UserID,
	}, channels.TargetPurposeIdentityUser, direct.Identity.ProviderTargetEnvelope)
	if err != nil {
		t.Fatalf("open persisted identity target: %v", err)
	}
	if openedTarget.ExternalUserID != "user-1" || openedTarget.ProviderTarget != "user-target-1" {
		t.Fatalf("opened persisted identity target = %#v", openedTarget)
	}

	group, err := mapper.Map(ctx, platformpostgres.IdentityMappingRequest{
		Scope:                      scope,
		BindingID:                  binding.BindingID,
		Channel:                    binding.Channel,
		Kind:                       channels.ConversationGroup,
		ExternalSenderID:           "user-1",
		ExternalChatID:             "chat-1",
		ProviderSenderTarget:       "user-target-1",
		ProviderConversationTarget: "chat-target-1",
	})
	if err != nil {
		t.Fatalf("map group message: %v", err)
	}
	if group.Identity.UserID != direct.Identity.UserID || group.Conversation == nil || group.SessionPrincipalID != group.Conversation.ConversationID {
		t.Fatalf("group mapping is invalid: %#v", group)
	}
	secondGroup, err := mapper.Map(ctx, platformpostgres.IdentityMappingRequest{
		Scope:                      scope,
		BindingID:                  binding.BindingID,
		Channel:                    binding.Channel,
		Kind:                       channels.ConversationGroup,
		ExternalSenderID:           "user-2",
		ExternalChatID:             "chat-1",
		ProviderSenderTarget:       "user-target-2",
		ProviderConversationTarget: "chat-target-1",
	})
	if err != nil {
		t.Fatalf("map second group member: %v", err)
	}
	if secondGroup.Conversation.ConversationID != group.Conversation.ConversationID {
		t.Fatalf("second group member mapping is invalid: %#v", secondGroup)
	}
	topic, err := mapper.Map(ctx, platformpostgres.IdentityMappingRequest{
		Scope:                scope,
		BindingID:            binding.BindingID,
		Channel:              binding.Channel,
		Kind:                 channels.ConversationTopic,
		ExternalSenderID:     "user-1",
		ExternalChatID:       "chat-1",
		ExternalThreadID:     "thread-1",
		ProviderSenderTarget: "user-target-1",
		ProviderThreadTarget: "thread-target-1",
	})
	if err != nil {
		t.Fatalf("map topic message: %v", err)
	}
	if topic.Conversation == nil || topic.Conversation.ConversationID == group.Conversation.ConversationID || topic.SessionPrincipalID != topic.Conversation.ConversationID {
		t.Fatalf("topic mapping is invalid: %#v", topic)
	}

	otherBinding := seedIdentityMappingBinding(t, ctx, store, scope, "feishu-binding", channels.ChannelFeishu)
	otherBindingMapping, err := mapper.Map(ctx, platformpostgres.IdentityMappingRequest{
		Scope:                scope,
		BindingID:            otherBinding.BindingID,
		Channel:              otherBinding.Channel,
		Kind:                 channels.ConversationDirect,
		ExternalSenderID:     "user-1",
		ProviderSenderTarget: "feishu-user-1",
	})
	if err != nil {
		t.Fatalf("map same user in another binding: %v", err)
	}
	if otherBindingMapping.Identity.UserID == direct.Identity.UserID {
		t.Fatal("same external user crossed binding scope")
	}

	otherAppScope := tenant.Scope{TenantID: scope.TenantID, AppID: "billing"}
	seedIdentityMappingApp(t, ctx, store, otherAppScope)
	otherAppBinding := seedIdentityMappingBinding(t, ctx, store, otherAppScope, "wecom-binding", channels.ChannelWeCom)
	otherAppMapping, err := mapper.Map(ctx, platformpostgres.IdentityMappingRequest{
		Scope:                otherAppScope,
		BindingID:            otherAppBinding.BindingID,
		Channel:              otherAppBinding.Channel,
		Kind:                 channels.ConversationDirect,
		ExternalSenderID:     "user-1",
		ProviderSenderTarget: "billing-user-target",
	})
	if err != nil {
		t.Fatalf("map same user in another app: %v", err)
	}
	if otherAppMapping.Identity.UserID == direct.Identity.UserID {
		t.Fatal("same external user crossed app scope")
	}

	otherTenantScope := tenant.Scope{TenantID: fmt.Sprintf("im04-other-tenant-%d", time.Now().UnixNano()), AppID: "support"}
	otherTenantBinding := seedIdentityMappingScope(t, ctx, store, otherTenantScope, "wecom-binding", channels.ChannelWeCom)
	otherTenantMapping, err := mapper.Map(ctx, platformpostgres.IdentityMappingRequest{
		Scope:                otherTenantScope,
		BindingID:            otherTenantBinding.BindingID,
		Channel:              otherTenantBinding.Channel,
		Kind:                 channels.ConversationDirect,
		ExternalSenderID:     "user-1",
		ProviderSenderTarget: "other-tenant-user-target",
	})
	if err != nil {
		t.Fatalf("map same user in another tenant: %v", err)
	}
	if otherTenantMapping.Identity.UserID == direct.Identity.UserID {
		t.Fatal("same external user crossed tenant scope")
	}
}

func TestIdentityMapperRevalidatesBindingChannelAndStatus(t *testing.T) {
	pool := openIntegrationPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, err := platformpostgres.New(pool)
	if err != nil {
		t.Fatalf("new postgres store: %v", err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	scope := tenant.Scope{TenantID: fmt.Sprintf("im04-revalidate-%d", time.Now().UnixNano()), AppID: "support"}
	binding := seedIdentityMappingScope(t, ctx, store, scope, "wecom-binding", channels.ChannelWeCom)
	mapper, err := platformpostgres.NewIdentityMapper(store, integrationExternalIDHasher{}, newIntegrationTargetProtector(t, "v1"), []string{"v1"})
	if err != nil {
		t.Fatalf("new identity mapper: %v", err)
	}
	request := platformpostgres.IdentityMappingRequest{
		Scope:                scope,
		BindingID:            binding.BindingID,
		Channel:              channels.ChannelFeishu,
		Kind:                 channels.ConversationDirect,
		ExternalSenderID:     "user-1",
		ProviderSenderTarget: "user-target-1",
	}
	if _, err := mapper.Map(ctx, request); !errors.Is(err, channels.ErrBindingChannelMismatch) {
		t.Fatalf("wrong channel error = %v, want channel mismatch", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE platform.channel_binding SET status = 'SUSPENDED' WHERE tenant_id = $1 AND app_id = $2 AND binding_id = $3`, scope.TenantID, scope.AppID, binding.BindingID); err != nil {
		t.Fatalf("suspend binding: %v", err)
	}
	request.Channel = channels.ChannelWeCom
	if _, err := mapper.Map(ctx, request); !errors.Is(err, channels.ErrBindingInactive) {
		t.Fatalf("suspended binding error = %v, want inactive", err)
	}
}

func TestIdentityMapperFindsMappingsAfterHMACKeyRotation(t *testing.T) {
	pool := openIntegrationPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, err := platformpostgres.New(pool)
	if err != nil {
		t.Fatalf("new postgres store: %v", err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	scope := tenant.Scope{TenantID: fmt.Sprintf("im04-rotation-%d", time.Now().UnixNano()), AppID: "support"}
	binding := seedIdentityMappingScope(t, ctx, store, scope, "wecom-binding", channels.ChannelWeCom)
	request := platformpostgres.IdentityMappingRequest{
		Scope:                scope,
		BindingID:            binding.BindingID,
		Channel:              binding.Channel,
		Kind:                 channels.ConversationDirect,
		ExternalSenderID:     "rotated-user",
		ProviderSenderTarget: "rotated-user-target",
	}
	v1Mapper, err := platformpostgres.NewIdentityMapper(store, integrationExternalIDHasher{}, newIntegrationTargetProtector(t, "v1"), []string{"v1"})
	if err != nil {
		t.Fatalf("new v1 identity mapper: %v", err)
	}
	first, err := v1Mapper.Map(ctx, request)
	if err != nil {
		t.Fatalf("map with v1 key: %v", err)
	}
	v2Mapper, err := platformpostgres.NewIdentityMapper(store, integrationExternalIDHasher{activeKeyVersion: "v2"}, newIntegrationTargetProtector(t, "v1"), []string{"v2", "v1"})
	if err != nil {
		t.Fatalf("new rotated identity mapper: %v", err)
	}
	second, err := v2Mapper.Map(ctx, request)
	if err != nil {
		t.Fatalf("map with rotated keys: %v", err)
	}
	if first.Identity.UserID != second.Identity.UserID {
		t.Fatalf("rotated mapping user IDs = %q and %q", first.Identity.UserID, second.Identity.UserID)
	}
	newMapping, err := v2Mapper.Map(ctx, platformpostgres.IdentityMappingRequest{
		Scope:                scope,
		BindingID:            binding.BindingID,
		Channel:              binding.Channel,
		Kind:                 channels.ConversationDirect,
		ExternalSenderID:     "new-v2-user",
		ProviderSenderTarget: "new-v2-user-target",
	})
	if err != nil {
		t.Fatalf("map new user with v2 key: %v", err)
	}
	if newMapping.Identity.KeyVersion != "v2" {
		t.Fatalf("new mapping key version = %q, want v2", newMapping.Identity.KeyVersion)
	}
}

func TestIdentityMapperConcurrentFirstInsertReturnsOnePrincipal(t *testing.T) {
	pool := openIntegrationPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, err := platformpostgres.New(pool)
	if err != nil {
		t.Fatalf("new postgres store: %v", err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	scope := tenant.Scope{TenantID: fmt.Sprintf("im04-concurrent-%d", time.Now().UnixNano()), AppID: "support"}
	binding := seedIdentityMappingScope(t, ctx, store, scope, "wecom-binding", channels.ChannelWeCom)
	mapper, err := platformpostgres.NewIdentityMapper(store, integrationExternalIDHasher{}, newIntegrationTargetProtector(t, "v1"), []string{"v1"})
	if err != nil {
		t.Fatalf("new identity mapper: %v", err)
	}
	request := platformpostgres.IdentityMappingRequest{
		Scope:                      scope,
		BindingID:                  binding.BindingID,
		Channel:                    binding.Channel,
		Kind:                       channels.ConversationGroup,
		ExternalSenderID:           "concurrent-user",
		ExternalChatID:             "concurrent-chat",
		ProviderSenderTarget:       "concurrent-user-target",
		ProviderConversationTarget: "concurrent-chat-target",
	}

	const workers = 8
	results := make(chan string, workers)
	errors := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			mapped, mapErr := mapper.Map(ctx, request)
			if mapErr != nil {
				errors <- mapErr
				return
			}
			results <- mapped.Conversation.ConversationID
		}()
	}
	wg.Wait()
	close(results)
	close(errors)
	for mapErr := range errors {
		t.Fatalf("concurrent mapping: %v", mapErr)
	}
	var conversationID string
	for result := range results {
		if conversationID == "" {
			conversationID = result
		} else if result != conversationID {
			t.Fatalf("concurrent conversation IDs = %q and %q", conversationID, result)
		}
	}
	var count int
	if err := pool.QueryRow(ctx, `
SELECT count(*)
FROM platform.channel_conversation
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = $3`,
		scope.TenantID, scope.AppID, binding.BindingID).Scan(&count); err != nil {
		t.Fatalf("count conversations: %v", err)
	}
	if count != 1 {
		t.Fatalf("conversation count = %d, want 1", count)
	}
	var identityCount int
	if err := pool.QueryRow(ctx, `
SELECT count(*)
FROM platform.channel_identity
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = $3`,
		scope.TenantID, scope.AppID, binding.BindingID).Scan(&identityCount); err != nil {
		t.Fatalf("count identities: %v", err)
	}
	if identityCount != 1 {
		t.Fatalf("identity count = %d, want 1", identityCount)
	}
}

func seedIdentityMappingScope(
	t *testing.T,
	ctx context.Context,
	store *platformpostgres.Store,
	scope tenant.Scope,
	bindingID string,
	channel channels.Channel,
) channels.Binding {
	t.Helper()
	if err := store.CreateTenant(ctx, tenant.Tenant{ID: scope.TenantID, Name: scope.TenantID, Status: tenant.StatusActive}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	seedIdentityMappingApp(t, ctx, store, scope)
	return seedIdentityMappingBinding(t, ctx, store, scope, bindingID, channel)
}

func seedIdentityMappingApp(
	t *testing.T,
	ctx context.Context,
	store *platformpostgres.Store,
	scope tenant.Scope,
) {
	t.Helper()
	config := integrationAppConfig("v1", "im04-model")
	config.TenantID = scope.TenantID
	config.AppID = scope.AppID
	if err := store.CreateAgentApp(ctx, tenant.AgentApp{
		TenantID: scope.TenantID, AppID: scope.AppID, Name: "IM04", ActiveConfigVersion: config.Version, Status: tenant.StatusActive,
	}, config); err != nil {
		t.Fatalf("create agent app: %v", err)
	}
}

func seedIdentityMappingBinding(
	t *testing.T,
	ctx context.Context,
	store *platformpostgres.Store,
	scope tenant.Scope,
	bindingID string,
	channel channels.Channel,
) channels.Binding {
	t.Helper()
	binding := channels.Binding{
		TenantID:        scope.TenantID,
		AppID:           scope.AppID,
		BindingID:       bindingID,
		Channel:         channel,
		ExternalAccount: bindingID + "-account",
		Secret:          tenant.SecretRef{Name: bindingID + "-secret", Version: "1"},
		Status:          channels.BindingActive,
	}
	publicRouteID, err := channels.NewPublicRouteID()
	if err != nil {
		t.Fatalf("generate public route id: %v", err)
	}
	binding.PublicRouteID = publicRouteID
	binding.BindingRevision = 1
	if err := store.CreateChannelBinding(ctx, binding); err != nil {
		t.Fatalf("create channel binding: %v", err)
	}
	return binding
}

type integrationExternalIDHasher struct {
	activeKeyVersion string
}

func (h integrationExternalIDHasher) Hash(ctx context.Context, scope tenant.Scope, bindingID string, kind channels.ExternalIDKind, externalID string) (string, string, error) {
	version := h.activeKeyVersion
	if version == "" {
		version = "v1"
	}
	return integrationHashWithVersion(ctx, scope, bindingID, kind, externalID, version)
}

func (integrationExternalIDHasher) HashWithVersion(ctx context.Context, scope tenant.Scope, bindingID string, kind channels.ExternalIDKind, externalID, keyVersion string) (string, error) {
	if keyVersion != "v1" && keyVersion != "v2" {
		return "", fmt.Errorf("unsupported test key version %q", keyVersion)
	}
	hash, _, err := integrationHashWithVersion(ctx, scope, bindingID, kind, externalID, keyVersion)
	return hash, err
}

func integrationHashWithVersion(_ context.Context, scope tenant.Scope, bindingID string, kind channels.ExternalIDKind, externalID, keyVersion string) (string, string, error) {
	normalized, err := channels.NormalizeExternalID(externalID)
	if err != nil {
		return "", "", err
	}
	digest := sha256.Sum256([]byte(scope.TenantID + "\x00" + scope.AppID + "\x00" + bindingID + "\x00" + keyVersion + "\x00" + string(kind) + "\x00" + normalized))
	return hex.EncodeToString(digest[:]), keyVersion, nil
}

func newIntegrationTargetProtector(t *testing.T, activeKeyVersion string) channels.TargetProtector {
	t.Helper()
	protector, err := platformsecret.NewAEADTargetProtector(integrationSecretProvider{}, activeKeyVersion)
	if err != nil {
		t.Fatalf("new integration target protector: %v", err)
	}
	return protector
}

type integrationSecretProvider struct{}

func (integrationSecretProvider) ResolveSecret(_ context.Context, scope tenant.Scope, ref tenant.SecretRef) (string, error) {
	digest := sha256.Sum256([]byte(scope.TenantID + "\x00" + scope.AppID + "\x00" + ref.Name + "\x00" + ref.Version))
	return base64.RawURLEncoding.EncodeToString(digest[:]), nil
}
