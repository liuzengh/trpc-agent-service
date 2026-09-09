package main

import (
	"context"
	"fmt"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/admin"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/feishu"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/repository"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func feishuTenantPayload(tenantID string, version int, feishuAppID string, enabled bool) []byte {
	return []byte(fmt.Sprintf(`schema_version: 1
tenants:
- tenant_id: %s
  name: %s
  enabled: true
  config_version: %d
  audit: {enabled: true, retention_days: 30, store_content: false}
  apps:
  - app_id: assistant
    name: Assistant
    enabled: true
    config: {instruction: Help the user.}
    model: {provider: mock, name: mock}
    tools: {allow: [echo], deny: [], require_approval: []}
    channels:
    - binding_id: feishu-a
      type: feishu
      provider_account_id: %s
      token: {provider: env, key: PR10_FEISHU_VERIFICATION_TOKEN}
      secret: {provider: env, key: PR10_FEISHU_APP_SECRET}
      encryption_key: {provider: env, key: PR10_FEISHU_ENCRYPT_KEY}
      enabled: %t
    storage:
      session: {type: inmemory}
      memory: {type: inmemory}
      summary: {type: inmemory}
      artifact: {type: inmemory}
      knowledge: {type: inmemory}
      audit: {type: inmemory}
`, tenantID, tenantID, version, feishuAppID, enabled))
}

func newFeishuDeliveryRoutes(t *testing.T) (*publishedDeliveryRoutes, repository.Store, context.Context) {
	t.Helper()
	t.Setenv("PR10_FEISHU_VERIFICATION_TOKEN", "verification-fixture")
	t.Setenv("PR10_FEISHU_APP_SECRET", "app-secret-fixture")
	t.Setenv("PR10_FEISHU_ENCRYPT_KEY", "encrypt-fixture")
	store := repository.NewMemoryStore()
	published, err := config.NewPublishedCache(store)
	if err != nil {
		t.Fatal(err)
	}
	routes := &publishedDeliveryRoutes{published: published, senders: make(map[deliverySenderKey]channels.TextSender)}
	return routes, store, context.Background()
}

func feishuOutbound(version tenant.ConfigVersion) gateway.OutboundMessage {
	return gateway.OutboundMessage{TenantID: "tenant-a", AppID: "assistant", BindingID: "feishu-a", ConfigVersion: version, ExternalUserID: "ou_alice", Text: "hello"}
}

func publishPayload(t *testing.T, store repository.Store, ctx context.Context, tenantID string, expected tenant.ConfigVersion, payload []byte) {
	t.Helper()
	service, err := admin.NewService(store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Publish(ctx, tenantID, expected, payload); err != nil {
		t.Fatal(err)
	}
}

func TestFeishuSenderSwitchesWithPublishedVersion(t *testing.T) {
	routes, store, ctx := newFeishuDeliveryRoutes(t)
	publishPayload(t, store, ctx, "tenant-a", 0, feishuTenantPayload("tenant-a", 1, "cli_v1", true))

	senderV1, err := routes.Resolve(feishuOutbound(1))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := senderV1.(*feishu.Sender); !ok {
		t.Fatalf("sender type %T", senderV1)
	}

	// Publishing v2 swaps the sender for new requests while the v1 sender
	// stays pinned for old requests, all without a restart.
	publishPayload(t, store, ctx, "tenant-a", 1, feishuTenantPayload("tenant-a", 2, "cli_v2", true))
	senderV2, err := routes.Resolve(feishuOutbound(2))
	if err != nil {
		t.Fatal(err)
	}
	if senderV1 == senderV2 {
		t.Fatal("v2 must build a new sender")
	}
	againV1, err := routes.Resolve(feishuOutbound(1))
	if err != nil || againV1 != senderV1 {
		t.Fatal("v1 sender must stay cached for in-flight work")
	}

	// Disabling the binding in v3 rejects new deliveries but never touches v1.
	publishPayload(t, store, ctx, "tenant-a", 2, feishuTenantPayload("tenant-a", 3, "cli_v2", false))
	if _, err := routes.Resolve(feishuOutbound(3)); err == nil {
		t.Fatal("disabled binding must reject new deliveries")
	}
	if _, err := routes.Resolve(feishuOutbound(1)); err != nil {
		t.Fatal("old versions must keep their sender")
	}

	// Rollback creates a new immutable version whose content matches v1.
	service, err := admin.NewService(store)
	if err != nil {
		t.Fatal(err)
	}
	rolled, err := service.Rollback(ctx, "tenant-a", 3, 1)
	if err != nil {
		t.Fatal(err)
	}
	senderV4, err := routes.Resolve(feishuOutbound(rolled.Version))
	if err != nil {
		t.Fatalf("rollback version must resolve: %v", err)
	}
	if senderV4 == senderV2 {
		t.Fatal("rollback must not resurrect the disabled-era sender")
	}
}

func TestFeishuSenderBuildFailureKeepsPreviousSenders(t *testing.T) {
	routes, store, ctx := newFeishuDeliveryRoutes(t)
	publishPayload(t, store, ctx, "tenant-a", 0, feishuTenantPayload("tenant-a", 1, "cli_v1", true))
	if _, err := routes.Resolve(feishuOutbound(1)); err != nil {
		t.Fatal(err)
	}
	// v2 references a missing secret: the build fails and the pinned v1
	// sender keeps serving.
	t.Setenv("PR10_FEISHU_APP_SECRET", "")
	publishPayload(t, store, ctx, "tenant-a", 1, feishuTenantPayload("tenant-a", 2, "cli_v2", true))
	if _, err := routes.Resolve(feishuOutbound(2)); err == nil {
		t.Fatal("unresolvable secret must fail the sender build")
	}
	if _, err := routes.Resolve(feishuOutbound(1)); err != nil {
		t.Fatal("previous valid sender must survive a failed switch")
	}
}

func TestFeishuRouteKeysStayTenantScoped(t *testing.T) {
	routes, _, _ := newFeishuDeliveryRoutes(t)
	// Without a database the key list is empty; Resolve still pins per tenant.
	if _, err := routes.Resolve(gateway.OutboundMessage{TenantID: "", AppID: "assistant", BindingID: "feishu-a", ConfigVersion: 1, Text: "x", ExternalUserID: "u"}); err == nil {
		t.Fatal("empty tenant must fail")
	}
}
