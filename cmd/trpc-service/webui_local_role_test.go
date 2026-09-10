package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agentapp"
	agentmemory "github.com/liuzengh/trpc-agent-service/trpcservice/agentapp/inmemory"
	configdomain "github.com/liuzengh/trpc-agent-service/trpcservice/config"
	configmemory "github.com/liuzengh/trpc-agent-service/trpcservice/config/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	governancememory "github.com/liuzengh/trpc-agent-service/trpcservice/governance/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/migration/knowledgedriver"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
	secretfs "github.com/liuzengh/trpc-agent-service/trpcservice/secrets/filesystem"
	serviceknowledge "github.com/liuzengh/trpc-agent-service/trpcservice/storage/knowledge"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	tenantmemory "github.com/liuzengh/trpc-agent-service/trpcservice/tenant/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tool/localnote"
)

func TestLocalFixtureMetadataDigestMatchesKnowledgeImage(t *testing.T) {
	content := "fixture content"
	metadata := map[string]string{"title": "Fixture title"}
	digest, err := localFixtureMetadataDigest(metadata)
	if err != nil {
		t.Fatal(err)
	}
	image := serviceknowledge.ChunkRecord{
		TenantID: "tenant", KnowledgeID: "knowledge", KnowledgeVersion: 1, ChunkID: "chunk",
		SourceDigest: localFixtureDigest("source"), ContentDigest: localFixtureDigest(content), MetadataDigest: digest,
		EmbeddingProfileID: "embedder", EmbeddingVersion: 1, VectorGeneration: "generation",
		Content: content, Metadata: metadata, Vector: []float32{0.5}, CreatedAt: time.Now().UTC(),
	}.ChunkImage()
	if _, err := knowledgedriver.ImageDigest(image); err != nil {
		t.Fatalf("fixture image violates the durable metadata digest contract: %v", err)
	}
}

func TestLoadWebUILocalConfigDefaultsAndRejectsUnsafeInput(t *testing.T) {
	base := map[string]string{"TRPC_POSTGRES_DSN": "postgres://local", "TRPC_REDIS_ADDRESS": "redis:6379"}
	value, err := loadWebUILocalConfig(mapEnvironment(base))
	if err != nil {
		t.Fatal(err)
	}
	if value.ListenAddress != ":8080" || value.RouteKey != webUILocalRouteKey || value.Token != webUILocalToken || value.InstanceID != "standalone" ||
		value.APIKeyFile != "/run/secrets/deepseek_api_key" || value.ExclusiveRuntime {
		t.Fatalf("unexpected defaults: %+v", value)
	}
	exclusive := cloneEnvironment(base)
	exclusive["TRPC_WEBUI_LOCAL_EXCLUSIVE_RUNTIME"] = "true"
	configured, err := loadWebUILocalConfig(mapEnvironment(exclusive))
	if err != nil || !configured.ExclusiveRuntime {
		t.Fatalf("exclusive config=%+v err=%v", configured, err)
	}
	for _, item := range []struct{ name, value string }{
		{"TRPC_WEBUI_LOCAL_TOKEN", "short"},
		{"TRPC_WEBUI_DEEPSEEK_KEY_FILE", "relative/key"},
		{"TRPC_WEBUI_LOCAL_SECRET_ROOT", "relative/root"},
		{"TRPC_WEBUI_LOCAL_INSTANCE_ID", "node_a"},
	} {
		candidate := cloneEnvironment(base)
		candidate[item.name] = item.value
		if _, err := loadWebUILocalConfig(mapEnvironment(candidate)); err == nil {
			t.Fatalf("%s=%q accepted", item.name, item.value)
		}
	}
	feishu := cloneEnvironment(base)
	feishu["TRPC_FEISHU_LOCAL_ENABLED"] = "true"
	if _, err := loadWebUILocalConfig(mapEnvironment(feishu)); err == nil {
		t.Fatal("incomplete Feishu local configuration accepted")
	}
	feishu["FEISHU_APP_ID"] = "cli_local"
	feishu["FEISHU_APP_SECRET"] = "local-app-secret"
	feishu["FEISHU_VERIFICATION_TOKEN"] = "local-verification-token"
	feishu["FEISHU_ENCRYPT_KEY"] = "local-encrypt-key"
	configured, err = loadWebUILocalConfig(mapEnvironment(feishu))
	if err != nil || !configured.FeishuEnabled || configured.FeishuBotOpenID != "" {
		t.Fatalf("configured=%+v err=%v", configured, err)
	}
	feishu["FEISHU_BOT_OPEN_ID"] = "ou_local_bot"
	configured, err = loadWebUILocalConfig(mapEnvironment(feishu))
	if err != nil || configured.FeishuBotOpenID != "ou_local_bot" {
		t.Fatalf("configured=%+v err=%v", configured, err)
	}
	wecom := cloneEnvironment(base)
	wecom["TRPC_WECOM_LOCAL_ENABLED"] = "true"
	if _, err := loadWebUILocalConfig(mapEnvironment(wecom)); err == nil {
		t.Fatal("incomplete WeCom local configuration accepted")
	}
	wecom["WECOM_CORP_ID"] = "ww_local"
	wecom["WECOM_APP_SECRET"] = "local-corp-secret"
	wecom["WECOM_CALLBACK_TOKEN"] = "local-callback-token"
	wecom["WECOM_ENCODING_AES_KEY"] = "local-encoding-aes-key"
	for _, agentID := range []string{"", "0", "-1000002", "not-a-number"} {
		wecom["WECOM_AGENT_ID"] = agentID
		if _, err := loadWebUILocalConfig(mapEnvironment(wecom)); err == nil {
			t.Fatalf("WECOM_AGENT_ID=%q accepted", agentID)
		}
	}
	wecom["WECOM_AGENT_ID"] = "1000002"
	configured, err = loadWebUILocalConfig(mapEnvironment(wecom))
	if err != nil || !configured.WeComEnabled || configured.WeComCorpID != "ww_local" || configured.WeComAgentID != 1000002 {
		t.Fatalf("configured=%+v err=%v", configured, err)
	}
}

func TestWriteLocalSecretUsesScopedStableFilenameAndRejectsDrift(t *testing.T) {
	root := t.TempDir()
	scope := secrets.Scope{TenantID: webUILocalTenantID, Subject: webUILocalTenantID,
		Purpose: secrets.PurposePayloadEncrypt, ResourceID: "messaging-payload", ResourceVersion: 1}
	ref := secrets.SecretRef{Ref: payloadKeyRef, Version: 1}
	value := bytes.Repeat([]byte{0x5a}, 32)
	if err := writeLocalSecret(root, scope, ref, value); err != nil {
		t.Fatal(err)
	}
	name, err := secretfs.StableFilename(scope, ref)
	if err != nil {
		t.Fatal(err)
	}
	stored, err := os.ReadFile(filepath.Join(root, name))
	if err != nil || !bytes.Equal(stored, value) {
		t.Fatalf("stored=%x err=%v", stored, err)
	}
	info, err := os.Stat(filepath.Join(root, name))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("secret mode=%v", info.Mode().Perm())
	}
	if err := writeLocalSecret(root, scope, ref, bytes.Repeat([]byte{0x6b}, 32)); err == nil {
		t.Fatal("same generation secret drift accepted")
	}
}

type webUIPolicyMemory struct{ *governancememory.Store }

func (s webUIPolicyMemory) PublishPolicy(ctx context.Context, value governance.PolicySnapshot) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.Store.PublishPolicy(value)
}

func TestEnsureWebUILocalToolControlPlaneUpgradesOnce(t *testing.T) {
	ctx := context.Background()
	tenants := tenantmemory.New()
	apps := agentmemory.New()
	configs := configmemory.New(tenants, apps)
	policies := webUIPolicyMemory{governancememory.New(0, 0)}
	tenantMetadata := tenant.ChangeMetadata{ActorType: "system", ActorID: "test", ReasonCode: "seed", CorrelationID: "seed", TraceID: "seed"}
	root, err := tenants.Create(ctx, tenant.CreateInput{Tenant: tenant.Tenant{TenantID: webUILocalTenantID,
		TenantKey: "webui-local", DisplayName: "WebUI Local"}, ChangeMetadata: tenantMetadata})
	if err != nil {
		t.Fatal(err)
	}
	appMetadata := agentapp.ChangeMetadata{ActorType: "system", ActorID: "test", Reason: "seed", CorrelationID: "seed", TraceID: "seed"}
	app, err := apps.Create(ctx, agentapp.CreateInput{App: agentapp.AgentApp{TenantID: webUILocalTenantID,
		AgentAppID: webUILocalAppID, AgentAppKey: "assistant", DisplayName: "Assistant"}, ChangeMetadata: appMetadata})
	if err != nil {
		t.Fatal(err)
	}
	draft, err := apps.CreateDraft(ctx, agentapp.CreateDraftInput{TenantID: webUILocalTenantID, AgentAppID: webUILocalAppID,
		ExpectedAppVersion: app.Version, Revision: agentapp.Revision{AgentKind: agentapp.AgentKindLLM,
			Instruction: "old instruction", ModelProfileID: webUILocalModelID, ModelProfileVersion: 1}, ChangeMetadata: appMetadata})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = apps.Publish(ctx, agentapp.PublishInput{TenantID: webUILocalTenantID, AgentAppID: webUILocalAppID,
		Revision: draft.Revision, ExpectedAppVersion: app.Version + 1, ExpectedDraftVersion: draft.DraftVersion,
		ChangeMetadata: appMetadata}); err != nil {
		t.Fatal(err)
	}
	// This is the pre-upgrade local policy shape. It deliberately lacks the
	// allowed model and uses deny-by-default so the regression proves bootstrap
	// upgrades every prerequisite needed for an actual model call, not merely
	// the tool confirmation rule.
	oldPolicy := governance.PolicyV1{SchemaVersion: 1, DefaultAction: governance.ActionDeny,
		InputDLP: governance.DLPDisabled, OutputDLP: governance.DLPDisabled}
	digest, _, err := governance.PolicyDigest(oldPolicy)
	if err != nil {
		t.Fatal(err)
	}
	if err = policies.PublishPolicy(ctx, governance.PolicySnapshot{TenantID: webUILocalTenantID, Version: 1,
		SchemaVersion: 1, Policy: oldPolicy, ContentDigest: digest, PublishedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	published, err := configs.Publish(ctx, configdomain.PublishInput{TenantID: webUILocalTenantID,
		ExpectedTenantVersion: root.Version, Metadata: tenantMetadata,
		Payload: configdomain.ConfigV1{SchemaVersion: 1, DefaultAgentAppID: webUILocalAppID, PolicyVersion: 1}})
	if err != nil {
		t.Fatal(err)
	}
	root, snapshot, err := ensureWebUILocalToolControlPlane(ctx, tenants, apps, configs, policies, published.Tenant, published.Snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Payload.PolicyVersion != 2 || root.ActiveConfigVersion != snapshot.ConfigVersion {
		t.Fatalf("root=%#v snapshot=%#v", root, snapshot)
	}
	upgradedPolicy, err := policies.GetPolicy(ctx, webUILocalTenantID, 2)
	if err != nil || !webUILocalPolicyReady(upgradedPolicy.Policy) {
		t.Fatalf("policy=%#v err=%v", upgradedPolicy, err)
	}
	upgradedApp, err := apps.Get(ctx, webUILocalTenantID, webUILocalAppID)
	if err != nil {
		t.Fatal(err)
	}
	upgradedRevision, err := apps.GetRevision(ctx, webUILocalTenantID, webUILocalAppID, upgradedApp.CurrentRevision)
	if err != nil {
		t.Fatal(err)
	}
	childApp, err := apps.Get(ctx, webUILocalTenantID, webUILocalChildAppID)
	if err != nil {
		t.Fatal(err)
	}
	childRevision, err := apps.GetRevision(ctx, webUILocalTenantID, webUILocalChildAppID, childApp.CurrentRevision)
	if err != nil || !webUILocalGraphRevisionReady(upgradedRevision, childRevision) || !webUILocalLLMRevisionReady(childRevision) ||
		childRevision.ModelProfileVersion != webUILocalModelVersion || len(childRevision.ToolRefs) != 1 || childRevision.ToolRefs[0].ID != localnote.ID {
		t.Fatalf("revision=%#v err=%v", upgradedRevision, err)
	}
	priorRevision, priorAppVersion, priorChildVersion, priorConfigVersion, priorTenantVersion :=
		upgradedApp.CurrentRevision, upgradedApp.Version, childApp.Version, snapshot.ConfigVersion, root.Version
	root, snapshot, err = ensureWebUILocalToolControlPlane(ctx, tenants, apps, configs, policies, root, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	stableApp, err := apps.Get(ctx, webUILocalTenantID, webUILocalAppID)
	stableChild, childErr := apps.Get(ctx, webUILocalTenantID, webUILocalChildAppID)
	if err != nil || stableApp.CurrentRevision != priorRevision || stableApp.Version != priorAppVersion ||
		childErr != nil || stableChild.Version != priorChildVersion || snapshot.ConfigVersion != priorConfigVersion || root.Version != priorTenantVersion {
		t.Fatalf("non-idempotent app=%#v root=%#v snapshot=%#v err=%v", stableApp, root, snapshot, err)
	}
	feishu := webUILocalConfig{FeishuEnabled: true, FeishuAppID: "cli_local", FeishuAppSecret: "app-secret",
		FeishuVerificationToken: "verification-token", FeishuEncryptKey: "encrypt-key", FeishuBotOpenID: "ou_bot"}
	root, snapshot, err = ensureWebUILocalFeishuBinding(ctx, configs, root, snapshot, feishu)
	if err != nil || len(snapshot.Payload.ChannelBindings) != 1 {
		t.Fatalf("Feishu binding snapshot=%#v err=%v", snapshot, err)
	}
	binding := snapshot.Payload.ChannelBindings[0]
	if binding.BindingID != feishuLocalBindingID || binding.Channel != "feishu" || binding.ExternalAccountID != feishu.FeishuAppID ||
		binding.SendSecretRef.Ref != "secret://local/feishu-send" {
		t.Fatalf("binding=%#v", binding)
	}
	feishuConfigVersion, feishuTenantVersion := snapshot.ConfigVersion, root.Version
	root, snapshot, err = ensureWebUILocalFeishuBinding(ctx, configs, root, snapshot, feishu)
	if err != nil || snapshot.ConfigVersion != feishuConfigVersion || root.Version != feishuTenantVersion {
		t.Fatalf("non-idempotent Feishu binding root=%#v snapshot=%#v err=%v", root, snapshot, err)
	}
	rotated := feishu
	rotated.FeishuAppID = "cli_rotated"
	root, snapshot, err = ensureWebUILocalFeishuBinding(ctx, configs, root, snapshot, rotated)
	if err != nil || snapshot.ConfigVersion != feishuConfigVersion+1 || root.Version != feishuTenantVersion+1 ||
		snapshot.Payload.ChannelBindings[0].ExternalAccountID != rotated.FeishuAppID {
		t.Fatalf("rotated binding root=%#v snapshot=%#v err=%v", root, snapshot, err)
	}
	if _, snapshot, err = ensureWebUILocalWeComBinding(ctx, configs, root, snapshot, webUILocalConfig{}); err != nil {
		t.Fatal(err)
	}
	wecomDisabledVersion := snapshot.ConfigVersion
	wecom := webUILocalConfig{WeComEnabled: true, WeComCorpID: "ww_local", WeComAppSecret: "wecom-corp-secret",
		WeComCallbackToken: "wecom-callback-token", WeComEncodingAESKey: "wecom-encoding-aes-key", WeComAgentID: 1000002}
	root, snapshot, err = ensureWebUILocalWeComBinding(ctx, configs, root, snapshot, wecom)
	if err != nil || len(snapshot.Payload.ChannelBindings) != 2 || snapshot.ConfigVersion != wecomDisabledVersion+1 {
		t.Fatalf("WeCom binding snapshot=%#v err=%v", snapshot, err)
	}
	wecomBinding := snapshot.Payload.ChannelBindings[1]
	if wecomBinding.BindingID != wecomLocalBindingID || wecomBinding.Channel != "wecom" || wecomBinding.AgentAppID != webUILocalAppID ||
		wecomBinding.ExternalAccountID != wecom.WeComCorpID ||
		wecomBinding.SecretRef != (secrets.SecretRef{Ref: "secret://local/wecom-verify", Version: 1}) ||
		wecomBinding.SendSecretRef != (secrets.SecretRef{Ref: "secret://local/wecom-send", Version: 1}) {
		t.Fatalf("wecomBinding=%#v", wecomBinding)
	}
	wecomConfigVersion, wecomTenantVersion := snapshot.ConfigVersion, root.Version
	root, snapshot, err = ensureWebUILocalWeComBinding(ctx, configs, root, snapshot, wecom)
	if err != nil || snapshot.ConfigVersion != wecomConfigVersion || root.Version != wecomTenantVersion {
		t.Fatalf("non-idempotent WeCom binding root=%#v snapshot=%#v err=%v", root, snapshot, err)
	}
	wecomRotated := wecom
	wecomRotated.WeComCorpID = "ww_rotated"
	root, snapshot, err = ensureWebUILocalWeComBinding(ctx, configs, root, snapshot, wecomRotated)
	if err != nil || snapshot.ConfigVersion != wecomConfigVersion+1 || root.Version != wecomTenantVersion+1 ||
		snapshot.Payload.ChannelBindings[1].ExternalAccountID != wecomRotated.WeComCorpID {
		t.Fatalf("rotated WeCom binding root=%#v snapshot=%#v err=%v", root, snapshot, err)
	}
	invalid := wecomRotated
	invalid.WeComAgentID = 0
	if _, _, err = ensureWebUILocalWeComBinding(ctx, configs, root, snapshot, invalid); err == nil {
		t.Fatal("invalid WeCom local control plane accepted")
	}
}
