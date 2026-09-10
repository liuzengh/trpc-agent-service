package integration_test

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	lark "github.com/larksuite/oapi-sdk-go/v3"

	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/feishu"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/wecom"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
)

// TestLiveIMCredentialSmoke performs visible provider effects.
// It is intentionally opt-in and reads credentials only from owner-only files
// so secrets never appear in environment values, arguments, logs, or errors.
func TestLiveIMCredentialSmoke(t *testing.T) {
	if os.Getenv("TRPC_LIVE_IM_PROVIDER_SMOKE") != "1" {
		t.Skip("requires explicit Feishu/WeCom provider smoke")
	}
	providers := selectedLiveIMProviders(t, os.Getenv("TRPC_LIVE_IM_PROVIDERS"))
	required := make([]string, 0, 6)
	if providers["feishu"] {
		required = append(required, "TRPC_LIVE_FEISHU_SECRET_FILE", "TRPC_LIVE_FEISHU_APP_ID", "TRPC_LIVE_FEISHU_MESSAGE_ID")
	}
	if providers["wecom"] {
		required = append(required, "TRPC_LIVE_WECOM_SECRET_FILE", "TRPC_LIVE_WECOM_CORP_ID", "TRPC_LIVE_WECOM_USER_ID")
	}
	for _, name := range required {
		if strings.TrimSpace(os.Getenv(name)) == "" {
			t.Fatalf("%s is required", name)
		}
	}
	random := make([]byte, 12)
	if _, err := rand.Read(random); err != nil {
		t.Fatal(err)
	}
	nonce := "live-im-provider-" + hex.EncodeToString(random)
	client := &http.Client{Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	if providers["feishu"] {
		feishuDestination := channel.ReplyDestination{TenantID: "live-provider-smoke", Channel: "feishu",
			ChannelBindingID: "live-feishu", ExternalAccountID: strings.TrimSpace(os.Getenv("TRPC_LIVE_FEISHU_APP_ID")), ConfigVersion: 1}
		feishuSecrets := smokeFileSecretResolver{path: os.Getenv("TRPC_LIVE_FEISHU_SECRET_FILE")}
		feishuCredentials := &feishu.CredentialProvider{Secrets: feishuSecrets, Client: client}
		feishuAdapter := &feishu.Adapter{Sender: feishu.OfficialSender{Clients: &feishu.ClientCache{Credentials: feishuCredentials,
			NewClient: func(appID, appSecret string) *lark.Client {
				return lark.NewClient(appID, appSecret, lark.WithHttpClient(client))
			}}}}
		feishuResult, err := feishuAdapter.Deliver(ctx, smokeDeliveryRequest(feishuDestination, nonce,
			channel.DeliveryTarget{Channel: "feishu", ExternalAccountID: feishuDestination.ExternalAccountID,
				ExternalMessageID: strings.TrimSpace(os.Getenv("TRPC_LIVE_FEISHU_MESSAGE_ID"))}))
		if err != nil || !feishuResult.Delivered || feishuResult.ProviderMessageID == "" {
			t.Fatalf("Feishu real delivery failed: delivered=%t err=%v", feishuResult.Delivered, sanitizeProviderSmokeError(err))
		}
	}

	if providers["wecom"] {
		wecomDestination := channel.ReplyDestination{TenantID: "live-provider-smoke", Channel: "wecom",
			ChannelBindingID: "live-wecom", ExternalAccountID: strings.TrimSpace(os.Getenv("TRPC_LIVE_WECOM_CORP_ID")), ConfigVersion: 1}
		wecomTokens := &wecom.TokenProvider{Secrets: smokeFileSecretResolver{path: os.Getenv("TRPC_LIVE_WECOM_SECRET_FILE")}, Client: client}
		wecomAdapter := &wecom.Adapter{Sender: wecom.OfficialSender{Tokens: wecomTokens, Client: client}}
		wecomResult, err := wecomAdapter.Deliver(ctx, smokeDeliveryRequest(wecomDestination, nonce,
			channel.DeliveryTarget{Channel: "wecom", ExternalAccountID: wecomDestination.ExternalAccountID,
				ExternalUserID: strings.TrimSpace(os.Getenv("TRPC_LIVE_WECOM_USER_ID"))}))
		if err != nil || !wecomResult.Delivered || wecomResult.ProviderMessageID == "" {
			t.Fatalf("WeCom real delivery failed: delivered=%t err=%v", wecomResult.Delivered, sanitizeProviderSmokeError(err))
		}
	}
	t.Log("selected real IM provider messages accepted with scoped file credentials")
}

func selectedLiveIMProviders(t *testing.T, raw string) map[string]bool {
	t.Helper()
	selected := map[string]bool{}
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "all" {
		selected["feishu"], selected["wecom"] = true, true
		return selected
	}
	for _, name := range strings.Split(raw, ",") {
		name = strings.TrimSpace(name)
		if name != "feishu" && name != "wecom" {
			t.Fatalf("unsupported TRPC_LIVE_IM_PROVIDERS entry %q", name)
		}
		selected[name] = true
	}
	return selected
}

func smokeDeliveryRequest(destination channel.ReplyDestination, content string, target channel.DeliveryTarget) channel.DeliveryRequest {
	digest := sha256.Sum256([]byte(content))
	deliveryKey := "live:" + hex.EncodeToString(digest[:16])
	event := channel.ReplyEvent{SchemaVersion: 1, TenantID: destination.TenantID, RequestID: deliveryKey,
		ChannelBindingID: destination.ChannelBindingID, DeliveryKey: deliveryKey, ConfigVersion: destination.ConfigVersion,
		EventSeq: 1, Kind: "message.completed", ContentRef: "smoke://" + deliveryKey, Target: target, Final: true}
	return channel.DeliveryRequest{Event: event, ClientRequestID: deliveryKey, Target: target,
		Content: []byte(content), ContentDigest: hex.EncodeToString(digest[:])}
}

type smokeFileSecretResolver struct{ path string }

func (r smokeFileSecretResolver) Resolve(ctx context.Context, destination channel.ReplyDestination) (secrets.SecretValue, error) {
	if err := ctx.Err(); err != nil {
		return secrets.SecretValue{}, err
	}
	path := filepath.Clean(strings.TrimSpace(r.path))
	if !filepath.IsAbs(path) || destination.TenantID == "" || destination.ChannelBindingID == "" {
		return secrets.SecretValue{}, runtime.ErrInvariantViolation
	}
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 || info.Size() < 2 || info.Size() > 64<<10 {
		return secrets.SecretValue{}, runtime.ErrVersionMismatch
	}
	value, err := os.ReadFile(path)
	if err != nil {
		return secrets.SecretValue{}, runtime.ErrBackendUnavailable
	}
	return secrets.SecretValue{Bytes: value, Version: 1}, nil
}

func sanitizeProviderSmokeError(err error) error {
	if err == nil {
		return nil
	}
	return runtime.ErrBackendUnavailable
}

func TestLiveIMProviderSmokeSecretFilesAreOwnerOnlyAndAbsolute(t *testing.T) {
	destination := channel.ReplyDestination{TenantID: "tenant", Channel: "feishu", ChannelBindingID: "binding",
		ExternalAccountID: "app", ConfigVersion: 1}
	if _, err := (smokeFileSecretResolver{path: "relative.json"}).Resolve(context.Background(), destination); err == nil {
		t.Fatal("relative credential path accepted")
	}
	path := filepath.Join(t.TempDir(), "credential.json")
	if err := os.WriteFile(path, []byte(`{"app_id":"app","app_secret":"secret"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	value, err := (smokeFileSecretResolver{path: path}).Resolve(context.Background(), destination)
	if err != nil || value.Version != 1 || len(value.Bytes) == 0 {
		t.Fatalf("owner-only credential value=%#v err=%v", value, err)
	}
	clear(value.Bytes)
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := (smokeFileSecretResolver{path: path}).Resolve(context.Background(), destination); err == nil {
		t.Fatal("group/world-readable credential accepted")
	}
}
