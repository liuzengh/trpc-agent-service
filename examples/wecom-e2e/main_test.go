package main

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha1" // #nosec G505 -- WeCom requires SHA-1 callback signatures.
	"database/sql"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/XnLemon/trpc-agent-service/migrations"
	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	agentinmemory "github.com/XnLemon/trpc-agent-service/trpcservice/app/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	backendinmemory "github.com/XnLemon/trpc-agent-service/trpcservice/backend/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	channelsinmemory "github.com/XnLemon/trpc-agent-service/trpcservice/channels/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels/wecom"
	"github.com/XnLemon/trpc-agent-service/trpcservice/gateway"
	"github.com/XnLemon/trpc-agent-service/trpcservice/model"
	modelinmemory "github.com/XnLemon/trpc-agent-service/trpcservice/model/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/outbox"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime"
	runtimerunner "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/runner"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	runtimestoragepostgres "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/postgres"
	storagepostgres "github.com/XnLemon/trpc-agent-service/trpcservice/storage/postgres"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
	tenantinmemory "github.com/XnLemon/trpc-agent-service/trpcservice/tenant/inmemory"
	trpcagent "trpc.group/trpc-go/trpc-agent-go/agent"
	trpcevent "trpc.group/trpc-go/trpc-agent-go/event"
	trpcmodel "trpc.group/trpc-go/trpc-agent-go/model"
)

const (
	wecomRouteKey = "wecom-e2e-route"
	wecomToken    = "wecom-e2e-callback-token" // #nosec G101 -- deterministic test fixture, not a credential.
	wecomSecret   = "wecom-e2e-app-secret"     // #nosec G101 -- deterministic test fixture, not a credential.
	wecomReceive  = "corp-e2e"
	wecomAgentID  = "1"
)

// TestWeComCallbackOutboxE2E exercises callback, Gateway durable storage,
// duplicate idempotency, and provider delivery without external credentials.
//
//nolint:gocyclo // The E2E intentionally keeps the complete callback-to-outbox contract in one scenario.
func TestWeComCallbackOutboxE2E(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv("POSTGRES_MIGRATION_TEST_DSN"))
	if dsn == "" {
		t.Skip("POSTGRES_MIGRATION_TEST_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db, err := storagepostgres.Open(ctx, dsn, storagepostgres.Options{MaxOpenConns: 4, MaxIdleConns: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if err := migrations.Apply(ctx, db); err != nil {
		t.Fatal(err)
	}
	fixture := newWeComFixture(t, ctx, db)
	providerServer := newProviderServer(t)
	defer providerServer.Close()
	provider := &wecom.BindingProvider{Bindings: fixture.channels, Credentials: fixture.credentials, BaseURL: providerServer.URL, HTTPClient: providerServer.Client()}
	worker, err := outbox.New(outbox.Config{Store: fixture.store, MessageStore: fixture.store, Provider: provider, TenantID: fixture.tenant.TenantID, Owner: "wecom-example-e2e", LeaseDuration: 30 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	recording := &recordingDispatcher{delegate: fixture.dispatcher}
	handler, err := wecom.New(wecom.Config{Dispatcher: recording, Token: wecomToken, EncodingAESKey: fixture.encodingAESKey, ReceiveID: wecomReceive, AgentID: wecomAgentID, RouteKey: wecomRouteKey, Target: fixture.target})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = handler.Close() }()
	server := httptest.NewServer(handler)
	defer server.Close()

	ciphertext := encryptCallback(t, fixture.aesKey, wecomReceive, "wecom-e2e-message-1", "deterministic callback")
	requestURL := callbackURL(server.URL, ciphertext)
	response := postCallback(t, ctx, requestURL, ciphertext)
	if response.StatusCode != http.StatusOK || response.Body != "success" {
		t.Fatalf("callback response = %+v, dispatch error = %v", response, recording.lastError())
	}
	rows, err := waitForReplyCandidates(ctx, fixture.store, fixture.tenant.TenantID, fixture.runner.reply)
	if err != nil || len(rows) != 1 || rows[0].Payload != fixture.runner.reply {
		t.Fatalf("reply candidates = %+v, err=%v", rows, err)
	}
	processed, err := worker.RunOnce(ctx)
	if err != nil || processed != 1 {
		t.Fatalf("outbox worker processed=%d err=%v", processed, err)
	}
	reply, err := fixture.store.GetReply(ctx, fixture.tenant.TenantID, rows[0].ReplyID, rows[0].SegmentIndex)
	if err != nil || reply.Status != runtimestorage.ReplySent || reply.ProviderMessageID == "" {
		t.Fatalf("reply = %+v, err=%v", reply, err)
	}
	event, err := fixture.store.GetMessage(ctx, fixture.tenant.TenantID, rows[0].EventID)
	if err != nil || event.Status != runtimestorage.EventReplied {
		t.Fatalf("event = %+v, err=%v", event, err)
	}
	if fixture.runner.calls.Load() != 1 {
		t.Fatalf("runner calls after first callback = %d", fixture.runner.calls.Load())
	}
	duplicate := postCallback(t, ctx, requestURL, ciphertext)
	if duplicate.StatusCode != http.StatusOK || duplicate.Body != "success" {
		t.Fatalf("duplicate callback response = %+v", duplicate)
	}
	if fixture.runner.calls.Load() != 1 {
		t.Fatalf("duplicate callback executed runner %d times", fixture.runner.calls.Load())
	}
}

func waitForReplyCandidates(ctx context.Context, store runtimestorage.ReplyStore, tenantID, payload string) ([]runtimestorage.ReplyOutbox, error) {
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		rows, err := store.ListReplyCandidates(waitCtx, tenantID)
		if err != nil {
			return nil, err
		}
		if len(rows) == 1 && rows[0].Payload == payload {
			return rows, nil
		}
		select {
		case <-waitCtx.Done():
			return rows, waitCtx.Err()
		case <-ticker.C:
		}
	}
}

type weComFixture struct {
	tenant         *tenant.Tenant
	channels       *channelsinmemory.InMemoryRepository
	credentials    fixtureCredentials
	encodingAESKey string
	aesKey         []byte
	target         channels.RoutingTarget
	store          *runtimestoragepostgres.Store
	dispatcher     *gateway.Dispatcher
	runner         *weComRunner
}

//nolint:gocyclo // Fixture assembly mirrors the tenant/app/binding execution boundary.
func newWeComFixture(t *testing.T, ctx context.Context, db *sql.DB) weComFixture {
	t.Helper()
	modelCatalog, err := model.NewProviderCatalog(model.ProviderSpec{Provider: "fake", Models: []string{"deterministic"}, EndpointPolicy: model.FieldForbidden, SecretRefPolicy: model.FieldForbidden})
	if err != nil {
		t.Fatal(err)
	}
	backendCatalog, err := backend.NewProviderCatalog(backend.ProviderSpec{Provider: "inmemory", Capabilities: []backend.Capability{backend.CapabilitySession}, EndpointPolicy: backend.FieldForbidden, SecretRefPolicy: backend.FieldForbidden})
	if err != nil {
		t.Fatal(err)
	}
	tenantRepo := tenantinmemory.NewRepository()
	root, err := tenantRepo.Create(ctx, tenant.CreateInput{TenantKey: fmt.Sprintf("wecom-e2e-%d", time.Now().UnixNano()), DisplayName: "WeCom E2E", AuditRetentionDays: 30, LogMaskingLevel: tenant.MaskingStrict, TraceSamplingRate: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO public.tenant (tenant_id, tenant_key, display_name, status, audit_retention_days, log_masking_level, trace_sampling_rate, version, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`, root.TenantID, root.TenantKey, root.DisplayName, string(root.Status), root.AuditRetentionDays, string(root.LogMaskingLevel), root.TraceSamplingRate, root.Version, root.CreatedAt, root.UpdatedAt); err != nil {
		t.Fatal(err)
	}
	modelRepo := modelinmemory.NewRepository(modelCatalog)
	modelProfile, _, err := modelRepo.Create(ctx, model.CreateInput{TenantID: root.TenantID, ProfileKey: "deterministic", DisplayName: "Deterministic", Configuration: model.Configuration{Provider: "fake", Model: "deterministic"}, Metadata: model.ChangeMetadata{ActorType: "example", ActorID: "wecom-e2e", Reason: "test", CorrelationID: "wecom-e2e"}})
	if err != nil {
		t.Fatal(err)
	}
	backendRepo := backendinmemory.NewRepository(backendCatalog)
	backendProfile, _, err := backendRepo.Create(ctx, backend.CreateInput{TenantID: root.TenantID, ProfileKey: "session", DisplayName: "Session", Bindings: []backend.CapabilityBinding{{Capability: backend.CapabilitySession, Provider: "inmemory"}}, Metadata: backend.ChangeMetadata{ActorType: "example", ActorID: "wecom-e2e", Reason: "test", CorrelationID: "wecom-e2e"}})
	if err != nil {
		t.Fatal(err)
	}
	appRepo := agentinmemory.NewRepository()
	appRoot, err := appRepo.Create(ctx, appmodel.CreateInput{TenantID: root.TenantID, AppKey: "wecom-e2e", DisplayName: "WeCom E2E", Description: "Deterministic WeCom callback"})
	if err != nil {
		t.Fatal(err)
	}
	draft, err := appRepo.CreateDraft(ctx, appmodel.CreateDraftInput{TenantID: root.TenantID, AppID: appRoot.AppID, ExpectedAppVersion: appRoot.Version, Configuration: appmodel.DraftConfiguration{Instruction: "Reply deterministically.", ModelProfileID: modelProfile.ProfileID, Runtime: appmodel.DefaultRuntimePolicy()}})
	if err != nil {
		t.Fatal(err)
	}
	published, _, _, err := appRepo.Publish(ctx, appmodel.PublishInput{TenantID: root.TenantID, AppID: appRoot.AppID, Revision: draft.Revision, ExpectedAppVersion: appRoot.Version, ExpectedDraftVersion: draft.DraftVersion, TenantActive: true, Metadata: appmodel.ChangeMetadata{ActorType: "example", ActorID: "wecom-e2e", Reason: "test", CorrelationID: "wecom-e2e"}})
	if err != nil {
		t.Fatal(err)
	}
	appID, backendID := published.AppID, backendProfile.ProfileID
	root, err = tenantRepo.UpdateConfiguration(ctx, tenant.UpdateConfigurationInput{TenantID: root.TenantID, ExpectedVersion: root.Version, DisplayName: root.DisplayName, AuditRetentionDays: 30, LogMaskingLevel: tenant.MaskingStrict, TraceSamplingRate: 1, DefaultAgentAppID: &appID, DefaultBackendProfileID: &backendID})
	if err != nil {
		t.Fatal(err)
	}
	routeDigest, err := channels.DigestPublicRouteKey(channels.ChannelWeCom, wecomRouteKey)
	if err != nil {
		t.Fatal(err)
	}
	channelRepo := channelsinmemory.NewRepository()
	binding, _, err := channelRepo.Create(ctx, channels.CreateInput{TenantID: root.TenantID, BindingKey: "wecom-e2e", Channel: channels.ChannelWeCom, ProviderAccountID: "corp-e2e:1", PublicRouteKeyDigest: routeDigest, AppID: published.AppID, SecretRef: "examples/wecom-e2e", Protocol: channels.ProtocolConfiguration{WeCom: &channels.WeComProtocolConfiguration{CorpID: "corp-e2e", AgentID: wecomAgentID, ReceiveID: wecomReceive}}, Metadata: channels.ChangeMetadata{ActorType: "example", ActorID: "wecom-e2e", Reason: "test", CorrelationID: "wecom-e2e"}})
	if err != nil {
		t.Fatal(err)
	}
	binding, _, err = channelRepo.Activate(ctx, channels.TransitionStatusInput{TenantID: binding.TenantID, BindingID: binding.BindingID, ExpectedVersion: binding.Version, Metadata: channels.ChangeMetadata{ActorType: "example", ActorID: "wecom-e2e", Reason: "test", CorrelationID: "wecom-e2e"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO public.agent_app (tenant_id, app_id, app_key, display_name, description, status, version, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,'draft',$6,$7,$8)`, root.TenantID, published.AppID, "wecom-e2e", "WeCom E2E", "Deterministic WeCom callback", published.Version, published.CreatedAt, published.UpdatedAt); err != nil {
		t.Fatal(err)
	}
	protocolConfig := fmt.Sprintf(`{"wecom":{"corp_id":%q,"agent_id":%q,"receive_id":%q}}`, wecomReceive, wecomAgentID, wecomReceive)
	if _, err := db.ExecContext(ctx, `INSERT INTO public.channel_binding (tenant_id, binding_id, binding_key, channel, provider_account_id, public_route_key_digest, app_id, secret_ref, protocol_config, status, version, config_digest, created_at, updated_at) VALUES ($1,$2,$3,'wecom',$4,$5,$6,$7,$8::jsonb,'active',$9,$10,$11,$12)`, root.TenantID, binding.BindingID, binding.BindingKey, binding.ProviderAccountID, routeDigest, published.AppID, binding.SecretRef, protocolConfig, binding.Version, binding.ConfigDigest, binding.CreatedAt, binding.UpdatedAt); err != nil {
		t.Fatal(err)
	}
	key, secret := bytes.Repeat([]byte{1}, 32), "callback-secret"
	resolver := channelsinmemory.NewFakeCandidateResolver(channelRepo, map[channels.SecretScope]string{{TenantID: binding.TenantID, SecretRef: binding.SecretRef}: secret})
	candidates, err := channelRepo.LookupCandidates(ctx, channels.ChannelWeCom, routeDigest)
	if err != nil || len(candidates) != 1 {
		t.Fatalf("candidates=%d err=%v", len(candidates), err)
	}
	handle, err := resolver.ResolveCandidate(ctx, channels.CandidateSecretRequest{Candidate: candidates[0], Purpose: channels.PurposeWebhookVerification})
	if err != nil {
		t.Fatal(err)
	}
	verification := channels.VerificationRequest{Purpose: channels.PurposeWebhookVerification, Timestamp: time.Now().UTC(), Nonce: "wecom-e2e", MessageDigest: strings.Repeat("a", 64), ReceiveID: wecomReceive}
	verification.Signature = channelsinmemory.SignFakeRequest(secret, verification)
	verified, err := resolver.Verify(ctx, handle, verification)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := tenant.NewConfigurationSnapshot(root)
	if err != nil {
		t.Fatal(err)
	}
	target, err := channels.NewRoutingTarget(snapshot, binding, published, verified)
	if err != nil {
		t.Fatal(err)
	}
	planResolver, err := gateway.NewPlanResolver(runtime.PlanResolverConfig{Tenants: tenantRepo, Apps: appRepo, Models: modelRepo, Backends: backendRepo, ModelCatalog: modelCatalog, BackendCatalog: backendCatalog})
	if err != nil {
		t.Fatal(err)
	}
	runner := &weComRunner{reply: "wecom-e2e-ok"}
	registry, err := runtimerunner.NewRunnerRegistry(runtimerunner.RunnerRegistryConfig{Factory: func(context.Context, runtime.ExecutionPlan) (runtimerunner.Runner, error) { return runner, nil }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = registry.Close() })
	store := runtimestoragepostgres.New(db)
	dispatcher, err := gateway.NewDispatcher(gateway.DispatchConfig{Resolver: planResolver, Registry: registry, SessionStore: store, MessageStore: store, ReplyBatchStore: store, Attachments: store, AttachmentStore: store})
	if err != nil {
		t.Fatal(err)
	}
	return weComFixture{tenant: root, channels: channelRepo, credentials: fixtureCredentials{tenantID: binding.TenantID, secretRef: binding.SecretRef, token: wecomToken, aesKey: base64.RawStdEncoding.EncodeToString(key), appSecret: wecomSecret}, encodingAESKey: base64.RawStdEncoding.EncodeToString(key), aesKey: key, target: target, store: store, dispatcher: dispatcher, runner: runner}
}

type fixtureCredentials struct{ tenantID, secretRef, token, aesKey, appSecret string }

func (c fixtureCredentials) Resolve(ctx context.Context, scope channels.SecretScope) (wecom.Credentials, error) {
	if ctx == nil {
		return wecom.Credentials{}, errors.New("context is required")
	}
	if err := ctx.Err(); err != nil {
		return wecom.Credentials{}, err
	}
	if scope.TenantID != c.tenantID || scope.SecretRef != c.secretRef {
		return wecom.Credentials{}, errors.New("credential scope mismatch")
	}
	return wecom.Credentials{CallbackToken: c.token, EncodingAESKey: c.aesKey, AppSecret: c.appSecret}, nil
}

type weComRunner struct {
	reply string
	calls atomic.Int32
}

func (r *weComRunner) Run(ctx context.Context, _, _ string, _ trpcmodel.Message, _ ...trpcagent.RunOption) (<-chan *trpcevent.Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.calls.Add(1)
	events := make(chan *trpcevent.Event, 2)
	events <- &trpcevent.Event{Response: &trpcmodel.Response{Choices: []trpcmodel.Choice{{Delta: trpcmodel.Message{Content: r.reply}}}}}
	events <- &trpcevent.Event{Response: &trpcmodel.Response{Done: true}}
	close(events)
	return events, nil
}
func (*weComRunner) Close() error { return nil }

type recordingDispatcher struct {
	delegate gateway.DispatchService
	err      atomic.Value
}

func (d *recordingDispatcher) Dispatch(ctx context.Context, request gateway.DispatchRequest) (<-chan gateway.DispatchEvent, error) {
	stream, err := d.delegate.Dispatch(ctx, request)
	if err != nil {
		d.err.Store(err)
	}
	return stream, err
}

func (d *recordingDispatcher) lastError() error {
	value := d.err.Load()
	if value == nil {
		return nil
	}
	return value.(error)
}

func newProviderServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/cgi-bin/gettoken":
			_, _ = io.WriteString(w, `{"errcode":0,"access_token":"wecom-e2e-provider-token","expires_in":3600}`)
		case "/cgi-bin/message/send":
			var payload map[string]any
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil || payload["agentid"] != float64(1) || payload["touser"] == "" {
				http.Error(w, "invalid payload", http.StatusBadRequest)
				return
			}
			_, _ = io.WriteString(w, `{"errcode":0,"msgid":"wecom-e2e-provider-message"}`)
		default:
			http.NotFound(w, r)
		}
	}))
}

func callbackURL(serverURL, ciphertext string) string {
	parts := []string{wecomToken, "1700000000", "nonce-wecom-e2e", ciphertext}
	sort.Strings(parts)
	sum := sha1.Sum([]byte(strings.Join(parts, ""))) // #nosec G401 -- WeCom requires SHA-1 callback signatures.
	return serverURL + "/wecom/callback/" + wecomRouteKey + "?msg_signature=" + fmt.Sprintf("%x", sum) + "&timestamp=1700000000&nonce=nonce-wecom-e2e"
}

func postCallback(t *testing.T, ctx context.Context, requestURL, ciphertext string) struct {
	StatusCode int
	Body       string
} {
	t.Helper()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, requestURL, strings.NewReader("<xml><Encrypt><![CDATA["+ciphertext+"]]></Encrypt></xml>"))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "text/xml")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return struct {
		StatusCode int
		Body       string
	}{StatusCode: response.StatusCode, Body: string(body)}
}

func encryptCallback(t *testing.T, key []byte, receiveID, messageID, content string) string {
	t.Helper()
	plain := []byte("<xml><MsgId>" + messageID + "</MsgId><FromUserName>e2e-user</FromUserName><MsgType>text</MsgType><AgentID>1</AgentID><Content>" + content + "</Content></xml>")
	value := make([]byte, 16+4+len(plain)+len(receiveID))
	for index := range value[:16] {
		value[index] = byte(index + 1)
	}
	if len(plain) > int(^uint32(0)) {
		t.Fatal("callback plaintext exceeds WeCom uint32 length")
	}
	binary.BigEndian.PutUint32(value[16:20], uint32(len(plain))) // #nosec G115 -- bounded by the guard above.
	copy(value[20:], plain)
	copy(value[20+len(plain):], receiveID)
	padding := aes.BlockSize - len(value)%aes.BlockSize
	value = append(value, bytes.Repeat([]byte{byte(padding)}, padding)...)
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatal(err)
	}
	encrypted := make([]byte, len(value))
	cipher.NewCBCEncrypter(block, key[:aes.BlockSize]).CryptBlocks(encrypted, value)
	return base64.StdEncoding.EncodeToString(encrypted)
}
