package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	agentinmemory "github.com/XnLemon/trpc-agent-service/trpcservice/app/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	backendinmemory "github.com/XnLemon/trpc-agent-service/trpcservice/backend/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	channelsinmemory "github.com/XnLemon/trpc-agent-service/trpcservice/channels/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels/telegram"
	"github.com/XnLemon/trpc-agent-service/trpcservice/gateway"
	"github.com/XnLemon/trpc-agent-service/trpcservice/model"
	modelinmemory "github.com/XnLemon/trpc-agent-service/trpcservice/model/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/outbox"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime"
	runtimerunner "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/runner"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
	tenantinmemory "github.com/XnLemon/trpc-agent-service/trpcservice/tenant/inmemory"
	"github.com/go-telegram/bot"
	"github.com/go-telegram/bot/models"
	trpcagent "trpc.group/trpc-go/trpc-agent-go/agent"
	trpcevent "trpc.group/trpc-go/trpc-agent-go/event"
	trpcmodel "trpc.group/trpc-go/trpc-agent-go/model"
)

// TestTelegramProviderOutboxRunnerE2E exercises the complete durable reply
// path with the real Telegram Provider. It is opt-in because it sends one
// message through the protected Telegram Bot API credentials.
func TestTelegramProviderOutboxRunnerE2E(t *testing.T) {
	receiverToken := strings.TrimSpace(os.Getenv("TELEGRAM_BOT_TOKEN"))
	senderToken := strings.TrimSpace(os.Getenv("TELEGRAM_SENDER_BOT_TOKEN"))
	if receiverToken == "" || senderToken == "" {
		t.Skip("requires TELEGRAM_BOT_TOKEN and TELEGRAM_SENDER_BOT_TOKEN")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	receiver, err := bot.New(receiverToken, bot.WithSkipGetMe())
	if err != nil {
		t.Fatal("receiver bot initialization failed")
	}
	sender, err := bot.New(senderToken, bot.WithSkipGetMe())
	if err != nil {
		t.Fatal("sender bot initialization failed")
	}
	receiverIdentity, err := receiver.GetMe(ctx)
	if err != nil || receiverIdentity == nil || !receiverIdentity.IsBot || receiverIdentity.ID <= 0 {
		t.Fatal("receiver bot identity check failed")
	}
	senderIdentity, err := sender.GetMe(ctx)
	if err != nil || senderIdentity == nil || !senderIdentity.IsBot || senderIdentity.ID <= 0 || senderIdentity.ID == receiverIdentity.ID {
		t.Fatal("sender bot identity check failed")
	}

	provider, err := telegram.NewProvider(receiver, senderIdentity.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	runTelegramOutboxScenario(t, ctx, provider, strconv.FormatInt(receiverIdentity.ID, 10), strconv.FormatInt(senderIdentity.ID, 10))
}

func TestTelegramProviderOutboxRunnerE2EDeterministic(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client := &exampleProviderBot{nextMessageID: 9001}
	provider, err := telegram.NewProvider(client, 99, 0)
	if err != nil {
		t.Fatal(err)
	}
	runTelegramOutboxScenario(t, ctx, provider, "99", "42")
	if got := client.sendCalls.Load(); got != 1 {
		t.Fatalf("provider send calls = %d, want 1", got)
	}
}

func runTelegramOutboxScenario(t *testing.T, ctx context.Context, provider outbox.Provider, providerAccountID, peerID string) {
	t.Helper()
	fixture := newDurableTelegramFixture(t, providerAccountID)
	store := inmemory.New()
	runner := &telegramE2ERunner{reply: fmt.Sprintf("telegram-outbox-runner-e2e-%d", time.Now().UTC().UnixNano())}
	registry, err := runtimerunner.NewRunnerRegistry(runtimerunner.RunnerRegistryConfig{Factory: func(context.Context, runtime.ExecutionPlan) (runtimerunner.Runner, error) { return runner, nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = registry.Close() }()
	dispatcher, err := gateway.NewDispatcher(gateway.DispatchConfig{Resolver: fixture.resolver, Registry: registry, SessionStore: store, MessageStore: store, ReplyBatchStore: store, Attachments: store, AttachmentStore: store})
	if err != nil {
		t.Fatal(err)
	}
	principal, err := gateway.NewChannelPrincipal(fixture.target)
	if err != nil {
		t.Fatal(err)
	}
	externalMessageID := fmt.Sprintf("telegram-outbox-runner-%d", time.Now().UTC().UnixNano())
	stream, err := dispatcher.Dispatch(ctx, gateway.DispatchRequest{Principal: principal, RequestID: externalMessageID, Message: gateway.InboundMessage{
		Content: "runner reply request", ExternalMessageID: externalMessageID, ExternalUserID: peerID,
		ConversationKind: channels.ConversationDirect, ExternalPeerID: peerID,
	}})
	if err != nil {
		t.Fatal(err)
	}
	dispatchEvents := collectTelegramE2EDispatchEvents(stream)
	assertTelegramOutboxDispatch(t, dispatchEvents, runner)
	rows, err := store.ListReplyCandidates(ctx, fixture.target.TenantID)
	assertTelegramOutboxRows(t, rows, err, runner.reply)
	worker, err := outbox.New(outbox.Config{Store: store, MessageStore: store, Provider: provider, TenantID: fixture.target.TenantID, Owner: "telegram-example-e2e", LeaseDuration: 30 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	processed, err := worker.RunOnce(ctx)
	if err != nil || processed != 1 {
		t.Fatalf("outbox worker processed=%d err=%v", processed, err)
	}
	reply, err := store.GetReply(ctx, fixture.target.TenantID, rows[0].ReplyID, rows[0].SegmentIndex)
	assertTelegramOutboxReply(t, reply, err)
	event, err := store.GetMessage(ctx, fixture.target.TenantID, rows[0].EventID)
	if err != nil || event.Status != runtimestorage.EventReplied {
		t.Fatalf("message event = %+v, err=%v", event, err)
	}
}

func assertTelegramOutboxDispatch(t *testing.T, events []gateway.DispatchEvent, runner *telegramE2ERunner) {
	t.Helper()
	if len(events) != 2 || events[0].Type != gateway.DispatchEventMessage || events[0].Text != runner.reply || !events[1].Done {
		t.Fatalf("dispatcher events = %+v", events)
	}
	if got := runner.calls.Load(); got != 1 {
		t.Fatalf("Runner calls = %d, want 1", got)
	}
}

func assertTelegramOutboxRows(t *testing.T, rows []runtimestorage.ReplyOutbox, err error, reply string) {
	t.Helper()
	if err != nil || len(rows) != 1 || rows[0].Payload != reply {
		t.Fatalf("materialized outbox rows = %+v, err=%v", rows, err)
	}
}

func assertTelegramOutboxReply(t *testing.T, reply runtimestorage.ReplyOutbox, err error) {
	t.Helper()
	if err != nil || reply.Status != runtimestorage.ReplySent || reply.ProviderMessageID == "" {
		t.Fatalf("reply outbox = %+v, err=%v", reply, err)
	}
}

type exampleProviderBot struct {
	nextMessageID int
	sendCalls     atomic.Int32
}

func (*exampleProviderBot) Start(context.Context) {}

func (*exampleProviderBot) GetMe(context.Context) (*models.User, error) {
	return &models.User{ID: 99, IsBot: true}, nil
}

func (client *exampleProviderBot) SendMessage(ctx context.Context, _ *bot.SendMessageParams) (*models.Message, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	client.sendCalls.Add(1)
	return &models.Message{ID: client.nextMessageID}, nil
}

func collectTelegramE2EDispatchEvents(stream <-chan gateway.DispatchEvent) []gateway.DispatchEvent {
	var events []gateway.DispatchEvent
	for event := range stream {
		events = append(events, event)
	}
	return events
}

type telegramE2ERunner struct {
	reply string
	calls atomic.Int32
}

func (runner *telegramE2ERunner) Run(ctx context.Context, _, _ string, _ trpcmodel.Message, _ ...trpcagent.RunOption) (<-chan *trpcevent.Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	runner.calls.Add(1)
	events := make(chan *trpcevent.Event, 2)
	events <- &trpcevent.Event{Response: &trpcmodel.Response{Choices: []trpcmodel.Choice{{Delta: trpcmodel.Message{Content: runner.reply}}}}}
	events <- &trpcevent.Event{Response: &trpcmodel.Response{Done: true}}
	close(events)
	return events, nil
}

func (*telegramE2ERunner) Close() error { return nil }

type durableTelegramFixture struct {
	target   channels.RoutingTarget
	resolver *gateway.PlanResolver
}

func newDurableTelegramFixture(t *testing.T, providerAccountID string) durableTelegramFixture {
	t.Helper()
	ctx := context.Background()
	modelCatalog, err := model.NewProviderCatalog(model.ProviderSpec{Provider: "fake", Models: []string{"deterministic"}, EndpointPolicy: model.FieldForbidden, SecretRefPolicy: model.FieldForbidden})
	if err != nil {
		t.Fatal(err)
	}
	backendCatalog, err := backend.NewProviderCatalog(backend.ProviderSpec{
		Provider: "inmemory", Capabilities: []backend.Capability{backend.CapabilitySession}, EndpointPolicy: backend.FieldForbidden, SecretRefPolicy: backend.FieldForbidden,
		Options: map[string]backend.OptionSpec{"namespace": {Kind: backend.OptionString}},
	})
	if err != nil {
		t.Fatal(err)
	}
	tenants := tenantinmemory.NewRepository()
	root, err := tenants.Create(ctx, tenant.CreateInput{TenantKey: "telegram-outbox-e2e", DisplayName: "Telegram Outbox E2E", AuditRetentionDays: 30, LogMaskingLevel: tenant.MaskingStrict, TraceSamplingRate: 1})
	if err != nil {
		t.Fatal(err)
	}
	modelsRepo := modelinmemory.NewRepository(modelCatalog)
	modelProfile, _, err := modelsRepo.Create(ctx, model.CreateInput{TenantID: root.TenantID, ProfileKey: "primary-model", DisplayName: "Primary Model", Configuration: model.Configuration{Provider: "fake", Model: "deterministic"}, Metadata: modelMetadata()})
	if err != nil {
		t.Fatal(err)
	}
	backends := backendinmemory.NewRepository(backendCatalog)
	backendProfile, _, err := backends.Create(ctx, backend.CreateInput{TenantID: root.TenantID, ProfileKey: "primary-backend", DisplayName: "Primary Backend", Bindings: []backend.CapabilityBinding{{Capability: backend.CapabilitySession, Provider: "inmemory", Options: map[string]string{"namespace": "telegram-outbox-e2e"}}}, Metadata: backendMetadata()})
	if err != nil {
		t.Fatal(err)
	}
	apps := agentinmemory.NewRepository()
	appRoot, err := apps.Create(ctx, appmodel.CreateInput{TenantID: root.TenantID, AppKey: "telegram-outbox-e2e", DisplayName: "Telegram Outbox E2E", Description: "Durable Telegram reply test"})
	if err != nil {
		t.Fatal(err)
	}
	draft, err := apps.CreateDraft(ctx, appmodel.CreateDraftInput{TenantID: root.TenantID, AppID: appRoot.AppID, ExpectedAppVersion: appRoot.Version, Configuration: appmodel.DraftConfiguration{Description: "Telegram Outbox E2E", Instruction: "Reply deterministically.", ModelProfileID: modelProfile.ProfileID, Runtime: appmodel.DefaultRuntimePolicy()}})
	if err != nil {
		t.Fatal(err)
	}
	publishedApp, _, _, err := apps.Publish(ctx, appmodel.PublishInput{TenantID: root.TenantID, AppID: appRoot.AppID, Revision: draft.Revision, ExpectedAppVersion: appRoot.Version, ExpectedDraftVersion: draft.DraftVersion, TenantActive: true, Metadata: agentMetadata()})
	if err != nil {
		t.Fatal(err)
	}
	appID, backendID := publishedApp.AppID, backendProfile.ProfileID
	root, err = tenants.UpdateConfiguration(ctx, tenant.UpdateConfigurationInput{TenantID: root.TenantID, ExpectedVersion: root.Version, DisplayName: root.DisplayName, AuditRetentionDays: 30, LogMaskingLevel: tenant.MaskingStrict, TraceSamplingRate: 1, DefaultAgentAppID: &appID, DefaultBackendProfileID: &backendID})
	if err != nil {
		t.Fatal(err)
	}
	routeDigest, err := channels.DigestPublicRouteKey(channels.ChannelTelegram, "telegram-outbox-e2e")
	if err != nil {
		t.Fatal(err)
	}
	channelRepo := channelsinmemory.NewRepository()
	binding, _, err := channelRepo.Create(ctx, channels.CreateInput{TenantID: root.TenantID, BindingKey: "telegram-outbox-e2e", Channel: channels.ChannelTelegram, ProviderAccountID: providerAccountID, PublicRouteKeyDigest: routeDigest, AppID: publishedApp.AppID, SecretRef: "examples/telegram-e2e", Protocol: channels.ProtocolConfiguration{Telegram: &channels.TelegramProtocolConfiguration{}}, Metadata: exampleMetadata()})
	if err != nil {
		t.Fatal(err)
	}
	binding, _, err = channelRepo.Activate(ctx, channels.TransitionStatusInput{TenantID: binding.TenantID, BindingID: binding.BindingID, ExpectedVersion: binding.Version, Metadata: exampleMetadata()})
	if err != nil {
		t.Fatal(err)
	}
	secret := "offline-verifier-secret" // #nosec G101 -- deterministic fixture secret for an offline test.
	channelResolver := channelsinmemory.NewFakeCandidateResolver(channelRepo, map[channels.SecretScope]string{{TenantID: binding.TenantID, SecretRef: binding.SecretRef}: secret})
	candidates, err := channelRepo.LookupCandidates(ctx, channels.ChannelTelegram, routeDigest)
	if err != nil || len(candidates) != 1 {
		t.Fatalf("channel candidates=%d err=%v", len(candidates), err)
	}
	handle, err := channelResolver.ResolveCandidate(ctx, channels.CandidateSecretRequest{Candidate: candidates[0], Purpose: channels.PurposeWebhookVerification})
	if err != nil {
		t.Fatal(err)
	}
	verification := channels.VerificationRequest{Purpose: channels.PurposeWebhookVerification, Timestamp: time.Now().UTC(), Nonce: "telegram-outbox-e2e", MessageDigest: strings.Repeat("a", 64)}
	verification.Signature = channelsinmemory.SignFakeRequest(secret, verification)
	verified, err := channelResolver.Verify(ctx, handle, verification)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := tenant.NewConfigurationSnapshot(root)
	if err != nil {
		t.Fatal(err)
	}
	target, err := channels.NewRoutingTarget(snapshot, binding, publishedApp, verified)
	if err != nil {
		t.Fatal(err)
	}
	planResolver, err := gateway.NewPlanResolver(runtime.PlanResolverConfig{Tenants: tenants, Apps: apps, Models: modelsRepo, Backends: backends, ModelCatalog: modelCatalog, BackendCatalog: backendCatalog})
	if err != nil {
		t.Fatal(err)
	}
	return durableTelegramFixture{target: target, resolver: planResolver}
}

func agentMetadata() appmodel.ChangeMetadata {
	return appmodel.ChangeMetadata{ActorType: "example", ActorID: "telegram-outbox-e2e", Reason: "durable Telegram reply test", CorrelationID: "telegram-outbox-e2e"}
}

func modelMetadata() model.ChangeMetadata {
	return model.ChangeMetadata{ActorType: "example", ActorID: "telegram-outbox-e2e", Reason: "durable Telegram reply test", CorrelationID: "telegram-outbox-e2e"}
}

func backendMetadata() backend.ChangeMetadata {
	return backend.ChangeMetadata{ActorType: "example", ActorID: "telegram-outbox-e2e", Reason: "durable Telegram reply test", CorrelationID: "telegram-outbox-e2e"}
}
