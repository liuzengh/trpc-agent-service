//go:build integration && e2e

package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5/pgxpool"
	larkevent "github.com/larksuite/oapi-sdk-go/v3/event"
	larkdispatcher "github.com/larksuite/oapi-sdk-go/v3/event/dispatcher"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/feishu"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/wecom"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	platformpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/postgres"
	platformredis "github.com/liuzengh/trpc-agent-service/trpcservice/redis"
	"github.com/liuzengh/trpc-agent-service/trpcservice/relay"
	platformsecret "github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	"trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	frameworkrunner "trpc.group/trpc-go/trpc-agent-go/runner"
)

const deterministicIMAppID = "support"

// TestDeterministicFeishuE2E drives the production Feishu SDK event boundary
// without a Feishu account. Only the provider transport is fake; admission,
// mapping, PostgreSQL outbox, Redis, Worker, session locking, and reply
// projection all remain real.
func TestDeterministicFeishuE2E(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	pipeline := newDeterministicPipeline(t, ctx, "feishu", "worker-feishu-e2e")
	defer pipeline.Close(t)

	bindings := seedDeterministicBindings(t, ctx, pipeline.store, channels.ChannelFeishu, "feishu")
	replies := &deterministicReplyClient{}
	pipeline.providers = func(context.Context, worker.ReplyDelivery) (worker.ReplyProvider, error) {
		return worker.ReplyProvider{Client: replies}, nil
	}
	pipeline.start(t)
	var feishuFactoryCalls atomic.Int32
	var feishuFailures sync.Map

	events := map[string][]*larkim.P2MessageReceiveV1{
		bindings[0].BindingID: {
			deterministicFeishuTextEvent(bindings[0].ExternalAccount, "feishu-a-direct", "same-user", "p2p", "hello direct"),
			deterministicFeishuTextEvent(bindings[0].ExternalAccount, "feishu-a-direct", "same-user", "p2p", "hello direct"),
			deterministicFeishuTextEvent(bindings[0].ExternalAccount, "feishu-a-group", "same-user", "group", "hello group"),
		},
		bindings[1].BindingID: {
			deterministicFeishuTextEvent(bindings[1].ExternalAccount, "feishu-b-direct", "same-user", "p2p", "hello other tenant"),
		},
	}
	adapter, err := feishu.NewAdapter(
		pipeline.store,
		gateway.New(pipeline.store),
		deterministicSecretProvider{},
		feishu.WithClientFactory(func(binding channels.BindingSnapshot, _ string, handler *larkdispatcher.EventDispatcher) feishu.Client {
			state, _ := feishuFailures.LoadOrStore(binding.BindingID, &atomic.Bool{})
			feishuFactoryCalls.Add(1)
			return &deterministicFeishuClient{
				handler:   handler,
				events:    events[binding.BindingID],
				failFirst: state.(*atomic.Bool),
				onError:   func(err error) { t.Logf("deterministic Feishu event %s: %v", binding.BindingID, err) },
			}
		}),
		feishu.WithReconnectDelay(time.Millisecond, 5*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("new Feishu deterministic adapter: %v", err)
	}

	adapterCtx, adapterCancel := context.WithCancel(ctx)
	defer adapterCancel()
	adapterDone := make(chan error, 1)
	go func() { adapterDone <- adapter.Run(adapterCtx) }()

	waitFor(t, ctx, "Feishu provider events", func() bool {
		return pipeline.countInbox(ctx, bindings[0].TenantID, bindings[0].AppID, bindings[0].BindingID) == 2 &&
			pipeline.countInbox(ctx, bindings[1].TenantID, bindings[1].AppID, bindings[1].BindingID) == 1
	})
	if got := feishuFactoryCalls.Load(); got < int32(len(bindings)*2) {
		t.Fatalf("Feishu client factory calls = %d, want at least %d after reconnect", got, len(bindings)*2)
	}
	assertDeterministicPipeline(t, ctx, pipeline, bindings, replies, []string{
		"feishu-a-direct", "feishu-a-group", "feishu-b-direct",
	})

	if _, err := pipeline.store.SetChannelBindingStatus(ctx, bindings[0].TenantID, bindings[0].AppID, bindings[0].BindingID, channels.BindingSuspended); err != nil {
		t.Fatalf("suspend Feishu binding through lifecycle API: %v", err)
	}
	if err := adapter.HandleMessage(ctx, bindings[0].Snapshot(), deterministicFeishuTextEvent(bindings[0].ExternalAccount, "feishu-a-suspended", "same-user", "p2p", "must be rejected")); !errors.Is(err, channels.ErrBindingInactive) {
		t.Fatalf("suspended Feishu event error = %v, want binding inactive", err)
	}
	if got := pipeline.countInbox(ctx, bindings[0].TenantID, bindings[0].AppID, bindings[0].BindingID); got != 2 {
		t.Fatalf("suspended Feishu event inbox count = %d, want 2", got)
	}

	adapterCancel()
	if err := adapter.Close(context.Background()); err != nil {
		t.Fatalf("close Feishu adapter: %v", err)
	}
	if err := <-adapterDone; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Feishu adapter run: %v", err)
	}

	direct := pipeline.executionForEvent(t, ctx, bindings[0], "feishu-a-direct")
	group := pipeline.executionForEvent(t, ctx, bindings[0], "feishu-a-group")
	other := pipeline.executionForEvent(t, ctx, bindings[1], "feishu-b-direct")
	if direct.SessionPrincipalID == group.SessionPrincipalID {
		t.Fatalf("Feishu direct and group sessions share principal %q", direct.SessionPrincipalID)
	}
	if direct.SessionPrincipalID == other.SessionPrincipalID {
		t.Fatal("Feishu same external user crossed tenant scope")
	}

	writeDeterministicSummary(t, deterministicIMSummary{
		Provider:             "feishu",
		Tenant:               bindings[0].TenantID,
		App:                  bindings[0].AppID,
		InboundEventID:       "feishu-a-direct",
		ExecutionIDs:         []string{direct.RequestID, group.RequestID, other.RequestID},
		WorkerOwner:          pipeline.workerOwner,
		RunnerCallCount:      pipeline.runners.Count(),
		ConnectionAttempts:   int(feishuFactoryCalls.Load()),
		DuplicateSuppressed:  pipeline.countInbox(ctx, bindings[0].TenantID, bindings[0].AppID, bindings[0].BindingID) == 2,
		ReplyAttemptCount:    replies.TotalAttempts(),
		ProviderReceipt:      replies.LastReceipt(),
		FinalExecutionStatus: direct.Status,
		FinalReplyStatus:     pipeline.replyStatus(t, ctx, direct.RequestID).Status,
	})
}

// TestDeterministicWeComE2E feeds the real AI Bot WebSocket frame shape into
// the production WebSocket client and lets the same durable internal path
// process the event and send a real aibot_send_msg frame back to the fake.
func TestDeterministicWeComE2E(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	replyServer := newDeterministicWeComServer(t)
	defer replyServer.Close()
	adapterServer := newDeterministicWeComServer(t)
	defer adapterServer.Close()
	adapterServer.DropFirstConnection()

	pipeline := newDeterministicPipeline(t, ctx, "wecom", "worker-wecom-e2e")
	defer pipeline.Close(t)
	bindings := seedDeterministicBindings(t, ctx, pipeline.store, channels.ChannelWeCom, "wecom")

	outbound := make(map[string]*wecom.OutboundClient, len(bindings))
	for _, binding := range bindings {
		client, err := wecom.NewOutboundClient(
			ctx,
			deterministicSecretProvider{},
			binding.Snapshot(),
			wecom.WithClientOptions(
				wecom.WithWebSocketURL(replyServer.URL()),
				wecom.WithWebSocketReconnectDelay(time.Millisecond, 5*time.Millisecond),
			),
		)
		if err != nil {
			t.Fatalf("new WeCom outbound client: %v", err)
		}
		outbound[binding.BindingID] = client
		defer func(client *wecom.OutboundClient) { _ = client.Close(context.Background()) }(client)
	}
	pipeline.providers = func(_ context.Context, delivery worker.ReplyDelivery) (worker.ReplyProvider, error) {
		client := outbound[delivery.Reply.BindingID]
		if client == nil {
			return worker.ReplyProvider{}, fmt.Errorf("missing WeCom outbound client for %s", delivery.Reply.BindingID)
		}
		return worker.ReplyProvider{Client: client}, nil
	}
	pipeline.start(t)

	adapterServer.SetEvents(map[string][]wecom.Message{
		bindings[0].ExternalAccount: {
			{MessageID: "wecom-a-direct", AIBotID: bindings[0].ExternalAccount, ChatType: "single", From: wecom.MessageFrom{UserID: "same-user"}, MessageType: "text", Text: wecom.MessageText{Content: "hello direct"}},
			{MessageID: "wecom-a-direct", AIBotID: bindings[0].ExternalAccount, ChatType: "single", From: wecom.MessageFrom{UserID: "same-user"}, MessageType: "text", Text: wecom.MessageText{Content: "hello direct"}},
			{MessageID: "wecom-a-group", AIBotID: bindings[0].ExternalAccount, ChatID: "shared-chat", ChatType: "group", From: wecom.MessageFrom{UserID: "same-user"}, MessageType: "text", Text: wecom.MessageText{Content: "hello group"}},
		},
		bindings[1].ExternalAccount: {
			{MessageID: "wecom-b-direct", AIBotID: bindings[1].ExternalAccount, ChatType: "single", From: wecom.MessageFrom{UserID: "same-user"}, MessageType: "text", Text: wecom.MessageText{Content: "hello other tenant"}},
		},
	})

	adapter, err := wecom.NewAdapter(
		pipeline.store,
		gateway.New(pipeline.store),
		deterministicSecretProvider{},
		wecom.WithClientFactory(func(binding channels.BindingSnapshot, secret string, handler wecom.MessageHandler) (wecom.ClientRunner, error) {
			return wecom.NewClient(
				binding.ExternalAccount,
				secret,
				wecom.WithWebSocketURL(adapterServer.URL()),
				wecom.WithWebSocketReconnectDelay(time.Millisecond, 5*time.Millisecond),
				wecom.WithMessageHandler(handler),
			)
		}),
		wecom.WithReconnectDelay(time.Millisecond, 5*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("new WeCom deterministic adapter: %v", err)
	}
	adapterCtx, adapterCancel := context.WithCancel(ctx)
	defer adapterCancel()
	adapterDone := make(chan error, 1)
	go func() { adapterDone <- adapter.Run(adapterCtx) }()

	waitFor(t, ctx, "WeCom provider callback frames", func() bool { return adapterServer.InboundFrames() == 4 })
	waitFor(t, ctx, "WeCom full pipeline", func() bool {
		return pipeline.countInbox(ctx, bindings[0].TenantID, bindings[0].AppID, bindings[0].BindingID) == 2 &&
			pipeline.countInbox(ctx, bindings[1].TenantID, bindings[1].AppID, bindings[1].BindingID) == 1 &&
			replyServer.OutboundFrames() == 3
	})
	for _, binding := range bindings {
		if got := adapterServer.ConnectionCount(binding.ExternalAccount); got < 2 {
			t.Fatalf("WeCom connections for %s = %d, want at least 2 after reconnect", binding.ExternalAccount, got)
		}
	}
	assertDeterministicPipeline(t, ctx, pipeline, bindings, nil, []string{
		"wecom-a-direct", "wecom-a-group", "wecom-b-direct",
	})
	if replyServer.OutboundFrames() != 3 {
		t.Fatalf("WeCom aibot_send_msg frames = %d, want 3", replyServer.OutboundFrames())
	}

	if _, err := pipeline.store.SetChannelBindingStatus(ctx, bindings[0].TenantID, bindings[0].AppID, bindings[0].BindingID, channels.BindingSuspended); err != nil {
		t.Fatalf("suspend WeCom binding through lifecycle API: %v", err)
	}
	if err := adapter.HandleMessage(ctx, bindings[0].Snapshot(), wecom.Message{
		MessageID: "wecom-a-suspended", AIBotID: bindings[0].ExternalAccount, ChatType: "single",
		From: wecom.MessageFrom{UserID: "same-user"}, MessageType: "text", Text: wecom.MessageText{Content: "must be rejected"},
	}); !errors.Is(err, channels.ErrBindingInactive) {
		t.Fatalf("suspended WeCom event error = %v, want binding inactive", err)
	}
	if got := pipeline.countInbox(ctx, bindings[0].TenantID, bindings[0].AppID, bindings[0].BindingID); got != 2 {
		t.Fatalf("suspended WeCom event inbox count = %d, want 2", got)
	}

	adapterCancel()
	if err := adapter.Close(context.Background()); err != nil {
		t.Fatalf("close WeCom adapter: %v", err)
	}
	if err := <-adapterDone; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("WeCom adapter run: %v", err)
	}

	direct := pipeline.executionForEvent(t, ctx, bindings[0], "wecom-a-direct")
	group := pipeline.executionForEvent(t, ctx, bindings[0], "wecom-a-group")
	other := pipeline.executionForEvent(t, ctx, bindings[1], "wecom-b-direct")
	if direct.SessionPrincipalID == group.SessionPrincipalID {
		t.Fatalf("WeCom direct and group sessions share principal %q", direct.SessionPrincipalID)
	}
	if direct.SessionPrincipalID == other.SessionPrincipalID {
		t.Fatal("WeCom same external user crossed tenant scope")
	}

	writeDeterministicSummary(t, deterministicIMSummary{
		Provider:             "wecom",
		Tenant:               bindings[0].TenantID,
		App:                  bindings[0].AppID,
		InboundEventID:       "wecom-a-direct",
		ExecutionIDs:         []string{direct.RequestID, group.RequestID, other.RequestID},
		WorkerOwner:          pipeline.workerOwner,
		RunnerCallCount:      pipeline.runners.Count(),
		ConnectionAttempts:   adapterServer.TotalConnections(),
		DuplicateSuppressed:  pipeline.countInbox(ctx, bindings[0].TenantID, bindings[0].AppID, bindings[0].BindingID) == 2,
		ReplyAttemptCount:    replyServer.OutboundFrames(),
		ProviderReceipt:      replyServer.LastReceipt(),
		FinalExecutionStatus: direct.Status,
		FinalReplyStatus:     pipeline.replyStatus(t, ctx, direct.RequestID).Status,
	})
}

func TestDeterministicReplyProviderScenarios(t *testing.T) {
	type scenarioCase struct {
		name           string
		scenario       deterministicProviderScenario
		wantErr        bool
		wantRetryable  bool
		wantUncertain  bool
		wantRetryAfter time.Duration
	}
	cases := []scenarioCase{
		{name: "success", scenario: deterministicProviderSuccess},
		{name: "retryable transport", scenario: deterministicProviderRetryableTransport, wantErr: true, wantRetryable: true},
		{name: "provider 5xx", scenario: deterministicProvider5xx, wantErr: true, wantRetryable: true},
		{name: "rate limit", scenario: deterministicProviderRateLimit, wantErr: true, wantRetryable: true, wantRetryAfter: 25 * time.Millisecond},
		{name: "permanent", scenario: deterministicProviderPermanent, wantErr: true},
		{name: "uncertain", scenario: deterministicProviderUncertain, wantErr: true, wantUncertain: true},
		{name: "delayed response", scenario: deterministicProviderDelayed},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			client := &deterministicReplyClient{
				scenario:   testCase.scenario,
				failures:   1,
				retryAfter: testCase.wantRetryAfter,
				delay:      2 * time.Millisecond,
			}
			started := time.Now()
			receipt, err := client.SendOnce(context.Background(), channels.Reply{RequestID: "request-1", ReplyID: "reply-1", Text: "deterministic"}, "target-1")
			if (err != nil) != testCase.wantErr {
				t.Fatalf("error = %v, want error=%t", err, testCase.wantErr)
			}
			if !testCase.wantErr && receipt.ProviderMessageID == "" {
				t.Fatal("successful deterministic provider call has no receipt")
			}
			if testCase.scenario == deterministicProviderDelayed && time.Since(started) < 2*time.Millisecond {
				t.Fatal("delayed deterministic provider returned too early")
			}
			if err == nil {
				return
			}
			var retryable interface{ IsRetryable() bool }
			if got := errors.As(err, &retryable) && retryable.IsRetryable(); got != testCase.wantRetryable {
				t.Fatalf("retryable = %t, want %t", got, testCase.wantRetryable)
			}
			var uncertain interface{ IsSideEffectUncertain() bool }
			if got := errors.As(err, &uncertain) && uncertain.IsSideEffectUncertain(); got != testCase.wantUncertain {
				t.Fatalf("uncertain = %t, want %t", got, testCase.wantUncertain)
			}
			var retryAfter interface{ RetryAfter() time.Duration }
			if got := errors.As(err, &retryAfter); got && retryAfter.RetryAfter() != testCase.wantRetryAfter {
				t.Fatalf("retry-after = %s, want %s", retryAfter.RetryAfter(), testCase.wantRetryAfter)
			}
		})
	}
}

// TestDeterministicReplyOutboxFailureScenarios drives provider failures through
// the real admission, execution journal, Reply Projection, and Reply Outbox.
// The provider client is the only fake boundary in this test.
func TestDeterministicReplyOutboxFailureScenarios(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	pipeline := newDeterministicPipeline(t, ctx, "feishu-reply-failures", "worker-feishu-reply-failures")
	defer pipeline.Close(t)
	bindings := seedDeterministicBindings(t, ctx, pipeline.store, channels.ChannelFeishu, "feishu-reply-failures")

	retryable := &deterministicReplyClient{
		scenario:   deterministicProviderRateLimit,
		failures:   1,
		retryAfter: 75 * time.Millisecond,
	}
	permanent := &deterministicReplyClient{scenario: deterministicProviderPermanent}
	uncertain := &deterministicReplyClient{scenario: deterministicProviderUncertain}
	router := &deterministicReplyRouter{clients: []*deterministicReplyClient{retryable, permanent, uncertain}}
	pipeline.providers = func(context.Context, worker.ReplyDelivery) (worker.ReplyProvider, error) {
		return worker.ReplyProvider{Client: router}, nil
	}
	pipeline.start(t)

	adapter, err := feishu.NewAdapter(
		pipeline.store,
		gateway.New(pipeline.store),
		deterministicSecretProvider{},
	)
	if err != nil {
		t.Fatalf("new Feishu Reply Outbox adapter: %v", err)
	}

	cases := []struct {
		name          string
		binding       channels.Binding
		eventID       string
		wantStatus    string
		wantAttempts  int
		wantErrorType string
		wantReceipt   bool
		client        *deterministicReplyClient
	}{
		{
			name:          "retryable rate limit",
			binding:       bindings[0],
			eventID:       "feishu-reply-retryable",
			wantStatus:    "SENT",
			wantAttempts:  2,
			wantErrorType: "provider_retryable",
			wantReceipt:   true,
			client:        retryable,
		},
		{
			name:          "permanent failure",
			binding:       bindings[1],
			eventID:       "feishu-reply-permanent",
			wantStatus:    "PERMANENTLY_FAILED",
			wantAttempts:  1,
			wantErrorType: "provider_permanent",
			client:        permanent,
		},
		{
			name:          "uncertain delivery",
			binding:       bindings[0],
			eventID:       "feishu-reply-uncertain",
			wantStatus:    "UNCERTAIN",
			wantAttempts:  1,
			wantErrorType: "provider_result_unknown",
			client:        uncertain,
		},
	}

	wantInbox := make(map[string]int, len(bindings))
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			wantInbox[testCase.binding.BindingID]++
			if err := adapter.HandleMessage(ctx, testCase.binding.Snapshot(), deterministicFeishuTextEvent(
				testCase.binding.ExternalAccount,
				testCase.eventID,
				"same-user",
				"p2p",
				"reply failure scenario",
			)); err != nil {
				t.Fatalf("admit provider failure event: %v", err)
			}

			waitFor(t, ctx, "Reply Outbox admission", func() bool {
				return pipeline.countInbox(ctx, testCase.binding.TenantID, testCase.binding.AppID, testCase.binding.BindingID) >= wantInbox[testCase.binding.BindingID]
			})
			var execution deterministicExecution
			waitFor(t, ctx, "durable execution", func() bool {
				var readErr error
				execution, readErr = pipeline.readExecution(ctx, testCase.binding, testCase.eventID)
				return readErr == nil && execution.Status == "SUCCEEDED" && execution.DispatchStatus == "CONSUMED"
			})
			if execution.Status != "SUCCEEDED" || execution.DispatchStatus != "CONSUMED" {
				t.Fatalf("execution state = %#v, want succeeded and consumed", execution)
			}

			var status deterministicReplyStatus
			waitFor(t, ctx, "durable Reply Outbox transition", func() bool {
				var readErr error
				status, readErr = pipeline.readReplyStatus(ctx, execution.RequestID)
				return readErr == nil && status.Status == testCase.wantStatus
			})
			if status.Attempt != testCase.wantAttempts || status.ErrorType != testCase.wantErrorType {
				t.Fatalf("Reply Outbox state = %#v, want attempts=%d error_type=%q", status, testCase.wantAttempts, testCase.wantErrorType)
			}
			if testCase.wantReceipt && status.ProviderMessageID == "" {
				t.Fatal("successful retry has no provider receipt")
			}
			if !testCase.wantReceipt && status.ProviderMessageID != "" {
				t.Fatalf("failed Reply Outbox has provider receipt %q", status.ProviderMessageID)
			}
			if attempts := testCase.client.TotalAttempts(); attempts != testCase.wantAttempts {
				t.Fatalf("provider calls = %d, want %d", attempts, testCase.wantAttempts)
			}
			t.Logf("reply evidence request=%s status=%s attempt=%d error_type=%s provider_calls=%d", execution.RequestID, status.Status, status.Attempt, status.ErrorType, testCase.client.TotalAttempts())
		})
	}

	if times := retryable.AttemptTimes(); len(times) != 2 || times[1].Sub(times[0]) < retryable.retryAfter {
		t.Fatalf("retry timing = %#v, want Retry-After >= %s", times, retryable.retryAfter)
	}
	uncertainAttempts := uncertain.TotalAttempts()
	select {
	case <-time.After(100 * time.Millisecond):
		if got := uncertain.TotalAttempts(); got != uncertainAttempts {
			t.Fatalf("uncertain provider calls = %d after terminal state, want %d", got, uncertainAttempts)
		}
	case <-ctx.Done():
		t.Fatalf("wait after uncertain delivery: %v", ctx.Err())
	}
}

type deterministicPipeline struct {
	ctx         context.Context
	pool        *pgxpool.Pool
	store       *platformpostgres.Store
	redis       *platformredis.Client
	stream      *platformredis.Stream
	relay       *relay.Relay
	consumer    *worker.Consumer
	sender      *worker.ReplySender
	runners     *deterministicRunnerFactory
	workerOwner string
	providers   func(context.Context, worker.ReplyDelivery) (worker.ReplyProvider, error)
	stop        context.CancelFunc
	runDone     chan error
}

func newDeterministicPipeline(
	t *testing.T,
	ctx context.Context,
	provider, workerOwner string,
) *deterministicPipeline {
	t.Helper()
	pool := openIntegrationPool(t)
	store, err := platformpostgres.New(pool, platformpostgres.WithChannelIdentityMapping(
		integrationExternalIDHasher{}, newIntegrationTargetProtector(t, "v1"), []string{"v1"},
	))
	if err != nil {
		t.Fatalf("new deterministic %s store: %v", provider, err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate deterministic %s store: %v", provider, err)
	}
	redisClient, err := platformredis.NewClient(ctx, *relayRedisTestURL)
	if err != nil {
		t.Fatalf("new deterministic %s Redis client: %v", provider, err)
	}
	stream, err := platformredis.NewStream(redisClient, "trpc-agent-service:im-deterministic:"+provider+":"+uuid.NewString(), "workers", 10*time.Millisecond)
	if err != nil {
		t.Fatalf("new deterministic %s Redis stream: %v", provider, err)
	}
	if err := stream.Init(ctx); err != nil {
		t.Fatalf("initialize deterministic %s Redis stream: %v", provider, err)
	}
	locker, err := platformredis.NewSessionLocker(redisClient, 10*time.Second)
	if err != nil {
		t.Fatalf("new deterministic session locker: %v", err)
	}
	pipeline := &deterministicPipeline{
		ctx:         ctx,
		pool:        pool,
		store:       store,
		redis:       redisClient,
		stream:      stream,
		runners:     &deterministicRunnerFactory{},
		workerOwner: workerOwner,
	}
	pipeline.relay, err = relay.New(store, stream, "relay-"+provider+"-e2e")
	if err != nil {
		t.Fatalf("new deterministic relay: %v", err)
	}
	journal, err := platformpostgres.NewExecutionEventJournal(store, platformpostgres.WithReplyEventBuilder(worker.BuildReplyEvent))
	if err != nil {
		t.Fatalf("new deterministic execution journal: %v", err)
	}
	service := worker.New(store, pipeline.runners.Build, locker, journal, nil)
	service.ModelTimeout = 5 * time.Second
	pipeline.consumer, err = worker.NewConsumerWithOptions(service, stream, store, workerOwner, worker.ConsumerOptions{
		LeaseDuration: 5 * time.Second,
		PollInterval:  10 * time.Millisecond,
		Concurrency:   1,
	})
	if err != nil {
		t.Fatalf("new deterministic worker consumer: %v", err)
	}
	pipeline.sender, err = worker.NewReplySender(
		store,
		store.ResolveReplyTarget,
		func(ctx context.Context, delivery worker.ReplyDelivery) (worker.ReplyProvider, error) {
			if pipeline.providers == nil {
				return worker.ReplyProvider{}, errors.New("deterministic reply provider is not configured")
			}
			return pipeline.providers(ctx, delivery)
		},
		worker.ReplySenderOptions{
			Owner:        "reply-" + provider + "-e2e",
			Lease:        5 * time.Second,
			SendTimeout:  2 * time.Second,
			PollInterval: 10 * time.Millisecond,
			BatchSize:    16,
			MaxAttempts:  4,
		},
	)
	if err != nil {
		t.Fatalf("new deterministic reply sender: %v", err)
	}
	t.Cleanup(func() { _ = redisClient.Close() })
	return pipeline
}

func (p *deterministicPipeline) start(t *testing.T) {
	t.Helper()
	if p.stop != nil {
		t.Fatal("deterministic pipeline already started")
	}
	runCtx, cancel := context.WithCancel(p.ctx)
	p.stop = cancel
	p.runDone = make(chan error, 3)
	go func() { p.runDone <- p.relay.Run(runCtx) }()
	go func() { p.runDone <- p.consumer.Run(runCtx) }()
	go func() { p.runDone <- p.sender.Run(runCtx) }()
}

func (p *deterministicPipeline) Close(t *testing.T) {
	t.Helper()
	if p.stop == nil {
		return
	}
	p.consumer.StopClaiming()
	p.stop()
	for range 3 {
		select {
		case err := <-p.runDone:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("deterministic pipeline loop: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("deterministic pipeline loop did not stop")
		}
	}
	p.stop = nil
}

func seedDeterministicBindings(t *testing.T, ctx context.Context, store *platformpostgres.Store, channel channels.Channel, prefix string) []channels.Binding {
	t.Helper()
	result := make([]channels.Binding, 0, 2)
	runID := uuid.NewString()
	for _, suffix := range []string{"a", "b"} {
		scope := tenant.Scope{TenantID: fmt.Sprintf("im-deterministic-%s-%s-%d", prefix, suffix, time.Now().UnixNano()), AppID: deterministicIMAppID}
		binding := seedIdentityMappingScope(t, ctx, store, scope, prefix+"-binding-"+suffix+"-"+runID, channel)
		result = append(result, binding)
	}
	return result
}

type deterministicExecution struct {
	RequestID          string
	Status             string
	SessionPrincipalID string
	SessionID          string
	UserID             string
	ConfigVersion      string
	DispatchStatus     string
}

func (p *deterministicPipeline) executionForEvent(t *testing.T, ctx context.Context, binding channels.Binding, eventID string) deterministicExecution {
	t.Helper()
	result, err := p.readExecution(ctx, binding, eventID)
	if err != nil {
		t.Fatalf("read %s execution: %v", eventID, err)
	}
	return result
}

func (p *deterministicPipeline) readExecution(ctx context.Context, binding channels.Binding, eventID string) (deterministicExecution, error) {
	var result deterministicExecution
	err := p.pool.QueryRow(ctx, `
SELECT e.request_id, e.status, e.session_principal_id, e.session_id, e.user_id,
       e.config_version, o.status
FROM platform.channel_inbox i
JOIN platform.execution e
  ON e.tenant_id = i.tenant_id AND e.app_id = i.app_id AND e.request_id = i.request_id
JOIN platform.dispatch_outbox o
  ON o.tenant_id = e.tenant_id AND o.app_id = e.app_id AND o.request_id = e.request_id
WHERE i.tenant_id = $1 AND i.app_id = $2 AND i.binding_id = $3 AND i.external_message_id = $4`,
		binding.TenantID, binding.AppID, binding.BindingID, eventID).Scan(
		&result.RequestID, &result.Status, &result.SessionPrincipalID, &result.SessionID,
		&result.UserID, &result.ConfigVersion, &result.DispatchStatus)
	if err != nil {
		return deterministicExecution{}, err
	}
	return result, nil
}

func (p *deterministicPipeline) countInbox(ctx context.Context, tenantID, appID, bindingID string) int {
	var count int
	if err := p.pool.QueryRow(ctx, `SELECT count(*) FROM platform.channel_inbox WHERE tenant_id=$1 AND app_id=$2 AND binding_id=$3`, tenantID, appID, bindingID).Scan(&count); err != nil {
		return 0
	}
	return count
}

type deterministicReplyStatus struct {
	Status            string
	Attempt           int
	ProviderMessageID string
	ErrorType         string
}

func (p *deterministicPipeline) replyStatus(t *testing.T, ctx context.Context, requestID string) deterministicReplyStatus {
	t.Helper()
	status, err := p.readReplyStatus(ctx, requestID)
	if err != nil {
		t.Fatalf("read reply status for %s: %v", requestID, err)
	}
	return status
}

func (p *deterministicPipeline) readReplyStatus(ctx context.Context, requestID string) (deterministicReplyStatus, error) {
	var status deterministicReplyStatus
	err := p.pool.QueryRow(ctx, `
SELECT status, attempt, COALESCE(provider_message_id, ''), last_error_type
FROM platform.reply_outbox WHERE request_id=$1 ORDER BY created_at LIMIT 1`, requestID).Scan(
		&status.Status, &status.Attempt, &status.ProviderMessageID, &status.ErrorType)
	return status, err
}

func assertDeterministicPipeline(
	t *testing.T,
	ctx context.Context,
	pipeline *deterministicPipeline,
	bindings []channels.Binding,
	replies *deterministicReplyClient,
	eventIDs []string,
) {
	t.Helper()
	waitFor(t, ctx, "durable execution completion", func() bool {
		for _, binding := range bindings {
			var count int
			if err := pipeline.pool.QueryRow(ctx, `SELECT count(*) FROM platform.execution WHERE tenant_id=$1 AND app_id=$2`, binding.TenantID, binding.AppID).Scan(&count); err != nil {
				return false
			}
			want := 1
			if binding == bindings[0] {
				want = 2
			}
			if count != want {
				return false
			}
		}
		return pipeline.runners.Count() == 3
	})
	waitFor(t, ctx, "reply delivery completion", func() bool {
		for _, binding := range bindings {
			var count int
			if err := pipeline.pool.QueryRow(ctx, `
SELECT count(*)
FROM platform.reply_outbox r
JOIN platform.execution e
  ON e.tenant_id = r.tenant_id AND e.app_id = r.app_id AND e.request_id = r.request_id
WHERE e.tenant_id=$1 AND e.app_id=$2 AND r.status='SENT'`, binding.TenantID, binding.AppID).Scan(&count); err != nil {
				return false
			}
			want := 1
			if binding == bindings[0] {
				want = 2
			}
			if count != want {
				return false
			}
		}
		return true
	})
	for _, eventID := range eventIDs {
		var binding channels.Binding
		if strings.Contains(eventID, "-a-") {
			binding = bindings[0]
		} else {
			binding = bindings[1]
		}
		execution := pipeline.executionForEvent(t, ctx, binding, eventID)
		if execution.Status != "SUCCEEDED" || execution.ConfigVersion != "v1" || execution.DispatchStatus != "CONSUMED" {
			t.Fatalf("%s durable state = %#v", eventID, execution)
		}
		status := pipeline.replyStatus(t, ctx, execution.RequestID)
		if status.Status != "SENT" || status.ProviderMessageID == "" || status.Attempt != 1 {
			t.Fatalf("%s reply state = %#v", eventID, status)
		}
	}
	if pipeline.runners.Count() != len(eventIDs) {
		t.Fatalf("runner calls = %d, want %d after duplicate suppression", pipeline.runners.Count(), len(eventIDs))
	}
	if replies != nil && replies.TotalAttempts() != len(eventIDs) {
		t.Fatalf("fake Feishu send attempts = %d, want %d", replies.TotalAttempts(), len(eventIDs))
	}
}

func waitFor(t *testing.T, ctx context.Context, name string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		if condition() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", name)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("waiting for %s: %v", name, ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
}

type deterministicRunnerFactory struct {
	mu    sync.Mutex
	calls []deterministicRunnerCall
}

type deterministicRunnerCall struct {
	RequestID string
	UserID    string
	SessionID string
}

func (f *deterministicRunnerFactory) Build(_ context.Context, exec worker.Execution) (frameworkrunner.Runner, error) {
	return &deterministicRunner{factory: f, requestID: exec.RequestID}, nil
}

func (f *deterministicRunnerFactory) record(call deterministicRunnerCall) {
	f.mu.Lock()
	f.calls = append(f.calls, call)
	f.mu.Unlock()
}

func (f *deterministicRunnerFactory) Count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

type deterministicRunner struct {
	factory   *deterministicRunnerFactory
	requestID string
}

func (r *deterministicRunner) Run(
	_ context.Context,
	userID, sessionID string,
	_ model.Message,
	_ ...agent.RunOption,
) (<-chan *event.Event, error) {
	r.factory.record(deterministicRunnerCall{RequestID: r.requestID, UserID: userID, SessionID: sessionID})
	result := make(chan *event.Event, 2)
	result <- event.NewResponseEvent(r.requestID, "deterministic-runner", &model.Response{
		Object: model.ObjectTypeChatCompletion,
		Done:   true,
		Choices: []model.Choice{{
			Index:   0,
			Message: model.NewAssistantMessage("deterministic reply"),
		}},
	})
	result <- &event.Event{
		RequestID: r.requestID,
		Response:  &model.Response{Object: model.ObjectTypeRunnerCompletion, Done: true},
	}
	close(result)
	return result, nil
}

func (r *deterministicRunner) Close() error { return nil }

type deterministicReplyClient struct {
	mu         sync.Mutex
	attempts   []deterministicReplyAttempt
	scenario   deterministicProviderScenario
	failures   int
	retryAfter time.Duration
	delay      time.Duration
}

type deterministicReplyRouter struct {
	mu      sync.Mutex
	clients []*deterministicReplyClient
	routes  map[string]*deterministicReplyClient
	next    int
}

type deterministicProviderScenario string

const (
	deterministicProviderSuccess            deterministicProviderScenario = "success"
	deterministicProviderRetryableTransport deterministicProviderScenario = "retryable_transport"
	deterministicProvider5xx                deterministicProviderScenario = "provider_5xx"
	deterministicProviderRateLimit          deterministicProviderScenario = "rate_limit"
	deterministicProviderPermanent          deterministicProviderScenario = "permanent"
	deterministicProviderUncertain          deterministicProviderScenario = "uncertain"
	deterministicProviderDelayed            deterministicProviderScenario = "delayed"
)

type deterministicReplyAttempt struct {
	RequestID string
	Target    string
	Text      string
	StartedAt time.Time
	Receipt   string
	Status    string
	Error     string
}

func (c *deterministicReplyClient) SendOnce(ctx context.Context, reply channels.Reply, target string) (channels.ProviderReceipt, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	startedAt := time.Now()
	c.mu.Lock()
	attempt := len(c.attempts) + 1
	scenario := c.scenario
	failures := c.failures
	retryAfter := c.retryAfter
	delay := c.delay
	c.mu.Unlock()
	if scenario == "" {
		scenario = deterministicProviderSuccess
	}
	if delay > 0 {
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return channels.ProviderReceipt{}, ctx.Err()
		case <-timer.C:
		}
	}
	receipt := "feishu-receipt-" + strconv.Itoa(attempt)
	var sendErr error
	switch scenario {
	case deterministicProviderRetryableTransport:
		if attempt <= defaultDeterministicFailures(failures) {
			sendErr = deterministicProviderError{scenario: scenario, retryable: true}
		}
	case deterministicProvider5xx:
		if attempt <= defaultDeterministicFailures(failures) {
			sendErr = deterministicProviderError{scenario: scenario, retryable: true, statusCode: 503}
		}
	case deterministicProviderRateLimit:
		if attempt <= defaultDeterministicFailures(failures) {
			sendErr = deterministicProviderError{scenario: scenario, retryable: true, retryAfter: retryAfter}
		}
	case deterministicProviderPermanent:
		sendErr = deterministicProviderError{scenario: scenario}
	case deterministicProviderUncertain:
		sendErr = deterministicProviderError{scenario: scenario, uncertain: true}
	case deterministicProviderSuccess, deterministicProviderDelayed:
	default:
		sendErr = errors.New("unknown deterministic provider scenario")
	}
	c.mu.Lock()
	attemptRecord := deterministicReplyAttempt{RequestID: reply.RequestID, Target: target, Text: reply.Text, StartedAt: startedAt, Receipt: receipt}
	if sendErr != nil {
		attemptRecord.Error = sendErr.Error()
		attemptRecord.Receipt = ""
		attemptRecord.Status = "FAILED"
	} else {
		attemptRecord.Status = "SENT"
	}
	c.attempts = append(c.attempts, attemptRecord)
	c.mu.Unlock()
	if sendErr != nil {
		return channels.ProviderReceipt{}, sendErr
	}
	return channels.ProviderReceipt{ProviderMessageID: receipt}, nil
}

func (r *deterministicReplyRouter) SendOnce(ctx context.Context, reply channels.Reply, target string) (channels.ProviderReceipt, error) {
	if r == nil {
		return channels.ProviderReceipt{}, errors.New("deterministic reply router is nil")
	}
	r.mu.Lock()
	if r.routes == nil {
		r.routes = make(map[string]*deterministicReplyClient)
	}
	client := r.routes[reply.RequestID]
	if client == nil && r.next < len(r.clients) {
		client = r.clients[r.next]
		r.next++
		r.routes[reply.RequestID] = client
	}
	r.mu.Unlock()
	if client == nil {
		return channels.ProviderReceipt{}, fmt.Errorf("no deterministic reply client for request %s", reply.RequestID)
	}
	return client.SendOnce(ctx, reply, target)
}

func defaultDeterministicFailures(value int) int {
	if value <= 0 {
		return 1
	}
	return value
}

type deterministicProviderError struct {
	scenario   deterministicProviderScenario
	retryable  bool
	uncertain  bool
	retryAfter time.Duration
	statusCode int
}

func (e deterministicProviderError) Error() string {
	if e.statusCode != 0 {
		return fmt.Sprintf("deterministic provider status %d", e.statusCode)
	}
	return "deterministic provider " + string(e.scenario)
}

func (e deterministicProviderError) IsRetryable() bool           { return e.retryable }
func (e deterministicProviderError) IsSideEffectUncertain() bool { return e.uncertain }
func (e deterministicProviderError) RetryAfter() time.Duration   { return e.retryAfter }

func (c *deterministicReplyClient) TotalAttempts() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.attempts)
}

func (c *deterministicReplyClient) AttemptTimes() []time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([]time.Time, 0, len(c.attempts))
	for _, attempt := range c.attempts {
		result = append(result, attempt.StartedAt)
	}
	return result
}

func (c *deterministicReplyClient) LastReceipt() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.attempts) == 0 {
		return ""
	}
	return c.attempts[len(c.attempts)-1].Receipt
}

type deterministicSecretProvider struct{}

func (deterministicSecretProvider) ResolveSecret(context.Context, tenant.Scope, tenant.SecretRef) (string, error) {
	return "deterministic-provider-secret", nil
}

var _ platformsecret.SecretProvider = deterministicSecretProvider{}

type deterministicFeishuClient struct {
	handler   *larkdispatcher.EventDispatcher
	events    []*larkim.P2MessageReceiveV1
	failFirst *atomic.Bool
	onError   func(error)
}

func (c *deterministicFeishuClient) Start(ctx context.Context) error {
	if c.failFirst != nil && c.failFirst.CompareAndSwap(false, true) {
		return errors.New("deterministic Feishu connection failure")
	}
	for _, received := range c.events {
		payload, err := json.Marshal(received)
		if err == nil {
			_, err = c.handler.Do(ctx, payload)
		}
		if err != nil {
			if c.onError != nil {
				c.onError(err)
			}
			return err
		}
	}
	<-ctx.Done()
	return ctx.Err()
}

func (*deterministicFeishuClient) CloseAndWait(context.Context) error { return nil }

func deterministicFeishuTextEvent(appID, messageID, senderID, chatType, text string) *larkim.P2MessageReceiveV1 {
	content, _ := json.Marshal(map[string]string{"text": text})
	message := &larkim.EventMessage{
		MessageId:   stringPointer(messageID),
		ChatType:    stringPointer(chatType),
		MessageType: stringPointer("text"),
		Content:     stringPointer(string(content)),
	}
	if chatType == "group" {
		message.ChatId = stringPointer("shared-chat")
	}
	return &larkim.P2MessageReceiveV1{
		EventV2Base: &larkevent.EventV2Base{Header: &larkevent.EventHeader{
			EventID:   "event-" + messageID,
			EventType: "im.message.receive_v1",
			AppID:     appID,
			TenantKey: "attacker-tenant-must-not-route",
		}},
		Event: &larkim.P2MessageReceiveV1Data{
			Sender:  &larkim.EventSender{SenderId: &larkim.UserId{OpenId: &senderID}},
			Message: message,
		},
	}
}

func stringPointer(value string) *string { return &value }

type deterministicIMSummary struct {
	Provider             string   `json:"provider"`
	Tenant               string   `json:"tenant"`
	App                  string   `json:"app"`
	InboundEventID       string   `json:"inbound_event_id"`
	ExecutionIDs         []string `json:"execution_ids"`
	WorkerOwner          string   `json:"worker_owner"`
	RunnerCallCount      int      `json:"runner_call_count"`
	ConnectionAttempts   int      `json:"connection_attempts"`
	DuplicateSuppressed  bool     `json:"duplicate_suppressed"`
	ReplyAttemptCount    int      `json:"reply_attempt_count"`
	ProviderReceipt      string   `json:"provider_receipt"`
	FinalExecutionStatus string   `json:"final_execution_status"`
	FinalReplyStatus     string   `json:"final_reply_status"`
}

func writeDeterministicSummary(t *testing.T, summary deterministicIMSummary) {
	t.Helper()
	encoded, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		t.Fatalf("encode deterministic IM summary: %v", err)
	}
	t.Log(string(encoded))
	path := os.Getenv("IM_E2E_REPORT_PATH")
	if path == "" {
		return
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("create deterministic IM report directory: %v", err)
	}
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatalf("write deterministic IM report: %v", err)
	}
}

type deterministicWeComFrame struct {
	Cmd     string                    `json:"cmd,omitempty"`
	Headers deterministicWeComHeaders `json:"headers"`
	Body    json.RawMessage           `json:"body,omitempty"`
	ErrCode int                       `json:"errcode,omitempty"`
	ErrMsg  string                    `json:"errmsg,omitempty"`
}

type deterministicWeComHeaders struct {
	ReqID string `json:"req_id"`
}

type deterministicWeComServer struct {
	server              *httptest.Server
	mu                  sync.Mutex
	events              map[string][]wecom.Message
	connections         map[string]int
	dropFirstConnection bool
	attempts            []deterministicReplyAttempt
	inbound             atomic.Int32
	outbound            atomic.Int32
	serverErrors        chan error
}

func newDeterministicWeComServer(t *testing.T) *deterministicWeComServer {
	t.Helper()
	result := &deterministicWeComServer{
		events:       make(map[string][]wecom.Message),
		connections:  make(map[string]int),
		serverErrors: make(chan error, 8),
	}
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	result.server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		conn, err := upgrader.Upgrade(writer, request, nil)
		if err != nil {
			result.serverErrors <- err
			return
		}
		defer conn.Close()
		auth, err := readDeterministicWeComFrame(conn)
		if err != nil {
			result.serverErrors <- err
			return
		}
		var credentials struct {
			BotID  string `json:"bot_id"`
			Secret string `json:"secret"`
		}
		if err := json.Unmarshal(auth.Body, &credentials); err != nil {
			result.serverErrors <- err
			return
		}
		if credentials.BotID == "" || credentials.Secret != "deterministic-provider-secret" {
			result.serverErrors <- errors.New("deterministic WeCom credential frame is invalid")
			return
		}
		result.mu.Lock()
		result.connections[credentials.BotID]++
		connectionNumber := result.connections[credentials.BotID]
		events := append([]wecom.Message(nil), result.events[credentials.BotID]...)
		dropFirstConnection := result.dropFirstConnection && connectionNumber == 1
		sendEvents := connectionNumber == 1 || (result.dropFirstConnection && connectionNumber == 2)
		result.mu.Unlock()
		if dropFirstConnection {
			_ = conn.Close()
			return
		}
		if err := writeDeterministicWeComAck(conn, auth.Headers.ReqID); err != nil {
			result.serverErrors <- err
			return
		}
		if sendEvents {
			for _, message := range events {
				if err := writeDeterministicWeComFrame(conn, deterministicWeComFrame{
					Cmd:     "aibot_msg_callback",
					Headers: deterministicWeComHeaders{ReqID: "callback-" + message.MessageID},
					Body:    mustJSON(message),
				}); err != nil {
					result.serverErrors <- err
					return
				}
				result.inbound.Add(1)
			}
		}
		for {
			frame, err := readDeterministicWeComFrame(conn)
			if err != nil {
				return
			}
			if frame.Cmd != "aibot_send_msg" {
				continue
			}
			var body struct {
				ChatID   string `json:"chatid"`
				MsgType  string `json:"msgtype"`
				Markdown struct {
					Content string `json:"content"`
				} `json:"markdown"`
			}
			if err := json.Unmarshal(frame.Body, &body); err != nil {
				result.serverErrors <- err
				return
			}
			if body.ChatID == "" || body.MsgType != "markdown" || body.Markdown.Content == "" {
				result.serverErrors <- errors.New("invalid deterministic aibot_send_msg frame")
				return
			}
			result.mu.Lock()
			result.attempts = append(result.attempts, deterministicReplyAttempt{
				RequestID: frame.Headers.ReqID,
				Target:    body.ChatID,
				Text:      body.Markdown.Content,
				Receipt:   frame.Headers.ReqID,
				Status:    "SENT",
			})
			result.mu.Unlock()
			result.outbound.Add(1)
			if err := writeDeterministicWeComAck(conn, frame.Headers.ReqID); err != nil {
				result.serverErrors <- err
				return
			}
		}
	}))
	return result
}

func (s *deterministicWeComServer) URL() string {
	return "ws" + strings.TrimPrefix(s.server.URL, "http")
}

func (s *deterministicWeComServer) SetEvents(events map[string][]wecom.Message) {
	s.mu.Lock()
	s.events = events
	s.mu.Unlock()
}

func (s *deterministicWeComServer) DropFirstConnection() {
	s.mu.Lock()
	s.dropFirstConnection = true
	s.mu.Unlock()
}

func (s *deterministicWeComServer) ConnectionCount(botID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.connections[botID]
}

func (s *deterministicWeComServer) TotalConnections() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	total := 0
	for _, count := range s.connections {
		total += count
	}
	return total
}

func (s *deterministicWeComServer) InboundFrames() int  { return int(s.inbound.Load()) }
func (s *deterministicWeComServer) OutboundFrames() int { return int(s.outbound.Load()) }

func (s *deterministicWeComServer) LastReceipt() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.attempts) == 0 {
		return ""
	}
	return s.attempts[len(s.attempts)-1].Receipt
}

func (s *deterministicWeComServer) Close() {
	if s == nil || s.server == nil {
		return
	}
	s.server.Close()
}

func readDeterministicWeComFrame(conn *websocket.Conn) (deterministicWeComFrame, error) {
	_, payload, err := conn.ReadMessage()
	if err != nil {
		return deterministicWeComFrame{}, err
	}
	var frame deterministicWeComFrame
	if err := json.Unmarshal(payload, &frame); err != nil {
		return deterministicWeComFrame{}, err
	}
	return frame, nil
}

func writeDeterministicWeComFrame(conn *websocket.Conn, frame deterministicWeComFrame) error {
	payload, err := json.Marshal(frame)
	if err != nil {
		return err
	}
	return conn.WriteMessage(websocket.TextMessage, payload)
}

func writeDeterministicWeComAck(conn *websocket.Conn, requestID string) error {
	return writeDeterministicWeComFrame(conn, deterministicWeComFrame{
		Headers: deterministicWeComHeaders{ReqID: requestID},
		ErrMsg:  "ok",
	})
}

func mustJSON(value any) json.RawMessage {
	payload, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return payload
}

var _ frameworkrunner.Runner = (*deterministicRunner)(nil)
var _ channels.ProviderOutboundClient = (*deterministicReplyClient)(nil)
