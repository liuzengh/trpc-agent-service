package feishu

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	larkevent "github.com/larksuite/oapi-sdk-go/v3/event"
	larkdispatcher "github.com/larksuite/oapi-sdk-go/v3/event/dispatcher"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	platformsecret "github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestHandleNewCommandBypassesGateway(t *testing.T) {
	binding := testFeishuBinding("tenant-a", "support", "binding-a", "app-a", "app-secret")
	source, err := config.NewStaticBindingResolver(binding)
	if err != nil {
		t.Fatal(err)
	}
	admitter := newFeishuRecordingAdmitter()
	handler := &feishuNewSessionHandler{}
	adapter, err := NewAdapter(
		source,
		gateway.New(admitter),
		testFeishuSecrets{key: binding, value: "app-secret"},
		WithCommandHandler(handler),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := adapter.HandleMessage(context.Background(), binding.Snapshot(), feishuTextEvent(
		"app-a", "om-new", "ou-1", "p2p", "/new",
	)); err != nil {
		t.Fatalf("handle /new: %v", err)
	}
	if admitter.admittedCount() != 0 {
		t.Fatal("/new entered Gateway admission")
	}
	if handler.input.Text != "/new" || handler.requestID == "" {
		t.Fatalf("command handler request = %#v", handler)
	}
}

func TestHandleNewCommandFailureRecordsReply(t *testing.T) {
	binding := testFeishuBinding("tenant-a", "support", "binding-a", "app-a", "app-secret")
	source, err := config.NewStaticBindingResolver(binding)
	if err != nil {
		t.Fatal(err)
	}
	wantErr := errors.New("switch session failed")
	admitter := newFeishuRecordingAdmitter()
	adapter, err := NewAdapter(source, gateway.New(admitter), testFeishuSecrets{key: binding, value: "app-secret"},
		WithCommandHandler(&feishuNewSessionHandler{err: wantErr}))
	if err != nil {
		t.Fatal(err)
	}
	if err := adapter.HandleMessage(context.Background(), binding.Snapshot(), feishuTextEvent(
		"app-a", "om-new-failed", "ou-1", "p2p", "/new",
	)); !errors.Is(err, wantErr) {
		t.Fatalf("handle failed /new = %v, want %v", err, wantErr)
	}
	if admitter.failureRequest.RequestID == "" || admitter.failureRequest.ChannelInput == nil {
		t.Fatalf("failure request = %#v", admitter.failureRequest)
	}
}

func TestHandleMessageUsesAppIDBindingAndSendsChannelInputToGateway(t *testing.T) {
	binding := testFeishuBinding("tenant-a", "support-a", "binding-a", "app-a", "app-secret-a")
	source, err := config.NewStaticBindingResolver(binding)
	if err != nil {
		t.Fatal(err)
	}
	admitter := newFeishuRecordingAdmitter()
	adapter := newFeishuTestAdapter(t, source, admitter, testFeishuSecrets{
		key:   binding,
		value: "app-secret-a",
	})

	if err := adapter.HandleMessage(context.Background(), binding.Snapshot(), feishuTextEvent(
		"app-a", "om-1", "ou-1", "p2p", "hello",
	)); err != nil {
		t.Fatalf("handle Feishu WS event: %v", err)
	}

	request := admitter.lastRequest(t)
	if request.Identity.Tenant.TenantID != "tenant-a" || request.Identity.Tenant.AppID != "support-a" || request.Identity.Tenant.BindingID != "binding-a" {
		t.Fatalf("admission scope = %#v, want tenant-a/support-a/binding-a", request.Identity.Tenant)
	}
	if request.ChannelInput == nil || request.ChannelInput.ExternalMessageID != "om-1" || request.ChannelInput.Text != "hello" {
		t.Fatalf("channel input = %#v", request.ChannelInput)
	}
}

func TestHandleMessageDuplicateEventUsesGatewayIdempotency(t *testing.T) {
	binding := testFeishuBinding("tenant-a", "support", "binding-a", "app-a", "app-secret")
	source, err := config.NewStaticBindingResolver(binding)
	if err != nil {
		t.Fatal(err)
	}
	admitter := newFeishuRecordingAdmitter()
	adapter := newFeishuTestAdapter(t, source, admitter, testFeishuSecrets{
		key:   binding,
		value: "app-secret",
	})
	event := feishuTextEvent("app-a", "om-duplicate", "ou-1", "p2p", "same")
	for range 2 {
		if err := adapter.HandleMessage(context.Background(), binding.Snapshot(), event); err != nil {
			t.Fatalf("handle duplicate event: %v", err)
		}
	}
	if admitter.admittedCount() != 1 {
		t.Fatalf("admitted count = %d, want 1", admitter.admittedCount())
	}
}

func TestRunCreatesOneClientPerActiveBindingAndStopsOnCancellation(t *testing.T) {
	bindingA := testFeishuBinding("tenant-a", "support", "binding-a", "app-a", "secret-a")
	bindingB := testFeishuBinding("tenant-b", "support", "binding-b", "app-b", "secret-b")
	source, err := config.NewStaticBindingResolver(bindingA, bindingB)
	if err != nil {
		t.Fatal(err)
	}
	secrets := testFeishuSecrets{
		key:   bindingA,
		value: "secret-a",
		more:  map[string]string{testFeishuSecretKey(bindingB): "secret-b"},
	}
	started := make(chan feishuFakeClientInfo, 2)
	var clientsMu sync.Mutex
	var clients []*feishuBlockingClient
	adapter, err := NewAdapter(
		source,
		gateway.New(newFeishuRecordingAdmitter()),
		secrets,
		WithClock(func() time.Time { return time.Unix(100, 0) }),
		WithClientFactory(func(binding channels.BindingSnapshot, appSecret string, _ *larkdispatcher.EventDispatcher) Client {
			client := &feishuBlockingClient{started: make(chan struct{}), closed: make(chan struct{})}
			started <- feishuFakeClientInfo{binding: binding, secret: appSecret}
			clientsMu.Lock()
			clients = append(clients, client)
			clientsMu.Unlock()
			return client
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- adapter.Run(runCtx) }()
	seen := make(map[string]string)
	for range 2 {
		select {
		case info := <-started:
			seen[info.binding.TenantID+"/"+info.binding.BindingID] = info.secret
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for binding clients")
		}
	}
	if seen["tenant-a/binding-a"] != "secret-a" || seen["tenant-b/binding-b"] != "secret-b" {
		t.Fatalf("binding client secrets = %#v", seen)
	}
	cancel()
	select {
	case err := <-runDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("run error = %v, want context canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for adapter shutdown")
	}
	clientsMu.Lock()
	clientCount := len(clients)
	clientsMu.Unlock()
	if clientCount != 2 {
		t.Fatalf("client count = %d, want 2", clientCount)
	}
}

func TestRunReconcilesNewBindingWhileExistingClientBlocks(t *testing.T) {
	bindingA := testFeishuBinding("tenant-a", "support", "binding-a", "app-a", "secret-a")
	bindingB := testFeishuBinding("tenant-b", "support", "binding-b", "app-b", "secret-b")
	source := newMutableFeishuBindingSource(bindingA)
	secrets := testFeishuSecrets{
		key:   bindingA,
		value: "secret-a",
		more:  map[string]string{testFeishuSecretKey(bindingB): "secret-b"},
	}
	started := make(chan feishuFakeClientInfo, 2)
	adapter, err := NewAdapter(
		source,
		gateway.New(newFeishuRecordingAdmitter()),
		secrets,
		WithReconcileInterval(10*time.Millisecond),
		WithClientFactory(func(binding channels.BindingSnapshot, appSecret string, _ *larkdispatcher.EventDispatcher) Client {
			started <- feishuFakeClientInfo{binding: binding, secret: appSecret}
			return &feishuBlockingClient{started: make(chan struct{}), closed: make(chan struct{})}
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- adapter.Run(runCtx) }()
	select {
	case info := <-started:
		if info.binding.BindingID != bindingA.BindingID {
			t.Fatalf("first binding = %q, want %q", info.binding.BindingID, bindingA.BindingID)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first binding")
	}
	source.Set(bindingB)
	select {
	case info := <-started:
		if info.binding.BindingID != bindingB.BindingID || info.secret != "secret-b" {
			t.Fatalf("reconciled binding = %#v, want binding-b/secret-b", info)
		}
	case <-time.After(time.Second):
		t.Fatal("new active Feishu binding waited for unrelated client")
	}
	cancel()
	select {
	case err := <-runDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("run error = %v, want context canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for adapter shutdown")
	}
}

func TestCloseLetsRunOwnActiveClientShutdown(t *testing.T) {
	binding := testFeishuBinding("tenant-a", "support", "binding-a", "app-a", "secret-a")
	source, err := config.NewStaticBindingResolver(binding)
	if err != nil {
		t.Fatal(err)
	}
	client := &feishuCloseCountingClient{
		started:      make(chan struct{}),
		closeStarted: make(chan struct{}),
		closeGate:    make(chan struct{}),
	}
	adapter, err := NewAdapter(
		source,
		gateway.New(newFeishuRecordingAdmitter()),
		testFeishuSecrets{key: binding, value: "secret-a"},
		WithClientFactory(func(channels.BindingSnapshot, string, *larkdispatcher.EventDispatcher) Client {
			return client
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := make(chan error, 1)
	go func() { runDone <- adapter.Run(runCtx) }()
	select {
	case <-client.started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Feishu client")
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- adapter.Close(context.Background()) }()
	select {
	case <-client.closeStarted:
	case <-time.After(time.Second):
		t.Fatal("adapter Close did not close the active Feishu client")
	}
	close(client.closeGate)
	if err := <-closeDone; err != nil {
		t.Fatalf("close adapter: %v", err)
	}
	if err := <-runDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("run error = %v, want context canceled", err)
	}
	if got := client.closeCalls.Load(); got != 1 {
		t.Fatalf("Feishu client close calls = %d, want 1", got)
	}
}

func TestCloseBoundsNonCooperativeBindingShutdown(t *testing.T) {
	bindings := []channels.Binding{
		testFeishuBinding("tenant-a", "support", "binding-a", "app-a", "secret-a"),
		testFeishuBinding("tenant-b", "support", "binding-b", "app-b", "secret-b"),
		testFeishuBinding("tenant-c", "support", "binding-c", "app-c", "secret-c"),
	}
	source, err := config.NewStaticBindingResolver(bindings...)
	if err != nil {
		t.Fatal(err)
	}
	secrets := testFeishuSecrets{key: bindings[0], value: "secret-a", more: map[string]string{}}
	for _, binding := range bindings[1:] {
		secrets.more[testFeishuSecretKey(binding)] = "secret-" + string(binding.BindingID[len(binding.BindingID)-1])
	}
	started := make(chan struct{}, len(bindings))
	release := make(chan struct{})
	adapter, err := NewAdapter(
		source,
		gateway.New(newFeishuRecordingAdmitter()),
		secrets,
		WithClientFactory(func(channels.BindingSnapshot, string, *larkdispatcher.EventDispatcher) Client {
			return &feishuNonCooperativeClient{started: started, release: release}
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	runDone := make(chan error, 1)
	go func() { runDone <- adapter.Run(runCtx) }()
	for range bindings {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for non-cooperative binding")
		}
	}

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 20*time.Millisecond)
	startedAt := time.Now()
	err = adapter.Close(shutdownCtx)
	cancelShutdown()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("close error = %v, want deadline exceeded", err)
	}
	if elapsed := time.Since(startedAt); elapsed > 500*time.Millisecond {
		t.Fatalf("close elapsed = %s, want one bounded grace period", elapsed)
	}

	close(release)
	select {
	case <-runDone:
	case <-time.After(time.Second):
		t.Fatal("adapter run did not finish after releasing non-cooperative clients")
	}
}

func TestTimedOutBindingRunRemainsUntilFinished(t *testing.T) {
	binding := testFeishuBinding("tenant-a", "support", "binding-a", "app-a", "secret-a")
	source, err := config.NewStaticBindingResolver(binding)
	if err != nil {
		t.Fatal(err)
	}
	adapter := newFeishuTestAdapter(t, source, newFeishuRecordingAdmitter(), testFeishuSecrets{key: binding, value: "secret-a"})
	key := bindingKey(binding.Snapshot())
	_, cancelRun := context.WithCancel(context.Background())
	run := &feishuBindingRun{snapshot: binding, cancel: cancelRun, done: make(chan struct{})}
	adapter.runsMu.Lock()
	adapter.runs[key] = run
	adapter.runsMu.Unlock()

	stopCtx, cancelStop := context.WithTimeout(context.Background(), 20*time.Millisecond)
	adapter.stopBinding(stopCtx, key, run)
	cancelStop()
	adapter.runsMu.Lock()
	_, stillPresent := adapter.runs[key]
	adapter.runsMu.Unlock()
	if !stillPresent {
		t.Fatal("timed-out binding run was removed before it finished")
	}

	var factoryCalls atomic.Int32
	adapter.clientFactory = func(channels.BindingSnapshot, string, *larkdispatcher.EventDispatcher) Client {
		factoryCalls.Add(1)
		return &feishuFailingClient{}
	}
	if err := adapter.reconcileBindings(context.Background(), []channels.Binding{binding}, make(chan feishuBindingRunResult, 1)); err != nil {
		t.Fatalf("reconcile binding: %v", err)
	}
	if got := factoryCalls.Load(); got != 0 {
		t.Fatalf("replacement client factory calls = %d, want 0", got)
	}

	close(run.done)
	adapter.finishBindingRun(feishuBindingRunResult{key: key, run: run})
}

func TestRunRebuildsChangedBindingAndStopsSuspendedBinding(t *testing.T) {
	binding := testFeishuBinding("tenant-a", "support", "binding-a", "app-a", "secret-a")
	updated := binding
	updated.BindingRevision = 2
	updated.Secret = tenant.SecretRef{Name: "secret-b", Version: "v1"}
	source := newMutableFeishuBindingSource(binding)
	secrets := testFeishuSecrets{
		key:   binding,
		value: "secret-a",
		more:  map[string]string{testFeishuSecretKey(updated): "secret-b"},
	}
	started := make(chan feishuFakeClientInfo, 2)
	clients := make(chan *feishuBlockingClient, 2)
	adapter, err := NewAdapter(
		source,
		gateway.New(newFeishuRecordingAdmitter()),
		secrets,
		WithReconcileInterval(10*time.Millisecond),
		WithClientFactory(func(binding channels.BindingSnapshot, appSecret string, _ *larkdispatcher.EventDispatcher) Client {
			client := &feishuBlockingClient{started: make(chan struct{}), closed: make(chan struct{})}
			started <- feishuFakeClientInfo{binding: binding, secret: appSecret}
			clients <- client
			return client
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := make(chan error, 1)
	go func() { runDone <- adapter.Run(runCtx) }()
	first := <-clients
	firstInfo := <-started
	if firstInfo.binding.BindingRevision != 1 || firstInfo.secret != "secret-a" {
		t.Fatalf("initial binding = %#v", firstInfo)
	}
	source.Set(updated)
	second := <-clients
	secondInfo := <-started
	if secondInfo.binding.BindingRevision != 2 || secondInfo.secret != "secret-b" {
		t.Fatalf("updated binding = %#v", secondInfo)
	}
	if err := adapter.HandleMessage(context.Background(), binding.Snapshot(), feishuTextEvent("app-a", "stale-message", "ou-1", "p2p", "stale")); !errors.Is(err, gateway.ErrChannelBindingSnapshotStale) {
		t.Fatalf("stale Feishu message error = %v", err)
	}
	select {
	case <-first.closed:
	case <-time.After(time.Second):
		t.Fatal("old Feishu client was not closed after revision change")
	}

	suspended := updated
	suspended.Status = channels.BindingSuspended
	source.Set(suspended)
	select {
	case <-second.closed:
	case <-time.After(time.Second):
		t.Fatal("active Feishu client was not closed after suspension")
	}
	cancel()
	select {
	case err := <-runDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("run error = %v, want context canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Feishu shutdown")
	}
}

func TestRunReconnectsAfterClientCloseWithBoundedDelay(t *testing.T) {
	binding := testFeishuBinding("tenant-a", "support", "binding-a", "app-a", "secret-a")
	source, err := config.NewStaticBindingResolver(binding)
	if err != nil {
		t.Fatal(err)
	}
	starts := make(chan time.Time, 4)
	var attempts atomic.Int32
	adapter, err := NewAdapter(
		source,
		gateway.New(newFeishuRecordingAdmitter()),
		testFeishuSecrets{key: binding, value: "secret-a"},
		WithReconnectDelay(20*time.Millisecond, 100*time.Millisecond),
		WithClientFactory(func(channels.BindingSnapshot, string, *larkdispatcher.EventDispatcher) Client {
			attempts.Add(1)
			starts <- time.Now()
			return &feishuFailingClient{}
		}),
	)
	if err != nil {
		t.Fatal(err)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- adapter.Run(runCtx) }()
	var first, second time.Time
	select {
	case first = <-starts:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first Feishu client")
	}
	select {
	case second = <-starts:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Feishu reconnect")
	}
	if delay := second.Sub(first); delay < 15*time.Millisecond {
		t.Fatalf("Feishu reconnect delay = %s, want bounded backoff >= 15ms", delay)
	}
	if attempts.Load() != 2 {
		t.Fatalf("Feishu client attempts before cancellation = %d, want 2", attempts.Load())
	}
	cancel()
	select {
	case err := <-runDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Feishu run error = %v, want context canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Feishu shutdown")
	}
}

func TestRunUsesCappedReconnectBackoff(t *testing.T) {
	binding := testFeishuBinding("tenant-a", "support", "binding-a", "app-a", "secret-a")
	source, err := config.NewStaticBindingResolver(binding)
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := NewAdapter(
		source,
		gateway.New(newFeishuRecordingAdmitter()),
		testFeishuSecrets{key: binding, value: "secret-a"},
		WithReconnectDelay(5*time.Millisecond, 20*time.Millisecond),
		WithClientFactory(func(channels.BindingSnapshot, string, *larkdispatcher.EventDispatcher) Client {
			return &feishuFailingClient{}
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	stopErr := errors.New("stop reconnect test")
	var delays []time.Duration
	adapter.waitReconnect = func(_ context.Context, delay time.Duration) error {
		delays = append(delays, delay)
		if len(delays) == 4 {
			return stopErr
		}
		return nil
	}
	runDone := make(chan error, 1)
	go func() { runDone <- adapter.Run(context.Background()) }()
	select {
	case err := <-runDone:
		if !errors.Is(err, stopErr) {
			t.Fatalf("run error = %v, want reconnect test stop error", err)
		}
	case <-time.After(time.Second):
		t.Fatal("reconnect test did not finish")
	}
	expected := []time.Duration{5 * time.Millisecond, 10 * time.Millisecond, 20 * time.Millisecond, 20 * time.Millisecond}
	if len(delays) != len(expected) {
		t.Fatalf("reconnect delays = %v, want %v", delays, expected)
	}
	for i := range expected {
		if delays[i] != expected[i] {
			t.Fatalf("reconnect delay[%d] = %s, want %s", i, delays[i], expected[i])
		}
	}
}

type feishuFakeClientInfo struct {
	binding channels.BindingSnapshot
	secret  string
}

type feishuBlockingClient struct {
	started chan struct{}
	closed  chan struct{}
}

type feishuCloseCountingClient struct {
	started      chan struct{}
	closeStarted chan struct{}
	closeGate    chan struct{}
	closeCalls   atomic.Int32
}

type feishuNonCooperativeClient struct {
	started chan<- struct{}
	release <-chan struct{}
}

func (c *feishuCloseCountingClient) Start(ctx context.Context) error {
	close(c.started)
	<-ctx.Done()
	return ctx.Err()
}

func (c *feishuCloseCountingClient) CloseAndWait(ctx context.Context) error {
	if c.closeCalls.Add(1) == 1 {
		close(c.closeStarted)
	}
	select {
	case <-c.closeGate:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

var _ Client = (*feishuCloseCountingClient)(nil)

type feishuFailingClient struct{}

func (c *feishuNonCooperativeClient) Start(ctx context.Context) error {
	c.started <- struct{}{}
	<-c.release
	return ctx.Err()
}

func (*feishuNonCooperativeClient) CloseAndWait(context.Context) error { return nil }

var _ Client = (*feishuNonCooperativeClient)(nil)

type mutableFeishuBindingSource struct {
	mu       sync.RWMutex
	bindings map[string]channels.Binding
}

func newMutableFeishuBindingSource(bindings ...channels.Binding) *mutableFeishuBindingSource {
	source := &mutableFeishuBindingSource{bindings: make(map[string]channels.Binding)}
	for _, binding := range bindings {
		source.bindings[bindingKey(binding.Snapshot())] = binding
	}
	return source
}

func (s *mutableFeishuBindingSource) Set(bindings ...channels.Binding) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, binding := range bindings {
		s.bindings[bindingKey(binding.Snapshot())] = binding
	}
}

func (s *mutableFeishuBindingSource) ResolveBinding(_ context.Context, tenantID, appID, bindingID string) (channels.Binding, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	binding, ok := s.bindings[tenantID+"\x00"+appID+"\x00"+bindingID]
	if !ok {
		return channels.Binding{}, errors.New("binding not found")
	}
	return binding, nil
}

func (s *mutableFeishuBindingSource) ListActiveChannelBindings(_ context.Context, channel channels.Channel) ([]channels.Binding, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]channels.Binding, 0, len(s.bindings))
	for _, binding := range s.bindings {
		if binding.Channel == channel && binding.Status == channels.BindingActive {
			result = append(result, binding)
		}
	}
	return result, nil
}

func (*feishuFailingClient) Start(context.Context) error        { return errors.New("fake websocket closed") }
func (*feishuFailingClient) CloseAndWait(context.Context) error { return nil }

var _ Client = (*feishuFailingClient)(nil)

func (c *feishuBlockingClient) Start(ctx context.Context) error {
	close(c.started)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.closed:
		return nil
	}
}

func (c *feishuBlockingClient) CloseAndWait(context.Context) error {
	select {
	case <-c.closed:
	default:
		close(c.closed)
	}
	return nil
}

var _ Client = (*feishuBlockingClient)(nil)

type feishuRecordingAdmitter struct {
	mu             sync.Mutex
	requests       []gateway.AdmissionRequest
	failureRequest gateway.AdmissionRequest
	results        map[string]gateway.AdmissionResult
}

type feishuNewSessionHandler struct {
	requestID string
	input     channels.ChannelInput
	err       error
}

func (h *feishuNewSessionHandler) HandleNewSession(_ context.Context, request channels.NewSessionRequest) error {
	h.requestID = request.RequestID
	h.input = request.Input
	return h.err
}

func newFeishuRecordingAdmitter() *feishuRecordingAdmitter {
	return &feishuRecordingAdmitter{results: make(map[string]gateway.AdmissionResult)}
}

func (a *feishuRecordingAdmitter) Admit(_ context.Context, request gateway.AdmissionRequest) (gateway.AdmissionResult, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.requests = append(a.requests, request)
	if result, ok := a.results[request.IdempotencyKey]; ok {
		result.Replayed = true
		return result, nil
	}
	result := gateway.AdmissionResult{
		RequestID:     request.RequestID,
		ConfigVersion: "v1",
		TurnSeq:       int64(len(a.results) + 1),
		Status:        gateway.AdmissionStatusAdmitted,
	}
	a.results[request.IdempotencyKey] = result
	return result, nil
}

func (a *feishuRecordingAdmitter) RecordChannelFailure(_ context.Context, request gateway.AdmissionRequest) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.failureRequest = request
	return nil
}

func (a *feishuRecordingAdmitter) lastRequest(t *testing.T) gateway.AdmissionRequest {
	t.Helper()
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.requests) == 0 {
		t.Fatal("gateway received no request")
	}
	return a.requests[len(a.requests)-1]
}

func (a *feishuRecordingAdmitter) admittedCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.results)
}

type testFeishuSecrets struct {
	key   channels.Binding
	value string
	more  map[string]string
}

func (p testFeishuSecrets) ResolveSecret(_ context.Context, scope tenant.Scope, ref tenant.SecretRef) (string, error) {
	key := scope.TenantID + "\x00" + scope.AppID + "\x00" + ref.Name
	if key == testFeishuSecretKey(p.key) {
		return p.value, nil
	}
	if value := p.more[key]; value != "" {
		return value, nil
	}
	return "", errors.New("test Feishu secret not found")
}

func testFeishuSecretKey(binding channels.Binding) string {
	return binding.TenantID + "\x00" + binding.AppID + "\x00" + binding.Secret.Name
}

func newFeishuTestAdapter(
	t *testing.T,
	source channels.BindingSource,
	admitter *feishuRecordingAdmitter,
	secrets platformsecret.SecretProvider,
) *Adapter {
	t.Helper()
	adapter, err := NewAdapter(source, gateway.New(admitter), secrets)
	if err != nil {
		t.Fatal(err)
	}
	return adapter
}

func testFeishuBinding(tenantID, appID, bindingID, externalAccount, secret string) channels.Binding {
	return channels.Binding{
		TenantID:        tenantID,
		AppID:           appID,
		BindingID:       bindingID,
		Channel:         channels.ChannelFeishu,
		ExternalAccount: externalAccount,
		Secret:          tenant.SecretRef{Name: secret, Version: "v1"},
		BindingRevision: 1,
		Status:          channels.BindingActive,
	}
}

func feishuTextEvent(appID, messageID, senderID, chatType, text string) *larkim.P2MessageReceiveV1 {
	return &larkim.P2MessageReceiveV1{
		EventV2Base: &larkevent.EventV2Base{Header: &larkevent.EventHeader{
			EventID:   "event-" + messageID,
			EventType: feishuMessageEventType,
			AppID:     appID,
		}},
		Event: &larkim.P2MessageReceiveV1Data{
			Sender: &larkim.EventSender{SenderId: &larkim.UserId{OpenId: &senderID}},
			Message: &larkim.EventMessage{
				MessageId:   &messageID,
				ChatType:    &chatType,
				MessageType: stringPointer("text"),
				Content:     stringPointer(`{"text":"` + text + `"}`),
			},
		},
	}
}

func stringPointer(value string) *string {
	return &value
}

var _ platformsecret.SecretProvider = testFeishuSecrets{}
