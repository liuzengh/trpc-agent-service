package feishu

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	larkdispatcher "github.com/larksuite/oapi-sdk-go/v3/event/dispatcher"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
	larkws "github.com/larksuite/oapi-sdk-go/v3/ws"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	platformlog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	platformmetrics "github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	platformsecret "github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	platformtelemetry "github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"go.opentelemetry.io/otel/attribute"
)

const (
	defaultReconnectInitial  = time.Second
	defaultReconnectMax      = 30 * time.Second
	defaultReconcileInterval = time.Second
	clientCloseTimeout       = 5 * time.Second
	feishuMessageTargetTTL   = time.Hour
)

var errAttachmentIngestorReq = errors.New("attachment ingestor is required")

// Client is the lifecycle surface used by the adapter. The production value
// is the official Feishu SDK WebSocket client.
type Client interface {
	Start(context.Context) error
	CloseAndWait(context.Context) error
}

// ClientFactory is a test seam for the official SDK client construction. A
// factory is called once for each active tenant/application/binding.
type ClientFactory func(
	binding channels.BindingSnapshot,
	appSecret string,
	handler *larkdispatcher.EventDispatcher,
) Client

// AdapterOption configures a Feishu long-connection adapter.
type AdapterOption func(*Adapter) error

// WithClock supplies the clock used for inbound timestamps and message target expiry.
func WithClock(clock func() time.Time) AdapterOption {
	return func(adapter *Adapter) error {
		if clock == nil {
			return errors.New("clock is required")
		}
		adapter.now = clock
		return nil
	}
}

// WithAttachmentIngestor registers the IM-07 media materialization boundary.
func WithAttachmentIngestor(ingestor channels.AttachmentIngestor) AdapterOption {
	return func(adapter *Adapter) error {
		adapter.attachmentIngestor = ingestor
		return nil
	}
}

// WithRecallAdmitter registers the durable, provider-neutral recall boundary.
func WithRecallAdmitter(admitter channels.RecallAdmitter) AdapterOption {
	return func(adapter *Adapter) error {
		adapter.recallAdmitter = admitter
		return nil
	}
}

// WithMetrics attaches the low-cardinality IM event metrics recorder.
func WithMetrics(recorder *platformmetrics.Recorder) AdapterOption {
	return func(adapter *Adapter) error {
		adapter.metrics = recorder
		return nil
	}
}

// WithCommandHandler registers the durable platform command boundary used by
// /new. Commands are handled before Gateway and never enter Runner admission.
func WithCommandHandler(handler channels.NewSessionHandler) AdapterOption {
	return func(adapter *Adapter) error {
		if handler == nil {
			return errors.New("new session command handler is required")
		}
		adapter.commandHandler = handler
		return nil
	}
}

// WithClientFactory replaces only the SDK client constructor. Production code
// should leave it unset so the official Feishu WebSocket SDK is used.
func WithClientFactory(factory ClientFactory) AdapterOption {
	return func(adapter *Adapter) error {
		if factory == nil {
			return errors.New("client factory is required")
		}
		adapter.clientFactory = factory
		return nil
	}
}

// WithReconnectDelay configures the small adapter-level recovery delay used
// when the SDK client itself terminates. The SDK also performs its own normal
// WebSocket reconnects.
func WithReconnectDelay(initial, maximum time.Duration) AdapterOption {
	return func(adapter *Adapter) error {
		if initial <= 0 || maximum < initial {
			return errors.New("reconnect delay is invalid")
		}
		adapter.reconnectInitial = initial
		adapter.reconnectMax = maximum
		return nil
	}
}

// WithReconcileInterval controls how often the adapter refreshes active
// binding snapshots. A short interval is useful for tests and deployments
// that need prompt control-plane changes without restarting the service.
func WithReconcileInterval(interval time.Duration) AdapterOption {
	return func(adapter *Adapter) error {
		if interval <= 0 {
			return errors.New("reconcile interval is invalid")
		}
		adapter.reconcileInterval = interval
		return nil
	}
}

// Adapter owns one official Feishu long-connection client per active Binding
// and submits normalized events to Gateway. It does not call Runner or own
// Reply Outbox delivery.
type Adapter struct {
	bindings           channels.BindingSource
	admissionGateway   *gateway.Gateway
	secrets            platformsecret.SecretProvider
	attachmentIngestor channels.AttachmentIngestor
	commandHandler     channels.NewSessionHandler
	recallAdmitter     channels.RecallAdmitter
	now                func() time.Time
	metrics            *platformmetrics.Recorder
	clientFactory      ClientFactory
	waitReconnect      func(context.Context, time.Duration) error
	reconnectInitial   time.Duration
	reconnectMax       time.Duration
	reconcileInterval  time.Duration

	runMu     sync.Mutex
	runCancel context.CancelFunc
	runDone   chan struct{}
	clientsMu sync.Mutex
	clients   map[string]Client
	runsMu    sync.Mutex
	runs      map[string]*feishuBindingRun
}

type feishuBindingRun struct {
	snapshot channels.Binding
	cancel   context.CancelFunc
	done     chan struct{}
	stopping atomic.Bool
	clientMu sync.Mutex
	client   *feishuClientHandle
}

type feishuClientHandle struct {
	client    Client
	closeOnce sync.Once
	closeErr  error
}

func (h *feishuClientHandle) close(ctx context.Context) error {
	if h == nil || h.client == nil {
		return nil
	}
	h.closeOnce.Do(func() {
		h.closeErr = h.client.CloseAndWait(ctx)
	})
	return h.closeErr
}

type feishuBindingRunResult struct {
	key string
	run *feishuBindingRun
	err error
}

// NewAdapter creates a Feishu adapter using the official SDK's WebSocket
// event subscription. Binding.ExternalAccount is App ID and Binding.Secret is
// the App Secret reference.
func NewAdapter(
	bindings channels.BindingSource,
	admissionGateway *gateway.Gateway,
	secrets platformsecret.SecretProvider,
	opts ...AdapterOption,
) (*Adapter, error) {
	if bindings == nil {
		return nil, errors.New("binding source is required")
	}
	if admissionGateway == nil {
		return nil, errors.New("gateway is required")
	}
	if secrets == nil {
		return nil, errors.New("secret provider is required")
	}
	adapter := &Adapter{
		bindings:          bindings,
		admissionGateway:  admissionGateway,
		secrets:           secrets,
		now:               time.Now,
		clientFactory:     defaultClientFactory,
		waitReconnect:     waitReconnect,
		reconnectInitial:  defaultReconnectInitial,
		reconnectMax:      defaultReconnectMax,
		reconcileInterval: defaultReconcileInterval,
		clients:           make(map[string]Client),
		runs:              make(map[string]*feishuBindingRun),
	}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(adapter); err != nil {
			return nil, err
		}
	}
	return adapter, nil
}

func defaultClientFactory(
	binding channels.BindingSnapshot,
	appSecret string,
	handler *larkdispatcher.EventDispatcher,
) Client {
	return larkws.NewClient(
		binding.ExternalAccount,
		appSecret,
		larkws.WithEventHandler(handler),
		larkws.WithAutoReconnect(true),
		larkws.WithOnError(func(err error) {
			log.Printf("feishu websocket error: %s", platformlog.SafeError(err))
		}),
	)
}

// Run reconciles active bindings and runs one SDK client for each binding.
// Binding additions and updates do not wait for unrelated long connections to
// exit; changed/removed snapshots are canceled and closed independently.
func (a *Adapter) Run(ctx context.Context) error {
	if a == nil || a.bindings == nil || a.admissionGateway == nil || a.secrets == nil {
		return errors.New("feishu adapter is not initialized")
	}
	if ctx == nil {
		return errors.New("context is required")
	}
	runCtx, cancel := context.WithCancel(ctx)
	runDone := make(chan struct{})
	a.runMu.Lock()
	if a.runCancel != nil {
		a.runMu.Unlock()
		cancel()
		return errors.New("feishu adapter is already running")
	}
	a.runCancel = cancel
	a.runDone = runDone
	a.runMu.Unlock()
	defer func() {
		cancel()
		close(runDone)
		a.runMu.Lock()
		a.runCancel = nil
		a.runDone = nil
		a.runMu.Unlock()
	}()
	results := make(chan feishuBindingRunResult, 32)
	ticker := time.NewTicker(a.reconcileInterval)
	defer ticker.Stop()
	for {
		bindings, err := a.bindings.ListActiveChannelBindings(runCtx, channels.ChannelFeishu)
		if err != nil {
			a.stopAllBindings(runCtx)
			return fmt.Errorf("list active feishu bindings: %w", err)
		}
		if err := a.reconcileBindings(runCtx, bindings, results); err != nil {
			a.stopAllBindings(runCtx)
			return err
		}
		select {
		case <-runCtx.Done():
			stopCtx, stopCancel := context.WithTimeout(context.Background(), clientCloseTimeout)
			a.stopAllBindings(stopCtx)
			stopCancel()
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return runCtx.Err()
		case result := <-results:
			a.finishBindingRun(result)
			if result.err != nil && !errors.Is(result.err, context.Canceled) {
				a.stopAllBindings(runCtx)
				return result.err
			}
		case <-ticker.C:
		}
	}
}

func (a *Adapter) reconcileBindings(
	ctx context.Context,
	bindings []channels.Binding,
	results chan<- feishuBindingRunResult,
) error {
	desired := make(map[string]channels.Binding, len(bindings))
	for _, binding := range bindings {
		if err := binding.Validate(); err != nil {
			return fmt.Errorf("feishu binding: %w", err)
		}
		desired[bindingKey(binding.Snapshot())] = binding
	}
	a.runsMu.Lock()
	current := make(map[string]*feishuBindingRun, len(a.runs))
	for key, run := range a.runs {
		current[key] = run
	}
	a.runsMu.Unlock()
	for key, run := range current {
		binding, ok := desired[key]
		if !ok {
			a.stopBinding(ctx, key, run)
			continue
		}
		if feishuBindingRuntimeEqual(run.snapshot, binding) {
			continue
		}
		a.stopBinding(ctx, key, run)
	}
	for key, binding := range desired {
		a.runsMu.Lock()
		run := a.runs[key]
		a.runsMu.Unlock()
		if run != nil {
			continue
		}
		bindingCtx, cancel := context.WithCancel(ctx)
		run = &feishuBindingRun{snapshot: binding, cancel: cancel, done: make(chan struct{})}
		a.runsMu.Lock()
		if existing := a.runs[key]; existing != nil {
			a.runsMu.Unlock()
			cancel()
			continue
		}
		a.runs[key] = run
		a.runsMu.Unlock()
		go func(key string, run *feishuBindingRun, binding channels.Binding, bindingCtx context.Context) {
			err := a.runBinding(bindingCtx, binding, run)
			close(run.done)
			if run.stopping.Load() {
				a.finishBindingRun(feishuBindingRunResult{key: key, run: run, err: err})
			}
			select {
			case results <- feishuBindingRunResult{key: key, run: run, err: err}:
			case <-bindingCtx.Done():
			}
		}(key, run, binding, bindingCtx)
	}
	return nil
}

func feishuBindingRuntimeEqual(left, right channels.Binding) bool {
	return left.TenantID == right.TenantID && left.AppID == right.AppID &&
		left.BindingID == right.BindingID && left.Channel == right.Channel &&
		left.ExternalAccount == right.ExternalAccount && left.Secret == right.Secret &&
		left.BindingRevision == right.BindingRevision && left.Status == right.Status
}

func (a *Adapter) finishBindingRun(result feishuBindingRunResult) {
	a.runsMu.Lock()
	if current := a.runs[result.key]; current == result.run {
		delete(a.runs, result.key)
	}
	a.runsMu.Unlock()
}

func (a *Adapter) stopAllBindings(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	a.runsMu.Lock()
	current := make(map[string]*feishuBindingRun, len(a.runs))
	for key, run := range a.runs {
		current[key] = run
	}
	a.runsMu.Unlock()
	var wg sync.WaitGroup
	for key, run := range current {
		wg.Add(1)
		go func(key string, run *feishuBindingRun) {
			defer wg.Done()
			a.stopBinding(ctx, key, run)
		}(key, run)
	}
	wg.Wait()
}

func (a *Adapter) stopBinding(ctx context.Context, key string, run *feishuBindingRun) {
	if run == nil {
		return
	}
	stopCtx, stopCancel := boundedStopContext(ctx)
	defer stopCancel()
	a.runsMu.Lock()
	if current := a.runs[key]; current != run {
		a.runsMu.Unlock()
		return
	}
	a.runsMu.Unlock()
	run.stopping.Store(true)
	run.clientMu.Lock()
	client := run.client
	run.clientMu.Unlock()
	run.cancel()
	if client != nil {
		closeFeishuClient(stopCtx, client)
	}
	select {
	case <-run.done:
		a.finishBindingRun(feishuBindingRunResult{key: key, run: run})
	case <-stopCtx.Done():
	}
}

func (a *Adapter) runBinding(ctx context.Context, binding channels.Binding, run *feishuBindingRun) error {
	if err := binding.Validate(); err != nil {
		return fmt.Errorf("feishu binding: %w", err)
	}
	reconnectDelay := a.reconnectInitial
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		current, err := a.bindings.ResolveBinding(ctx, binding.TenantID, binding.AppID, binding.BindingID)
		if err != nil {
			return fmt.Errorf("resolve feishu binding: %w", err)
		}
		if current.Status != channels.BindingActive {
			return nil
		}
		if current.Channel != channels.ChannelFeishu || current.BindingRevision != binding.BindingRevision || current.ExternalAccount != binding.ExternalAccount {
			if current.Channel != channels.ChannelFeishu {
				return nil
			}
			if err := current.Validate(); err != nil {
				return fmt.Errorf("updated feishu binding: %w", err)
			}
			binding = current
			continue
		}
		appSecret, err := a.secrets.ResolveSecret(ctx, current.Scope(), current.Secret)
		if err != nil {
			return fmt.Errorf("resolve feishu app secret: %w", err)
		}
		if appSecret == "" {
			return errors.New("feishu app secret is required")
		}
		bindingSnapshot := current.Snapshot()
		handler := a.eventDispatcher(bindingSnapshot)
		client := a.clientFactory(bindingSnapshot, appSecret, handler)
		if client == nil {
			return errors.New("feishu client factory returned nil")
		}
		key := bindingKey(bindingSnapshot)
		handle := &feishuClientHandle{client: client}
		run.clientMu.Lock()
		run.client = handle
		run.clientMu.Unlock()
		a.trackClient(key, client)
		a.recordConnection(ctx, bindingSnapshot, channels.ConnectionReady, nil)
		startErr := client.Start(ctx)
		a.untrackClient(key, client)
		closeFeishuClient(ctx, handle)
		run.clientMu.Lock()
		if run.client == handle {
			run.client = nil
		}
		run.clientMu.Unlock()
		if ctx.Err() != nil {
			a.recordConnection(context.WithoutCancel(ctx), bindingSnapshot, channels.ConnectionNotReady, nil)
			return ctx.Err()
		}
		if startErr != nil {
			// Recreate the SDK client after a terminal SDK failure; its normal
			// connection drops are already handled by WithAutoReconnect(true).
			a.recordConnection(ctx, bindingSnapshot, channels.ConnectionDegraded, startErr)
		} else {
			reconnectDelay = a.reconnectInitial
		}
		wait := a.waitReconnect
		if wait == nil {
			wait = waitReconnect
		}
		if err := wait(ctx, reconnectDelay); err != nil {
			return err
		}
		reconnectDelay = nextBackoff(reconnectDelay, a.reconnectMax)
	}
}

func closeFeishuClient(ctx context.Context, handle *feishuClientHandle) {
	if handle == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	closeCtx, cancel := context.WithTimeout(ctx, clientCloseTimeout)
	defer cancel()
	_ = handle.close(closeCtx)
}

func (a *Adapter) recordConnection(
	ctx context.Context,
	binding channels.BindingSnapshot,
	status channels.ConnectionStatus,
	cause error,
) {
	reporter, ok := a.bindings.(channels.ConnectionStatusReporter)
	if !ok {
		return
	}
	if err := reporter.RecordChannelConnection(ctx, binding.TenantID, binding.AppID, binding.BindingID, status, cause); err != nil {
		log.Printf("record feishu connection status failed: %s", platformlog.SafeError(err))
	}
}

func (a *Adapter) eventDispatcher(binding channels.BindingSnapshot) *larkdispatcher.EventDispatcher {
	dispatcher := larkdispatcher.NewEventDispatcher("", "")
	dispatcher.OnP2MessageReceiveV1(func(ctx context.Context, event *larkim.P2MessageReceiveV1) error {
		return a.HandleMessage(ctx, binding, event)
	})
	if a.recallAdmitter != nil {
		dispatcher.OnP2MessageRecalledV1(func(ctx context.Context, event *larkim.P2MessageRecalledV1) error {
			return a.HandleRecall(ctx, binding, event)
		})
	}
	return dispatcher
}

// HandleMessage is the authenticated SDK event boundary. It is also useful
// for contract tests because it exercises the exact SDK event conversion.
func (a *Adapter) HandleMessage(
	ctx context.Context,
	binding channels.BindingSnapshot,
	received *larkim.P2MessageReceiveV1,
) error {
	if err := a.ensureCurrentBinding(ctx, binding); err != nil {
		return err
	}
	envelope, err := normalizeMessageEvent(binding, received)
	if err != nil {
		return err
	}
	input, err := a.channelInput(ctx, envelope)
	if err != nil {
		return err
	}
	requestID := uuid.NewString()
	eventCtx, span := platformtelemetry.StartSpan(ctx, "channel.event",
		attribute.String("channel", string(channels.ChannelFeishu)),
		attribute.String("binding_id", binding.BindingID),
	)
	defer span.End()
	runtimeContext := tenant.RuntimeContext{
		TenantID:  binding.TenantID,
		AppID:     binding.AppID,
		Channel:   string(binding.Channel),
		BindingID: binding.BindingID,
		SessionID: channels.DefaultSessionID,
		TraceID:   requestID,
	}
	identityResolver, err := gateway.NewChannelBindingInputIdentityResolverFromBinding(binding, runtimeContext)
	if err != nil {
		return fmt.Errorf("feishu admission identity: %w", err)
	}
	identity, err := identityResolver.ResolveAdmissionIdentity(eventCtx)
	if err != nil {
		return fmt.Errorf("resolve feishu admission identity: %w", err)
	}
	admissionRequest := gateway.Request{
		RequestID:      requestID,
		IdempotencyKey: envelope.ExternalMessageID,
		Tenant:         identityResolver,
		ChannelInput:   &input,
	}
	if channels.RoutePlatformCommand(input) == channels.PlatformCommandNewSession {
		if a.commandHandler == nil {
			err = errors.New("new session command handler is not configured")
		} else {
			err = a.commandHandler.HandleNewSession(eventCtx, channels.NewSessionRequest{
				RequestID: requestID,
				Input:     input,
			})
		}
		if err != nil {
			failureRequest := gateway.AdmissionRequest{
				RequestID: requestID, IdempotencyKey: envelope.ExternalMessageID,
				Identity: identity, ChannelInput: &input,
				Message: gateway.Message{Text: input.Text, ArtifactRefs: append([]string(nil), input.ArtifactRefs...)},
			}
			err = errors.Join(err, a.admissionGateway.RecordChannelFailure(eventCtx, failureRequest))
			platformtelemetry.MarkError(span, "command", err)
		}
		if a.metrics != nil {
			a.metrics.RecordIMCallback(eventCtx, platformmetrics.Labels{Channel: string(channels.ChannelFeishu)}, errorType(err))
		}
		return err
	}
	if len(envelope.Media) > 0 {
		pinnedIngestor, ok := a.attachmentIngestor.(channels.PinnedAttachmentIngestor)
		if !ok {
			return errAttachmentIngestorReq
		}
		media := append([]channels.ProviderMediaRef(nil), envelope.Media...)
		_, err = a.admissionGateway.HandleChannel(eventCtx, admissionRequest,
			func(
				prepareCtx context.Context,
				prepareInput channels.ChannelInput,
				configVersion string,
			) (channels.ChannelInput, func(context.Context) error, error) {
				return pinnedIngestor.PreparePinned(prepareCtx, prepareInput, media, configVersion)
			})
	} else {
		_, err = a.admissionGateway.Handle(eventCtx, admissionRequest)
	}
	if err != nil {
		platformtelemetry.MarkError(span, "admission", err)
	}
	if a.metrics != nil {
		a.metrics.RecordIMCallback(eventCtx, platformmetrics.Labels{Channel: string(channels.ChannelFeishu)}, errorType(err))
	}
	return err
}

// HandleRecall forwards an SDK recall event through the existing durable
// recall boundary.
func (a *Adapter) HandleRecall(
	ctx context.Context,
	binding channels.BindingSnapshot,
	recalled *larkim.P2MessageRecalledV1,
) error {
	if a.recallAdmitter == nil {
		return errors.New("recall admitter is required")
	}
	if err := a.ensureCurrentBinding(ctx, binding); err != nil {
		return err
	}
	payload := []byte(nil)
	if recalled != nil && recalled.EventReq != nil {
		payload = recalled.EventReq.Body
	}
	if len(payload) == 0 {
		var err error
		payload, err = json.Marshal(recalled)
		if err != nil {
			return fmt.Errorf("encode feishu recall event: %w", err)
		}
	}
	payloadHash := sha256.Sum256(payload)
	request, err := normalizeRecallEvent(binding, recalled, payloadHash[:])
	if err != nil {
		return err
	}
	if _, err := a.recallAdmitter.AdmitRecall(ctx, request); err != nil {
		return fmt.Errorf("admit feishu recall: %w", err)
	}
	return nil
}

func (a *Adapter) ensureCurrentBinding(ctx context.Context, snapshot channels.BindingSnapshot) error {
	if ctx == nil {
		return errors.New("context is required")
	}
	current, err := a.bindings.ResolveBinding(ctx, snapshot.TenantID, snapshot.AppID, snapshot.BindingID)
	if err != nil {
		return err
	}
	if current.Status != channels.BindingActive {
		return channels.ErrBindingInactive
	}
	if current.Channel != snapshot.Channel || current.BindingRevision != snapshot.BindingRevision || current.ExternalAccount != snapshot.ExternalAccount {
		return gateway.ErrChannelBindingSnapshotStale
	}
	return nil
}

func (a *Adapter) channelInput(ctx context.Context, envelope VerifiedProviderEnvelope) (channels.ChannelInput, error) {
	if err := envelope.Validate(); err != nil {
		return channels.ChannelInput{}, err
	}
	input := channels.ChannelInput{
		TenantID:          envelope.TenantID,
		AppID:             envelope.AppID,
		Channel:           envelope.Channel,
		BindingID:         envelope.BindingID,
		BindingRevision:   envelope.BindingRevision,
		ExternalMessageID: envelope.ExternalMessageID,
		Conversation:      channels.ChannelConversation{Kind: envelope.ConversationKind},
		MessageType:       envelope.MessageType,
		Text:              envelope.Text,
		ProviderTimestamp: envelope.ProviderTimestamp,
		ReceivedAt:        a.now().UTC(),
	}
	input, err := channels.NewChannelInput(input, envelope.mapping)
	if err != nil {
		return channels.ChannelInput{}, err
	}
	return channels.WithMessageReplyTarget(input, channels.MessageReplyTarget{
		ProviderTarget: envelope.replyTarget,
		ExpiresAt:      a.now().UTC().Add(feishuMessageTargetTTL),
	})
}

// Close requests every active SDK client to stop and waits for graceful exit.
func (a *Adapter) Close(ctx context.Context) error {
	if a == nil {
		return nil
	}
	if ctx == nil {
		return errors.New("context is required")
	}
	a.runMu.Lock()
	runCancel := a.runCancel
	runDone := a.runDone
	a.runMu.Unlock()
	stopCtx, stopCancel := boundedStopContext(ctx)
	defer stopCancel()
	if runCancel != nil {
		runCancel()
		a.stopAllBindings(stopCtx)
		select {
		case <-runDone:
			return nil
		case <-stopCtx.Done():
			return stopCtx.Err()
		}
	}
	a.clientsMu.Lock()
	clients := make([]Client, 0, len(a.clients))
	for _, client := range a.clients {
		clients = append(clients, client)
	}
	a.clientsMu.Unlock()
	var result error
	for _, client := range clients {
		result = errors.Join(result, client.CloseAndWait(stopCtx))
	}
	return result
}

func (a *Adapter) trackClient(key string, client Client) {
	a.clientsMu.Lock()
	a.clients[key] = client
	a.clientsMu.Unlock()
}

func (a *Adapter) untrackClient(key string, client Client) {
	a.clientsMu.Lock()
	if current, ok := a.clients[key]; ok && current == client {
		delete(a.clients, key)
	}
	a.clientsMu.Unlock()
}

func bindingKey(binding channels.BindingSnapshot) string {
	return binding.TenantID + "\x00" + binding.AppID + "\x00" + binding.BindingID
}

func boundedStopContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, ok := ctx.Deadline(); ok {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, clientCloseTimeout)
}

func waitReconnect(ctx context.Context, initial time.Duration) error {
	if initial <= 0 {
		initial = defaultReconnectInitial
	}
	timer := time.NewTimer(initial)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func nextBackoff(current, maximum time.Duration) time.Duration {
	next := current * 2
	if next > maximum {
		return maximum
	}
	return next
}

func errorType(err error) string {
	if err == nil {
		return ""
	}
	return "admission"
}
