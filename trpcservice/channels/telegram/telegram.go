// Package telegram implements the tenant-scoped Telegram long-polling
// Channel Adapter.
package telegram

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/attachment"
	"github.com/XnLemon/trpc-agent-service/trpcservice/audit"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	"github.com/XnLemon/trpc-agent-service/trpcservice/gateway"
	"github.com/XnLemon/trpc-agent-service/trpcservice/gateway/replies"
	"github.com/XnLemon/trpc-agent-service/trpcservice/metrics"
	"github.com/XnLemon/trpc-agent-service/trpcservice/observability"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	"github.com/go-telegram/bot"
	"github.com/go-telegram/bot/models"
)

const (
	defaultPollTimeout     = time.Minute
	minimumPollTimeout     = 2 * time.Second
	maximumPollTimeout     = 10 * time.Minute
	maximumTokenRunes      = 1024
	maximumReplyRunes      = 4096
	failureReply           = "Sorry, I couldn't process that message."
	defaultAttachmentBytes = 64 << 20
	maximumAttachmentBytes = 64 << 20
)

var (
	// ErrInvalid reports malformed adapter configuration or update input.
	ErrInvalid = errors.New("invalid telegram adapter input")
	// ErrNotReady reports that the adapter has no usable Bot client.
	ErrNotReady = errors.New("telegram adapter is not ready")
	// ErrClosed reports an adapter or its owned process-local state after close.
	ErrClosed = errors.New("telegram adapter is closed")
	// ErrAlreadyRunning reports a second concurrent Run call.
	ErrAlreadyRunning = errors.New("telegram adapter is already running")
	// ErrInitialization reports a redacted Bot construction or getMe failure.
	ErrInitialization = errors.New("telegram bot initialization failed")
	// ErrBotIdentityMismatch reports a Bot identity different from the trusted
	// Binding provider account.
	ErrBotIdentityMismatch = errors.New("telegram bot identity does not match binding")
	// ErrInvalidUpdate reports a malformed supported update shape.
	ErrInvalidUpdate = errors.New("invalid telegram update")
	// ErrUnsupportedUpdate reports an update outside the first text-only scope.
	ErrUnsupportedUpdate = errors.New("unsupported telegram update")
	// ErrDuplicateUpdate reports an update already being handled by this process.
	ErrDuplicateUpdate = errors.New("duplicate telegram update")
	// ErrDispatch reports a redacted Gateway dispatch failure.
	ErrDispatch = errors.New("telegram dispatch failed")
	// ErrSendMessage reports a redacted Telegram sendMessage failure.
	ErrSendMessage = errors.New("telegram send message failed")
	// ErrPolling reports a redacted SDK polling error delivered to ErrorHook.
	ErrPolling = errors.New("telegram polling failed")
	// ErrAttachment reports a redacted media download or storage failure.
	ErrAttachment = errors.New("telegram attachment processing failed")
)

// ErrorOperation identifies the safe operation category supplied to ErrorHook.
type ErrorOperation string

const (
	// ErrorOperationInitialization identifies construction or getMe failures.
	ErrorOperationInitialization ErrorOperation = "initialization"
	// ErrorOperationPolling identifies long-polling failures.
	ErrorOperationPolling ErrorOperation = "polling"
	// ErrorOperationUpdate identifies rejected or unsupported updates.
	ErrorOperationUpdate ErrorOperation = "update"
	// ErrorOperationDispatch identifies Gateway execution failures.
	ErrorOperationDispatch ErrorOperation = "dispatch"
	// ErrorOperationSend identifies outbound sendMessage failures.
	ErrorOperationSend ErrorOperation = "send"
)

// ErrorEvent is the redacted payload passed to an ErrorHook. Err is always a
// stable adapter sentinel and never a provider error, token, or stack trace.
type ErrorEvent struct {
	Operation ErrorOperation
	Err       error
}

// ErrorHook observes stable adapter failures without receiving provider
// details or runtime secrets.
type ErrorHook func(ErrorEvent)

// BotClient is the small SDK surface required by the adapter. A fake client
// can implement it without credentials or network access.
type BotClient interface {
	Start(context.Context)
	GetMe(context.Context) (*models.User, error)
	SendMessage(context.Context, *bot.SendMessageParams) (*models.Message, error)
}

// MediaDownloader downloads one authenticated Telegram file into the adapter's
// attachment store. Implementations must not expose provider URLs or tokens to
// callers.
type MediaDownloader interface {
	Download(context.Context, string) (io.ReadCloser, error)
}

type telegramFileClient interface {
	GetFile(context.Context, *bot.GetFileParams) (*models.File, error)
	FileDownloadLink(*models.File) string
}

type telegramMediaDownloader struct {
	client     telegramFileClient
	httpClient bot.HttpClient
	maximum    int64
}

func (downloader telegramMediaDownloader) Download(ctx context.Context, fileID string) (io.ReadCloser, error) {
	if ctx == nil || downloader.client == nil || downloader.httpClient == nil || fileID == "" || downloader.maximum < 1 {
		return nil, ErrAttachment
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	file, err := downloader.client.GetFile(ctx, &bot.GetFileParams{FileID: fileID})
	if err != nil {
		return nil, telegramAttachmentError(ctx)
	}
	fileURL, err := downloader.fileDownloadURL(file)
	if err != nil {
		return nil, ErrAttachment
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, fileURL.String(), nil)
	if err != nil {
		return nil, ErrAttachment
	}
	response, err := downloader.httpClient.Do(request)
	if err != nil {
		return nil, telegramAttachmentError(ctx)
	}
	data, err := readTelegramMediaResponse(ctx, response, downloader.maximum)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (downloader telegramMediaDownloader) fileDownloadURL(file *models.File) (*url.URL, error) {
	if file == nil || file.FilePath == "" {
		return nil, ErrAttachment
	}
	fileURL, err := url.Parse(downloader.client.FileDownloadLink(file))
	if err != nil || fileURL.Scheme != "https" || fileURL.Host == "" || fileURL.User != nil || fileURL.RawQuery != "" || fileURL.Fragment != "" {
		return nil, ErrAttachment
	}
	return fileURL, nil
}

func readTelegramMediaResponse(ctx context.Context, response *http.Response, maximum int64) ([]byte, error) {
	if response == nil || response.Body == nil {
		return nil, ErrAttachment
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices || response.ContentLength > maximum {
		return nil, ErrAttachment
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, maximum+1))
	if err != nil {
		return nil, telegramAttachmentError(ctx)
	}
	if int64(len(data)) > maximum {
		return nil, ErrAttachment
	}
	return data, nil
}

func telegramAttachmentError(ctx context.Context) error {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	return ErrAttachment
}

// BotFactoryConfig contains non-secret options for constructing one BotClient.
// The token is passed separately to BotFactory.New and is never stored in this
// configuration value.
type BotFactoryConfig struct {
	// Handler receives updates from the SDK's long-polling consumer.
	Handler bot.HandlerFunc
	// APIBaseURL overrides the Telegram API origin when non-empty.
	APIBaseURL string
	// HTTPClient is the optional HTTP transport used by the SDK.
	HTTPClient bot.HttpClient
	// PollTimeout is the Telegram getUpdates long-poll timeout.
	PollTimeout time.Duration
	// Workers is the explicitly validated SDK update worker count.
	Workers int
	// OnPollingError receives no raw error; it only signals that polling failed.
	OnPollingError func()
}

// BotFactory constructs a BotClient with the supplied runtime token and safe
// options. Production uses the public github.com/go-telegram/bot SDK; tests
// inject a fake implementation.
type BotFactory interface {
	New(string, BotFactoryConfig) (BotClient, error)
}

// BotFactoryFunc adapts a function into a BotFactory.
type BotFactoryFunc func(string, BotFactoryConfig) (BotClient, error)

// New implements BotFactory for a BotFactoryFunc.
func (factory BotFactoryFunc) New(token string, config BotFactoryConfig) (BotClient, error) {
	if factory == nil {
		return nil, ErrInvalid
	}
	return factory(token, config)
}

// Config defines one tenant-scoped Telegram Binding adapter. BotToken is a
// runtime-only secret and must not be persisted or placed in diagnostics.
type Config struct {
	// BotToken is resolved before construction and retained only by the SDK
	// client created for this adapter.
	BotToken string
	// Target is the trusted, non-secret route for exactly one active Binding.
	Target channels.RoutingTarget
	// Dispatcher is the existing protocol-neutral Gateway execution service.
	Dispatcher gateway.DispatchService
	// Idempotency optionally supplies a shared process-local store. When nil,
	// the adapter owns a new process-local store.
	Idempotency *gateway.IdempotencyStore
	// APIBaseURL optionally overrides the Telegram HTTPS API origin.
	APIBaseURL string
	// HTTPClient optionally supplies the SDK HTTP transport.
	HTTPClient bot.HttpClient
	// PollTimeout optionally overrides the long-poll timeout.
	PollTimeout time.Duration
	// Workers controls SDK update workers. Zero defaults to one.
	Workers int
	// ErrorHook observes stable, redacted adapter failures.
	ErrorHook ErrorHook
	// AuditWriter receives mandatory ingress and delivery outcome facts.
	AuditWriter audit.Writer
	// Factory optionally replaces the public SDK factory for tests.
	Factory BotFactory
	// Observability supplies provider-neutral trace and metric hooks.
	Observability observability.Provider
	// Attachments is the explicit durable boundary for native inbound media.
	// When nil, media keeps the legacy text-marker behavior.
	Attachments runtimestorage.AttachmentStore
	// MediaDownloader optionally replaces the authenticated Telegram file
	// downloader. It is primarily useful for deterministic tests.
	MediaDownloader MediaDownloader
	// MaxAttachmentBytes bounds each downloaded media object. Zero defaults to
	// the protocol-neutral attachment limit.
	MaxAttachmentBytes int64
}

// Adapter owns one trusted Telegram Binding and routes its updates through the
// existing Gateway contracts. It does not create or cache a Runner directly.
type Adapter struct {
	client             BotClient
	username           string
	dispatcher         gateway.DispatchService
	principal          gateway.Principal
	target             channels.RoutingTarget
	idempotency        *gateway.IdempotencyStore
	ownIdempotency     bool
	errorHook          ErrorHook
	audit              audit.Recorder
	telemetry          observability.Provider
	metrics            metrics.Catalog
	attachments        runtimestorage.AttachmentStore
	mediaDownloader    MediaDownloader
	maxAttachmentBytes int64

	mu        sync.RWMutex
	closed    bool
	runCancel context.CancelFunc
}

var _ channels.PollingAdapter = (*Adapter)(nil)

type normalizedConfig struct {
	token              string
	target             channels.RoutingTarget
	principal          gateway.Principal
	apiBaseURL         string
	pollTimeout        time.Duration
	workers            int
	maxAttachmentBytes int64
	providerAcctID     string
}

// New validates the trusted route, constructs the Bot client, and verifies its
// getMe identity before returning an adapter that can handle updates.
func New(ctx context.Context, config Config) (*Adapter, error) {
	normalized, err := normalizeConfig(ctx, config)
	if err != nil {
		return nil, err
	}
	idempotency := config.Idempotency
	ownIdempotency := false
	if idempotency == nil {
		idempotency, err = gateway.NewIdempotencyStore(gateway.IdempotencyConfig{})
		if err != nil {
			return nil, fmt.Errorf("%w: idempotency store is unavailable", ErrInvalid)
		}
		ownIdempotency = true
	}
	factory := config.Factory
	if factory == nil {
		factory = sdkBotFactory{}
	}
	adapter := &Adapter{
		dispatcher: config.Dispatcher, principal: normalized.principal, target: normalized.target,
		idempotency: idempotency, ownIdempotency: ownIdempotency, errorHook: config.ErrorHook,
		audit:       audit.NewRecorder(config.AuditWriter, normalized.target.TenantID),
		attachments: config.Attachments, maxAttachmentBytes: normalized.maxAttachmentBytes,
	}
	if config.Observability == nil {
		config.Observability = observability.NewNoopProvider()
	}
	adapter.telemetry = config.Observability
	adapter.metrics = metrics.New(config.Observability)
	client, err := factory.New(normalized.token, BotFactoryConfig{
		Handler:        adapter.sdkHandler(),
		APIBaseURL:     normalized.apiBaseURL,
		HTTPClient:     config.HTTPClient,
		PollTimeout:    normalized.pollTimeout,
		Workers:        normalized.workers,
		OnPollingError: func() { adapter.report(ErrorOperationPolling, ErrPolling) },
	})
	if err != nil || client == nil {
		adapter.report(ErrorOperationInitialization, ErrInitialization)
		_ = adapter.closeOwnedIdempotency()
		return nil, ErrInitialization
	}
	adapter.client = client
	if err := adapter.verifyIdentity(ctx, normalized.providerAcctID); err != nil {
		_ = adapter.closeOwnedIdempotency()
		return nil, err
	}
	adapter.mediaDownloader = config.MediaDownloader
	if adapter.mediaDownloader == nil {
		if fileClient, ok := client.(telegramFileClient); ok && config.Attachments != nil {
			adapter.mediaDownloader = telegramMediaDownloader{client: fileClient, httpClient: configuredHTTPClient(config.HTTPClient, normalized.pollTimeout), maximum: normalized.maxAttachmentBytes}
		}
	}
	return adapter, nil
}

func normalizeConfig(ctx context.Context, config Config) (normalizedConfig, error) {
	if ctx == nil {
		return normalizedConfig{}, fmt.Errorf("%w: context is required", ErrInvalid)
	}
	if err := ctx.Err(); err != nil {
		return normalizedConfig{}, err
	}
	token, err := normalizeToken(config.BotToken)
	if err != nil {
		return normalizedConfig{}, err
	}
	if err := config.Target.Validate(); err != nil {
		return normalizedConfig{}, fmt.Errorf("%w: trusted routing target is invalid", ErrInvalid)
	}
	if config.Target.Channel != channels.ChannelTelegram {
		return normalizedConfig{}, fmt.Errorf("%w: routing target is not Telegram", ErrInvalid)
	}
	providerAccountID, err := strconv.ParseInt(config.Target.ProviderAccountID, 10, 64)
	if err != nil || providerAccountID <= 0 || strconv.FormatInt(providerAccountID, 10) != config.Target.ProviderAccountID {
		return normalizedConfig{}, fmt.Errorf("%w: Telegram provider account ID is not canonical", ErrInvalid)
	}
	if config.Dispatcher == nil {
		return normalizedConfig{}, fmt.Errorf("%w: dispatcher is required", ErrInvalid)
	}
	apiBaseURL, err := normalizeAPIBaseURL(config.APIBaseURL)
	if err != nil {
		return normalizedConfig{}, err
	}
	pollTimeout, err := normalizePollTimeout(config.PollTimeout)
	if err != nil {
		return normalizedConfig{}, err
	}
	workers, err := normalizeWorkers(config.Workers)
	if err != nil {
		return normalizedConfig{}, err
	}
	maxAttachmentBytes, err := normalizeAttachmentBytes(config.MaxAttachmentBytes)
	if err != nil {
		return normalizedConfig{}, err
	}
	principal, err := gateway.NewChannelPrincipal(config.Target)
	if err != nil {
		return normalizedConfig{}, fmt.Errorf("%w: trusted principal is invalid", ErrInvalid)
	}
	return normalizedConfig{token: token, target: config.Target, principal: principal, apiBaseURL: apiBaseURL, pollTimeout: pollTimeout, workers: workers, maxAttachmentBytes: maxAttachmentBytes, providerAcctID: strconv.FormatInt(providerAccountID, 10)}, nil
}

func (adapter *Adapter) verifyIdentity(ctx context.Context, providerAccountID string) error {
	me, err := adapter.client.GetMe(ctx)
	if err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return contextErr
		}
		adapter.report(ErrorOperationInitialization, ErrInitialization)
		return ErrInitialization
	}
	if me == nil || !me.IsBot || me.ID <= 0 || strconv.FormatInt(me.ID, 10) != providerAccountID {
		adapter.report(ErrorOperationInitialization, ErrBotIdentityMismatch)
		return ErrBotIdentityMismatch
	}
	adapter.mu.Lock()
	adapter.username = strings.TrimSpace(me.Username)
	adapter.mu.Unlock()
	return nil
}

// Username returns the provider handle returned by Telegram during identity
// verification. It is used only to build the operator's first-chat link.
func (adapter *Adapter) Username() string {
	if adapter == nil {
		return ""
	}
	adapter.mu.RLock()
	defer adapter.mu.RUnlock()
	return adapter.username
}

// Ready reports whether the adapter has an authenticated Bot client and has
// not been closed.
func (adapter *Adapter) Ready() bool {
	if adapter == nil {
		return false
	}
	adapter.mu.RLock()
	defer adapter.mu.RUnlock()
	return adapter.client != nil && !adapter.closed
}

// Run starts blocking Telegram long polling and returns after ctx is canceled
// or Close cancels the run. The SDK owns its polling and worker goroutines.
func (adapter *Adapter) Run(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is required", ErrInvalid)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if adapter == nil {
		return ErrNotReady
	}
	adapter.mu.Lock()
	closed, client := adapter.closed, adapter.client
	if closed {
		adapter.mu.Unlock()
		return ErrClosed
	}
	if client == nil {
		adapter.mu.Unlock()
		return ErrNotReady
	}
	if adapter.runCancel != nil {
		adapter.mu.Unlock()
		return ErrAlreadyRunning
	}
	runContext, cancel := context.WithCancel(ctx)
	adapter.runCancel = cancel
	adapter.mu.Unlock()
	defer func() {
		adapter.mu.Lock()
		adapter.runCancel = nil
		adapter.mu.Unlock()
		cancel()
	}()
	client.Start(runContext)
	return nil
}

// Channel identifies the protocol owned by this Adapter.
func (*Adapter) Channel() channels.Channel { return channels.ChannelTelegram }

// Close closes only idempotency state owned by the adapter. Injected stores and
// HTTP clients remain owned by their callers; polling is stopped by canceling
// the Context passed to Run.
func (adapter *Adapter) Close() error {
	if adapter == nil {
		return nil
	}
	adapter.mu.Lock()
	if adapter.closed {
		adapter.mu.Unlock()
		return nil
	}
	adapter.closed = true
	cancel := adapter.runCancel
	adapter.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	return adapter.closeOwnedIdempotency()
}

// HandleUpdate validates and processes one Telegram update. It is exposed for
// deterministic tests and for the SDK's default handler.
func (adapter *Adapter) HandleUpdate(ctx context.Context, update *models.Update) (err error) {
	if adapter == nil {
		return ErrNotReady
	}
	if ctx == nil {
		err := fmt.Errorf("%w: context is required", ErrInvalid)
		adapter.report(ErrorOperationUpdate, ErrInvalid)
		return err
	}
	started := time.Now()
	operationCtx, _, finish := observability.StartOperation(ctx, adapter.telemetry, observability.OperationChannelReceive, "channel")
	_ = adapter.metrics.Request(operationCtx, map[string]string{"component": "channel", "operation": observability.OperationChannelReceive, "channel": "telegram", "status": "started"})
	defer func() {
		finish(err)
		_ = adapter.metrics.Operation(operationCtx, started, map[string]string{"component": "channel", "operation": observability.OperationChannelReceive, "channel": "telegram"}, err)
	}()
	ctx = operationCtx
	if err := ctx.Err(); err != nil {
		return err
	}
	adapter.mu.RLock()
	closed, client := adapter.closed, adapter.client
	adapter.mu.RUnlock()
	if closed {
		return ErrClosed
	}
	if client == nil || adapter.idempotency == nil {
		return ErrNotReady
	}
	message, err := adapter.normalizeUpdate(ctx, update)
	if err != nil {
		adapter.report(ErrorOperationUpdate, err)
		return err
	}
	claim, replay, err := adapter.beginUpdate(ctx, message)
	if err != nil {
		return err
	}
	if claim == nil {
		return adapter.handleReplay(ctx, update.Message, message, replay)
	}
	return adapter.handleClaimedUpdate(ctx, update.Message, message, claim)
}

func (adapter *Adapter) beginUpdate(ctx context.Context, message gateway.InboundMessage) (*gateway.IdempotencyClaim, []gateway.DispatchEvent, error) {
	claim, replay, err := adapter.idempotency.Begin(ctx, adapter.principal, message)
	if err == nil {
		return claim, replay, nil
	}
	if errors.Is(err, gateway.ErrDuplicateMessage) {
		if auditErr := adapter.audit.Record(ctx, audit.Event{
			EventType: audit.EventIMIngressDuplicate, RequestID: message.ExternalMessageID,
			UserID: message.ExternalUserID, Decision: audit.DecisionDuplicate, ErrorType: string(audit.ErrorDuplicate),
		}); auditErr != nil {
			adapter.report(ErrorOperationUpdate, ErrDispatch)
			return nil, nil, ErrDispatch
		}
		return nil, nil, ErrDuplicateUpdate
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return nil, nil, err
	}
	if errors.Is(err, gateway.ErrClosed) {
		return nil, nil, ErrClosed
	}
	return nil, nil, ErrInvalid
}

func (adapter *Adapter) handleReplay(ctx context.Context, message *models.Message, inbound gateway.InboundMessage, replay []gateway.DispatchEvent) error {
	if auditErr := adapter.audit.Record(ctx, audit.Event{
		EventType: audit.EventIMIngressAccepted, RequestID: inbound.ExternalMessageID,
		UserID: inbound.ExternalUserID, Decision: audit.DecisionAccepted,
	}); auditErr != nil {
		return ErrDispatch
	}
	if err := adapter.sendEvents(ctx, message, replay); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		adapter.report(ErrorOperationSend, ErrSendMessage)
		return err
	}
	return nil
}

func (adapter *Adapter) handleClaimedUpdate(ctx context.Context, message *models.Message, inbound gateway.InboundMessage, claim *gateway.IdempotencyClaim) error {
	if auditErr := adapter.audit.Record(ctx, audit.Event{
		EventType: audit.EventIMIngressAccepted, RequestID: inbound.ExternalMessageID,
		UserID: inbound.ExternalUserID, Decision: audit.DecisionAccepted,
	}); auditErr != nil {
		_ = claim.Fail()
		return ErrDispatch
	}

	events, dispatchErr := adapter.dispatch(ctx, inbound)
	if dispatchErr != nil {
		_ = claim.Fail()
		if errors.Is(dispatchErr, context.Canceled) || errors.Is(dispatchErr, context.DeadlineExceeded) {
			return dispatchErr
		}
		adapter.report(ErrorOperationDispatch, ErrDispatch)
		if sendErr := adapter.sendText(ctx, message, failureReply); sendErr != nil {
			if !errors.Is(sendErr, context.Canceled) && !errors.Is(sendErr, context.DeadlineExceeded) {
				adapter.report(ErrorOperationSend, ErrSendMessage)
			}
		}
		return ErrDispatch
	}
	if err := claim.Complete(events); err != nil {
		return ErrDispatch
	}
	if err := adapter.sendEvents(ctx, message, events); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		adapter.report(ErrorOperationSend, ErrSendMessage)
		return err
	}
	if auditErr := adapter.audit.Record(ctx, audit.Event{
		EventType: audit.EventIMDeliverySent, RequestID: inbound.ExternalMessageID,
		UserID: inbound.ExternalUserID, Decision: audit.DecisionAccepted,
	}); auditErr != nil {
		return ErrDispatch
	}
	return nil
}

func (adapter *Adapter) sdkHandler() bot.HandlerFunc {
	return func(ctx context.Context, _ *bot.Bot, update *models.Update) {
		_ = adapter.HandleUpdate(ctx, update)
	}
}

func (adapter *Adapter) dispatch(ctx context.Context, message gateway.InboundMessage) ([]gateway.DispatchEvent, error) {
	stream, err := adapter.dispatcher.Dispatch(ctx, gateway.DispatchRequest{
		Principal: adapter.principal, Message: message, RequestID: message.ExternalMessageID,
	})
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		return nil, ErrDispatch
	}
	if stream == nil {
		return nil, ErrDispatch
	}
	events := make([]gateway.DispatchEvent, 0, 4)
	done := false
	failed := false
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case event, ok := <-stream:
			if !ok {
				if !done || failed {
					return nil, ErrDispatch
				}
				return events, nil
			}
			events = append(events, event)
			if event.Type == gateway.DispatchEventError {
				failed = true
			}
			if event.Done {
				done = true
			}
		}
	}
}

func (adapter *Adapter) sendEvents(ctx context.Context, message *models.Message, events []gateway.DispatchEvent) error {
	reply := replies.Render(events)
	return adapter.sendText(ctx, message, reply.Text)
}

func (adapter *Adapter) sendText(ctx context.Context, message *models.Message, text string) (err error) {
	if message == nil {
		return ErrInvalidUpdate
	}
	if adapter == nil || adapter.client == nil {
		return ErrNotReady
	}
	started := time.Now()
	operationCtx, _, finish := observability.StartOperation(ctx, adapter.telemetry, observability.OperationChannelSend, "channel")
	defer func() {
		finish(err)
		_ = adapter.metrics.Operation(operationCtx, started, map[string]string{"component": "channel", "operation": observability.OperationChannelSend, "channel": "telegram", "provider": "other"}, err)
	}()
	ctx = operationCtx
	chunks := splitText(text, maximumReplyRunes)
	for _, chunk := range chunks {
		if err := ctx.Err(); err != nil {
			return err
		}
		_, err := adapter.client.SendMessage(ctx, &bot.SendMessageParams{
			ChatID: message.Chat.ID, MessageThreadID: message.MessageThreadID, Text: chunk,
		})
		if err != nil {
			_ = adapter.metrics.Delivery(ctx, map[string]string{"component": "channel", "channel": "telegram", "provider": "other", "status": "failure", "error_class": "error"})
			return ErrSendMessage
		}
		_ = adapter.metrics.Delivery(ctx, map[string]string{"component": "channel", "channel": "telegram", "provider": "other", "status": "success", "error_class": ""})
	}
	return nil
}

func normalizeUpdate(target channels.RoutingTarget, update *models.Update) (gateway.InboundMessage, error) {
	if update == nil || update.ID < 0 {
		return gateway.InboundMessage{}, ErrInvalidUpdate
	}
	if hasUnsupportedUpdate(update) || update.Message == nil {
		return gateway.InboundMessage{}, ErrUnsupportedUpdate
	}
	message := update.Message
	if hasUnsupportedMessage(message) {
		return gateway.InboundMessage{}, ErrUnsupportedUpdate
	}
	content, contentType, ok := messageContent(message)
	if !ok {
		return gateway.InboundMessage{}, ErrUnsupportedUpdate
	}
	if message.From == nil || message.From.ID <= 0 || message.Chat.ID == 0 {
		return gateway.InboundMessage{}, ErrInvalidUpdate
	}
	if message.MessageThreadID < 0 {
		return gateway.InboundMessage{}, ErrInvalidUpdate
	}
	inbound := gateway.InboundMessage{
		Content: content, ContentType: contentType,
		ExternalMessageID: externalMessageID(target, update.ID),
		ExternalUserID:    strconv.FormatInt(message.From.ID, 10),
	}
	switch message.Chat.Type {
	case models.ChatTypePrivate:
		inbound.ConversationKind = channels.ConversationDirect
		inbound.ExternalPeerID = strconv.FormatInt(message.Chat.ID, 10)
	case models.ChatTypeGroup, models.ChatTypeSupergroup:
		inbound.ConversationKind = channels.ConversationGroup
		inbound.ExternalChatID = strconv.FormatInt(message.Chat.ID, 10)
	default:
		return gateway.InboundMessage{}, ErrUnsupportedUpdate
	}
	if message.MessageThreadID > 0 {
		inbound.ExternalThreadID = strconv.Itoa(message.MessageThreadID)
	}
	normalized, err := inbound.Normalize()
	if err != nil {
		return gateway.InboundMessage{}, ErrInvalidUpdate
	}
	return normalized, nil
}

func (adapter *Adapter) normalizeUpdate(ctx context.Context, update *models.Update) (gateway.InboundMessage, error) {
	inbound, err := normalizeUpdate(adapter.target, update)
	if err != nil || adapter.attachments == nil || adapter.mediaDownloader == nil || update == nil || update.Message == nil {
		return inbound, err
	}
	references, err := adapter.ingestAttachments(ctx, inbound.ExternalMessageID, update.Message)
	if err != nil {
		return gateway.InboundMessage{}, err
	}
	if len(references) == 0 {
		return inbound, nil
	}
	inbound.Attachments = references
	inbound.ContentType = gateway.ContentTypeMedia
	return inbound.Normalize()
}

type telegramAttachment struct {
	fileID   string
	kind     attachment.Kind
	mimeType string
	name     string
}

func (adapter *Adapter) ingestAttachments(ctx context.Context, externalMessageID string, message *models.Message) ([]attachment.Reference, error) {
	descriptors := nativeAttachments(message)
	if len(descriptors) == 0 {
		return nil, nil
	}
	references := make([]attachment.Reference, 0, len(descriptors))
	for index, descriptor := range descriptors {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		reader, err := adapter.mediaDownloader.Download(ctx, descriptor.fileID)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil, err
			}
			return nil, ErrAttachment
		}
		if reader == nil {
			return nil, ErrAttachment
		}
		data, readErr := io.ReadAll(io.LimitReader(reader, adapter.maxAttachmentBytes+1))
		closeErr := reader.Close()
		if readErr != nil || closeErr != nil {
			if contextErr := ctx.Err(); contextErr != nil {
				return nil, contextErr
			}
			return nil, ErrAttachment
		}
		if int64(len(data)) == 0 || int64(len(data)) > adapter.maxAttachmentBytes {
			return nil, ErrAttachment
		}
		upload := attachment.Upload{
			ID: attachmentID(externalMessageID, index, descriptor.fileID), Kind: descriptor.kind,
			MIMEType: descriptor.mimeType, Name: descriptor.name, Size: int64(len(data)),
			Provider: "telegram", ProviderID: descriptor.fileID,
		}
		reference, err := adapter.attachments.PutAttachment(ctx, adapter.target.TenantID, upload, bytes.NewReader(data))
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil, err
			}
			return nil, ErrAttachment
		}
		references = append(references, reference)
	}
	return references, nil
}

func nativeAttachments(message *models.Message) []telegramAttachment {
	if message == nil {
		return nil
	}
	if photo := largestPhoto(message.Photo); photo != nil {
		return []telegramAttachment{{fileID: photo.FileID, kind: attachment.KindImage, mimeType: "image/jpeg", name: photo.FileID + ".jpg"}}
	}
	if message.Video != nil && strings.TrimSpace(message.Video.FileID) != "" {
		return []telegramAttachment{{fileID: message.Video.FileID, kind: attachment.KindVideo, mimeType: mediaMIME(attachment.KindVideo, message.Video.MimeType), name: mediaName(message.Video.FileName, message.Video.FileID, ".mp4")}}
	}
	if message.Animation != nil && strings.TrimSpace(message.Animation.FileID) != "" {
		return []telegramAttachment{{fileID: message.Animation.FileID, kind: attachment.KindVideo, mimeType: mediaMIME(attachment.KindVideo, message.Animation.MimeType), name: mediaName(message.Animation.FileName, message.Animation.FileID, ".mp4")}}
	}
	if message.Audio != nil && strings.TrimSpace(message.Audio.FileID) != "" {
		return []telegramAttachment{{fileID: message.Audio.FileID, kind: attachment.KindAudio, mimeType: mediaMIME(attachment.KindAudio, message.Audio.MimeType), name: mediaName(message.Audio.FileName, message.Audio.FileID, ".mp3")}}
	}
	if message.Voice != nil && strings.TrimSpace(message.Voice.FileID) != "" {
		return []telegramAttachment{{fileID: message.Voice.FileID, kind: attachment.KindAudio, mimeType: mediaMIME(attachment.KindAudio, message.Voice.MimeType), name: message.Voice.FileID + ".ogg"}}
	}
	if message.Document != nil && strings.TrimSpace(message.Document.FileID) != "" {
		mimeType := strings.TrimSpace(strings.ToLower(message.Document.MimeType))
		kind := attachment.KindDocument
		if strings.HasPrefix(mimeType, "image/") {
			kind = attachment.KindImage
		} else if strings.HasPrefix(mimeType, "video/") {
			kind = attachment.KindVideo
		} else if strings.HasPrefix(mimeType, "audio/") {
			kind = attachment.KindAudio
		}
		if !validMIME(mimeType) || !kindSupportsMIME(kind, mimeType) {
			mimeType = "application/octet-stream"
			kind = attachment.KindDocument
		}
		return []telegramAttachment{{fileID: message.Document.FileID, kind: kind, mimeType: mimeType, name: mediaName(message.Document.FileName, message.Document.FileID, "")}}
	}
	if message.VideoNote != nil && strings.TrimSpace(message.VideoNote.FileID) != "" {
		return []telegramAttachment{{fileID: message.VideoNote.FileID, kind: attachment.KindVideo, mimeType: "video/mp4", name: message.VideoNote.FileID + ".mp4"}}
	}
	return nil
}

func largestPhoto(photos []models.PhotoSize) *models.PhotoSize {
	var largest *models.PhotoSize
	for index := range photos {
		photo := &photos[index]
		if strings.TrimSpace(photo.FileID) == "" {
			continue
		}
		if largest == nil || photo.FileSize > largest.FileSize || photo.Width*photo.Height > largest.Width*largest.Height {
			largest = photo
		}
	}
	return largest
}

func mediaMIME(kind attachment.Kind, value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if validMIME(value) && kindSupportsMIME(kind, value) {
		return value
	}
	switch kind {
	case attachment.KindVideo:
		return "video/mp4"
	case attachment.KindAudio:
		return "audio/mpeg"
	default:
		return "application/octet-stream"
	}
}

func validMIME(value string) bool {
	parsed, params, err := mime.ParseMediaType(value)
	return err == nil && parsed == value && len(params) == 0 && strings.Contains(value, "/")
}

func kindSupportsMIME(kind attachment.Kind, value string) bool {
	switch kind {
	case attachment.KindImage:
		return strings.HasPrefix(value, "image/")
	case attachment.KindVideo:
		return strings.HasPrefix(value, "video/")
	case attachment.KindAudio:
		return strings.HasPrefix(value, "audio/")
	case attachment.KindDocument:
		return !strings.HasPrefix(value, "image/") && !strings.HasPrefix(value, "video/") && !strings.HasPrefix(value, "audio/")
	default:
		return false
	}
}

func mediaName(name, fileID, suffix string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return fileID + suffix
	}
	for _, character := range name {
		if character < 0x20 || character == 0x7f || character == '/' || character == '\\' {
			return fileID + suffix
		}
	}
	return name
}

func attachmentID(externalMessageID string, ordinal int, providerID string) string {
	digest := sha256.Sum256([]byte(encodeParts(externalMessageID, strconv.Itoa(ordinal), providerID)))
	return "att_" + hex.EncodeToString(digest[:])
}

func messageContent(message *models.Message) (string, string, bool) {
	if message == nil {
		return "", "", false
	}
	if strings.TrimSpace(message.Text) != "" {
		return message.Text, gateway.ContentTypeText, true
	}
	if strings.TrimSpace(message.Caption) != "" && hasMedia(message) {
		return message.Caption, gateway.ContentTypeMedia, true
	}
	if hasMedia(message) {
		return "[telegram media]", gateway.ContentTypeMedia, true
	}
	if message.RichMessage != nil {
		return "[telegram rich message]", gateway.ContentTypeRich, true
	}
	return "", "", false
}

func hasMedia(message *models.Message) bool {
	if message == nil {
		return false
	}
	if len(message.Photo) > 0 {
		for _, photo := range message.Photo {
			if strings.TrimSpace(photo.FileID) != "" {
				return true
			}
		}
	}
	return hasPrimaryMedia(message) || hasSecondaryMedia(message)
}

func hasPrimaryMedia(message *models.Message) bool {
	return message.Animation != nil || message.Audio != nil || message.Document != nil || message.PaidMedia != nil || message.Sticker != nil || message.Story != nil || message.Video != nil || message.VideoNote != nil || message.Voice != nil || message.Checklist != nil || message.Contact != nil
}

func hasSecondaryMedia(message *models.Message) bool {
	return message.Dice != nil || message.Game != nil || message.Poll != nil || message.Venue != nil || message.Location != nil || message.Invoice != nil || message.SuccessfulPayment != nil || message.RefundedPayment != nil || message.UsersShared != nil || message.ChatShared != nil || message.Gift != nil || message.UniqueGift != nil || message.GiftUpgradeSent != nil || message.LivePhoto != nil
}

func hasUnsupportedMessage(message *models.Message) bool {
	if message == nil {
		return true
	}
	for _, entity := range message.Entities {
		if entity.Type == models.MessageEntityTypeBotCommand {
			return true
		}
	}
	return hasUnsupportedMessageMetadata(message) || hasUnsupportedMessageChatEvents(message)
}

func hasUnsupportedMessageMetadata(message *models.Message) bool {
	return hasUnsupportedMessageRoutingMetadata(message) || hasUnsupportedMessageReplyMetadata(message)
}

func hasUnsupportedMessageRoutingMetadata(message *models.Message) bool {
	return message.DirectMessagesTopic != nil || message.SenderChat != nil || message.SenderBusinessBot != nil || message.ReceiverUser != nil || message.BusinessConnectionID != ""
}

func hasUnsupportedMessageReplyMetadata(message *models.Message) bool {
	return message.HasMediaSpoiler || message.MediaGroupID != "" || message.ReplyToStore != nil || message.SuggestedPostInfo != nil || message.EffectID != "" || message.EditDate != 0 || message.GuestQueryID != "" || message.ReplyToPollOptionID != ""
}

func hasUnsupportedMessageMedia(message *models.Message) bool {
	return hasUnsupportedMessageMediaPrimary(message) || hasUnsupportedMessageMediaSecondary(message)
}

func hasUnsupportedMessageMediaPrimary(message *models.Message) bool {
	return message.Animation != nil || message.Audio != nil || message.Document != nil || message.PaidMedia != nil || len(message.Photo) > 0 || message.Sticker != nil || message.Story != nil || message.Video != nil || message.VideoNote != nil || message.Voice != nil || message.Checklist != nil || message.Contact != nil
}

func hasUnsupportedMessageMediaSecondary(message *models.Message) bool {
	return message.Dice != nil || message.Game != nil || message.Poll != nil || message.Venue != nil || message.Location != nil || message.Invoice != nil || message.SuccessfulPayment != nil || message.RefundedPayment != nil || message.UsersShared != nil || message.ChatShared != nil || message.Gift != nil || message.UniqueGift != nil || message.GiftUpgradeSent != nil || message.LivePhoto != nil
}

func hasUnsupportedMessageChatEvents(message *models.Message) bool {
	return hasUnsupportedMessageChatLifecycle(message) || hasUnsupportedMessageChatTopics(message) || hasUnsupportedMessageChatCommerce(message)
}

func hasUnsupportedMessageChatLifecycle(message *models.Message) bool {
	return hasUnsupportedMessageChatLifecycleCore(message) || hasUnsupportedMessageChatLifecycleMetadata(message)
}

func hasUnsupportedMessageChatLifecycleCore(message *models.Message) bool {
	return len(message.NewChatMembers) > 0 || message.LeftChatMember != nil || message.NewChatTitle != "" || len(message.NewChatPhoto) > 0 || message.DeleteChatPhoto || message.GroupChatCreated || message.SupergroupChatCreated || message.ChannelChatCreated || message.MessageAutoDeleteTimerChanged != nil || message.MigrateToChatID != 0 || message.MigrateFromChatID != 0
}

func hasUnsupportedMessageChatLifecycleMetadata(message *models.Message) bool {
	return message.PinnedMessage != nil || message.ConnectedWebsite != "" || message.WriteAccessAllowed != nil || message.PassportData != nil || message.ProximityAlertTriggered != nil || message.BoostAdded != nil || message.ChatBackgroundSet != nil || message.ChecklistTasksDone != nil || message.ChecklistTasksAdded != nil || message.DirectMessagePriceChanged != nil
}

func hasUnsupportedMessageChatTopics(message *models.Message) bool {
	return message.ForumTopicCreated != nil || message.ForumTopicEdited != nil || message.ForumTopicClosed != nil || message.ForumTopicReopened != nil || message.GeneralForumTopicHidden != nil || message.GeneralForumTopicUnhidden != nil || message.GiveawayCreated != nil || message.Giveaway != nil || message.GiveawayWinners != nil || message.GiveawayCompleted != nil || message.PaidMessagePriceChanged != nil || message.ChatOwnerLeft != nil || message.ChatOwnerChanged != nil || message.CommunityChatAdded != nil || message.CommunityChatRemoved != nil
}

func hasUnsupportedMessageChatCommerce(message *models.Message) bool {
	return message.SuggestedPostApproved != nil || message.SuggestedPostApprovalFailed != nil || message.SuggestedPostDeclined != nil || message.SuggestedPostPaid != nil || message.SuggestedPostRefunded != nil || message.VideoChatScheduled != nil || message.VideoChatStarted != nil || message.VideoChatEnded != nil || message.VideoChatParticipantsInvited != nil || message.WebAppData != nil || message.ManagedBotCreated != nil || message.PollOptionAdded != nil || message.PollOptionDeleted != nil || message.GuestBotCallerUser != nil || message.GuestBotCallerChat != nil
}

func hasUnsupportedUpdate(update *models.Update) bool {
	return hasUnsupportedUpdateMessages(update) || hasUnsupportedUpdateInteractions(update) || hasUnsupportedUpdateMembership(update)
}

func hasUnsupportedUpdateMessages(update *models.Update) bool {
	return update.EditedMessage != nil || update.ChannelPost != nil || update.EditedChannelPost != nil || update.BusinessConnection != nil || update.BusinessMessage != nil || update.EditedBusinessMessage != nil || update.DeletedBusinessMessages != nil
}

func hasUnsupportedUpdateInteractions(update *models.Update) bool {
	return update.MessageReaction != nil || update.MessageReactionCount != nil || update.InlineQuery != nil || update.ChosenInlineResult != nil || update.CallbackQuery != nil || update.ShippingQuery != nil || update.PreCheckoutQuery != nil || update.PurchasedPaidMedia != nil || update.Poll != nil || update.PollAnswer != nil || update.ManagedBot != nil || update.GuestMessage != nil
}

func hasUnsupportedUpdateMembership(update *models.Update) bool {
	return update.MyChatMember != nil || update.ChatMember != nil || update.ChatJoinRequest != nil || update.ChatBoost != nil || update.RemovedChatBoost != nil || update.Subscription != nil
}

func externalMessageID(target channels.RoutingTarget, updateID int64) string {
	return encodeParts("telegram-update", target.BindingID, strconv.FormatInt(updateID, 10))
}

func encodeParts(parts ...string) string {
	var builder strings.Builder
	for _, part := range parts {
		builder.WriteString(strconv.Itoa(len([]byte(part))))
		builder.WriteByte(':')
		builder.WriteString(part)
	}
	return builder.String()
}

func splitText(text string, maximum int) []string {
	if text == "" {
		return nil
	}
	runes := []rune(text)
	if len(runes) <= maximum {
		return []string{text}
	}
	chunks := make([]string, 0, (len(runes)+maximum-1)/maximum)
	for len(runes) > 0 {
		end := maximum
		if len(runes) < end {
			end = len(runes)
		}
		chunks = append(chunks, string(runes[:end]))
		runes = runes[end:]
	}
	return chunks
}

func normalizeToken(token string) (string, error) {
	if token == "" || strings.TrimSpace(token) != token || len([]rune(token)) > maximumTokenRunes || hasControl(token) {
		return "", fmt.Errorf("%w: bot token is invalid", ErrInvalid)
	}
	return token, nil
}

func normalizeAPIBaseURL(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", nil
	}
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || (parsed.Path != "" && parsed.Path != "/") || parsed.RawPath != "" || parsed.RawQuery != "" || parsed.Fragment != "" || hasControl(value) {
		return "", fmt.Errorf("%w: API base URL must be an HTTPS origin", ErrInvalid)
	}
	return strings.TrimRight(value, "/"), nil
}

func normalizePollTimeout(value time.Duration) (time.Duration, error) {
	if value == 0 {
		return defaultPollTimeout, nil
	}
	if value < minimumPollTimeout || value > maximumPollTimeout {
		return 0, fmt.Errorf("%w: polling timeout is outside supported bounds", ErrInvalid)
	}
	return value, nil
}

func normalizeWorkers(value int) (int, error) {
	if value == 0 {
		return 1, nil
	}
	if value < 1 {
		return 0, fmt.Errorf("%w: worker count must be positive", ErrInvalid)
	}
	return value, nil
}

func normalizeAttachmentBytes(value int64) (int64, error) {
	if value == 0 {
		return defaultAttachmentBytes, nil
	}
	if value < 1 || value > maximumAttachmentBytes {
		return 0, fmt.Errorf("%w: attachment size limit is outside supported bounds", ErrInvalid)
	}
	return value, nil
}

func hasControl(value string) bool {
	for _, character := range value {
		if character < 0x20 || character == 0x7f {
			return true
		}
	}
	return false
}

func (adapter *Adapter) report(operation ErrorOperation, err error) {
	if adapter == nil || adapter.errorHook == nil || err == nil {
		return
	}
	adapter.errorHook(ErrorEvent{Operation: operation, Err: err})
}

func (adapter *Adapter) closeOwnedIdempotency() error {
	if adapter == nil || !adapter.ownIdempotency || adapter.idempotency == nil {
		return nil
	}
	return adapter.idempotency.Close()
}

type sdkBotFactory struct{}

func (sdkBotFactory) New(token string, config BotFactoryConfig) (BotClient, error) {
	if config.Handler == nil || config.Workers < 1 || config.PollTimeout < minimumPollTimeout {
		return nil, ErrInvalid
	}
	options := []bot.Option{
		bot.WithSkipGetMe(), bot.WithDefaultHandler(config.Handler), bot.WithNotAsyncHandlers(), bot.WithWorkers(config.Workers),
		bot.WithHTTPClient(config.PollTimeout, configuredHTTPClient(config.HTTPClient, config.PollTimeout)),
		bot.WithErrorsHandler(func(error) {
			if config.OnPollingError != nil {
				config.OnPollingError()
			}
		}),
	}
	if config.APIBaseURL != "" {
		options = append(options, bot.WithServerURL(config.APIBaseURL))
	}
	return bot.New(token, options...)
}

func configuredHTTPClient(client bot.HttpClient, pollTimeout time.Duration) bot.HttpClient {
	if client != nil {
		return client
	}
	return &http.Client{Timeout: pollTimeout + 5*time.Second}
}
