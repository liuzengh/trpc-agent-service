// Package wecom implements HTTPS callbacks for WeCom self-built application
// Bindings.
package wecom

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha1" // #nosec G505 -- WeCom requires SHA-1 callback signatures.
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	"github.com/XnLemon/trpc-agent-service/trpcservice/attachment"
	"github.com/XnLemon/trpc-agent-service/trpcservice/audit"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	"github.com/XnLemon/trpc-agent-service/trpcservice/gateway"
	"github.com/XnLemon/trpc-agent-service/trpcservice/metrics"
	"github.com/XnLemon/trpc-agent-service/trpcservice/observability"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
	"github.com/google/uuid"
)

var (
	// ErrInvalid reports malformed WeCom callback configuration or payload.
	ErrInvalid = errors.New("invalid wecom callback")
	// ErrVerification reports a failed WeCom callback signature or decryption check.
	ErrVerification = errors.New("wecom callback verification failed")
	// ErrAttachment reports a redacted WeCom media ingestion failure.
	ErrAttachment = errors.New("wecom attachment processing failed")
)

const (
	wecomBlockSize          = 32
	defaultAttachmentBytes  = 64 << 20
	maximumAttachmentBytes  = 64 << 20
	wecomMediaContentRunes  = 128
	wecomDefaultFileMIME    = "application/octet-stream"
	wecomDefaultImageMIME   = "image/jpeg"
	wecomDefaultVideoMIME   = "video/mp4"
	wecomDefaultVoiceMIME   = "audio/amr"
	wecomDefaultVoiceSuffix = ".amr"
)

// Credentials is the private credential bundle for one Binding SecretRef.
type Credentials struct {
	CallbackToken  string
	EncodingAESKey string
	AppSecret      string
}

// CredentialResolver resolves the one secret bundle that a verified Binding
// is permitted to use.
type CredentialResolver interface {
	Resolve(context.Context, channels.SecretScope) (Credentials, error)
}

// MediaDownloadRequest carries the verified Binding context required to fetch
// one WeCom media object. It stays inside the channel boundary and is never
// passed to Runner.
type MediaDownloadRequest struct {
	TenantID     string
	BindingID    string
	CorpID       string
	AgentID      string
	AppSecret    string
	MediaID      string
	Kind         attachment.Kind
	MIMEType     string
	MaximumBytes int64
}

// MediaDownloader downloads one authenticated WeCom media object into the
// handler-owned attachment store. It must not expose provider URLs or tokens.
type MediaDownloader interface {
	Download(context.Context, MediaDownloadRequest) (io.ReadCloser, error)
}

// Config contains either a static callback target or the dependencies required
// to resolve a current trusted Binding for each callback.
type Config struct {
	Token            string
	EncodingAESKey   string
	ReceiveID        string
	AgentID          string
	AppSecret        string
	RouteKey         string
	Target           channels.RoutingTarget
	Dispatcher       gateway.DispatchService
	MaxBodyBytes     int64
	ExecutionTimeout time.Duration
	// Attachments is the explicit durable boundary for native inbound media.
	// When nil, media callback types are rejected fail-closed.
	Attachments runtimestorage.AttachmentStore
	// MediaDownloader performs authenticated provider media downloads before
	// bytes enter the protocol-neutral attachment store.
	MediaDownloader MediaDownloader
	// MaxAttachmentBytes bounds each downloaded media object. Zero defaults to
	// the protocol-neutral attachment limit.
	MaxAttachmentBytes int64

	Candidates  channels.CandidateConsumer
	Tenants     tenant.Repository
	Apps        appmodel.Repository
	Credentials CredentialResolver
	// AuditWriter receives mandatory accepted and duplicate ingress facts.
	AuditWriter audit.Writer
	// Observability supplies provider-neutral trace and metric hooks.
	Observability observability.Provider
}

type callbackState struct {
	token     string
	receiveID string
	agentID   string
	corpID    string
	appSecret string
	key       []byte
	principal gateway.Principal
}

// Handler owns accepted execution drains. BeginShutdown prevents new drains
// and Close joins every drain before the owning Runtime releases dependencies.
type Handler struct {
	static *callbackState
	// These retained fields preserve the package's focused cryptographic tests
	// and are populated together with static. Dynamic callbacks use a local
	// verified state instead.
	token, receiveID   string
	key                []byte
	routeKey           string
	dynamic            bool
	candidates         channels.CandidateConsumer
	tenants            tenant.Repository
	apps               appmodel.Repository
	credentials        CredentialResolver
	dispatcher         gateway.DispatchService
	maxBodyBytes       int64
	executionTimeout   time.Duration
	attachments        runtimestorage.AttachmentStore
	mediaDownloader    MediaDownloader
	maxAttachmentBytes int64
	auditWriter        audit.Writer
	telemetry          observability.Provider
	metrics            metrics.Catalog

	mu      sync.Mutex
	closing bool
	baseCtx context.Context
	cancel  context.CancelFunc
	drains  sync.WaitGroup
}

var _ channels.WebhookAdapter = (*Handler)(nil)

// New validates a callback Handler. Dynamic mode receives the complete
// trusted target only after protocol verification.
//
//nolint:gocyclo
func New(config Config) (*Handler, error) {
	if config.Dispatcher == nil {
		return nil, ErrInvalid
	}
	if config.MaxBodyBytes == 0 {
		config.MaxBodyBytes = 1 << 20
	}
	if config.MaxBodyBytes < 1 {
		return nil, ErrInvalid
	}
	if config.ExecutionTimeout == 0 {
		config.ExecutionTimeout = 4 * time.Minute
	}
	if config.ExecutionTimeout < 1 {
		return nil, ErrInvalid
	}
	maxAttachmentBytes, err := normalizeAttachmentBytes(config.MaxAttachmentBytes)
	if err != nil || (config.Attachments == nil) != (config.MediaDownloader == nil) {
		return nil, ErrInvalid
	}
	baseCtx, cancel := context.WithCancel(context.Background())
	if config.Observability == nil {
		config.Observability = observability.NewNoopProvider()
	}
	handler := &Handler{
		routeKey: strings.Trim(config.RouteKey, "/"), dispatcher: config.Dispatcher,
		maxBodyBytes: config.MaxBodyBytes, executionTimeout: config.ExecutionTimeout,
		attachments: config.Attachments, mediaDownloader: config.MediaDownloader, maxAttachmentBytes: maxAttachmentBytes,
		auditWriter: config.AuditWriter, baseCtx: baseCtx, cancel: cancel,
	}
	handler.telemetry, handler.metrics = config.Observability, metrics.New(config.Observability)
	if config.Candidates != nil || config.Tenants != nil || config.Apps != nil || config.Credentials != nil {
		if config.Candidates == nil || config.Tenants == nil || config.Apps == nil || config.Credentials == nil || handler.routeKey != "" {
			cancel()
			return nil, ErrInvalid
		}
		handler.dynamic = true
		handler.candidates, handler.tenants, handler.apps, handler.credentials = config.Candidates, config.Tenants, config.Apps, config.Credentials
		return handler, nil
	}
	if strings.TrimSpace(config.Token) == "" || strings.TrimSpace(config.ReceiveID) == "" || strings.TrimSpace(config.AgentID) == "" {
		cancel()
		return nil, ErrInvalid
	}
	if config.Attachments != nil && strings.TrimSpace(config.AppSecret) == "" {
		cancel()
		return nil, ErrInvalid
	}
	if err := config.Target.Validate(); err != nil || config.Target.Channel != channels.ChannelWeCom {
		cancel()
		return nil, ErrInvalid
	}
	principal, err := gateway.NewChannelPrincipal(config.Target)
	if err != nil {
		cancel()
		return nil, ErrInvalid
	}
	key, err := decodeAESKey(config.EncodingAESKey)
	if err != nil {
		cancel()
		return nil, ErrInvalid
	}
	handler.static = &callbackState{
		token: config.Token, receiveID: config.ReceiveID, agentID: strings.TrimSpace(config.AgentID),
		corpID: config.Target.ProviderAccountID, appSecret: strings.TrimSpace(config.AppSecret),
		key: key, principal: principal,
	}
	handler.token, handler.receiveID, handler.key = config.Token, config.ReceiveID, key
	return handler, nil
}

// ServeHTTP verifies the URL challenge or accepts one encrypted text message.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h == nil || r == nil || !h.matchesRoute(r.URL.Path) {
		http.NotFound(w, r)
		return
	}
	switch r.Method {
	case http.MethodGet:
		h.handleChallenge(w, r)
	case http.MethodPost:
		h.handleMessage(w, r)
	default:
		w.Header().Set("Allow", "GET, POST")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (h *Handler) matchesRoute(path string) bool {
	if h.dynamic {
		_, ok := callbackRouteKey(path)
		return ok
	}
	if h.routeKey == "" {
		return true
	}
	routeKey, ok := callbackRouteKey(path)
	return ok && routeKey == h.routeKey
}

func callbackRouteKey(path string) (string, bool) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) != 3 || parts[0] != "wecom" || parts[1] != "callback" || strings.TrimSpace(parts[2]) == "" {
		return "", false
	}
	return parts[2], true
}

func (h *Handler) handleChallenge(w http.ResponseWriter, r *http.Request) {
	plain, _, err := h.verify(r, r.URL.Query().Get("echostr"))
	if err != nil {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = io.WriteString(w, string(plain))
}

//nolint:gocyclo // Callback handling intentionally keeps protocol validation and admission in one ordered boundary.
func (h *Handler) handleMessage(w http.ResponseWriter, r *http.Request) {
	capture := &statusCaptureWriter{ResponseWriter: w}
	w = capture
	started := time.Now()
	operationCtx, _, finish := observability.StartOperation(r.Context(), h.telemetry, observability.OperationChannelReceive, "channel")
	_ = h.metrics.Request(operationCtx, map[string]string{"component": "channel", "operation": observability.OperationChannelReceive, "channel": "wecom", "status": "started"})
	defer func() {
		var outcome error
		if ctxErr := r.Context().Err(); ctxErr != nil {
			outcome = ctxErr
		} else if capture.status >= http.StatusBadRequest {
			outcome = errors.New("wecom callback failed")
		}
		finish(outcome)
		_ = h.metrics.Operation(operationCtx, started, map[string]string{"component": "channel", "operation": observability.OperationChannelReceive, "channel": "wecom"}, outcome)
	}()
	r = r.WithContext(operationCtx)
	defer func() { _ = r.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(r.Body, h.maxBodyBytes+1))
	if err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if int64(len(body)) > h.maxBodyBytes {
		http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
		return
	}
	decoder := xml.NewDecoder(bytes.NewReader(body))
	var envelope callbackEnvelope
	if err := decoder.Decode(&envelope); err != nil || envelope.Encrypt == "" {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	var trailing callbackEnvelope
	if err := decoder.Decode(&trailing); err != io.EOF {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	plain, state, err := h.verify(r, envelope.Encrypt)
	if err != nil {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	var message inboundXML
	if err := xml.Unmarshal(plain, &message); err != nil || !validInboundEnvelope(message, state.agentID) || h.validateInboundMessage(message) != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	executionCtx, cancel, ok := h.beginDrain(operationCtx)
	if !ok {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
		return
	}
	accepted := make(chan struct{}, 1)
	result := make(chan error, 1)
	requestID, traceID := uuid.NewString(), uuid.NewString()
	go func() {
		defer h.drains.Done()
		defer cancel()
		inbound, buildErr := h.buildInboundMessage(executionCtx, state, message)
		if buildErr != nil {
			logIngressBuildFailure(requestID, traceID, buildErr)
			result <- buildErr
			return
		}
		stream, dispatchErr := h.dispatcher.Dispatch(executionCtx, gateway.DispatchRequest{Accepted: accepted, Principal: state.principal, RequestID: requestID, TraceID: traceID, Message: inbound})
		if dispatchErr == nil && stream != nil {
			for range stream {
			}
		}
		result <- dispatchErr
	}()
	select {
	case <-accepted:
		h.writeIngressSuccess(w, r.Context(), state.principal, message, requestID, traceID, audit.EventIMIngressAccepted, audit.DecisionAccepted, "")
	case dispatchErr := <-result:
		// Dispatch implementations may notify acceptance immediately before
		// returning a completed result. Since both channels can then be ready,
		// make acceptance take precedence so the mandatory ingress audit is not
		// skipped by select's random ready-case choice.
		if h.tryAcceptedIngress(accepted, w, r.Context(), state.principal, message, requestID, traceID) {
			return
		}
		if dispatchErr == nil {
			h.writeSuccess(w)
			return
		}
		if errors.Is(dispatchErr, gateway.ErrDuplicateMessage) {
			h.writeIngressSuccess(w, r.Context(), state.principal, message, requestID, traceID, audit.EventIMIngressDuplicate, audit.DecisionDuplicate, string(audit.ErrorDuplicate))
			return
		}
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	case <-r.Context().Done():
	}
}

func validInboundEnvelope(message inboundXML, agentID string) bool {
	return strings.TrimSpace(message.MsgID) != "" && strings.TrimSpace(message.FromUserName) != "" && strings.TrimSpace(message.AgentID) == strings.TrimSpace(agentID)
}

func (h *Handler) validateInboundMessage(message inboundXML) error {
	switch normalizedMessageType(message.MsgType) {
	case "text":
		if strings.TrimSpace(message.Content) == "" {
			return ErrInvalid
		}
		return nil
	case "image", "file", "voice", "video":
		if h == nil || h.attachments == nil || h.mediaDownloader == nil {
			return ErrInvalid
		}
		_, err := wecomAttachmentDescriptor(message)
		return err
	default:
		return ErrInvalid
	}
}

func (h *Handler) buildInboundMessage(ctx context.Context, state callbackState, message inboundXML) (gateway.InboundMessage, error) {
	inbound := gateway.InboundMessage{
		ExternalMessageID: strings.TrimSpace(message.MsgID),
		ExternalUserID:    strings.TrimSpace(message.FromUserName),
	}
	if chatID := strings.TrimSpace(message.ChatID); chatID != "" {
		inbound.ConversationKind = channels.ConversationGroup
		inbound.ExternalChatID = chatID
	} else {
		inbound.ConversationKind = channels.ConversationDirect
		inbound.ExternalPeerID = strings.TrimSpace(message.FromUserName)
	}
	if normalizedMessageType(message.MsgType) == "text" {
		inbound.Content = message.Content
		inbound.ContentType = gateway.ContentTypeText
		return inbound.Normalize()
	}
	reference, err := h.ingestAttachment(ctx, state, message)
	if err != nil {
		return gateway.InboundMessage{}, err
	}
	inbound.Content = wecomMediaContent(reference)
	inbound.ContentType = gateway.ContentTypeMedia
	inbound.Attachments = []attachment.Reference{reference}
	return inbound.Normalize()
}

type wecomAttachment struct {
	mediaID  string
	kind     attachment.Kind
	mimeType string
	name     string
}

func (h *Handler) ingestAttachment(ctx context.Context, state callbackState, message inboundXML) (attachment.Reference, error) {
	descriptor, err := wecomAttachmentDescriptor(message)
	if err != nil {
		return attachment.Reference{}, ErrAttachment
	}
	if err := ctx.Err(); err != nil {
		return attachment.Reference{}, err
	}
	download := MediaDownloadRequest{
		TenantID:     state.principal.TenantID(),
		CorpID:       state.corpID,
		AgentID:      state.agentID,
		AppSecret:    state.appSecret,
		MediaID:      descriptor.mediaID,
		Kind:         descriptor.kind,
		MIMEType:     descriptor.mimeType,
		MaximumBytes: h.maxAttachmentBytes,
	}
	if target, ok := state.principal.RoutingTarget(); ok {
		download.BindingID = target.BindingID
		if download.CorpID == "" {
			download.CorpID = target.ProviderAccountID
		}
	}
	reader, err := h.mediaDownloader.Download(ctx, download)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return attachment.Reference{}, err
		}
		return attachment.Reference{}, ErrAttachment
	}
	if reader == nil {
		return attachment.Reference{}, ErrAttachment
	}
	data, readErr := io.ReadAll(io.LimitReader(reader, h.maxAttachmentBytes+1))
	closeErr := reader.Close()
	if readErr != nil || closeErr != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return attachment.Reference{}, contextErr
		}
		return attachment.Reference{}, ErrAttachment
	}
	if int64(len(data)) == 0 || int64(len(data)) > h.maxAttachmentBytes {
		return attachment.Reference{}, ErrAttachment
	}
	bindingID := download.BindingID
	upload := attachment.Upload{
		ID:         attachmentID(bindingID, strings.TrimSpace(message.MsgID), 0, descriptor.mediaID),
		Kind:       descriptor.kind,
		MIMEType:   descriptor.mimeType,
		Name:       descriptor.name,
		Size:       int64(len(data)),
		Provider:   "wecom",
		ProviderID: descriptor.mediaID,
	}
	reference, err := h.attachments.PutAttachment(ctx, state.principal.TenantID(), upload, bytes.NewReader(data))
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return attachment.Reference{}, err
		}
		return attachment.Reference{}, ErrAttachment
	}
	return reference, nil
}

func wecomAttachmentDescriptor(message inboundXML) (wecomAttachment, error) {
	mediaID := strings.TrimSpace(message.MediaID)
	if mediaID == "" {
		return wecomAttachment{}, ErrInvalid
	}
	switch normalizedMessageType(message.MsgType) {
	case "image":
		return wecomAttachment{mediaID: mediaID, kind: attachment.KindImage, mimeType: wecomDefaultImageMIME, name: mediaName("", mediaID, ".jpg")}, nil
	case "file":
		return wecomAttachment{mediaID: mediaID, kind: attachment.KindDocument, mimeType: wecomDefaultFileMIME, name: mediaName(message.FileName, mediaID, "")}, nil
	case "voice":
		mimeType, suffix := voiceMIME(message.Format)
		return wecomAttachment{mediaID: mediaID, kind: attachment.KindAudio, mimeType: mimeType, name: mediaName("", mediaID, suffix)}, nil
	case "video":
		return wecomAttachment{mediaID: mediaID, kind: attachment.KindVideo, mimeType: wecomDefaultVideoMIME, name: mediaName("", mediaID, ".mp4")}, nil
	default:
		return wecomAttachment{}, ErrInvalid
	}
}

func normalizedMessageType(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func voiceMIME(format string) (string, string) {
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "mp3":
		return "audio/mpeg", ".mp3"
	case "wav":
		return "audio/wav", ".wav"
	case "m4a":
		return "audio/mp4", ".m4a"
	case "ogg":
		return "audio/ogg", ".ogg"
	case "speex":
		return "audio/speex", ".speex"
	default:
		return wecomDefaultVoiceMIME, wecomDefaultVoiceSuffix
	}
}

func mediaName(name, providerID, suffix string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return providerID + suffix
	}
	for _, character := range name {
		if character < 0x20 || character == 0x7f || character == '/' || character == '\\' {
			return providerID + suffix
		}
	}
	return name
}

func wecomMediaContent(reference attachment.Reference) string {
	base := "[wecom " + string(reference.Kind) + " attachment"
	if reference.Name != "" {
		withName := base + ": " + reference.Name + "]"
		if len([]rune(withName)) <= wecomMediaContentRunes {
			return withName
		}
	}
	return base + "]"
}

func attachmentID(bindingID, externalMessageID string, ordinal int, providerID string) string {
	digest := sha256.Sum256([]byte(encodeParts(bindingID, externalMessageID, strconv.Itoa(ordinal), providerID)))
	return "att_" + hex.EncodeToString(digest[:])
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

func (h *Handler) tryAcceptedIngress(accepted <-chan struct{}, w http.ResponseWriter, ctx context.Context, principal gateway.Principal, message inboundXML, requestID, traceID string) bool {
	select {
	case <-accepted:
		h.writeIngressSuccess(w, ctx, principal, message, requestID, traceID, audit.EventIMIngressAccepted, audit.DecisionAccepted, "")
		return true
	default:
		return false
	}
}

type statusCaptureWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusCaptureWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusCaptureWriter) Write(body []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(body)
}

func (h *Handler) writeIngressSuccess(w http.ResponseWriter, ctx context.Context, principal gateway.Principal, message inboundXML, requestID, traceID string, eventType audit.EventType, decision audit.Decision, errorType string) {
	if err := h.recordIngress(ctx, principal, message, requestID, traceID, eventType, decision, errorType); err != nil {
		logIngressAuditFailure(requestID, traceID, err)
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
		return
	}
	h.writeSuccess(w)
}

func (h *Handler) recordIngress(ctx context.Context, principal gateway.Principal, message inboundXML, requestID, traceID string, eventType audit.EventType, decision audit.Decision, errorType string) error {
	if h == nil || h.auditWriter == nil {
		return nil
	}
	event := audit.Event{SchemaVersion: audit.SchemaVersion, EventID: audit.NewEventID(requestID, string(eventType)), EventType: eventType, TenantID: principal.TenantID(), Channel: string(channels.ChannelWeCom), UserID: message.FromUserName, AgentAppID: principal.AppID(), Decision: decision, ErrorType: errorType, RequestID: requestID, TraceID: traceID, ActorType: string(principal.Kind()), ActorID: principal.SubjectID(), OccurredAt: time.Now().UTC()}
	_, err := h.auditWriter.Append(ctx, event)
	return err
}

func (h *Handler) beginDrain(parent context.Context) (context.Context, context.CancelFunc, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closing {
		return nil, nil, false
	}
	if parent == nil {
		parent = h.baseCtx
		if parent == nil {
			parent = context.Background()
		}
	}
	// Keep the verified receive context as the trace parent while also making
	// handler shutdown cancel every accepted drain. A request context alone is
	// insufficient because Close must join in-flight dispatches immediately.
	merged, mergeCancel := context.WithCancel(context.WithoutCancel(parent))
	base := h.baseCtx
	if base == nil {
		base = context.Background()
	}
	stopBase := context.AfterFunc(base, mergeCancel)
	withTimeout, timeoutCancel := context.WithTimeout(merged, h.executionTimeout)
	cancel := func() {
		timeoutCancel()
		mergeCancel()
		stopBase()
	}
	h.drains.Add(1)
	return withTimeout, cancel, true
}

// BeginShutdown prevents accepting new execution drains and cancels drains already owned by this Handler.
func (h *Handler) BeginShutdown() {
	if h == nil {
		return
	}
	h.mu.Lock()
	if !h.closing {
		h.closing = true
		h.cancel()
	}
	h.mu.Unlock()
}

// Channel identifies the protocol owned by this Handler.
func (*Handler) Channel() channels.Channel { return channels.ChannelWeCom }

// Close joins accepted execution drains after canceling their process context.
func (h *Handler) Close() error {
	if h == nil {
		return nil
	}
	h.BeginShutdown()
	h.drains.Wait()
	return nil
}

func (h *Handler) verify(r *http.Request, ciphertext string) ([]byte, callbackState, error) {
	if h.static != nil {
		if !validSignature(h.static.token, r.URL.Query().Get("msg_signature"), r.URL.Query().Get("timestamp"), r.URL.Query().Get("nonce"), ciphertext) {
			return nil, callbackState{}, ErrVerification
		}
		plain, err := decrypt(h.static.key, h.static.receiveID, ciphertext)
		return plain, *h.static, err
	}
	routeKey, ok := callbackRouteKey(r.URL.Path)
	if !ok {
		return nil, callbackState{}, ErrVerification
	}
	digest, err := channels.DigestPublicRouteKey(channels.ChannelWeCom, routeKey)
	if err != nil {
		return nil, callbackState{}, ErrVerification
	}
	candidates, err := h.candidates.LookupCandidates(r.Context(), channels.ChannelWeCom, digest)
	if err != nil {
		return nil, callbackState{}, ErrVerification
	}
	for _, candidate := range candidates {
		var verifiedState callbackState
		var verifiedPlain []byte
		target, resolveErr := channels.ResolveCandidateRoutingTarget(r.Context(), h.candidates, h.tenants, h.apps, candidate, func(ctx context.Context, binding channels.Binding) error {
			if binding.Channel != channels.ChannelWeCom || binding.Protocol.WeCom == nil {
				return ErrVerification
			}
			credentials, credentialErr := h.credentials.Resolve(ctx, channels.SecretScope{TenantID: binding.TenantID, SecretRef: binding.SecretRef})
			if credentialErr != nil {
				return credentialErr
			}
			key, keyErr := decodeAESKey(credentials.EncodingAESKey)
			if keyErr != nil || !validSignature(credentials.CallbackToken, r.URL.Query().Get("msg_signature"), r.URL.Query().Get("timestamp"), r.URL.Query().Get("nonce"), ciphertext) {
				return ErrVerification
			}
			plain, decryptErr := decrypt(key, binding.Protocol.WeCom.ReceiveID, ciphertext)
			if decryptErr != nil {
				return decryptErr
			}
			verifiedState = callbackState{
				token: credentials.CallbackToken, receiveID: binding.Protocol.WeCom.ReceiveID,
				agentID: binding.Protocol.WeCom.AgentID, corpID: binding.Protocol.WeCom.CorpID,
				appSecret: credentials.AppSecret, key: key,
			}
			verifiedPlain = plain
			return nil
		})
		if resolveErr != nil {
			if channels.IsContextCancellation(resolveErr) {
				return nil, callbackState{}, resolveErr
			}
			continue
		}
		principal, principalErr := gateway.NewChannelPrincipal(target)
		if principalErr != nil {
			continue
		}
		verifiedState.principal = principal
		return verifiedPlain, verifiedState, nil
	}
	return nil, callbackState{}, ErrVerification
}

func (h *Handler) writeSuccess(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = io.WriteString(w, "success")
}
func (h *Handler) validSignature(signature, timestamp, nonce, ciphertext string) bool {
	if h == nil {
		return false
	}
	if h.static != nil {
		return validSignature(h.static.token, signature, timestamp, nonce, ciphertext)
	}
	return validSignature(h.token, signature, timestamp, nonce, ciphertext)
}
func validSignature(token, signature, timestamp, nonce, ciphertext string) bool {
	if token == "" || signature == "" || timestamp == "" || nonce == "" || ciphertext == "" {
		return false
	}
	parts := []string{token, timestamp, nonce, ciphertext}
	sort.Strings(parts)
	sum := sha1.Sum([]byte(strings.Join(parts, ""))) // #nosec G401 -- required by the WeCom protocol.
	want := hex.EncodeToString(sum[:])
	return subtle.ConstantTimeCompare([]byte(signature), []byte(want)) == 1
}

func normalizeAttachmentBytes(value int64) (int64, error) {
	if value == 0 {
		return defaultAttachmentBytes, nil
	}
	if value < 1 || value > maximumAttachmentBytes {
		return 0, ErrInvalid
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

func decodeAESKey(value string) ([]byte, error) {
	key, err := base64.StdEncoding.DecodeString(value + "=")
	if err != nil || len(key) != 32 {
		return nil, ErrInvalid
	}
	return key, nil
}
func (h *Handler) decrypt(value string) ([]byte, error) {
	if h == nil {
		return nil, ErrVerification
	}
	if h.static != nil {
		return decrypt(h.static.key, h.static.receiveID, value)
	}
	return decrypt(h.key, h.receiveID, value)
}
func decrypt(key []byte, receiveID, value string) ([]byte, error) {
	ciphertext, err := base64.StdEncoding.DecodeString(value)
	if err != nil || len(ciphertext) == 0 || len(ciphertext)%aes.BlockSize != 0 {
		return nil, ErrVerification
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, ErrVerification
	}
	iv := make([]byte, aes.BlockSize)
	copy(iv, key)
	plain := make([]byte, len(ciphertext))
	cipher.NewCBCDecrypter(block, iv).CryptBlocks(plain, ciphertext)
	plain, err = unpad(plain)
	if err != nil || len(plain) < 20 {
		return nil, ErrVerification
	}
	size := int(binary.BigEndian.Uint32(plain[16:20]))
	if size < 0 || size > len(plain)-20 || subtle.ConstantTimeCompare(plain[20+size:], []byte(receiveID)) != 1 {
		return nil, ErrVerification
	}
	return append([]byte(nil), plain[20:20+size]...), nil
}
func unpad(value []byte) ([]byte, error) {
	if len(value) == 0 {
		return nil, ErrVerification
	}
	count := int(value[len(value)-1])
	if count < 1 || count > wecomBlockSize || count > len(value) || !bytes.Equal(value[len(value)-count:], bytes.Repeat([]byte{byte(count)}, count)) {
		return nil, ErrVerification
	}
	return value[:len(value)-count], nil
}

type callbackEnvelope struct {
	Encrypt string `xml:"Encrypt"`
}
type inboundXML struct {
	MsgID        string `xml:"MsgId"`
	FromUserName string `xml:"FromUserName"`
	ChatID       string `xml:"ChatId"`
	MsgType      string `xml:"MsgType"`
	AgentID      string `xml:"AgentID"`
	Content      string `xml:"Content"`
	MediaID      string `xml:"MediaId"`
	PicURL       string `xml:"PicUrl"`
	Format       string `xml:"Format"`
	FileName     string `xml:"FileName"`
}

var _ http.Handler = (*Handler)(nil)
