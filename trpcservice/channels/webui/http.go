package webui

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/identity"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/ingress"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/progress"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
)

type BrowserHandler struct {
	Callback    http.Handler
	Routes      ingress.Store
	Secrets     secrets.Provider
	Messages    MessageReader
	Results     messaging.ResultStore
	ReplyRoutes messaging.ReplyRouteStore
	Progress    interface {
		Subscribe(string, int) (<-chan progress.Event, func())
	}
	Confirmations interface {
		GetConfirmation(context.Context, string, string) (governance.Confirmation, error)
	}
	Actions governance.ConfirmationActionDecider
	Now     func() time.Time
	MaxSkew time.Duration
}

type browserReply struct {
	RequestID         string               `json:"request_id"`
	ProviderMessageID string               `json:"provider_message_id"`
	Text              string               `json:"text"`
	CreatedAt         time.Time            `json:"created_at"`
	ContentRef        string               `json:"content_ref"`
	Confirmation      *browserConfirmation `json:"confirmation,omitempty"`
}

type browserConfirmation struct {
	ConfirmationID string                       `json:"confirmation_id"`
	ToolID         string                       `json:"tool_id"`
	ToolVersion    int64                        `json:"tool_version"`
	State          governance.ConfirmationState `json:"state"`
	Version        int64                        `json:"version"`
	ExpiresAt      time.Time                    `json:"expires_at"`
}

type confirmationActionRequest struct {
	SchemaVersion   uint16 `json:"schema_version"`
	ConfirmationID  string `json:"confirmation_id"`
	ExpectedVersion int64  `json:"expected_version"`
	Decision        string `json:"decision"`
	ExternalUserID  string `json:"external_user_id"`
	ExternalChatID  string `json:"external_chat_id"`
}

func (h BrowserHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if h.Callback == nil || h.Routes == nil || h.Secrets == nil || h.Messages == nil || h.Results == nil {
		http.Error(writer, "service unavailable", http.StatusServiceUnavailable)
		return
	}
	switch request.URL.Path {
	case "/webui":
		http.Redirect(writer, request, "/webui/", http.StatusTemporaryRedirect)
	case "/webui/":
		if request.Method != http.MethodGet {
			methodNotAllowed(writer, "GET")
			return
		}
		writeAsset(writer, "text/html; charset=utf-8", webUIHTML)
	case "/webui/app.js":
		if request.Method != http.MethodGet {
			methodNotAllowed(writer, "GET")
			return
		}
		writeAsset(writer, "text/javascript; charset=utf-8", webUIJS)
	case "/webui/style.css":
		if request.Method != http.MethodGet {
			methodNotAllowed(writer, "GET")
			return
		}
		writeAsset(writer, "text/css; charset=utf-8", webUICSS)
	case "/webui/api/messages":
		if request.Method != http.MethodPost {
			methodNotAllowed(writer, "POST")
			return
		}
		h.Callback.ServeHTTP(writer, request)
	case "/webui/api/replies":
		if request.Method != http.MethodGet {
			methodNotAllowed(writer, "GET")
			return
		}
		h.replies(writer, request)
	case "/webui/api/progress":
		if request.Method != http.MethodGet {
			methodNotAllowed(writer, "GET")
			return
		}
		h.streamProgress(writer, request)
	case "/webui/api/confirmations/actions":
		if request.Method != http.MethodPost {
			methodNotAllowed(writer, "POST")
			return
		}
		h.confirmationAction(writer, request)
	default:
		http.NotFound(writer, request)
	}
}

func (h BrowserHandler) streamProgress(writer http.ResponseWriter, request *http.Request) {
	if h.Progress == nil || h.ReplyRoutes == nil {
		http.Error(writer, "service unavailable", http.StatusServiceUnavailable)
		return
	}
	query := request.URL.Query()
	routeKey := strings.TrimSpace(query.Get("route_key"))
	userID := strings.TrimSpace(query.Get("external_user_id"))
	chatID := strings.TrimSpace(query.Get("external_chat_id"))
	if routeKey == "" || userID == "" || chatID == "" {
		http.Error(writer, "invalid request", http.StatusBadRequest)
		return
	}
	route, err := h.authorize(request.Context(), request, routeKey)
	if err != nil {
		writeBrowserError(writer, err)
		return
	}
	flusher, ok := writer.(http.Flusher)
	if !ok {
		http.Error(writer, "streaming unavailable", http.StatusInternalServerError)
		return
	}
	stream, unsubscribe := h.Progress.Subscribe(route.TenantID, 32)
	defer unsubscribe()
	response := http.NewResponseController(writer)
	refreshWriteDeadline := func() { _ = response.SetWriteDeadline(time.Now().Add(5 * time.Second)) }
	writer.Header().Set("Content-Type", "text/event-stream")
	writer.Header().Set("Cache-Control", "no-store")
	writer.Header().Set("Connection", "keep-alive")
	writer.Header().Set("X-Content-Type-Options", "nosniff")
	refreshWriteDeadline()
	_, _ = io.WriteString(writer, "retry: 1000\n\n")
	flusher.Flush()
	for {
		select {
		case <-request.Context().Done():
			return
		case event, open := <-stream:
			if !open {
				return
			}
			if !h.progressMatchesRoute(request.Context(), route, userID, chatID, event) {
				continue
			}
			encoded, encodeErr := json.Marshal(struct {
				RequestID string `json:"request_id"`
				Kind      string `json:"kind"`
				Sequence  uint64 `json:"sequence"`
				Content   string `json:"content,omitempty"`
			}{RequestID: event.RequestID, Kind: event.Kind, Sequence: event.Sequence, Content: event.Content})
			if encodeErr != nil {
				continue
			}
			refreshWriteDeadline()
			if _, writeErr := fmt.Fprintf(writer, "event: progress\ndata: %s\n\n", encoded); writeErr != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func (h BrowserHandler) progressMatchesRoute(ctx context.Context, binding ingress.BindingRoute, userID, chatID string, event progress.Event) bool {
	if event.SchemaVersion != 1 || event.TenantID != binding.TenantID || event.RequestID == "" {
		return false
	}
	route, err := h.ReplyRoutes.ResolveReplyRoute(ctx, event.TenantID, event.RequestID)
	return err == nil && route.Channel == "webui" && route.ChannelBindingID == binding.ChannelBindingID &&
		route.ExternalAccountID == binding.ExternalAccountID && route.ExternalUserID == userID && route.ExternalChatID == chatID
}

func (h BrowserHandler) replies(writer http.ResponseWriter, request *http.Request) {
	query := request.URL.Query()
	routeKey := strings.TrimSpace(query.Get("route_key"))
	userID := strings.TrimSpace(query.Get("external_user_id"))
	chatID := strings.TrimSpace(query.Get("external_chat_id"))
	if routeKey == "" || userID == "" || chatID == "" {
		http.Error(writer, "invalid request", http.StatusBadRequest)
		return
	}
	route, err := h.authorize(request.Context(), request, routeKey)
	if err != nil {
		writeBrowserError(writer, err)
		return
	}
	messages, err := h.Messages.ListMessages(request.Context(), MessageQuery{TenantID: route.TenantID,
		ChannelBindingID: route.ChannelBindingID, ExternalAccountID: route.ExternalAccountID,
		ExternalUserID: userID, ExternalChatID: chatID, Limit: 100})
	if err != nil {
		writeBrowserError(writer, err)
		return
	}
	replies := make([]browserReply, 0, len(messages))
	for _, message := range messages {
		result, resultErr := messaging.ResolveReplyContent(request.Context(), h.Results, route.TenantID, message.RequestID, message.ContentRef)
		if resultErr != nil {
			writeBrowserError(writer, resultErr)
			return
		}
		if result.ResultRef != message.ContentRef || result.ContentDigest != message.ContentDigest {
			writeBrowserError(writer, runtime.ErrVersionMismatch)
			return
		}
		reply := browserReply{RequestID: message.RequestID, ProviderMessageID: message.ProviderMessageID,
			Text: string(result.Content), ContentRef: message.ContentRef, CreatedAt: message.CreatedAt}
		if strings.HasPrefix(message.ContentRef, "confirmation://") {
			confirmation, confirmationErr := h.browserConfirmation(request.Context(), route, message, result.Content)
			if confirmationErr != nil {
				writeBrowserError(writer, confirmationErr)
				return
			}
			reply.Confirmation = &confirmation
			reply.Text = "危险工具确认：" + confirmation.ToolID
		}
		replies = append(replies, reply)
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.Header().Set("Cache-Control", "no-store")
	writer.Header().Set("X-Content-Type-Options", "nosniff")
	_ = json.NewEncoder(writer).Encode(map[string]any{"messages": replies})
}

func (h BrowserHandler) browserConfirmation(ctx context.Context, route ingress.BindingRoute, message Message, content []byte) (browserConfirmation, error) {
	if h.Confirmations == nil {
		return browserConfirmation{}, runtime.ErrCapabilityUnsupported
	}
	var prompt struct {
		SchemaVersion  uint16    `json:"schema_version"`
		Kind           string    `json:"kind"`
		ConfirmationID string    `json:"confirmation_id"`
		ToolID         string    `json:"tool_id"`
		ToolVersion    int64     `json:"tool_version"`
		ExpiresAt      time.Time `json:"expires_at"`
	}
	decoder := json.NewDecoder(bytes.NewReader(content))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&prompt); err != nil {
		return browserConfirmation{}, runtime.ErrInvalidEnvelope
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return browserConfirmation{}, runtime.ErrInvalidEnvelope
	}
	if prompt.SchemaVersion != 1 || prompt.Kind != "tool_confirmation" || prompt.ConfirmationID == "" || prompt.ToolID == "" || prompt.ToolVersion < 1 || prompt.ExpiresAt.IsZero() ||
		message.ContentRef != "confirmation://"+route.TenantID+"/"+prompt.ConfirmationID {
		return browserConfirmation{}, runtime.ErrVersionMismatch
	}
	value, err := h.Confirmations.GetConfirmation(ctx, route.TenantID, prompt.ConfirmationID)
	if err != nil {
		return browserConfirmation{}, err
	}
	if value.RequestID != message.RequestID || value.ChannelBindingID != route.ChannelBindingID || value.Tool.ID != prompt.ToolID || value.Tool.Version != prompt.ToolVersion || !value.ExpiresAt.Equal(prompt.ExpiresAt) {
		return browserConfirmation{}, runtime.ErrTenantScope
	}
	return browserConfirmation{ConfirmationID: value.ConfirmationID, ToolID: value.Tool.ID, ToolVersion: value.Tool.Version,
		State: value.State, Version: value.Version, ExpiresAt: value.ExpiresAt}, nil
}

func (h BrowserHandler) confirmationAction(writer http.ResponseWriter, request *http.Request) {
	if h.Actions == nil || h.Confirmations == nil || h.ReplyRoutes == nil {
		http.Error(writer, "service unavailable", http.StatusServiceUnavailable)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(writer, request.Body, 16<<10))
	if err != nil || len(body) == 0 {
		http.Error(writer, "invalid request", http.StatusBadRequest)
		return
	}
	var input confirmationActionRequest
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		http.Error(writer, "invalid request", http.StatusBadRequest)
		return
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		http.Error(writer, "invalid request", http.StatusBadRequest)
		return
	}
	input.ConfirmationID, input.Decision = strings.TrimSpace(input.ConfirmationID), strings.TrimSpace(input.Decision)
	input.ExternalUserID, input.ExternalChatID = strings.TrimSpace(input.ExternalUserID), strings.TrimSpace(input.ExternalChatID)
	if input.SchemaVersion != 1 || input.ConfirmationID == "" || input.ExpectedVersion < 1 ||
		(input.Decision != "approve" && input.Decision != "deny") || input.ExternalUserID == "" || input.ExternalChatID == "" {
		http.Error(writer, "invalid request", http.StatusBadRequest)
		return
	}
	routeKey := strings.TrimSpace(request.URL.Query().Get("route_key"))
	if routeKey == "" {
		http.Error(writer, "invalid request", http.StatusBadRequest)
		return
	}
	signedPayload := append([]byte(request.Method+"\n"+request.URL.RequestURI()+"\n"), body...)
	route, err := h.authorizePayload(request.Context(), request, routeKey, signedPayload)
	if err != nil {
		writeBrowserError(writer, err)
		return
	}
	confirmation, err := h.Confirmations.GetConfirmation(request.Context(), route.TenantID, input.ConfirmationID)
	if err != nil {
		writeBrowserError(writer, err)
		return
	}
	replyRoute, err := h.ReplyRoutes.ResolveReplyRoute(request.Context(), route.TenantID, confirmation.RequestID)
	if err != nil {
		writeBrowserError(writer, err)
		return
	}
	if replyRoute.Channel != "webui" || replyRoute.ChannelBindingID != route.ChannelBindingID || replyRoute.ExternalAccountID != route.ExternalAccountID ||
		replyRoute.ExternalUserID != input.ExternalUserID || replyRoute.ExternalChatID != input.ExternalChatID {
		writeBrowserError(writer, runtime.ErrTenantScope)
		return
	}
	binding := channel.VerifiedBinding{TenantID: route.TenantID, AgentAppID: route.AgentAppID, ChannelBindingID: route.ChannelBindingID,
		Channel: route.Channel, ExternalAccountID: route.ExternalAccountID, TenantVersion: route.TenantVersion, BindingVersion: route.BindingVersion,
		IdentitySecretRef: route.IdentitySecretRef, SessionSecretRef: route.SessionSecretRef}
	actor, err := (identity.Mapper{Secrets: h.Secrets}).Map(request.Context(), binding, channel.ProviderEvent{Channel: "webui", ExternalAccountID: route.ExternalAccountID,
		ExternalUserID: input.ExternalUserID, ExternalChatID: input.ExternalChatID, ConversationType: "p2p"})
	if err != nil {
		writeBrowserError(writer, err)
		return
	}
	decided, err := h.Actions.DecideAction(request.Context(), governance.ConfirmationAction{TenantID: route.TenantID, ConfirmationID: input.ConfirmationID,
		SubjectID: actor.UserID, ChannelBindingID: route.ChannelBindingID, SessionID: actor.SessionID, Approve: input.Decision == "approve",
		ExpectedVersion: input.ExpectedVersion, DecidedAt: h.now()})
	if err != nil {
		writeBrowserError(writer, err)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(writer).Encode(map[string]any{"confirmation_id": decided.ConfirmationID, "state": decided.State, "version": decided.Version})
}

func (h BrowserHandler) authorize(ctx context.Context, request *http.Request, routeKey string) (ingress.BindingRoute, error) {
	return h.authorizePayload(ctx, request, routeKey, []byte(request.Method+"\n"+request.URL.RequestURI()))
}

func (h BrowserHandler) authorizePayload(ctx context.Context, request *http.Request, routeKey string, payload []byte) (ingress.BindingRoute, error) {
	route, err := h.Routes.ResolveBindingRoute(ctx, "webui", RouteKeyDigest(routeKey))
	if err != nil {
		return ingress.BindingRoute{}, err
	}
	if !route.Enabled || route.Channel != "webui" || route.ExternalAccountID == "" {
		return ingress.BindingRoute{}, runtime.ErrVersionMismatch
	}
	value, err := h.Secrets.Resolve(ctx, secrets.Scope{TenantID: route.TenantID, Subject: route.ChannelBindingID,
		Purpose: secrets.PurposeChannelVerify, ResourceID: route.ChannelBindingID, ResourceVersion: route.BindingVersion}, route.SecretRef)
	if err != nil {
		return ingress.BindingRoute{}, err
	}
	defer clear(value.Bytes)
	if value.Version != route.SecretRef.Version {
		return ingress.BindingRoute{}, runtime.ErrVersionMismatch
	}
	material, err := parseMaterial(value.Bytes)
	if err != nil || material.ExternalAccountID != route.ExternalAccountID {
		return ingress.BindingRoute{}, runtime.ErrVersionMismatch
	}
	now := time.Now().UTC()
	if h.Now != nil {
		now = h.Now().UTC()
	}
	maxSkew := h.MaxSkew
	if maxSkew <= 0 {
		maxSkew = 5 * time.Minute
	}
	if err := verifySignature(material.Token, request.Header.Get(headerTimestamp), request.Header.Get(headerNonce),
		request.Header.Get(headerSignature), payload, now, maxSkew); err != nil {
		return ingress.BindingRoute{}, err
	}
	return route, nil
}

func (h BrowserHandler) now() time.Time {
	if h.Now != nil {
		return h.Now().UTC()
	}
	return time.Now().UTC()
}

func writeAsset(writer http.ResponseWriter, contentType, content string) {
	writer.Header().Set("Content-Type", contentType)
	writer.Header().Set("Cache-Control", "no-store")
	writer.Header().Set("X-Content-Type-Options", "nosniff")
	writer.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' data:; base-uri 'none'; frame-ancestors 'none'; form-action 'self'")
	_, _ = writer.Write([]byte(content))
}

func methodNotAllowed(writer http.ResponseWriter, allowed string) {
	writer.Header().Set("Allow", allowed)
	http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
}

func writeBrowserError(writer http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, runtime.ErrInvalidEnvelope), errors.Is(err, runtime.ErrVersionMismatch),
		errors.Is(err, runtime.ErrTenantScope), errors.Is(err, runtime.ErrNotFound):
		status = http.StatusUnauthorized
	case errors.Is(err, runtime.ErrBackendUnavailable):
		status = http.StatusServiceUnavailable
	case errors.Is(err, runtime.ErrVersionConflict), errors.Is(err, runtime.ErrAlreadyTerminal), errors.Is(err, runtime.ErrCancelRequested):
		status = http.StatusConflict
	}
	http.Error(writer, http.StatusText(status), status)
}
