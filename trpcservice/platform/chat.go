package platform

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type chatRunResponse struct {
	SessionID string `json:"session_id"`
	RequestID string `json:"request_id"`
	Status    string `json:"status"`
}

type createChatSessionRequest struct {
	AppID     string `json:"app_id"`
	SessionID string `json:"session_id"`
	UserID    string `json:"user_id"`
}

type createChannelBindingRequest struct {
	Channel          string `json:"channel"`
	AppID            string `json:"app_id"`
	ConversationType string `json:"conversation_type"`
	ConversationID   string `json:"external_conversation_id"`
	UserID           string `json:"external_user_id"`
	SessionID        string `json:"session_id"`
	Secret           string `json:"secret,omitempty"`
}

type sendChatMessageRequest struct {
	Input string `json:"input"`
}

type cancelChatRunRequest struct {
	RequestID string `json:"request_id"`
}

type providerReplayContextKey struct{}

const storageOperationTimeout = 100 * time.Millisecond

func (h *AdminHandler) ProcessProviderMessage(ctx context.Context, provider, account string, body []byte) error {
	if h.providers == nil {
		return errors.New("provider runtime unavailable")
	}
	var subject, fallbackSubject, userID, text, messageID, replyReference, providerSession, rejectionCode string
	var providerSequence int64
	switch provider {
	case ChannelTelegram:
		var update telegramUpdate
		if err := json.Unmarshal(body, &update); err != nil || update.UpdateID <= 0 || update.Message.MessageID <= 0 || update.Message.Chat.ID == 0 {
			h.recordInvalidProviderMessage(provider, body)
			return ErrProviderMessageIgnored
		}
		subject = fmt.Sprint(update.Message.Chat.ID)
		userID = fmt.Sprint(update.Message.From.ID)
		if update.Message.Chat.Type == "private" {
			fallbackSubject = userID
		}
		text = update.Message.Text
		messageID = fmt.Sprint(update.Message.MessageID)
		providerSequence = update.UpdateID
		if text == "" {
			rejectionCode = "unsupported_media"
		}
	case ChannelEnterpriseWeChat:
		var frame struct {
			Command         string `json:"cmd"`
			ProviderSession string `json:"provider_session_id"`
			Headers         struct {
				RequestID string `json:"req_id"`
			} `json:"headers"`
			Body struct {
				MessageID string `json:"msgid"`
				BotID     string `json:"aibotid"`
				ChatID    string `json:"chatid"`
				ChatType  string `json:"chattype"`
				From      struct {
					UserID string `json:"userid"`
				} `json:"from"`
				MessageType string `json:"msgtype"`
				Text        struct {
					Content string `json:"content"`
				} `json:"text"`
			} `json:"body"`
		}
		if err := json.Unmarshal(body, &frame); err != nil || frame.Command != "aibot_msg_callback" {
			h.recordInvalidProviderMessage(provider, body)
			return ErrProviderMessageIgnored
		}
		if frame.Body.MessageType != "text" {
			rejectionCode = "unsupported_media"
		}
		subject, userID, text, messageID, replyReference, providerSession = frame.Body.ChatID, frame.Body.From.UserID, frame.Body.Text.Content, frame.Body.MessageID, frame.Headers.RequestID, frame.ProviderSession
		if frame.Body.ChatType == ConversationSingle && subject == "" {
			subject = userID
		}
		if subject == "" || userID == "" || messageID == "" || replyReference == "" || frame.Body.BotID != account || (rejectionCode == "" && text == "") {
			h.recordInvalidProviderMessage(provider, body)
			return ErrProviderMessageIgnored
		}
	default:
		return errors.New("unsupported provider")
	}
	requestID := "channel-" + messageID
	route, exists, routeErr := h.providers.Routes().ResolveAccountContext(ctx, provider, account, subject)
	if routeErr != nil {
		h.providers.recordRejected(provider, subject, requestID, "control_plane_unavailable", BotRoute{})
		return errors.New("control_plane_unavailable")
	}
	if !exists && fallbackSubject != "" && fallbackSubject != subject {
		subject = fallbackSubject
		route, exists, routeErr = h.providers.Routes().ResolveAccountContext(ctx, provider, account, subject)
		if routeErr != nil {
			h.providers.recordRejected(provider, subject, requestID, "control_plane_unavailable", BotRoute{})
			return errors.New("control_plane_unavailable")
		}
	}
	if !exists {
		h.providers.recordRejected(provider, subject, requestID, "unmapped", BotRoute{})
		return ErrProviderMessageIgnored
	}
	if !route.Enabled {
		h.providers.recordRejected(provider, subject, requestID, "disabled", route)
		return ErrProviderMessageIgnored
	}
	if route.ProviderAccount != "" && route.ProviderAccount != account {
		h.providers.recordRejected(provider, subject, requestID, "provider_account_denied", route)
		return ErrProviderMessageIgnored
	}
	if route.ProviderAccount == "" {
		// Account-neutral routes are the single-account compatibility form. Once
		// selected, use the authenticated runtime account for session, dedupe and
		// delivery identity so different Bot accounts never share state.
		route.ProviderAccount = account
	}
	if rejectionCode != "" {
		h.providers.recordRejected(provider, subject, requestID, rejectionCode, route)
		return ErrProviderMessageIgnored
	}
	if _, ok, err := h.platform.app(ctx, route.TenantID, route.AppID); err != nil || !ok {
		if err != nil {
			h.providers.recordRejected(provider, subject, requestID, "control_plane_unavailable", route)
			return errors.New("control_plane_unavailable")
		}
		h.providers.recordRejected(provider, subject, requestID, "route_unavailable", route)
		return errors.New("provider route unavailable")
	}
	binding := ChannelBinding{
		TenantID: route.TenantID, AppID: route.AppID, Channel: provider,
		ProviderAccount: account, ConversationType: route.ConversationType, ConversationID: subject, UserID: userID,
		SessionID: providerSessionID(provider, account, subject), Enabled: true, PlatformOwned: true, ReplyReference: replyReference, ProviderSession: providerSession,
	}
	if replay, _ := ctx.Value(providerReplayContextKey{}).(bool); replay {
		binding.ReplayProvider = binding.Channel
		binding.Channel = ChannelMock
	}
	if code := h.providers.acceptInbound(route, messageID, providerSequence); code != "" {
		h.providers.recordRejected(provider, subject, requestID, code, route)
		if code == "duplicate" {
			h.redeliverProviderReply(binding, requestID)
		}
		return ErrProviderMessageIgnored
	}
	h.providers.recordAccepted(route, requestID)
	result, err := h.startChatRun(chatRunOptions{
		tenant: TenantContext{TenantID: route.TenantID, UserID: userID, Role: RoleOperator},
		appID:  route.AppID, sessionID: binding.SessionID, input: text,
		requestID: requestID, userID: userID, binding: &binding,
	})
	if err != nil {
		var governanceErr *GovernanceError
		if errors.As(err, &governanceErr) {
			h.providers.recordRejected(provider, subject, requestID, governanceErr.Code, route)
			return ErrProviderMessageIgnored
		}
		h.providers.releaseInbound(route, messageID)
		h.providers.recordDelivery(ChannelBinding{Channel: route.Provider, ConversationID: route.ExternalSubject, TenantID: route.TenantID, AppID: route.AppID}, ChannelReply{MessageID: requestID}, "terminal_failed", "runner_unavailable", 0)
	} else if result.Status == "completed" {
		h.redeliverProviderReply(binding, requestID)
	}
	return err
}

func (h *AdminHandler) recordInvalidProviderMessage(provider string, body []byte) {
	if h.providers == nil {
		return
	}
	digest := sha256.Sum256(body)
	key := hex.EncodeToString(digest[:])[:16]
	h.providers.recordRejected(provider, "invalid:"+key, "channel-invalid-"+key, "callback_invalid", BotRoute{})
}

func (h *AdminHandler) redeliverProviderReply(binding ChannelBinding, requestID string) {
	h.chatWG.Add(1)
	go func() {
		defer h.chatWG.Done()
		store, release, err := h.acquireStore(h.chatCtx, binding.TenantID)
		if err != nil {
			return
		}
		defer release()
		events, err := store.ListSessionEvents(h.chatCtx, binding.TenantID, binding.SessionID, 0)
		if err != nil {
			return
		}
		var output string
		for _, event := range events {
			if event.IdempotencyKey != requestID+":completed" || event.Type != "message.completed" {
				continue
			}
			var payload struct {
				Output string `json:"output"`
			}
			if json.Unmarshal(event.Payload, &payload) == nil {
				output = payload.Output
			}
			break
		}
		if output == "" {
			return
		}
		_, _ = h.channels.Send(h.chatCtx, binding, ChannelReply{MessageID: requestID, Text: output})
	}()
}

func (h *AdminHandler) handleProviderReplay(w http.ResponseWriter, r *http.Request) {
	tenant, ok := trustedTenant(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "identity_required", "development identity is required")
		return
	}
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method must be POST")
		return
	}
	if h.providers == nil {
		writeError(w, http.StatusServiceUnavailable, "provider_unavailable", "provider runtime is unavailable")
		return
	}
	var request struct {
		Provider        string `json:"provider"`
		ProviderAccount string `json:"provider_account"`
		ExternalSubject string `json:"external_subject"`
		Text            string `json:"text"`
	}
	if err := decodeStrict(r, &request); err != nil || request.ExternalSubject == "" || strings.TrimSpace(request.Text) == "" {
		writeError(w, http.StatusBadRequest, "invalid_provider_replay", "provider, subject, and text are required")
		return
	}
	if request.Provider != ChannelTelegram && request.Provider != ChannelEnterpriseWeChat {
		writeError(w, http.StatusBadRequest, "invalid_provider_replay", "provider is unsupported")
		return
	}
	account := request.ProviderAccount
	if account == "" && request.Provider == ChannelTelegram {
		account = h.providers.config.TelegramUsername
	}
	if account == "" && request.Provider == ChannelEnterpriseWeChat {
		account = h.providers.config.WeComBotID
	}
	if account == "" {
		account = "replay-bot"
	}
	route, found := h.providers.Routes().ResolveAccount(request.Provider, account, request.ExternalSubject)
	if !found || !route.Enabled || !tenantCanSee(tenant, route.TenantID) {
		writeError(w, http.StatusNotFound, "provider_route_not_found", "enabled provider route was not found")
		return
	}
	if !tenantAllowsPlatformAdmin(tenant, route.TenantID) {
		writeError(w, http.StatusForbidden, "forbidden", "platform administrator role is required")
		return
	}
	sequence := time.Now().UnixNano()
	var messageID string
	var body []byte
	switch request.Provider {
	case ChannelTelegram:
		chatID, err := strconv.ParseInt(request.ExternalSubject, 10, 64)
		if err != nil || chatID == 0 {
			writeError(w, http.StatusBadRequest, "invalid_provider_replay", "Telegram subject must be a numeric chat ID")
			return
		}
		messageID = strconv.FormatInt(sequence, 10)
		body, _ = json.Marshal(map[string]any{"update_id": sequence, "message": map[string]any{"message_id": sequence, "chat": map[string]any{"id": chatID, "type": "private"}, "from": map[string]any{"id": chatID}, "text": request.Text}})
	case ChannelEnterpriseWeChat:
		messageID = "replay-" + newRequestID()
		body, _ = json.Marshal(map[string]any{
			"cmd": "aibot_msg_callback", "headers": map[string]string{"req_id": newRequestID()}, "provider_session_id": "local-replay",
			"body": map[string]any{"msgid": messageID, "aibotid": account, "chatid": request.ExternalSubject, "chattype": ConversationGroup, "from": map[string]string{"userid": "replay-user"}, "msgtype": "text", "text": map[string]string{"content": request.Text}},
		})
	}
	ctx := context.WithValue(r.Context(), providerReplayContextKey{}, true)
	if err := h.ProcessProviderMessage(ctx, request.Provider, account, body); err != nil {
		if errors.Is(err, ErrProviderMessageIgnored) {
			writeError(w, http.StatusNotFound, "provider_route_not_found", "enabled provider route was not found")
			return
		}
		writeError(w, http.StatusBadRequest, "provider_replay_failed", "provider replay could not be started")
		return
	}
	writeJSON(w, http.StatusAccepted, chatRunResponse{
		SessionID: providerSessionID(request.Provider, account, request.ExternalSubject), RequestID: "channel-" + messageID, Status: "running",
	})
}

func (h *AdminHandler) handleChannelBindings(w http.ResponseWriter, r *http.Request) {
	tenant, ok := trustedTenant(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "identity_required", "development identity is required")
		return
	}
	switch r.Method {
	case http.MethodGet:
		items, err := h.channels.ListBindings(r.Context(), tenant.TenantID)
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, "control_plane_unavailable", "Control Plane Store is unavailable")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"items": items})
	case http.MethodPost:
		if !canOperate(tenant.Role) {
			writeError(w, http.StatusForbidden, "forbidden", "operator role is required")
			return
		}
		var request createChannelBindingRequest
		if err := decodeStrict(r, &request); err != nil || !validResourceID(request.AppID) || request.ConversationID == "" || request.UserID == "" {
			writeError(w, http.StatusBadRequest, "invalid_channel_binding", "channel, app, conversation, and user identifiers are required")
			return
		}
		if _, exists, err := h.platform.app(r.Context(), tenant.TenantID, request.AppID); err != nil || !exists {
			if writeControlPlaneError(w, err) {
				return
			}
			writeError(w, http.StatusNotFound, "agent_app_not_found", "Agent App was not found")
			return
		}
		binding, err := h.channels.CreateBinding(r.Context(), tenant, request)
		if errors.Is(err, ErrDuplicateEvent) {
			writeError(w, http.StatusConflict, "channel_binding_exists", "channel binding already exists")
			return
		}
		if err != nil {
			if errors.Is(err, errControlPlaneUnavailable) {
				writeError(w, http.StatusServiceUnavailable, "control_plane_unavailable", "Control Plane Store is unavailable")
			} else {
				writeError(w, http.StatusBadRequest, "invalid_channel_binding", "channel binding is invalid")
			}
			return
		}
		store, release, err := h.acquireStore(r.Context(), tenant.TenantID)
		if err != nil {
			writeStorageError(w, err)
			return
		}
		defer release()
		if err := h.appendChatEvent(r.Context(), store, tenant.TenantID, binding.SessionID, "session-created", "session.created", map[string]string{
			"app_id": binding.AppID, "channel": binding.Channel, "user_id": binding.UserID, "conversation_type": binding.ConversationType,
		}); err != nil {
			writeError(w, http.StatusServiceUnavailable, "storage_error", "session event could not be persisted")
			return
		}
		if binding.Channel != ChannelMock {
			binding.Secret = ""
		}
		writeJSON(w, http.StatusCreated, binding)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method must be GET or POST")
	}
}

func (h *AdminHandler) handleChannelBindingResource(w http.ResponseWriter, r *http.Request, id string) {
	tenant, ok := trustedTenant(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "identity_required", "development identity is required")
		return
	}
	if id == "" || !canOperate(tenant.Role) {
		writeError(w, http.StatusForbidden, "forbidden", "operator role is required")
		return
	}
	switch r.Method {
	case http.MethodPatch:
		var request struct {
			Enabled *bool  `json:"enabled"`
			Secret  string `json:"secret"`
		}
		if err := decodeStrict(r, &request); err != nil || (request.Enabled == nil && strings.TrimSpace(request.Secret) == "") {
			writeError(w, http.StatusBadRequest, "invalid_channel_binding", "enabled or secret is required")
			return
		}
		var binding ChannelBinding
		var err error
		if request.Enabled != nil {
			binding, err = h.channels.UpdateBinding(r.Context(), tenant.TenantID, id, *request.Enabled)
		} else {
			binding, err = h.channels.ReplaceSecret(r.Context(), tenant.TenantID, id, request.Secret)
		}
		if errors.Is(err, ErrNotFound) {
			writeError(w, http.StatusNotFound, "channel_binding_not_found", "channel binding was not found")
			return
		}
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, "control_plane_unavailable", "Control Plane Store is unavailable")
			return
		}
		writeJSON(w, http.StatusOK, binding)
	case http.MethodDelete:
		if err := h.channels.DeleteBinding(r.Context(), tenant.TenantID, id); errors.Is(err, ErrNotFound) {
			writeError(w, http.StatusNotFound, "channel_binding_not_found", "channel binding was not found")
			return
		} else if err != nil {
			writeError(w, http.StatusServiceUnavailable, "control_plane_unavailable", "Control Plane Store is unavailable")
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method must be PATCH or DELETE")
	}
}

func (h *AdminHandler) handleMockChannelCallback(w http.ResponseWriter, r *http.Request) {
	if _, ok := trustedTenant(r); !ok {
		writeError(w, http.StatusUnauthorized, "identity_required", "development identity is required")
		return
	}
	h.handleProviderChannelCallback(w, r, ChannelMock)
}

func (h *AdminHandler) handleProviderChannelCallback(w http.ResponseWriter, r *http.Request, channel string) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method must be POST")
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_channel_callback", "callback body is invalid")
		return
	}
	var envelope struct {
		BindingID string `json:"binding_id"`
	}
	_ = json.Unmarshal(body, &envelope)
	if envelope.BindingID == "" {
		envelope.BindingID = r.URL.Query().Get("binding_id")
	}
	if envelope.BindingID == "" {
		envelope.BindingID = r.Header.Get("X-Channel-Binding-ID")
	}
	if envelope.BindingID == "" {
		writeError(w, http.StatusBadRequest, "invalid_channel_callback", "binding id is required")
		return
	}
	callback := ChannelCallback{
		Channel: channel, BindingID: envelope.BindingID, Body: body, Signature: callbackSignature(r, channel),
		Timestamp: r.URL.Query().Get("timestamp"), Nonce: r.URL.Query().Get("nonce"),
	}
	var binding ChannelBinding
	var message ChannelMessage
	if channel == ChannelMock {
		tenant, ok := trustedTenant(r)
		if !ok {
			writeError(w, http.StatusUnauthorized, "identity_required", "development identity is required")
			return
		}
		binding, message, err = h.channels.Receive(r.Context(), tenant.TenantID, callback)
	} else {
		binding, message, err = h.channels.ReceiveExternal(r.Context(), callback)
	}
	if errors.Is(err, ErrNotFound) {
		writeError(w, http.StatusNotFound, "channel_binding_not_found", "channel binding was not found")
		return
	}
	if err != nil {
		writeChannelError(w, err)
		return
	}
	tenant := TenantContext{TenantID: binding.TenantID, UserID: message.UserID, Role: RoleOperator}
	result, err := h.startChatRun(chatRunOptions{
		tenant: tenant, appID: binding.AppID, sessionID: binding.SessionID, input: message.Text,
		requestID: "channel-" + message.MessageID, userID: message.UserID, binding: &binding,
	})
	if err != nil {
		writeChatStartError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, result)
}

func callbackSignature(r *http.Request, channel string) string {
	switch channel {
	case ChannelMock:
		return r.Header.Get("X-Mock-Signature")
	case ChannelEnterpriseWeChat:
		return r.URL.Query().Get("msg_signature")
	case ChannelTelegram:
		return r.Header.Get("X-Telegram-Bot-Api-Secret-Token")
	default:
		return ""
	}
}

func (h *AdminHandler) handleMockFaults(w http.ResponseWriter, r *http.Request) {
	tenant, ok := trustedTenant(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "identity_required", "development identity is required")
		return
	}
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, h.channels.MockFaults(tenant.TenantID, r.URL.Query().Get("session_id")))
	case http.MethodPost:
		h.mu.Lock()
		enabled := h.faultInjectionEnabled
		h.mu.Unlock()
		if !enabled {
			writeError(w, http.StatusForbidden, "fault_injection_disabled", "fault injection is disabled")
			return
		}
		if !canOperate(tenant.Role) {
			writeError(w, http.StatusForbidden, "forbidden", "operator role is required")
			return
		}
		var request struct {
			Scenario  string `json:"scenario"`
			SessionID string `json:"session_id"`
		}
		if err := decodeStrict(r, &request); err != nil {
			writeError(w, http.StatusBadRequest, "invalid_mock_fault", "scenario is required")
			return
		}
		config, err := h.channels.ConfigureMockFaults(tenant.TenantID, request.SessionID, request.Scenario)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid_mock_fault", "scenario is invalid")
			return
		}
		writeJSON(w, http.StatusOK, config)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method must be GET or POST")
	}
}

func (h *AdminHandler) handleProviderStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method must be GET")
		return
	}
	tenant, ok := trustedTenant(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "identity_required", "development identity is required")
		return
	}
	if h.providers == nil {
		writeJSON(w, http.StatusOK, map[string]any{"items": []BotStatus{}})
		return
	}
	items := h.providers.Statuses()
	visible := make(map[string]struct{})
	for _, route := range h.providers.Routes().List() {
		if tenantCanSee(tenant, route.TenantID) {
			visible[route.Provider] = struct{}{}
		}
	}
	filtered := items[:0]
	for _, item := range items {
		if _, ok := visible[item.Provider]; ok {
			filtered = append(filtered, item)
		}
	}
	items = filtered
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (h *AdminHandler) handleProviderDeliveries(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method must be GET")
		return
	}
	tenant, ok := trustedTenant(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "identity_required", "development identity is required")
		return
	}
	if h.providers == nil {
		writeJSON(w, http.StatusOK, map[string]any{"items": []ProviderDelivery{}})
		return
	}
	items := h.providers.Deliveries("")
	filtered := items[:0]
	for _, item := range items {
		if tenantCanSee(tenant, item.TenantID) {
			filtered = append(filtered, item)
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": filtered})
}

func (h *AdminHandler) handleProviderRoutes(w http.ResponseWriter, r *http.Request) {
	tenant, ok := trustedTenant(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "identity_required", "development identity is required")
		return
	}
	if h.providers == nil {
		writeError(w, http.StatusServiceUnavailable, "provider_unavailable", "provider runtime is unavailable")
		return
	}
	switch r.Method {
	case http.MethodGet:
		items, err := h.providers.Routes().ListContext(r.Context())
		if err != nil {
			if writeControlPlaneError(w, err) {
				return
			}
			writeError(w, http.StatusServiceUnavailable, "provider_route_store_unavailable", "provider routes are unavailable")
			return
		}
		filtered := items[:0]
		for _, item := range items {
			if tenantCanSee(tenant, item.TenantID) {
				filtered = append(filtered, item)
			}
		}
		items = filtered
		writeJSON(w, http.StatusOK, map[string]any{"items": items})
	case http.MethodPost:
		var route BotRoute
		if err := decodeStrict(r, &route); err != nil || (route.Provider != ChannelTelegram && route.Provider != ChannelEnterpriseWeChat) {
			writeError(w, http.StatusBadRequest, "invalid_provider_route", "provider route is invalid")
			return
		}
		if !tenantAllowsPlatformAdmin(tenant, route.TenantID) {
			writeError(w, http.StatusForbidden, "forbidden", "platform administrator role is required")
			return
		}
		if _, ok, err := h.platform.app(r.Context(), route.TenantID, route.AppID); err != nil || !ok {
			if writeControlPlaneError(w, err) {
				return
			}
			writeError(w, http.StatusNotFound, "agent_app_not_found", "Agent App was not found")
			return
		}
		if err := h.providers.Routes().Upsert(route); err != nil {
			if writeControlPlaneError(w, err) {
				return
			}
			writeError(w, http.StatusConflict, "provider_route_conflict", "provider route conflicts with an existing mapping")
			return
		}
		route.Enabled = true
		writeJSON(w, http.StatusCreated, route)
	case http.MethodPatch:
		provider, account, subject := r.URL.Query().Get("provider"), r.URL.Query().Get("provider_account"), r.URL.Query().Get("external_subject")
		existing, exists, lookupErr := h.providers.Routes().ResolveAccountContext(r.Context(), provider, account, subject)
		if lookupErr != nil {
			if writeControlPlaneError(w, lookupErr) {
				return
			}
			writeError(w, http.StatusServiceUnavailable, "provider_route_store_unavailable", "provider routes are unavailable")
			return
		}
		if !exists || !tenantAllowsPlatformAdmin(tenant, existing.TenantID) {
			writeError(w, http.StatusForbidden, "forbidden", "platform administrator role is required")
			return
		}
		var request struct {
			TenantID         string `json:"tenant_id"`
			AppID            string `json:"app_id"`
			ProviderAccount  string `json:"provider_account"`
			ConversationType string `json:"conversation_type"`
			Enabled          *bool  `json:"enabled"`
		}
		if err := decodeStrict(r, &request); err != nil || request.Enabled == nil || request.TenantID == "" || request.AppID == "" {
			writeError(w, http.StatusBadRequest, "invalid_provider_route", "provider route update is invalid")
			return
		}
		if request.ProviderAccount == "" {
			request.ProviderAccount = existing.ProviderAccount
		}
		if request.ProviderAccount != existing.ProviderAccount {
			writeError(w, http.StatusConflict, "provider_route_identity_immutable", "delete and recreate a route to change its provider account")
			return
		}
		if _, ok, err := h.platform.app(r.Context(), request.TenantID, request.AppID); err != nil || !ok {
			if writeControlPlaneError(w, err) {
				return
			}
			writeError(w, http.StatusNotFound, "agent_app_not_found", "Agent App was not found")
			return
		}
		route, err := h.providers.Routes().UpdateAccount(provider, existing.ProviderAccount, subject, BotRoute{
			TenantID: request.TenantID, AppID: request.AppID, ConversationType: request.ConversationType, Enabled: *request.Enabled,
			ProviderAccount: request.ProviderAccount,
		})
		if errors.Is(err, ErrNotFound) {
			writeError(w, http.StatusNotFound, "provider_route_not_found", "provider route was not found")
			return
		}
		if err != nil {
			if writeControlPlaneError(w, err) {
				return
			}
			writeError(w, http.StatusConflict, "provider_route_conflict", "provider route could not be updated")
			return
		}
		writeJSON(w, http.StatusOK, route)
	case http.MethodDelete:
		provider, account, subject := r.URL.Query().Get("provider"), r.URL.Query().Get("provider_account"), r.URL.Query().Get("external_subject")
		existing, exists, lookupErr := h.providers.Routes().ResolveAccountContext(r.Context(), provider, account, subject)
		if lookupErr != nil {
			if writeControlPlaneError(w, lookupErr) {
				return
			}
			writeError(w, http.StatusServiceUnavailable, "provider_route_store_unavailable", "provider routes are unavailable")
			return
		}
		if !exists || !tenantAllowsPlatformAdmin(tenant, existing.TenantID) {
			writeError(w, http.StatusForbidden, "forbidden", "platform administrator role is required")
			return
		}
		if err := h.providers.Routes().DeleteAccount(provider, existing.ProviderAccount, subject); err != nil {
			writeError(w, http.StatusServiceUnavailable, "provider_route_store_unavailable", "provider route could not be deleted")
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method must be GET, POST, PATCH, or DELETE")
	}
}

func (h *AdminHandler) handleChatSessionResource(w http.ResponseWriter, r *http.Request, parts []string) {
	tenant, ok := trustedTenant(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "identity_required", "development identity is required")
		return
	}
	if len(parts) == 0 {
		h.handleChatSessions(w, r, tenant)
		return
	}
	sessionID := parts[0]
	if len(parts) == 2 {
		switch parts[1] {
		case "events":
			if r.Method != http.MethodGet {
				writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method must be GET")
				return
			}
			h.writeChatEvents(w, r, tenant, sessionID)
			return
		case "messages":
			h.handleSendChatMessage(w, r, tenant, sessionID)
			return
		case "cancel":
			h.handleCancelChatRun(w, r, tenant, sessionID)
			return
		case "stream":
			h.handleChatStream(w, r, tenant, sessionID)
			return
		}
	}
	writeError(w, http.StatusNotFound, "not_found", "resource was not found")
}

func (h *AdminHandler) handleChatSessions(w http.ResponseWriter, r *http.Request, tenant TenantContext) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method must be POST")
		return
	}
	if !canOperate(tenant.Role) {
		writeError(w, http.StatusForbidden, "forbidden", "operator role is required")
		return
	}
	var request createChatSessionRequest
	if err := decodeStrict(r, &request); err != nil || !validResourceID(request.AppID) {
		writeError(w, http.StatusBadRequest, "invalid_chat_session", "app id is required")
		return
	}
	if _, exists, err := h.platform.app(r.Context(), tenant.TenantID, request.AppID); err != nil || !exists {
		if writeControlPlaneError(w, err) {
			return
		}
		writeError(w, http.StatusNotFound, "agent_app_not_found", "Agent App was not found")
		return
	}
	sessionID := strings.TrimSpace(request.SessionID)
	if sessionID == "" {
		idBytes := make([]byte, 8)
		if _, err := rand.Read(idBytes); err != nil {
			writeError(w, http.StatusInternalServerError, "session_id_unavailable", "session id could not be generated")
			return
		}
		sessionID = "chat-" + hex.EncodeToString(idBytes)
	}
	if !validResourceID(sessionID) {
		writeError(w, http.StatusBadRequest, "invalid_chat_session", "session id is invalid")
		return
	}
	store, release, err := h.acquireStore(r.Context(), tenant.TenantID)
	if err != nil {
		writeStorageError(w, err)
		return
	}
	defer release()
	sessionCtx, cancelSessionRead := boundedStorageContext(r.Context())
	_, stateErr := store.GetSessionState(sessionCtx, tenant.TenantID, sessionID)
	cancelSessionRead()
	if stateErr == nil {
		writeError(w, http.StatusConflict, "chat_session_exists", "chat session already exists")
		return
	} else if !errors.Is(stateErr, ErrNotFound) {
		writeStorageError(w, stateErr)
		return
	}
	createCtx, cancelCreate := boundedStorageContext(r.Context())
	createErr := h.appendChatEvent(createCtx, store, tenant.TenantID, sessionID, "session-created", "session.created", map[string]string{
		"app_id": request.AppID, "user_id": tenant.UserID,
	})
	cancelCreate()
	if createErr != nil {
		writeStorageError(w, createErr)
		return
	}
	if _, err := h.channels.CreateBinding(r.Context(), tenant, createChannelBindingRequest{
		Channel: ChannelMock, AppID: request.AppID, ConversationType: ConversationSingle,
		ConversationID: "chat:" + sessionID, UserID: tenant.UserID, SessionID: sessionID,
	}); err != nil {
		writeError(w, http.StatusInternalServerError, "channel_binding_unavailable", "Mock IM binding could not be created")
		return
	}
	writeJSON(w, http.StatusCreated, Session{ID: sessionID, TenantID: tenant.TenantID, AppID: request.AppID, UserID: tenant.UserID})
}

func (h *AdminHandler) handleSendChatMessage(w http.ResponseWriter, r *http.Request, tenant TenantContext, sessionID string) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method must be POST")
		return
	}
	if !canOperate(tenant.Role) {
		writeError(w, http.StatusForbidden, "forbidden", "operator role is required")
		return
	}
	var request sendChatMessageRequest
	if err := decodeStrict(r, &request); err != nil || request.Input == "" {
		writeError(w, http.StatusBadRequest, "invalid_chat_message", "input is required")
		return
	}
	requestID := strings.TrimSpace(r.Header.Get("X-Request-ID"))
	if requestID == "" {
		requestID = newRequestID()
	}
	if !validIdempotencyKey(requestID) {
		writeError(w, http.StatusBadRequest, "invalid_request_id", "request id must be 1 to 128 printable ASCII characters")
		return
	}
	traceParent := strings.ToLower(strings.TrimSpace(r.Header.Get("traceparent")))
	if traceParent != "" && !validTraceParent(traceParent) {
		writeError(w, http.StatusBadRequest, "invalid_traceparent", "traceparent is invalid")
		return
	}
	events, err := h.chatEvents(r.Context(), tenant.TenantID, sessionID)
	if err != nil {
		writeStorageError(w, err)
		return
	}
	if len(events) == 0 {
		writeError(w, http.StatusNotFound, "chat_session_not_found", "chat session was not found")
		return
	}
	appID := chatSessionAppID(events)
	if appID == "" {
		writeError(w, http.StatusBadRequest, "chat_session_invalid", "chat session is invalid")
		return
	}
	result, err := h.startChatRun(chatRunOptions{
		tenant: tenant, appID: appID, sessionID: sessionID, input: request.Input,
		requestID: requestID, userID: tenant.UserID, traceParent: traceParent,
	})
	if err != nil {
		writeChatStartError(w, err)
		return
	}
	status := http.StatusAccepted
	if result.Status != "running" {
		status = http.StatusOK
	}
	writeJSON(w, status, result)
}

func (h *AdminHandler) handleCancelChatRun(w http.ResponseWriter, r *http.Request, tenant TenantContext, sessionID string) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method must be POST")
		return
	}
	if !canOperate(tenant.Role) {
		writeError(w, http.StatusForbidden, "forbidden", "operator role is required")
		return
	}
	var request cancelChatRunRequest
	if err := decodeStrict(r, &request); err != nil || !validIdempotencyKey(request.RequestID) {
		writeError(w, http.StatusBadRequest, "invalid_chat_run", "request id is required")
		return
	}
	events, err := h.chatEvents(r.Context(), tenant.TenantID, sessionID)
	if err != nil {
		writeStorageError(w, err)
		return
	}
	if len(events) == 0 {
		writeError(w, http.StatusNotFound, "chat_session_not_found", "chat session was not found")
		return
	}
	runExists := false
	for _, event := range events {
		if event.IdempotencyKey == request.RequestID+":input" {
			runExists = true
			break
		}
	}
	if !runExists {
		writeError(w, http.StatusNotFound, "chat_run_not_found", "chat run was not found")
		return
	}
	key := chatRunKey(tenant.TenantID, sessionID, request.RequestID)
	h.chatMu.Lock()
	active, running := h.activeRuns[key]
	h.chatMu.Unlock()
	if running {
		active.cancel()
	}
	status := chatRunStatus(events, request.RequestID)
	if h.runCoordinator != nil {
		disposition, err := h.runCoordinator.RequestCancel(r.Context(), RunKey{TenantID: tenant.TenantID, SessionID: sessionID, RequestID: request.RequestID})
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, "run_coordinator_unavailable", "run coordinator is unavailable")
			return
		}
		if disposition == CancelRequested && status == "running" {
			status = "cancellation_requested"
		}
	}
	if running {
		status = "running"
	}
	writeJSON(w, http.StatusOK, chatRunResponse{SessionID: sessionID, RequestID: request.RequestID, Status: status})
}

func (h *AdminHandler) writeChatEvents(w http.ResponseWriter, r *http.Request, tenant TenantContext, sessionID string) {
	events, err := h.chatEvents(r.Context(), tenant.TenantID, sessionID)
	if err != nil {
		writeStorageError(w, err)
		return
	}
	if len(events) == 0 {
		writeError(w, http.StatusNotFound, "chat_session_not_found", "chat session was not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": events})
}

type chatSSEEnvelope struct {
	EventID   string          `json:"event_id"`
	RequestID string          `json:"request_id"`
	SessionID string          `json:"session_id"`
	Sequence  uint64          `json:"sequence"`
	Type      string          `json:"type"`
	Data      json.RawMessage `json:"data"`
}

func (h *AdminHandler) handleChatStream(w http.ResponseWriter, r *http.Request, tenant TenantContext, sessionID string) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method must be GET")
		return
	}
	store, release, err := h.acquireStore(r.Context(), tenant.TenantID)
	if err != nil {
		writeStorageError(w, err)
		return
	}
	defer release()
	initialCtx, cancelInitial := boundedStorageContext(r.Context())
	all, err := store.ListSessionEvents(initialCtx, tenant.TenantID, sessionID, 0)
	cancelInitial()
	if err != nil {
		writeStorageError(w, err)
		return
	}
	if len(all) == 0 {
		writeError(w, http.StatusNotFound, "chat_session_not_found", "chat session was not found")
		return
	}
	after := parseUintQuery(r.URL.Query().Get("after"))
	if after == 0 && r.Header.Get("Last-Event-ID") != "" {
		lastEventID := r.Header.Get("Last-Event-ID")
		for _, event := range all {
			if event.ID == lastEventID {
				after = event.Sequence
				break
			}
		}
	}
	requestID := r.URL.Query().Get("request_id")
	if requestID != "" && !validIdempotencyKey(requestID) {
		writeError(w, http.StatusBadRequest, "invalid_request_id", "request id must be 1 to 128 printable ASCII characters")
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "stream_unsupported", "streaming is unsupported")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)

	terminal := false
	emit := func(events []SessionEvent) bool {
		for _, event := range events {
			envelope := chatSSEEnvelopeFor(event)
			if requestID != "" && envelope.RequestID != requestID {
				continue
			}
			if !writeSSEEvent(w, envelope) {
				return false
			}
			flusher.Flush()
			if isChatTerminalEvent(envelope) {
				terminal = true
				return false
			}
		}
		return true
	}
	initialEvents := make([]SessionEvent, 0, len(all))
	for _, event := range all {
		if event.Sequence > after {
			initialEvents = append(initialEvents, event)
		}
	}
	if !emit(initialEvents) {
		return
	}
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for !terminal {
		select {
		case <-r.Context().Done():
			return
		case <-h.chatCtx.Done():
			return
		case <-ticker.C:
			pollCtx, cancelPoll := boundedStorageContext(r.Context())
			events, err := store.ListSessionEvents(pollCtx, tenant.TenantID, sessionID, after)
			cancelPoll()
			if err != nil {
				return
			}
			if len(events) > 0 {
				after = events[len(events)-1].Sequence
			}
			if !emit(events) {
				return
			}
		}
	}
}

func parseUintQuery(value string) uint64 {
	var result uint64
	for _, char := range value {
		if char < '0' || char > '9' {
			return 0
		}
		result = result*10 + uint64(char-'0')
	}
	return result
}

func chatSSEEnvelopeFor(event SessionEvent) chatSSEEnvelope {
	var payload struct {
		RequestID string `json:"request_id"`
	}
	_ = json.Unmarshal(event.Payload, &payload)
	return chatSSEEnvelope{
		EventID: event.ID, RequestID: payload.RequestID, SessionID: event.SessionID,
		Sequence: event.Sequence, Type: event.Type, Data: append(json.RawMessage(nil), event.Payload...),
	}
}

func writeSSEEvent(w http.ResponseWriter, envelope chatSSEEnvelope) bool {
	encoded, err := json.Marshal(envelope)
	if err != nil {
		return false
	}
	if _, err := fmt.Fprintf(w, "id: %s\nretry: 1000\ndata: %s\n\n", envelope.EventID, encoded); err != nil {
		return false
	}
	return true
}

func isChatTerminalEvent(envelope chatSSEEnvelope) bool {
	switch envelope.Type {
	case "run.completed", "run.failed", "run.cancelled":
		return true
	default:
		return false
	}
}

type chatRunOptions struct {
	tenant         TenantContext
	appID          string
	sessionID      string
	input          string
	requestID      string
	userID         string
	binding        *ChannelBinding
	traceID        string
	traceParent    string
	policyRevision uint64
	fencingToken   uint64
	runPermit      RunPermit
}

type activeChatRun struct {
	cancel context.CancelFunc
	input  string
	done   <-chan struct{}
}

func (o chatRunOptions) provider() string {
	if o.binding == nil {
		return ""
	}
	if o.binding.ReplayProvider != "" {
		return o.binding.ReplayProvider
	}
	return o.binding.Channel
}

func (h *AdminHandler) startChatRun(options chatRunOptions) (chatRunResponse, error) {
	if !canOperate(options.tenant.Role) {
		return chatRunResponse{}, errors.New("forbidden")
	}
	if options.binding == nil {
		binding, ok, err := h.channels.BindingForSession(h.chatCtx, options.tenant.TenantID, options.sessionID)
		if err != nil {
			return chatRunResponse{}, err
		}
		if ok {
			options.binding = &binding
		}
	}
	governanceResult, err := h.governance.Evaluate(h.chatCtx, h.governanceRequest(options))
	if err != nil {
		return chatRunResponse{}, err
	}
	options.input = governanceResult.Input
	options.traceID = governanceResult.TraceID
	if options.traceParent == "" {
		options.traceParent = newTraceParent(options.traceID)
	}
	options.policyRevision = governanceResult.PolicyRevision
	started := false
	var runPermit RunPermit
	defer func() {
		if !started && governanceResult.NewExecution {
			completeGovernance(h.chatCtx, h.governance, h.governanceCompletion(options, "", 0, false, "storage_error", false))
		}
		if !started && runPermit != nil {
			runPermit.Release()
		}
	}()
	if options.binding != nil {
		if err := h.governance.RecordSpan(h.governanceRequest(options), options.traceID, "channel.callback", "ok"); err != nil {
			return chatRunResponse{}, &GovernanceError{Code: "audit_unavailable", TraceID: options.traceID}
		}
	}
	storageStarted := time.Now()
	store, release, err := h.acquireStore(h.chatCtx, options.tenant.TenantID)
	h.governance.RecordStorageLatencyFor(options.tenant.TenantID, options.appID, options.provider(), time.Since(storageStarted))
	if err != nil {
		if errors.Is(err, errControlPlaneUnavailable) {
			return chatRunResponse{}, errControlPlaneUnavailable
		}
		return chatRunResponse{}, storageFailureError(err)
	}
	defer func() {
		if !started {
			release()
		}
	}()
	key := chatRunKey(options.tenant.TenantID, options.sessionID, options.requestID)
	if h.runCoordinator != nil {
		var disposition ClaimDisposition
		runPermit, disposition, err = h.runCoordinator.Claim(h.chatCtx, RunKey{TenantID: options.tenant.TenantID, SessionID: options.sessionID, RequestID: options.requestID}, options.input)
		if err != nil {
			return chatRunResponse{}, err
		}
		switch disposition {
		case ClaimAlreadyRun:
			if events, listErr := store.ListSessionEvents(h.chatCtx, options.tenant.TenantID, options.sessionID, 0); listErr == nil {
				if terminal := chatTerminalEvent(events, options.requestID); terminal != nil {
					if reconciler, ok := h.runCoordinator.(runTerminalReconciler); ok {
						_ = reconciler.ReconcileTerminal(context.Background(), RunKey{TenantID: options.tenant.TenantID, SessionID: options.sessionID, RequestID: options.requestID}, RunTerminal{Type: runTerminalState(terminal.Type)})
					}
					return chatRunResponse{SessionID: options.sessionID, RequestID: options.requestID, Status: chatRunStatus(events, options.requestID)}, nil
				}
			}
			return chatRunResponse{SessionID: options.sessionID, RequestID: options.requestID, Status: "running"}, nil
		case ClaimExisting:
			status := "completed"
			if runPermit != nil {
				if terminal, ok := runPermit.(*memoryPermit); ok {
					status = runTerminalState(terminal.run.terminal.Type)
				} else if terminal, ok := runPermit.(*postgresPermit); ok {
					status = runTerminalState(terminal.terminalType)
				}
			}
			return chatRunResponse{SessionID: options.sessionID, RequestID: options.requestID, Status: status}, nil
		}
	}
	h.chatMu.Lock()
	if active, running := h.activeRuns[key]; running {
		if active.input != options.input {
			h.chatMu.Unlock()
			if runPermit != nil {
				runPermit.Release()
			}
			return chatRunResponse{}, errors.New("idempotency_key_reused")
		}
		h.chatMu.Unlock()
		if runPermit != nil {
			runPermit.Release()
		}
		return chatRunResponse{SessionID: options.sessionID, RequestID: options.requestID, Status: "running"}, nil
	}
	storageCtx, cancelStorage := context.WithTimeout(h.chatCtx, storageOperationTimeout)
	events, err := store.ListSessionEvents(storageCtx, options.tenant.TenantID, options.sessionID, 0)
	cancelStorage()
	if err != nil {
		h.chatMu.Unlock()
		return chatRunResponse{}, storageFailureError(err)
	}
	if err := h.governance.RecordSpan(h.governanceRequest(options), options.traceID, "storage.session.read", "ok"); err != nil {
		h.chatMu.Unlock()
		return chatRunResponse{}, &GovernanceError{Code: "audit_unavailable", TraceID: options.traceID}
	}
	h.governance.RecordStorageLatencyFor(options.tenant.TenantID, options.appID, options.provider(), time.Since(storageStarted))
	inputPayload, _ := json.Marshal(map[string]string{
		"app_id": options.appID, "input": options.input, "request_id": options.requestID, "user_id": options.userID, "trace_id": options.traceID, "traceparent": options.traceParent,
	})
	recoveryOptions := options
	existingInput := false
	existingStarted := false
	for _, event := range events {
		if event.IdempotencyKey != options.requestID+":input" {
			continue
		}
		var persistedInput struct {
			AppID       string `json:"app_id"`
			Input       string `json:"input"`
			UserID      string `json:"user_id"`
			RequestID   string `json:"request_id"`
			TraceID     string `json:"trace_id"`
			TraceParent string `json:"traceparent"`
		}
		if event.Type != "message.input" || json.Unmarshal(event.Payload, &persistedInput) != nil ||
			persistedInput.Input != options.input || (persistedInput.AppID != "" && persistedInput.AppID != options.appID) ||
			(persistedInput.RequestID != "" && persistedInput.RequestID != options.requestID) {
			h.chatMu.Unlock()
			return chatRunResponse{}, errors.New("idempotency_key_reused")
		}
		if chatTerminalEvent(events, options.requestID) != nil {
			if runPermit != nil {
				_ = runPermit.Finish(context.Background(), RunTerminal{Type: runTerminalState(chatRunStatus(events, options.requestID))})
			}
			h.chatMu.Unlock()
			return chatRunResponse{SessionID: options.sessionID, RequestID: options.requestID, Status: chatRunStatus(events, options.requestID)}, nil
		}
		if persistedInput.Input == "" {
			h.chatMu.Unlock()
			return chatRunResponse{}, errors.New("storage_error")
		}
		if persistedInput.AppID != "" {
			recoveryOptions.appID = persistedInput.AppID
		}
		if persistedInput.UserID != "" {
			recoveryOptions.userID = persistedInput.UserID
		}
		if persistedInput.TraceID != "" {
			recoveryOptions.traceID = persistedInput.TraceID
		}
		if persistedInput.TraceParent != "" {
			recoveryOptions.traceParent = persistedInput.TraceParent
		}
		recoveryOptions.input = persistedInput.Input
		options = recoveryOptions
		existingInput = true
		break
	}
	for _, event := range events {
		if event.IdempotencyKey == options.requestID+":started" {
			existingStarted = true
			break
		}
	}
	if !existingInput {
		appendCtx, cancelAppend := context.WithTimeout(h.chatCtx, storageOperationTimeout)
		appendErr := store.AppendSessionEvent(appendCtx, SessionEvent{
			TenantID: options.tenant.TenantID, SessionID: options.sessionID, IdempotencyKey: options.requestID + ":input",
			Type: "message.input", Payload: inputPayload,
		})
		cancelAppend()
		if appendErr != nil {
			h.chatMu.Unlock()
			if errors.Is(appendErr, ErrDuplicateEvent) {
				return chatRunResponse{}, errors.New("idempotency_key_reused")
			}
			return chatRunResponse{}, storageFailureError(appendErr)
		}
	}
	if !existingStarted {
		startedCtx, cancelStarted := context.WithTimeout(h.chatCtx, storageOperationTimeout)
		startedErr := h.appendChatEvent(startedCtx, store, options.tenant.TenantID, options.sessionID, options.requestID+":started", "run.started", map[string]string{
			"app_id": options.appID, "request_id": options.requestID,
		})
		cancelStarted()
		if startedErr != nil {
			h.chatMu.Unlock()
			return chatRunResponse{}, storageFailureError(startedErr)
		}
	}
	if err := h.governance.RecordSpan(h.governanceRequest(options), options.traceID, "storage.session.write", "ok"); err != nil {
		h.chatMu.Unlock()
		return chatRunResponse{}, &GovernanceError{Code: "audit_unavailable", TraceID: options.traceID}
	}
	policy, policyFound, err := h.governance.Policy(h.chatCtx, options.tenant.TenantID, options.appID)
	if err != nil {
		h.chatMu.Unlock()
		return chatRunResponse{}, &GovernanceError{Code: "control_plane_unavailable"}
	}
	if !policyFound {
		h.chatMu.Unlock()
		return chatRunResponse{}, &GovernanceError{Code: "policy_unavailable"}
	}
	runCtx, cancel := runtimeTimeoutContext(h.chatCtx, policy.runtimeTimeout())
	options.runPermit = runPermit
	if runPermit != nil {
		go func() {
			select {
			case <-runPermit.CancelRequested():
				cancel()
			case <-runPermit.Lost():
				cancel()
			case <-runCtx.Done():
			}
		}()
	}
	done := make(chan struct{})
	h.activeRuns[key] = activeChatRun{cancel: cancel, input: options.input, done: done}
	h.chatWG.Add(1)
	started = true
	h.chatMu.Unlock()

	go func() {
		defer h.chatWG.Done()
		defer func() {
			h.chatMu.Lock()
			delete(h.activeRuns, key)
			h.chatMu.Unlock()
			close(done)
			cancel()
			release()
		}()
		if waiter, ok := runPermit.(interface{ Wait(context.Context) error }); ok {
			if err := waiter.Wait(runCtx); err != nil {
				cancelRequested := false
				if runPermit != nil {
					select {
					case <-runPermit.CancelRequested():
						cancelRequested = true
					default:
					}
				}
				if runPermit != nil && errors.Is(err, context.Canceled) && cancelRequested {
					_ = runPermit.Finish(context.Background(), RunTerminal{Type: "cancelled", Error: "cancelled_before_start"})
					terminalCtx, cancelTerminal := context.WithTimeout(h.failureCtx, 2*time.Second)
					_ = h.appendCriticalChatEvent(terminalCtx, store, options.tenant.TenantID, options.sessionID, options.requestID+":terminal", "run.cancelled", h.chatIdentityPayload(options, map[string]string{"error": "cancelled_before_start"}))
					cancelTerminal()
				}
				return
			}
		}
		h.runChat(runCtx, store, options)
	}()
	return chatRunResponse{SessionID: options.sessionID, RequestID: options.requestID, Status: "running"}, nil
}

func runtimeTimeoutContext(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	timer := time.AfterFunc(timeout, cancel)
	return ctx, func() {
		timer.Stop()
		cancel()
	}
}

func (h *AdminHandler) runChat(ctx context.Context, store DataStore, options chatRunOptions) {
	governanceRequest := h.governanceRequest(options)
	defer func() {
		if options.runPermit == nil {
			return
		}
		terminalType := ""
		if events, err := store.ListSessionEvents(context.Background(), options.tenant.TenantID, options.sessionID, 0); err == nil {
			if terminal := chatTerminalEvent(events, options.requestID); terminal != nil {
				terminalType = terminal.Type
			}
		}
		if terminalType == "" {
			options.runPermit.Release()
			return
		}
		_ = options.runPermit.Finish(context.Background(), RunTerminal{Type: runTerminalState(terminalType)})
	}()
	contextReadStarted := time.Now()
	agentInput, err := contextualAgentInput(ctx, store, options)
	h.governance.RecordStorageLatencyFor(options.tenant.TenantID, options.appID, options.provider(), time.Since(contextReadStarted))
	if err != nil {
		h.finishChatStorageFailure(store, options, governanceRequest, err)
		return
	}
	contextSpans := []string{"storage.session_state.read", "storage.memory.read"}
	if _, ok := store.(KnowledgeStore); ok {
		contextSpans = append(contextSpans, "storage.knowledge.read")
	}
	for _, spanName := range contextSpans {
		if err := h.governance.RecordSpan(governanceRequest, options.traceID, spanName, "ok"); err != nil {
			return
		}
	}
	events, err := h.runtime.Stream(ctx, options.tenant, GatewayRequest{
		AppID: options.appID, SessionID: options.sessionID, Input: agentInput, RequestID: options.requestID, TraceID: options.traceID, TraceParent: options.traceParent, PolicyRevision: options.policyRevision,
		Channel: options.provider(), ExternalSubject: optionsExternalSubject(options),
	})
	if err != nil {
		if strings.Contains(err.Error(), "confirmation_required") {
			h.appendPendingConfirmationEvent(store, options)
			return
		}
		if spanErr := h.governance.RecordSpan(governanceRequest, options.traceID, "worker.execute", "error"); spanErr != nil {
			return
		}
		if markErr := h.governance.MarkExecutingToolsOutcomeUnknown(h.chatCtx, governanceRequest, options.traceID); markErr != nil {
			return
		}
		cancelled := err.Error() == "request_cancelled" || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
		errorType := "runner_failed"
		if cancelled {
			errorType = "cancelled"
		}
		if _, completeErr := completeGovernance(h.chatCtx, h.governance, h.governanceCompletion(options, "", 0, false, errorType, cancelled)); completeErr != nil {
			return
		}
		h.appendCurrentConfirmationEvent(store, options)
		eventType := "run.failed"
		code := "runner_failed"
		if cancelled {
			eventType = "run.cancelled"
			code = "cancelled"
		}
		if options.binding != nil && options.binding.PlatformOwned && h.providers != nil {
			h.providers.recordTerminalIfPending(providerDeliveryBinding(*options.binding), ChannelReply{MessageID: options.requestID}, code)
		}
		terminalCtx, cancel := context.WithTimeout(h.failureCtx, 2*time.Second)
		_ = h.appendCriticalChatEvent(terminalCtx, store, options.tenant.TenantID, options.sessionID, options.requestID+":terminal", eventType, h.chatIdentityPayload(options, map[string]string{"error": err.Error()}))
		cancel()
		return
	}
	for _, spanName := range []string{"worker.execute", "agent_factory.resolve", "runner.run"} {
		if err := h.governance.RecordSpan(governanceRequest, options.traceID, spanName, "ok"); err != nil {
			return
		}
	}
	var output string
	bufferOutput := h.governance.RequiresBufferedOutput(options.tenant.TenantID, options.appID)
	outputBlocked := false
	sawCompleted := false
	var usageTokens int64
	var usageKnown bool
	deltaNumber := 0
	for runtimeEvent := range events {
		if token, parseErr := strconv.ParseUint(runtimeEvent.Data["fencing_token"], 10, 64); parseErr == nil && token > 0 {
			options.fencingToken = token
		}
		if runtimeEvent.Type == "session.lease.acquired" {
			payload := h.chatIdentityPayload(options, runtimeEvent.Data)
			leaseKey := options.requestID + ":lease-" + strconv.FormatUint(options.fencingToken, 10)
			if err := h.appendChatEvent(ctx, store, options.tenant.TenantID, options.sessionID, leaseKey, runtimeEvent.Type, payload); err != nil {
				return
			}
			if err := h.cancelSupersededChatRuns(ctx, store, options); err != nil {
				return
			}
			continue
		}
		if tokens, known := runtimeUsage(runtimeEvent.Data); known {
			usageTokens, usageKnown = tokens, true
		}
		if runtimeEvent.Type == "message.delta" {
			delta := runtimeEvent.Data["delta"]
			output += delta
			if bufferOutput {
				continue
			}
			delta, _ = h.governance.FilterOutput(options.tenant.TenantID, options.appID, delta)
			key := options.requestID + ":delta"
			if deltaNumber > 0 {
				key += fmt.Sprintf("-%d", deltaNumber)
			}
			deltaNumber++
			payload := h.chatIdentityPayload(options, runtimeEvent.Data)
			payload["delta"] = delta
			delete(payload, "output")
			_ = h.appendChatEvent(ctx, store, options.tenant.TenantID, options.sessionID, key, runtimeEvent.Type, payload)
		}
		if runtimeEvent.Type == "message.completed" {
			sawCompleted = true
			if value := runtimeEvent.Data["output"]; value != "" {
				output = value
			}
			output, outputBlocked = h.governance.FilterOutput(options.tenant.TenantID, options.appID, output)
		}
		if runtimeEvent.Type == "run.failed" {
			if strings.Contains(runtimeEvent.Data["error"], "confirmation_required") {
				_ = h.governance.RecordSpan(governanceRequest, options.traceID, "runner.run", "error")
				if err := h.governance.MarkExecutingToolsOutcomeUnknown(h.chatCtx, governanceRequest, options.traceID); err != nil {
					return
				}
				h.appendPendingConfirmationEvent(store, options)
				return
			}
			_ = h.governance.RecordSpan(governanceRequest, options.traceID, "runner.run", "error")
			if err := h.governance.MarkExecutingToolsOutcomeUnknown(h.chatCtx, governanceRequest, options.traceID); err != nil {
				return
			}
			if _, err := completeGovernance(h.chatCtx, h.governance, h.governanceCompletion(options, "", usageTokens, usageKnown, "runner_failed", false)); err != nil {
				return
			}
			h.appendCurrentConfirmationEvent(store, options)
			if options.binding != nil && options.binding.PlatformOwned && h.providers != nil {
				h.providers.recordTerminalIfPending(providerDeliveryBinding(*options.binding), ChannelReply{MessageID: options.requestID}, "runner_failed")
			}
			terminalCtx, cancel := context.WithTimeout(h.failureCtx, 2*time.Second)
			payload := h.chatIdentityPayload(options, runtimeEvent.Data)
			payload["error"] = "run failed"
			_ = h.appendCriticalChatEvent(terminalCtx, store, options.tenant.TenantID, options.sessionID, options.requestID+":terminal", "run.failed", payload)
			cancel()
			return
		}
		if runtimeEvent.Type == "run.cancelled" {
			_ = h.governance.RecordSpan(governanceRequest, options.traceID, "runner.run", "cancelled")
			if err := h.governance.MarkExecutingToolsOutcomeUnknown(h.chatCtx, governanceRequest, options.traceID); err != nil {
				return
			}
			if _, err := completeGovernance(h.chatCtx, h.governance, h.governanceCompletion(options, "", usageTokens, usageKnown, "cancelled", true)); err != nil {
				return
			}
			h.appendCurrentConfirmationEvent(store, options)
			if options.binding != nil && options.binding.PlatformOwned && h.providers != nil {
				h.providers.recordTerminalIfPending(providerDeliveryBinding(*options.binding), ChannelReply{MessageID: options.requestID}, "cancelled")
			}
			terminalCtx, cancel := context.WithTimeout(h.failureCtx, 2*time.Second)
			payload := h.chatIdentityPayload(options, runtimeEvent.Data)
			payload["error"] = "run cancelled"
			_ = h.appendCriticalChatEvent(terminalCtx, store, options.tenant.TenantID, options.sessionID, options.requestID+":terminal", "run.cancelled", payload)
			cancel()
			return
		}
	}
	if ctx.Err() != nil {
		_ = h.governance.RecordSpan(governanceRequest, options.traceID, "runner.run", "cancelled")
		if err := h.governance.MarkExecutingToolsOutcomeUnknown(h.chatCtx, governanceRequest, options.traceID); err != nil {
			return
		}
		if _, err := completeGovernance(h.chatCtx, h.governance, h.governanceCompletion(options, "", usageTokens, usageKnown, "cancelled", true)); err != nil {
			return
		}
		h.appendCurrentConfirmationEvent(store, options)
		if options.binding != nil && options.binding.PlatformOwned && h.providers != nil {
			h.providers.recordTerminalIfPending(providerDeliveryBinding(*options.binding), ChannelReply{MessageID: options.requestID}, "cancelled")
		}
		terminalCtx, cancel := context.WithTimeout(h.failureCtx, 2*time.Second)
		_ = h.appendCriticalChatEvent(terminalCtx, store, options.tenant.TenantID, options.sessionID, options.requestID+":terminal", "run.cancelled", h.chatIdentityPayload(options, map[string]string{"error": "run cancelled"}))
		cancel()
		return
	}
	errorType := ""
	if outputBlocked {
		errorType = "output_guardrail"
	}
	if sawCompleted {
		if bufferOutput {
			payload := h.chatIdentityPayload(options, map[string]string{"delta": output})
			_ = h.appendChatEvent(ctx, store, options.tenant.TenantID, options.sessionID, options.requestID+":delta", "message.delta", payload)
		}
		payload := h.chatIdentityPayload(options, map[string]string{"output": output})
		if err := h.appendChatEvent(ctx, store, options.tenant.TenantID, options.sessionID, options.requestID+":completed", "message.completed", payload); err != nil {
			h.finishChatStorageFailure(store, options, governanceRequest, err)
			return
		}
		if err := store.PutMemory(ctx, MemoryRecord{
			TenantID: options.tenant.TenantID, SessionID: options.sessionID,
			Key: "latest_agent_reply", Value: output, FencingToken: options.fencingToken,
		}); err != nil {
			_, _ = completeGovernance(h.chatCtx, h.governance, h.governanceCompletion(options, "", usageTokens, usageKnown, "storage_error", false))
			terminalCtx, cancel := context.WithTimeout(h.failureCtx, 2*time.Second)
			_ = h.appendCriticalChatEvent(terminalCtx, store, options.tenant.TenantID, options.sessionID, options.requestID+":memory-failed", "memory.write.failed", h.chatIdentityPayload(options, map[string]string{"error": "memory write failed"}))
			_ = h.appendCriticalChatEvent(terminalCtx, store, options.tenant.TenantID, options.sessionID, options.requestID+":terminal", "run.failed", h.chatIdentityPayload(options, map[string]string{"error": "memory write failed"}))
			cancel()
			return
		}
		if err := h.governance.RecordSpan(governanceRequest, options.traceID, "storage.memory.write", "ok"); err != nil {
			return
		}
	}
	output, err = completeGovernance(h.chatCtx, h.governance, h.governanceCompletion(options, output, usageTokens, usageKnown, errorType, false))
	if err != nil {
		terminalCtx, cancel := context.WithTimeout(h.failureCtx, 2*time.Second)
		_ = h.appendCriticalChatEvent(terminalCtx, store, options.tenant.TenantID, options.sessionID, options.requestID+":terminal", "run.failed", h.chatIdentityPayload(options, map[string]string{"error": "audit unavailable"}))
		cancel()
		return
	}
	if artifacts, ok := store.(ArtifactStore); ok && sawCompleted {
		_, artifactErr := artifacts.PutArtifact(ctx, Artifact{
			TenantID: options.tenant.TenantID, SessionID: options.sessionID,
			Name:       "response-" + options.requestID + ".txt",
			ContentRef: "session-event://" + options.tenant.TenantID + "/" + options.sessionID + "/" + options.requestID + ":completed",
			RequestID:  options.requestID, TraceID: options.traceID, Status: "published",
			FencingToken: options.fencingToken,
		})
		if artifactErr != nil {
			terminalCtx, cancel := context.WithTimeout(h.failureCtx, 2*time.Second)
			_ = h.appendCriticalChatEvent(terminalCtx, store, options.tenant.TenantID, options.sessionID, options.requestID+":artifact-failed", "artifact.publication.failed", h.chatIdentityPayload(options, map[string]string{"error": "artifact publication failed"}))
			_ = h.appendCriticalChatEvent(terminalCtx, store, options.tenant.TenantID, options.sessionID, options.requestID+":terminal", "run.failed", h.chatIdentityPayload(options, map[string]string{"error": "artifact publication failed"}))
			cancel()
			return
		}
		if err := h.governance.RecordSpan(governanceRequest, options.traceID, "storage.artifact.publish", "ok"); err != nil {
			return
		}
	}
	if sawCompleted {
		if err := h.updateSessionSummary(ctx, store, options); err != nil {
			terminalCtx, cancel := context.WithTimeout(h.failureCtx, 2*time.Second)
			_ = h.appendCriticalChatEvent(terminalCtx, store, options.tenant.TenantID, options.sessionID, options.requestID+":terminal", "run.failed", h.chatIdentityPayload(options, map[string]string{"error": "summary write failed"}))
			cancel()
			return
		}
	}
	if options.binding != nil {
		delivery, err := h.channels.Send(ctx, *options.binding, ChannelReply{MessageID: options.requestID, Text: output})
		if err == nil {
			h.governance.RecordDeliveryFor(options.tenant.TenantID, options.appID, options.provider(), true)
			if err := h.governance.RecordSpan(governanceRequest, options.traceID, "channel.reply", "ok"); err != nil {
				return
			}
			if options.binding.PlatformOwned && options.binding.ReplayProvider != "" && h.providers != nil {
				h.providers.recordDelivery(providerDeliveryBinding(*options.binding), ChannelReply{MessageID: options.requestID}, "delivered", "", delivery.Attempts)
			}
			_ = h.appendChatEvent(ctx, store, options.tenant.TenantID, options.sessionID, options.requestID+":reply", "channel.reply", h.chatIdentityPayload(options, map[string]string{
				"message_id": delivery.MessageID, "text": output, "status": delivery.Status,
			}))
		} else {
			h.governance.RecordDeliveryFor(options.tenant.TenantID, options.appID, options.provider(), false)
			if spanErr := h.governance.RecordSpan(governanceRequest, options.traceID, "channel.reply", "error"); spanErr != nil {
				return
			}
			if options.binding.PlatformOwned && h.providers != nil {
				h.providers.recordTerminalIfPending(providerDeliveryBinding(*options.binding), ChannelReply{MessageID: options.requestID}, channelErrorCode(err))
			}
			deliveryCtx, cancel := context.WithTimeout(h.failureCtx, 2*time.Second)
			_ = h.appendChatEvent(deliveryCtx, store, options.tenant.TenantID, options.sessionID, options.requestID+":delivery", "channel.delivery", h.chatIdentityPayload(options, map[string]string{
				"message_id": options.requestID, "status": "failed", "code": channelErrorCode(err), "attempts": "bounded",
			}))
			cancel()
		}
	}
	if ctx.Err() != nil {
		terminalCtx, cancel := context.WithTimeout(h.failureCtx, 2*time.Second)
		_ = h.appendCriticalChatEvent(terminalCtx, store, options.tenant.TenantID, options.sessionID, options.requestID+":terminal", "run.cancelled", h.chatIdentityPayload(options, map[string]string{"error": "run cancelled"}))
		cancel()
		return
	}
	h.appendCurrentConfirmationEvent(store, options)
	terminalCtx, cancelTerminal := context.WithTimeout(h.failureCtx, 2*time.Second)
	_ = h.appendCriticalChatEvent(terminalCtx, store, options.tenant.TenantID, options.sessionID, options.requestID+":run-completed", "run.completed", h.chatIdentityPayload(options, nil))
	cancelTerminal()
}

func (h *AdminHandler) cancelSupersededChatRuns(ctx context.Context, store DataStore, options chatRunOptions) error {
	events, err := store.ListSessionEvents(ctx, options.tenant.TenantID, options.sessionID, 0)
	if err != nil {
		return err
	}
	started := make(map[string]struct{})
	terminal := make(map[string]struct{})
	identities := make(map[string]map[string]string)
	for _, event := range events {
		switch {
		case event.Type == "message.input" && strings.HasSuffix(event.IdempotencyKey, ":input"):
			requestID := strings.TrimSuffix(event.IdempotencyKey, ":input")
			var identity map[string]string
			if json.Unmarshal(event.Payload, &identity) == nil {
				identities[requestID] = identity
			}
		case event.Type == "run.started" && strings.HasSuffix(event.IdempotencyKey, ":started"):
			started[strings.TrimSuffix(event.IdempotencyKey, ":started")] = struct{}{}
		case (event.Type == "run.completed" || event.Type == "run.failed" || event.Type == "run.cancelled") && strings.HasSuffix(event.IdempotencyKey, ":terminal"):
			terminal[strings.TrimSuffix(event.IdempotencyKey, ":terminal")] = struct{}{}
		case event.Type == "run.completed" && strings.HasSuffix(event.IdempotencyKey, ":run-completed"):
			terminal[strings.TrimSuffix(event.IdempotencyKey, ":run-completed")] = struct{}{}
		}
	}
	for requestID := range started {
		if requestID == options.requestID {
			continue
		}
		if _, complete := terminal[requestID]; complete {
			continue
		}
		payload := make(map[string]string, len(identities[requestID])+4)
		for key, value := range identities[requestID] {
			payload[key] = value
		}
		payload["tenant_id"] = options.tenant.TenantID
		payload["session_id"] = options.sessionID
		payload["request_id"] = requestID
		payload["error"] = "lease_superseded"
		payload["fencing_token"] = strconv.FormatUint(options.fencingToken, 10)
		if err := h.appendChatEvent(ctx, store, options.tenant.TenantID, options.sessionID, requestID+":terminal", "run.cancelled", payload); err != nil && !errors.Is(err, ErrDuplicateEvent) {
			return err
		}
	}
	return nil
}

func contextualAgentInput(ctx context.Context, store DataStore, options chatRunOptions) (string, error) {
	sections := make([]string, 0, 3)
	state, stateErr := store.GetSessionState(ctx, options.tenant.TenantID, options.sessionID)
	if stateErr != nil && !errors.Is(stateErr, ErrNotFound) {
		return "", &contextReadError{span: "storage.session_state.read", err: stateErr}
	}
	if state.Summary != "" {
		sections = append(sections, "Session summary:\n"+state.Summary)
	}
	memory, err := store.ListMemory(ctx, options.tenant.TenantID, options.sessionID)
	if contextual, ok := store.(interface {
		ContextMemory(context.Context, string, string) ([]MemoryRecord, error)
	}); ok {
		memory, err = contextual.ContextMemory(ctx, options.tenant.TenantID, options.sessionID)
	}
	if err != nil {
		return "", &contextReadError{span: "storage.memory.read", err: err}
	}
	if len(memory) > 0 {
		var values []string
		for _, item := range memory {
			values = append(values, item.Key+"="+item.Value)
		}
		sections = append(sections, "Memory:\n"+strings.Join(values, "\n"))
	}
	if knowledge, ok := store.(KnowledgeStore); ok {
		records, err := knowledge.ListKnowledge(ctx, options.tenant.TenantID, options.appID)
		if searchable, ok := store.(interface {
			SearchKnowledge(context.Context, string, string, string) ([]KnowledgeRecord, error)
		}); ok {
			records, err = searchable.SearchKnowledge(ctx, options.tenant.TenantID, options.appID, options.input)
		}
		if err != nil {
			return "", &contextReadError{span: "storage.knowledge.read", err: err}
		}
		if len(records) > 0 {
			var values []string
			for _, item := range records {
				values = append(values, item.Content)
			}
			sections = append(sections, "Knowledge:\n"+strings.Join(values, "\n"))
		}
	}
	if len(sections) == 0 {
		return options.input, nil
	}
	return strings.Join(sections, "\n\n") + "\n\nUser:\n" + options.input, nil
}

type contextReadError struct {
	span string
	err  error
}

func (e *contextReadError) Error() string { return e.err.Error() }
func (e *contextReadError) Unwrap() error { return e.err }

func (h *AdminHandler) finishChatStorageFailure(store DataStore, options chatRunOptions, request GovernanceRequest, cause error) {
	spanName := "storage.context.read"
	var readErr *contextReadError
	if errors.As(cause, &readErr) {
		spanName = readErr.span
	}
	_ = h.governance.RecordSpan(request, options.traceID, spanName, "error")
	_, _ = completeGovernance(h.chatCtx, h.governance, h.governanceCompletion(options, "", 0, false, "storage_error", false))
	terminalCtx, cancel := context.WithTimeout(h.failureCtx, 2*time.Second)
	defer cancel()
	_ = h.appendCriticalChatEvent(terminalCtx, store, options.tenant.TenantID, options.sessionID, options.requestID+":terminal", "run.failed", h.chatIdentityPayload(options, map[string]string{"error": storageFailureError(cause).Error()}))
}

func newTraceParent(traceID string) string {
	traceID = strings.ToLower(strings.TrimSpace(traceID))
	if len(traceID) != 32 {
		buf := make([]byte, 16)
		_, _ = rand.Read(buf)
		traceID = hex.EncodeToString(buf)
	}
	span := make([]byte, 8)
	_, _ = rand.Read(span)
	return "00-" + traceID + "-" + hex.EncodeToString(span) + "-01"
}

func (h *AdminHandler) appendPendingConfirmationEvent(store DataStore, options chatRunOptions) {
	confirmation, found := h.governance.ConfirmationForRequest(options.tenant.TenantID, options.requestID)
	if !found {
		return
	}
	payload := h.chatIdentityPayload(options, map[string]string{
		"confirmation_id": confirmation.ID, "tool_name": confirmation.ToolName,
		"argument_summary": confirmation.ArgumentSummary, "status": string(confirmation.Status),
	})
	pendingCtx, cancel := context.WithTimeout(h.failureCtx, 2*time.Second)
	_ = h.appendCriticalChatEvent(pendingCtx, store, options.tenant.TenantID, options.sessionID, options.requestID+":confirmation-pending", "tool.confirmation.pending", payload)
	cancel()
}

func (h *AdminHandler) appendCurrentConfirmationEvent(store DataStore, options chatRunOptions) {
	confirmation, found := h.governance.ConfirmationForRequest(options.tenant.TenantID, options.requestID)
	if !found || confirmation.Status == ConfirmationPending {
		return
	}
	payload := h.chatIdentityPayload(options, map[string]string{
		"confirmation_id": confirmation.ID, "tool_name": confirmation.ToolName,
		"argument_summary": confirmation.ArgumentSummary, "status": string(confirmation.Status),
	})
	eventCtx, cancel := context.WithTimeout(h.failureCtx, 2*time.Second)
	_ = h.appendCriticalChatEvent(eventCtx, store, options.tenant.TenantID, options.sessionID, options.requestID+":confirmation-"+string(confirmation.Status), "tool.confirmation."+string(confirmation.Status), payload)
	cancel()
}

func providerDeliveryBinding(binding ChannelBinding) ChannelBinding {
	if binding.ReplayProvider != "" {
		binding.Channel = binding.ReplayProvider
	}
	return binding
}

func (h *AdminHandler) chatIdentityPayload(options chatRunOptions, values map[string]string) map[string]string {
	payload := map[string]string{
		"tenant_id": options.tenant.TenantID, "app_id": options.appID, "session_id": options.sessionID,
		"user_id": options.userID, "request_id": options.requestID, "trace_id": options.traceID,
		"policy_revision": strconv.FormatUint(options.policyRevision, 10),
	}
	if options.fencingToken > 0 {
		payload["fencing_token"] = strconv.FormatUint(options.fencingToken, 10)
	}
	if deployment, ok, _ := h.platform.activeDeployment(h.chatCtx, options.tenant.TenantID, options.appID); ok {
		payload["deployment_id"] = deployment.ID
		payload["version_id"] = deployment.VersionID
	}
	for key, value := range values {
		payload[key] = value
	}
	return payload
}

func (h *AdminHandler) governanceRequest(options chatRunOptions) GovernanceRequest {
	request := GovernanceRequest{
		TenantID: options.tenant.TenantID, AgentAppID: options.appID, UserID: options.userID,
		SessionID: options.sessionID, RequestID: options.requestID, Input: options.input, PolicyRevision: options.policyRevision,
	}
	if options.binding != nil {
		request.Channel = options.binding.Channel
		if options.binding.ReplayProvider != "" {
			request.Channel = options.binding.ReplayProvider
		}
		request.ProviderAccount = options.binding.ProviderAccount
		request.ConversationType = options.binding.ConversationType
		request.ExternalSubject = options.binding.ConversationID
	}
	if deployment, ok, _ := h.platform.activeDeployment(h.chatCtx, options.tenant.TenantID, options.appID); ok {
		if version, found, _ := h.platform.DeploymentVersion(h.chatCtx, DeploymentVersionRef{TenantID: options.tenant.TenantID, VersionID: deployment.VersionID}); found {
			request.RequiredTools = configStrings(version.Config, "tools")
			request.RequiredMCP = configStrings(version.Config, "mcp")
		}
	}
	return request
}

func (h *AdminHandler) governanceCompletion(options chatRunOptions, output string, tokens int64, usageKnown bool, errorType string, cancelled bool) GovernanceCompletion {
	completion := GovernanceCompletion{
		TenantID: options.tenant.TenantID, AgentAppID: options.appID, RequestID: options.requestID,
		UserID: options.userID, SessionID: options.sessionID, Output: output, Tokens: tokens,
		NoUsage: !usageKnown, ErrorType: errorType, Cancelled: cancelled,
	}
	request := h.governanceRequest(options)
	completion.Channel, completion.ExternalSubject = request.Channel, request.ExternalSubject
	return completion
}

func optionsExternalSubject(options chatRunOptions) string {
	if options.binding == nil {
		return ""
	}
	return options.binding.ConversationID
}

func runtimeUsage(data map[string]string) (int64, bool) {
	if data == nil || data["usage_known"] != "true" {
		return 0, false
	}
	tokens, err := strconv.ParseInt(data["usage_tokens"], 10, 64)
	if err != nil || tokens < 0 {
		return 0, false
	}
	return tokens, true
}

func configStrings(config map[string]any, key string) []string {
	raw, ok := config[key]
	if !ok {
		return nil
	}
	values := []string{}
	switch typed := raw.(type) {
	case []string:
		values = append(values, typed...)
	case []any:
		for _, item := range typed {
			if value, ok := item.(string); ok {
				values = append(values, value)
			}
		}
	}
	return normalizedValues(values)
}

func (h *AdminHandler) appendChatEvent(ctx context.Context, store DataStore, tenantID, sessionID, idempotencyKey, eventType string, payload any) error {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	var fencingToken uint64
	if fields, ok := payload.(map[string]string); ok {
		fencingToken, _ = strconv.ParseUint(fields["fencing_token"], 10, 64)
	}
	return store.AppendSessionEvent(ctx, SessionEvent{
		TenantID: tenantID, SessionID: sessionID, IdempotencyKey: idempotencyKey, Type: eventType, Payload: encoded,
		FencingToken: fencingToken,
	})
}

func (h *AdminHandler) appendCriticalChatEvent(ctx context.Context, store DataStore, tenantID, sessionID, idempotencyKey, eventType string, payload any) error {
	for {
		err := h.appendChatEvent(ctx, store, tenantID, sessionID, idempotencyKey, eventType, payload)
		if err == nil {
			return nil
		}
		if errors.Is(err, ErrDuplicateEvent) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (h *AdminHandler) chatEvents(ctx context.Context, tenantID, sessionID string) ([]SessionEvent, error) {
	store, release, err := h.acquireStore(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	defer release()
	storageCtx, cancel := context.WithTimeout(ctx, storageOperationTimeout)
	defer cancel()
	return store.ListSessionEvents(storageCtx, tenantID, sessionID, 0)
}

func boundedStorageContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, storageOperationTimeout)
}

func writeStorageError(w http.ResponseWriter, err error) {
	if errors.Is(err, errControlPlaneUnavailable) {
		writeError(w, http.StatusServiceUnavailable, "control_plane_unavailable", "Control Plane Store is unavailable")
		return
	}
	code := storageFailureError(err).Error()
	switch code {
	case "tenant_storage_migrating":
		writeError(w, http.StatusServiceUnavailable, code, "tenant storage is temporarily read-only during migration")
	case "storage_timeout":
		writeError(w, http.StatusGatewayTimeout, code, "storage operation timed out")
	case "request_cancelled":
		writeError(w, http.StatusRequestTimeout, code, "request was cancelled")
	default:
		writeError(w, http.StatusServiceUnavailable, code, "storage is unavailable")
	}
}

func chatRunKey(tenantID, sessionID, requestID string) string {
	return tenantID + "\x00" + sessionID + "\x00" + requestID
}

func chatTerminalEvent(events []SessionEvent, requestID string) *SessionEvent {
	for index := range events {
		if events[index].IdempotencyKey == requestID+":terminal" || events[index].IdempotencyKey == requestID+":run-completed" {
			return &events[index]
		}
	}
	return nil
}

func chatRunStatus(events []SessionEvent, requestID string) string {
	terminal := chatTerminalEvent(events, requestID)
	if terminal == nil {
		for _, event := range events {
			if event.IdempotencyKey == requestID+":confirmation-pending" {
				return "pending"
			}
		}
		return "running"
	}
	switch terminal.Type {
	case "run.cancelled":
		return "cancelled"
	case "run.failed":
		return "failed"
	default:
		return "completed"
	}
}

func chatSessionAppID(events []SessionEvent) string {
	for _, event := range events {
		if event.Type != "session.created" {
			continue
		}
		var payload struct {
			AppID string `json:"app_id"`
		}
		if json.Unmarshal(event.Payload, &payload) == nil && payload.AppID != "" {
			return payload.AppID
		}
	}
	return ""
}

func newRequestID() string {
	idBytes := make([]byte, 12)
	_, _ = rand.Read(idBytes)
	return "request-" + hex.EncodeToString(idBytes)
}

func storageFailureError(err error) error {
	if errors.Is(err, ErrTenantMigrating) {
		return errors.New("tenant_storage_migrating")
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return errors.New("storage_timeout")
	}
	if errors.Is(err, context.Canceled) {
		return errors.New("request_cancelled")
	}
	if errors.Is(err, errBackendRegistryClosing) {
		return errors.New("service_closing")
	}
	return errors.New("storage_unavailable")
}

func writeChatStartError(w http.ResponseWriter, err error) {
	var governanceErr *GovernanceError
	if errors.As(err, &governanceErr) {
		switch governanceErr.Code {
		case "tenant_rate_limited", "budget_exceeded":
			w.Header().Set("Retry-After", "60")
			writeError(w, http.StatusTooManyRequests, governanceErr.Code, "tenant governance limit exceeded")
		case "confirmation_required":
			writeJSON(w, http.StatusConflict, map[string]any{
				"error":           errorBody{Code: governanceErr.Code, Message: "dangerous Tool confirmation is required"},
				"confirmation_id": governanceErr.ConfirmationID, "trace_id": governanceErr.TraceID,
			})
		case "audit_unavailable":
			writeError(w, http.StatusServiceUnavailable, governanceErr.Code, "audit service is unavailable")
		default:
			writeError(w, http.StatusForbidden, governanceErr.Code, "governance policy denied the request")
		}
		return
	}
	switch err.Error() {
	case "forbidden":
		writeError(w, http.StatusForbidden, "forbidden", "operator role is required")
	case "idempotency_key_reused":
		writeError(w, http.StatusConflict, "idempotency_key_reused", "request id was already used with different input")
	case "storage_timeout":
		writeError(w, http.StatusGatewayTimeout, "storage_timeout", "storage operation timed out")
	case "request_cancelled":
		writeError(w, http.StatusRequestTimeout, "request_cancelled", "request was cancelled")
	case "service_closing":
		writeError(w, http.StatusServiceUnavailable, "service_closing", "service is closing")
	case "tenant_storage_migrating":
		writeError(w, http.StatusServiceUnavailable, "tenant_storage_migrating", "tenant storage is temporarily read-only during migration")
	case "storage_unavailable":
		writeError(w, http.StatusServiceUnavailable, "storage_unavailable", "storage is unavailable")
	case "control_plane_unavailable":
		writeError(w, http.StatusServiceUnavailable, "control_plane_unavailable", "Control Plane Store is unavailable")
	default:
		writeError(w, http.StatusServiceUnavailable, "storage_error", "session event could not be persisted")
	}
}

func writeChannelError(w http.ResponseWriter, err error) {
	code := channelErrorCode(err)
	status := http.StatusBadGateway
	switch code {
	case "channel_signature_invalid":
		status = http.StatusUnauthorized
	case "channel_callback_invalid", "channel_message_too_long", "channel_attachment_rejected":
		status = http.StatusBadRequest
	case "channel_rate_limited":
		status = http.StatusTooManyRequests
	case "channel_message_out_of_order":
		status = http.StatusConflict
	case "channel_timeout":
		status = http.StatusGatewayTimeout
	case "channel_disabled":
		status = http.StatusConflict
	}
	writeError(w, status, code, "channel delivery failed")
}
