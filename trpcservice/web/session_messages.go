package web

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	agentsession "trpc.group/trpc-go/trpc-agent-go/session"
)

var errSessionTranscriptUnavailable = errors.New("session transcript store is not configured")

const (
	defaultSessionMessageLimit = 50
	maxSessionMessageLimit     = 200
	minSessionEventScanBatch   = 64
	maxSessionEventScanBatch   = 512
)

type chatTranscriptMessage struct {
	ID          string                     `json:"id"`
	Role        string                     `json:"role"`
	Content     string                     `json:"content"`
	Time        string                     `json:"time"`
	Attachments []chatTranscriptAttachment `json:"attachments,omitempty"`
	Card        *channels.InteractiveCard  `json:"card,omitempty"`
	Source      *chatMessageSource         `json:"source,omitempty"`
	RequestID   string                     `json:"-"`
	EventTime   time.Time                  `json:"-"`
}

type chatTranscriptAttachment struct {
	Name string `json:"name"`
}

type chatMessageSource struct {
	Channel             string `json:"channel"`
	BindingID           string `json:"binding_id"`
	ConversationID      string `json:"conversation_id"`
	Scope               string `json:"scope"`
	ActorExternalUserID string `json:"actor_external_user_id"`
	ActorPlatformUserID string `json:"actor_platform_user_id,omitempty"`
	TriggerType         string `json:"trigger_type"`
}

func (c *consoleAPI) getSessionMessages(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		methodNotAllowed(writer, http.MethodGet)
		return
	}
	tenantID := resolveTenantParam(request)
	sessionKey := strings.TrimSpace(request.URL.Query().Get("session_key"))
	if tenantID == "" || sessionKey == "" {
		badRequest(writer, "tenant and session_key are required")
		return
	}
	if !requireTenantRead(writer, request, tenantID) {
		return
	}
	if c.dependencies.State == nil || c.dependencies.AgentSessions == nil {
		serverError(writer, "get session messages", errSessionTranscriptUnavailable)
		return
	}
	entry, err := c.dependencies.State.GetSession(request.Context(), tenantID, sessionKey)
	if err != nil {
		if errors.Is(err, storage.ErrSessionNotFound) {
			notFound(writer, "session does not exist")
			return
		}
		serverError(writer, "get platform session", err)
		return
	}
	user, _ := sessionUser(request)
	contentAudit := !canReadSession(user, entry)
	if contentAudit && !canAuditConversationContent(user, tenantID) {
		writeJSON(writer, http.StatusForbidden, map[string]any{"error": "forbidden: session transcript is not visible"})
		return
	}
	limit := defaultSessionMessageLimit
	if raw := strings.TrimSpace(request.URL.Query().Get("limit")); raw != "" {
		parsed, parseErr := strconv.Atoi(raw)
		if parseErr != nil || parsed < 1 || parsed > maxSessionMessageLimit {
			badRequest(writer, "limit must be between 1 and 200")
			return
		}
		limit = parsed
	}
	before := strings.TrimSpace(request.URL.Query().Get("before"))
	if len(before) > 512 {
		badRequest(writer, "before is not a valid message cursor")
		return
	}
	// A revision-0 platform Session is only a route shell created before the
	// first successful Worker execution. Authentication, authorization, and
	// request validation still apply, but there is no framework transcript to
	// resolve yet.
	if entry.Revision == 0 {
		writeJSON(writer, http.StatusOK, map[string]any{"messages": []chatTranscriptMessage{}, "next_cursor": ""})
		return
	}
	service, err := c.agentSessionService(request.Context(), tenantID, entry.AppCode)
	if err != nil {
		serverError(writer, "resolve agent Session backend", err)
		return
	}
	key := agentsession.Key{
		AppName: tenantID + "/" + entry.AppCode, UserID: entry.SubjectID, SessionID: entry.SessionKey,
	}
	messages, nextCursor, err := loadChatMessagePage(request.Context(), service, key, before, limit)
	if err != nil {
		if errors.Is(err, agentsession.ErrEventWindowAnchorNotFound) {
			badRequest(writer, "message cursor is no longer available; refresh the transcript")
			return
		}
		serverError(writer, "get agent session messages", err)
		return
	}
	messages = c.attachChatMessageSources(request.Context(), tenantID, sessionKey, messages)
	messages, err = c.attachChatMessageCards(request.Context(), tenantID, messages)
	if err != nil {
		serverError(writer, "attach chat message cards", err)
		return
	}
	if contentAudit {
		if err := c.recordConversationContentRead(request, user, entry); err != nil {
			serverError(writer, "record conversation content audit", err)
			return
		}
	}
	writeJSON(writer, http.StatusOK, map[string]any{"messages": messages, "next_cursor": nextCursor})
}

func (c *consoleAPI) attachChatMessageCards(ctx context.Context, tenantID string, messages []chatTranscriptMessage) ([]chatTranscriptMessage, error) {
	var finder storage.OutboxRequestBatchFinder
	if candidate, ok := c.dependencies.Replies.(storage.OutboxRequestBatchFinder); ok {
		finder = candidate
	} else if candidate, ok := c.dependencies.State.(storage.OutboxRequestBatchFinder); ok {
		finder = candidate
	}
	if finder == nil {
		return messages, nil
	}
	requestIDs := make([]string, 0, len(messages))
	seen := make(map[string]struct{}, len(messages))
	for _, message := range messages {
		if message.Role != string(model.RoleAssistant) || strings.TrimSpace(message.RequestID) == "" {
			continue
		}
		if _, ok := seen[message.RequestID]; ok {
			continue
		}
		seen[message.RequestID] = struct{}{}
		requestIDs = append(requestIDs, message.RequestID)
	}
	if len(requestIDs) == 0 {
		return messages, nil
	}
	events, err := finder.FindOutboxByRequestIDs(ctx, tenantID, requestIDs)
	if err != nil {
		return nil, err
	}
	cards := make(map[string]*channels.InteractiveCard, len(events))
	for _, outbox := range events {
		var payload struct {
			Card *channels.InteractiveCard `json:"card,omitempty"`
		}
		if err := json.Unmarshal(outbox.Payload, &payload); err != nil {
			return nil, fmt.Errorf("decode stored reply card: %w", err)
		}
		if payload.Card != nil {
			cards[outbox.RequestID] = payload.Card
		}
	}
	for index := range messages {
		if card := cards[messages[index].RequestID]; card != nil {
			messages[index].Card = card
		}
	}
	return messages, nil
}

func (c *consoleAPI) recordConversationContentRead(request *http.Request, user identity.SessionUser, entry storage.Session) error {
	return c.recordConversationContentAccess(request, user, entry,
		"conversation_content_read", "authorized tenant conversation content audit read")
}

func (c *consoleAPI) recordConversationArtifactRead(request *http.Request, user identity.SessionUser, entry storage.Session) error {
	return c.recordConversationContentAccess(request, user, entry,
		"conversation_artifact_read", "authorized tenant conversation artifact audit read")
}

func (c *consoleAPI) recordConversationContentAccess(request *http.Request, user identity.SessionUser, entry storage.Session, action, detail string) error {
	recorder, ok := c.dependencies.State.(storage.AuditRecorder)
	if !ok {
		return errors.New("tenant audit recorder is not configured")
	}
	return recorder.RecordAudit(request.Context(), storage.AuditEvent{
		ID: uuid.NewString(), TenantID: entry.TenantID, TraceID: uuid.NewString(), UserID: user.PlatformUserID,
		SessionID: entry.SessionKey, AgentName: entry.AppCode, Action: action, Result: "ok", Detail: detail,
	})
}

func (c *consoleAPI) attachChatMessageSources(ctx context.Context, tenantID, sessionKey string, messages []chatTranscriptMessage) []chatTranscriptMessage {
	requestIDs := make([]string, 0, len(messages))
	seen := make(map[string]struct{}, len(messages))
	for _, message := range messages {
		if message.Role != string(model.RoleUser) || strings.TrimSpace(message.RequestID) == "" {
			continue
		}
		if _, ok := seen[message.RequestID]; ok {
			continue
		}
		seen[message.RequestID] = struct{}{}
		requestIDs = append(requestIDs, message.RequestID)
	}
	if len(requestIDs) == 0 {
		return messages
	}
	var routes []storage.InboundMessageRoute
	if routeStore, ok := c.dependencies.State.(storage.SessionMessageRouteStore); ok {
		routes, _ = routeStore.ListInboundMessageRoutesByMessageIDs(ctx, tenantID, sessionKey, requestIDs)
	} else if ownershipStore, ok := c.dependencies.State.(storage.SessionOwnershipStore); ok {
		routes, _ = ownershipStore.ListInboundMessageRoutes(ctx, tenantID, sessionKey)
	}
	if len(routes) == 0 {
		return messages
	}
	byMessageID := make(map[string][]storage.InboundMessageRoute, len(routes))
	for _, route := range routes {
		byMessageID[route.MessageID] = append(byMessageID[route.MessageID], route)
	}
	usedByMessageID := make(map[string]int, len(byMessageID))
	for index := range messages {
		if messages[index].Role != string(model.RoleUser) {
			continue
		}
		candidates := byMessageID[messages[index].RequestID]
		if len(candidates) == 0 {
			continue
		}
		candidateIndex := usedByMessageID[messages[index].RequestID]
		if candidateIndex >= len(candidates) {
			continue
		}
		if !messages[index].EventTime.IsZero() {
			for candidateIndex < len(candidates)-1 && candidates[candidateIndex].CreatedAt.Before(messages[index].EventTime) {
				candidateIndex++
			}
		}
		route := candidates[candidateIndex]
		usedByMessageID[messages[index].RequestID] = candidateIndex + 1
		messages[index].Source = &chatMessageSource{
			Channel: route.Channel, BindingID: route.BindingID, ConversationID: route.ConversationID,
			Scope: route.Scope, ActorExternalUserID: route.ActorExternalUserID,
			ActorPlatformUserID: route.ActorPlatformUserID, TriggerType: route.TriggerType,
		}
	}
	return messages
}

func loadChatMessagePage(ctx context.Context, service agentsession.Service, key agentsession.Key, before string, limit int) ([]chatTranscriptMessage, string, error) {
	if before != "" {
		if windowService, ok := service.(agentsession.WindowService); ok {
			messages, next, err := loadChatMessagesBeforeAnchor(ctx, windowService, key, before, limit)
			if err == nil || (!errors.Is(err, agentsession.ErrEventWindowAnchorNotFound) && !errors.Is(err, agentsession.ErrEventPageUnsupported)) {
				return messages, next, err
			}
		}
		frameworkSession, err := service.GetSession(ctx, key)
		if err != nil {
			return nil, "", err
		}
		return loadChatMessagesFromSnapshot(frameworkSession, before, limit)
	}
	return loadLatestChatMessages(ctx, service, key, limit)
}

func loadLatestChatMessages(ctx context.Context, service agentsession.Service, key agentsession.Key, limit int) ([]chatTranscriptMessage, string, error) {
	batch := sessionEventScanBatch(limit)
	found := make([]chatTranscriptMessage, 0, limit+1)
	offset := 0
	for len(found) < limit+1 {
		frameworkSession, err := service.GetSession(ctx, key, agentsession.WithGetSessionEventPage(offset, batch))
		if errors.Is(err, agentsession.ErrEventPageUnsupported) {
			frameworkSession, err = service.GetSession(ctx, key)
			if err != nil {
				return nil, "", err
			}
			return loadChatMessagesFromSnapshot(frameworkSession, "", limit)
		}
		if err != nil {
			return nil, "", err
		}
		events := snapshotSessionEvents(frameworkSession)
		appendProjectedMessagesNewestFirst(&found, events, limit+1)
		offset += len(events)
		if len(events) < batch {
			break
		}
	}
	return finalizeChatMessagePage(found, limit)
}

func loadChatMessagesBeforeAnchor(ctx context.Context, service agentsession.WindowService, key agentsession.Key, anchor string, limit int) ([]chatTranscriptMessage, string, error) {
	batch := sessionEventScanBatch(limit)
	found := make([]chatTranscriptMessage, 0, limit+1)
	currentAnchor := anchor
	for len(found) < limit+1 {
		window, err := service.GetEventWindow(ctx, agentsession.EventWindowRequest{
			Key: key, AnchorEventID: currentAnchor, Before: batch, After: 0,
		})
		if err != nil {
			return nil, "", err
		}
		anchorIndex := -1
		for index := range window.Entries {
			if window.Entries[index].Event.ID == currentAnchor {
				anchorIndex = index
				break
			}
		}
		if anchorIndex < 0 {
			return nil, "", fmt.Errorf("%w: %s", agentsession.ErrEventWindowAnchorNotFound, currentAnchor)
		}
		older := make([]event.Event, 0, anchorIndex)
		for index := 0; index < anchorIndex; index++ {
			older = append(older, window.Entries[index].Event)
		}
		appendProjectedMessagesNewestFirst(&found, older, limit+1)
		if len(found) >= limit+1 || len(older) < batch {
			break
		}
		currentAnchor = older[0].ID
		if strings.TrimSpace(currentAnchor) == "" {
			return nil, "", errors.New("session event is missing an ID")
		}
	}
	return finalizeChatMessagePage(found, limit)
}

func loadChatMessagesFromSnapshot(sess *agentsession.Session, before string, limit int) ([]chatTranscriptMessage, string, error) {
	events := snapshotSessionEvents(sess)
	end := len(events)
	if before != "" {
		end = -1
		for index := range events {
			if events[index].ID == before {
				end = index
				break
			}
		}
		if end < 0 {
			return nil, "", fmt.Errorf("%w: %s", agentsession.ErrEventWindowAnchorNotFound, before)
		}
	}
	found := make([]chatTranscriptMessage, 0, limit+1)
	appendProjectedMessagesNewestFirst(&found, events[:end], limit+1)
	return finalizeChatMessagePage(found, limit)
}

func snapshotSessionEvents(sess *agentsession.Session) []event.Event {
	if sess == nil {
		return []event.Event{}
	}
	sess.EventMu.RLock()
	defer sess.EventMu.RUnlock()
	return append([]event.Event(nil), sess.Events...)
}

func appendProjectedMessagesNewestFirst(found *[]chatTranscriptMessage, events []event.Event, target int) {
	for index := len(events) - 1; index >= 0 && len(*found) < target; index-- {
		if message, ok := chatMessageFromEvent(&events[index]); ok {
			*found = append(*found, message)
		}
	}
}

func finalizeChatMessagePage(newestFirst []chatTranscriptMessage, limit int) ([]chatTranscriptMessage, string, error) {
	nextCursor := ""
	if len(newestFirst) > limit {
		newestFirst = newestFirst[:limit]
		nextCursor = strings.TrimSpace(newestFirst[len(newestFirst)-1].ID)
		if nextCursor == "" {
			return nil, "", errors.New("session event is missing an ID")
		}
	}
	for left, right := 0, len(newestFirst)-1; left < right; left, right = left+1, right-1 {
		newestFirst[left], newestFirst[right] = newestFirst[right], newestFirst[left]
	}
	return newestFirst, nextCursor, nil
}

func sessionEventScanBatch(visibleLimit int) int {
	batch := visibleLimit * 4
	if batch < minSessionEventScanBatch {
		return minSessionEventScanBatch
	}
	if batch > maxSessionEventScanBatch {
		return maxSessionEventScanBatch
	}
	return batch
}

func projectChatMessages(sess *agentsession.Session) []chatTranscriptMessage {
	if sess == nil {
		return []chatTranscriptMessage{}
	}
	sess.EventMu.RLock()
	defer sess.EventMu.RUnlock()
	messages := make([]chatTranscriptMessage, 0)
	for index := range sess.Events {
		if message, ok := chatMessageFromEvent(&sess.Events[index]); ok {
			messages = append(messages, message)
		}
	}
	return messages
}

func chatPreview(messages []chatTranscriptMessage) string {
	for _, message := range messages {
		if message.Role != string(model.RoleUser) {
			continue
		}
		if strings.TrimSpace(message.Content) != "" {
			return message.Content
		}
		if len(message.Attachments) > 0 {
			return "附件 · " + message.Attachments[0].Name
		}
	}
	return ""
}

func chatMessageFromEvent(evt *event.Event) (chatTranscriptMessage, bool) {
	if evt == nil || evt.Response == nil || evt.Response.IsPartial || len(evt.Response.Choices) == 0 {
		return chatTranscriptMessage{}, false
	}
	switch evt.Response.Object {
	case "", model.ObjectTypeChatCompletion, model.ObjectTypeChatCompletionChunk:
	default:
		return chatTranscriptMessage{}, false
	}
	msg := evt.Response.Choices[0].Message
	if len(msg.ToolCalls) > 0 && strings.TrimSpace(msg.Content) == "" {
		return chatTranscriptMessage{}, false
	}
	role := string(msg.Role)
	if role != string(model.RoleUser) && role != string(model.RoleAssistant) {
		return chatTranscriptMessage{}, false
	}
	content := strings.TrimSpace(msg.Content)
	if content == "" {
		return chatTranscriptMessage{}, false
	}
	attachments := []chatTranscriptAttachment(nil)
	if role == string(model.RoleUser) {
		content, attachments = visibleUserContent(content)
		if content == "" && len(attachments) == 0 {
			return chatTranscriptMessage{}, false
		}
	}
	stamp := evt.Timestamp
	if stamp.IsZero() {
		stamp = time.Now().UTC()
	}
	return chatTranscriptMessage{
		ID:          evt.ID,
		Role:        role,
		Content:     content,
		Time:        stamp.UTC().Format(time.RFC3339),
		Attachments: attachments,
		RequestID:   evt.RequestID,
		EventTime:   stamp.UTC(),
	}, true
}

func visibleUserContent(content string) (string, []chatTranscriptAttachment) {
	const marker = "附件「"
	first := strings.Index(content, marker)
	if first < 0 || (first > 0 && !strings.HasSuffix(content[:first], "\n\n")) {
		return strings.TrimSpace(content), nil
	}
	visible := strings.TrimSpace(content[:first])
	rest := content[first:]
	attachments := make([]chatTranscriptAttachment, 0, 1)
	for {
		if !strings.HasPrefix(rest, marker) {
			break
		}
		nameEnd := strings.Index(rest[len(marker):], "」内容：\n")
		if nameEnd < 0 {
			break
		}
		nameEnd += len(marker)
		name := strings.TrimSpace(rest[len(marker):nameEnd])
		if name == "" {
			break
		}
		attachments = append(attachments, chatTranscriptAttachment{Name: name})
		bodyStart := nameEnd + len("」内容：\n")
		next := strings.Index(rest[bodyStart:], "\n\n"+marker)
		if next < 0 {
			break
		}
		rest = rest[bodyStart+next+2:]
	}
	if len(attachments) == 0 {
		return strings.TrimSpace(content), nil
	}
	return visible, attachments
}

func matchListedSession(entry storage.Session, appCode, channel, subject, status string) bool {
	if appCode != "" && entry.AppCode != appCode {
		return false
	}
	if channel != "" {
		matched := false
		for _, conversation := range entry.Conversations {
			if conversation.Channel == channel {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	if subject != "" && entry.SubjectID != subject {
		return false
	}
	if status != "" && entry.Status != status {
		return false
	}
	return true
}
