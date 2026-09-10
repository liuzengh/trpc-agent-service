package web

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
	"github.com/liuzengh/trpc-agent-service/trpcservice/messaging"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	agentartifact "trpc.group/trpc-go/trpc-agent-go/artifact"
)

const (
	chatStreamIdleTimeout = 45 * time.Second
	chatStreamHeartbeat   = 2 * time.Second
	maxChatFiles          = 4
	maxChatRequestBytes   = int64(storage.MaxArtifactBytes) + 1<<20
)

// enqueueChat accepts browser input quickly and lets the Kafka Worker run the
// same Runtime, governance and Outbox path as an external IM connector.
func (c *consoleAPI) enqueueChat(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		methodNotAllowed(writer, http.MethodPost)
		return
	}
	body, uploads, err := parseChatSubmission(writer, request)
	if err != nil {
		badRequest(writer, err.Error())
		return
	}
	if request.MultipartForm != nil {
		defer request.MultipartForm.RemoveAll()
	}
	if strings.TrimSpace(body.RequestID) == "" {
		badRequest(writer, "request_id is required")
		return
	}
	if !requireTenantRead(writer, request, strings.TrimSpace(body.TenantID)) {
		return
	}
	user, ok := sessionUser(request)
	if !ok {
		http.Error(writer, "unauthorized", http.StatusUnauthorized)
		return
	}
	snapshot, sessionKey, inbound, err := c.buildWebInbound(request.Context(), body, user.PlatformUserID)
	if err != nil {
		badRequest(writer, err.Error())
		return
	}
	if strings.TrimSpace(inbound.Text) == "" && len(uploads) == 0 {
		badRequest(writer, "message text or file is required")
		return
	}
	inbound.WebOwnerID = user.PlatformUserID
	var ingressLease storage.Lease
	if c.dependencies.WebIdempotency != nil {
		key := "web-ingress:" + snapshot.Config.TenantID + ":" + inbound.MessageID
		acquired, err := c.dependencies.WebIdempotency.Acquire(request.Context(), key, time.Minute)
		if err != nil {
			serverError(writer, "acquire web request", err)
			return
		}
		switch acquired.State {
		case storage.LeaseAlreadyCompleted, storage.LeaseInProgress:
			writeJSON(writer, http.StatusAccepted, map[string]any{
				"event_id": inbound.MessageID, "session_key": sessionKey,
				"stream_url": "/api/v1/chat/stream?tenant=" + snapshot.Config.TenantID + "&event_id=" + inbound.MessageID,
			})
			return
		case storage.LeaseAcquired:
			ingressLease = acquired.Lease
		default:
			serverError(writer, "acquire web request", errors.New("unsupported idempotency state"))
			return
		}
	}
	var (
		artifactService agentartifact.Service
		artifactInfo    agentartifact.SessionInfo
	)
	if len(uploads) > 0 {
		if c.dependencies.ArtifactServices == nil {
			if ingressLease.Key != "" {
				_ = c.dependencies.WebIdempotency.Release(request.Context(), ingressLease)
			}
			serverError(writer, "store chat files", errors.New("artifact service is not configured"))
			return
		}
		artifactService, err = c.dependencies.ArtifactServices.ArtifactService(request.Context(), snapshot.Config)
		if err != nil {
			if ingressLease.Key != "" {
				_ = c.dependencies.WebIdempotency.Release(request.Context(), ingressLease)
			}
			serverError(writer, "resolve chat file storage", err)
			return
		}
		artifactInfo = agentartifact.SessionInfo{AppName: snapshot.Config.AppName(), UserID: user.PlatformUserID, SessionID: sessionKey}
		inbound.Files, err = saveChatUploads(request.Context(), artifactService, artifactInfo, inbound.MessageID, uploads)
		if err != nil {
			if ingressLease.Key != "" {
				_ = c.dependencies.WebIdempotency.Release(request.Context(), ingressLease)
			}
			serverError(writer, "store chat files", err)
			return
		}
	}
	selection := tenant.ReleaseSelection{Snapshot: snapshot, Variant: tenant.ReleaseStable}
	if rollout, rolloutErr := c.dependencies.Configurations.GetRollout(request.Context(), snapshot.Config.TenantID, snapshot.Config.AppCode); rolloutErr == nil {
		selection.RolloutGeneration = rollout.Generation
		if snapshot.Config.ConfigVersion == rollout.CandidateVersion {
			selection.Variant = tenant.ReleaseCandidate
		}
	} else if !errors.Is(rolloutErr, tenant.ErrRolloutNotFound) {
		deleteChatUploads(request.Context(), artifactService, artifactInfo, inbound.Files)
		if ingressLease.Key != "" {
			_ = c.dependencies.WebIdempotency.Release(request.Context(), ingressLease)
		}
		serverError(writer, "resolve web release metadata", rolloutErr)
		return
	}
	envelope, err := messaging.NewInboundEnvelopeWithContext(request.Context(), selection, "web-console", sessionKey, inbound, c.dependencies.ExecutionManifests)
	if err != nil {
		deleteChatUploads(request.Context(), artifactService, artifactInfo, inbound.Files)
		if ingressLease.Key != "" {
			_ = c.dependencies.WebIdempotency.Release(request.Context(), ingressLease)
		}
		badRequest(writer, err.Error())
		return
	}
	if err := c.dependencies.Producer.Publish(request.Context(), envelope); err != nil {
		deleteChatUploads(request.Context(), artifactService, artifactInfo, inbound.Files)
		if ingressLease.Key != "" {
			_ = c.dependencies.WebIdempotency.Release(request.Context(), ingressLease)
		}
		serverError(writer, "publish web chat", err)
		return
	}
	if ingressLease.Key != "" {
		if err := c.dependencies.WebIdempotency.Complete(request.Context(), ingressLease, 24*time.Hour); err != nil {
			serverError(writer, "complete web request", err)
			return
		}
	}
	writeJSON(writer, http.StatusAccepted, map[string]any{
		"event_id": inbound.MessageID, "session_key": sessionKey,
		"stream_url": "/api/v1/chat/stream?tenant=" + snapshot.Config.TenantID + "&event_id=" + inbound.MessageID,
	})
}

func parseChatSubmission(writer http.ResponseWriter, request *http.Request) (chatRequest, []*multipart.FileHeader, error) {
	if !strings.HasPrefix(strings.ToLower(request.Header.Get("Content-Type")), "multipart/form-data") {
		var body chatRequest
		err := decodeJSONBody(request, &body)
		return body, nil, err
	}
	request.Body = http.MaxBytesReader(writer, request.Body, maxChatRequestBytes)
	if err := request.ParseMultipartForm(storage.MaxArtifactBytes); err != nil {
		return chatRequest{}, nil, fmt.Errorf("parse chat upload: %w", err)
	}
	uploads := request.MultipartForm.File["files"]
	if len(uploads) > maxChatFiles {
		return chatRequest{}, nil, fmt.Errorf("at most %d files are allowed", maxChatFiles)
	}
	var total int64
	for _, upload := range uploads {
		total += upload.Size
	}
	if total > int64(storage.MaxArtifactBytes) {
		return chatRequest{}, nil, fmt.Errorf("files exceed the %d byte upload limit", storage.MaxArtifactBytes)
	}
	return chatRequest{
		TenantID: request.FormValue("tenant_id"), AppCode: request.FormValue("app_code"),
		ConversationID: request.FormValue("conversation_id"),
		Text:           request.FormValue("text"), RequestID: request.FormValue("request_id"),
	}, uploads, nil
}

func saveChatUploads(ctx context.Context, service agentartifact.Service, info agentartifact.SessionInfo, messageID string, uploads []*multipart.FileHeader) ([]channels.InboundFile, error) {
	received := make([]channels.ReceivedFile, 0, len(uploads))
	for _, upload := range uploads {
		reader, err := upload.Open()
		if err != nil {
			return nil, fmt.Errorf("open %q: %w", upload.Filename, err)
		}
		data, readErr := io.ReadAll(io.LimitReader(reader, int64(storage.MaxArtifactBytes)+1))
		closeErr := reader.Close()
		if readErr != nil {
			return nil, fmt.Errorf("read %q: %w", upload.Filename, readErr)
		}
		if closeErr != nil {
			return nil, fmt.Errorf("close %q: %w", upload.Filename, closeErr)
		}
		if int64(len(data)) > storage.MaxArtifactBytes {
			return nil, fmt.Errorf("file %q exceeds the %d byte upload limit", upload.Filename, storage.MaxArtifactBytes)
		}
		received = append(received, channels.ReceivedFile{
			Name: upload.Filename, MimeType: upload.Header.Get("Content-Type"), Data: data,
		})
	}
	return messaging.StageInboundFiles(ctx, service, info, messageID, received)
}

func deleteChatUploads(ctx context.Context, service agentartifact.Service, info agentartifact.SessionInfo, files []channels.InboundFile) {
	messaging.DeleteInboundFiles(ctx, service, info, files)
}

// chatStream replays a Redis Stream from Last-Event-ID, then waits for new
// deltas. A PostgreSQL Outbox reply remains the durable fallback when the
// short-lived stream has expired or Redis was unavailable during token output.
func (c *consoleAPI) chatStream(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		methodNotAllowed(writer, http.MethodGet)
		return
	}
	tenantID, requestID := request.URL.Query().Get("tenant"), request.URL.Query().Get("event_id")
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(requestID) == "" {
		badRequest(writer, "tenant and event_id query parameters are required")
		return
	}
	if !requireTenantRead(writer, request, tenantID) {
		return
	}
	user, ok := sessionUser(request)
	if !ok {
		http.Error(writer, "unauthorized", http.StatusUnauthorized)
		return
	}
	afterID := strings.TrimSpace(request.Header.Get("Last-Event-ID"))
	if afterID != "" && !validStreamID(afterID) {
		badRequest(writer, "Last-Event-ID is invalid")
		return
	}

	stream := newSSEWriter(writer)
	if reply, ready, err := c.completedWebReply(request.Context(), tenantID, requestID); err != nil {
		stream.error("read queued reply failed")
		return
	} else if ready {
		if !reply.visibleTo(user) {
			writeJSON(writer, http.StatusForbidden, map[string]any{"error": "forbidden: reply belongs to another user"})
			return
		}
		stream.begin()
		stream.doneWithID(reply.EventID, reply.Text)
		return
	}
	stream.begin()
	if c.dependencies.ReplySubscriber == nil {
		stream.error("web reply stream is unavailable")
		return
	}
	replies, cancel, err := c.dependencies.ReplySubscriber.Subscribe(request.Context(), tenantID, user.PlatformUserID, requestID, afterID)
	if err != nil {
		stream.error("subscribe queued reply failed")
		return
	}
	defer cancel()

	// The first durable re-read closes the race between the initial lookup and
	// live subscription. No reply can be lost even if a worker finishes there.
	if reply, ready, err := c.completedWebReply(request.Context(), tenantID, requestID); err != nil {
		stream.error("read queued reply failed")
		return
	} else if ready {
		if !reply.visibleTo(user) {
			stream.error("reply access denied")
			return
		}
		stream.doneWithID(reply.EventID, reply.Text)
		return
	}

	heartbeat := time.NewTicker(chatStreamHeartbeat)
	defer heartbeat.Stop()
	idle := time.NewTimer(chatStreamIdleTimeout)
	defer idle.Stop()
	for {
		select {
		case <-request.Context().Done():
			return
		case <-heartbeat.C:
			if fallback, ready, err := c.completedWebReply(request.Context(), tenantID, requestID); err == nil && ready && fallback.visibleTo(user) {
				stream.doneWithID(fallback.EventID, fallback.Text)
				return
			}
			stream.keepAlive()
		case <-idle.C:
			stream.error("reply stream timed out; reconnect to resume")
			return
		case reply, ok := <-replies:
			if !ok {
				if fallback, ready, err := c.completedWebReply(request.Context(), tenantID, requestID); err == nil && ready && fallback.visibleTo(user) {
					stream.doneWithID(fallback.EventID, fallback.Text)
				} else {
					stream.error("reply stream closed")
				}
				return
			}
			if !idle.Stop() {
				select {
				case <-idle.C:
				default:
				}
			}
			idle.Reset(chatStreamIdleTimeout)
			switch reply.Type {
			case "delta":
				stream.deltaWithID(reply.ID, reply.Content)
			case "done":
				stream.doneWithID(reply.ID, reply.Reply)
				return
			}
		}
	}
}

type completedWebReply struct {
	Text    string
	EventID string
	OwnerID string
}

func (reply completedWebReply) visibleTo(user identity.SessionUser) bool {
	return strings.TrimSpace(reply.OwnerID) != "" && (user.IsSystemAdmin || reply.OwnerID == user.PlatformUserID)
}

func (c *consoleAPI) completedWebReply(ctx context.Context, tenantID, requestID string) (completedWebReply, bool, error) {
	if c.dependencies.Replies == nil {
		return completedWebReply{}, false, nil
	}
	event, err := c.dependencies.Replies.FindOutboxByRequestID(ctx, tenantID, requestID)
	if errors.Is(err, storage.ErrOutboxEventNotFound) {
		return completedWebReply{}, false, nil
	}
	if err != nil {
		return completedWebReply{}, false, err
	}
	reply, ownerID, err := decodeStoredReply(event)
	if err != nil {
		return completedWebReply{}, false, err
	}
	return completedWebReply{Text: reply, EventID: event.DeliveryReceipt, OwnerID: ownerID}, true, nil
}

func decodeStoredReply(event storage.OutboxEvent) (string, string, error) {
	var payload struct {
		Channel    channels.Channel `json:"channel"`
		Text       string           `json:"text"`
		WebOwnerID string           `json:"web_owner_id"`
	}
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return "", "", err
	}
	if payload.Channel != channels.Web || strings.TrimSpace(payload.Text) == "" || strings.TrimSpace(payload.WebOwnerID) == "" {
		return "", "", errors.New("not an owned web reply")
	}
	return payload.Text, payload.WebOwnerID, nil
}
