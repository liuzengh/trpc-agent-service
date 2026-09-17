package webui

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/identity"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/ingress"
	ingressmemory "github.com/liuzengh/trpc-agent-service/trpcservice/channels/ingress/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
	secretmemory "github.com/liuzengh/trpc-agent-service/trpcservice/secrets/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
	messagingmemory "github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging/inmemory"
)

type browserConfirmationCoordinator struct {
	governance.ConfirmationCoordinator
	value         governance.Confirmation
	decisionCalls int
}

func (c *browserConfirmationCoordinator) GetConfirmation(_ context.Context, tenantID, confirmationID string) (governance.Confirmation, error) {
	if tenantID != c.value.TenantID || confirmationID != c.value.ConfirmationID {
		return governance.Confirmation{}, runtime.ErrNotFound
	}
	return c.value, nil
}

func (c *browserConfirmationCoordinator) Decide(_ context.Context, input governance.ConfirmationDecision) (governance.Confirmation, error) {
	if c.value.State != governance.ConfirmationPending {
		return c.value, nil
	}
	if input.ExpectedVersion != c.value.Version {
		return governance.Confirmation{}, runtime.ErrVersionConflict
	}
	c.decisionCalls++
	c.value.Version++
	c.value.DecisionAt = input.DecidedAt
	if input.Approve {
		c.value.State = governance.ConfirmationApproved
	} else {
		c.value.State = governance.ConfirmationDenied
	}
	return c.value, nil
}

func TestBrowserHandlerServesUIAndAuthenticatedDurableReplies(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	routes := ingressmemory.New()
	secretProvider := secretmemory.New()
	mailbox := NewMemoryMailbox()
	results := messagingmemory.New()
	secretRef := secrets.SecretRef{Ref: "webui-verify", Version: 1}
	route := ingress.BindingRoute{OpaqueBindingID: "opaque-webui-binding", Channel: "webui", RouteKeyDigest: RouteKeyDigest("local-route"),
		TenantID: "tenant-a", AgentAppID: "app-a", ChannelBindingID: "webui-main", ExternalAccountID: "local-account",
		TenantVersion: 1, BindingVersion: 1, SecretRef: secretRef,
		IdentitySecretRef: secrets.SecretRef{Ref: "identity", Version: 1}, SessionSecretRef: secrets.SecretRef{Ref: "session", Version: 1}, Enabled: true}
	if err := routes.PutBindingRoute(context.Background(), route); err != nil {
		t.Fatal(err)
	}
	token := "0123456789abcdef0123456789abcdef"
	material, _ := json.Marshal(VerificationMaterial{Token: token, ExternalAccountID: route.ExternalAccountID})
	secretProvider.Put(secrets.Scope{TenantID: route.TenantID, Subject: route.ChannelBindingID, Purpose: secrets.PurposeChannelVerify,
		ResourceID: route.ChannelBindingID, ResourceVersion: route.BindingVersion}, secretRef, material)
	content := []byte("durable browser reply")
	digest := sha256.Sum256(content)
	if err := results.PutResult(context.Background(), messaging.ResultRecord{TenantID: route.TenantID, RequestID: "request-1",
		ResultRef: "result://request-1", ContentDigest: hex.EncodeToString(digest[:]), Content: content, KeyVersion: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := mailbox.PutMessage(context.Background(), Message{TenantID: route.TenantID, ConfigVersion: 1,
		ChannelBindingID: route.ChannelBindingID, ExternalAccountID: route.ExternalAccountID,
		ExternalUserID: "user-1", ExternalChatID: "chat-1", RequestID: "request-1", ClientRequestID: "client-1",
		ProviderMessageID: "webui-message-1", ContentRef: "result://request-1", ContentDigest: hex.EncodeToString(digest[:]), CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	callbackCalls := 0
	handler := BrowserHandler{Callback: http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		callbackCalls++
		writer.WriteHeader(http.StatusOK)
	}), Routes: routes, Secrets: secretProvider, Messages: mailbox, Results: results, Now: func() time.Time { return now }}

	page := httptest.NewRecorder()
	handler.ServeHTTP(page, httptest.NewRequest(http.MethodGet, "/webui/", nil))
	if page.Code != http.StatusOK || !strings.Contains(page.Body.String(), "WebUI IM") ||
		!strings.Contains(page.Body.String(), "WebUI durable confirmation") || page.Header().Get("Content-Security-Policy") == "" {
		t.Fatalf("page code=%d headers=%v body=%q", page.Code, page.Header(), page.Body.String())
	}

	uri := "/webui/api/replies?route_key=local-route&external_user_id=user-1&external_chat_id=chat-1"
	timestamp, nonce := "1800000000", "nonce-read"
	request := httptest.NewRequest(http.MethodGet, uri, nil)
	request.Header.Set(headerTimestamp, timestamp)
	request.Header.Set(headerNonce, nonce)
	request.Header.Set(headerSignature, signatureFor(token, timestamp, nonce, []byte(http.MethodGet+"\n"+uri)))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "durable browser reply") || !strings.Contains(response.Body.String(), "request-1") {
		t.Fatalf("reply code=%d body=%q", response.Code, response.Body.String())
	}

	request = httptest.NewRequest(http.MethodGet, uri, nil)
	request.Header.Set(headerTimestamp, timestamp)
	request.Header.Set(headerNonce, nonce)
	request.Header.Set(headerSignature, signatureFor("wrong-token-000000000", timestamp, nonce, []byte(http.MethodGet+"\n"+uri)))
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized code=%d", response.Code)
	}

	post := httptest.NewRequest(http.MethodPost, "/webui/api/messages?route_key=local-route", strings.NewReader(`{}`))
	handler.ServeHTTP(httptest.NewRecorder(), post)
	if callbackCalls != 1 {
		t.Fatalf("callback calls=%d", callbackCalls)
	}
}

func TestBrowserHandlerConfirmationActionBindsSignedActorAndIsIdempotent(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	routes, secretProvider, mailbox, results := ingressmemory.New(), secretmemory.New(), NewMemoryMailbox(), messagingmemory.New()
	route := ingress.BindingRoute{OpaqueBindingID: "opaque-webui-binding", Channel: "webui", RouteKeyDigest: RouteKeyDigest("local-route"),
		TenantID: "tenant-a", AgentAppID: "app-a", ChannelBindingID: "webui-main", ExternalAccountID: "local-account",
		TenantVersion: 1, BindingVersion: 1, SecretRef: secrets.SecretRef{Ref: "webui-verify", Version: 1},
		IdentitySecretRef: secrets.SecretRef{Ref: "identity", Version: 1}, SessionSecretRef: secrets.SecretRef{Ref: "session", Version: 1}, Enabled: true}
	if err := routes.PutBindingRoute(context.Background(), route); err != nil {
		t.Fatal(err)
	}
	token := "0123456789abcdef0123456789abcdef"
	material, _ := json.Marshal(VerificationMaterial{Token: token, ExternalAccountID: route.ExternalAccountID})
	secretProvider.Put(secrets.Scope{TenantID: route.TenantID, Subject: route.ChannelBindingID, Purpose: secrets.PurposeChannelVerify,
		ResourceID: route.ChannelBindingID, ResourceVersion: route.BindingVersion}, route.SecretRef, material)
	secretProvider.Put(secrets.Scope{TenantID: route.TenantID, Subject: route.TenantID, Purpose: secrets.PurposeTenantIdentity,
		ResourceID: route.TenantID, ResourceVersion: 1}, route.IdentitySecretRef, []byte("tenant-identity-key"))
	secretProvider.Put(secrets.Scope{TenantID: route.TenantID, Subject: route.TenantID, Purpose: secrets.PurposeTenantSession,
		ResourceID: route.TenantID, ResourceVersion: 1}, route.SessionSecretRef, []byte("tenant-session-key"))
	binding := channel.VerifiedBinding{TenantID: route.TenantID, AgentAppID: route.AgentAppID, ChannelBindingID: route.ChannelBindingID,
		Channel: route.Channel, ExternalAccountID: route.ExternalAccountID, TenantVersion: route.TenantVersion, BindingVersion: route.BindingVersion,
		IdentitySecretRef: route.IdentitySecretRef, SessionSecretRef: route.SessionSecretRef}
	actor, err := (identity.Mapper{Secrets: secretProvider}).Map(context.Background(), binding, channel.ProviderEvent{Channel: "webui",
		ExternalAccountID: route.ExternalAccountID, ExternalUserID: "user-1", ExternalChatID: "chat-1", ConversationType: "p2p"})
	if err != nil {
		t.Fatal(err)
	}
	confirmationID := "conf_0123456789abcdef0123456789abcdef"
	expiresAt := now.Add(time.Hour)
	prompt, _ := json.Marshal(map[string]any{"schema_version": 1, "kind": "tool_confirmation", "confirmation_id": confirmationID,
		"tool_id": "dangerous-tool", "tool_version": 2, "expires_at": expiresAt.Format(time.RFC3339Nano)})
	digest := sha256.Sum256(prompt)
	contentRef := "confirmation://" + route.TenantID + "/" + confirmationID
	if err := results.PutInteraction(context.Background(), messaging.InteractionRecord{TenantID: route.TenantID, RequestID: "request-1",
		ContentRef: contentRef, ContentDigest: hex.EncodeToString(digest[:]), Content: prompt, KeyVersion: 1}); err != nil {
		t.Fatal(err)
	}
	if err := results.PutReplyRoute(messaging.ReplyRoute{TenantID: route.TenantID, RequestID: "request-1", Channel: "webui",
		ChannelBindingID: route.ChannelBindingID, ExternalAccountID: route.ExternalAccountID, ExternalUserID: "user-1", ExternalChatID: "chat-1", ConfigVersion: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := mailbox.PutMessage(context.Background(), Message{TenantID: route.TenantID, ConfigVersion: 1,
		ChannelBindingID: route.ChannelBindingID, ExternalAccountID: route.ExternalAccountID, ExternalUserID: "user-1", ExternalChatID: "chat-1",
		RequestID: "request-1", ClientRequestID: "client-1", ProviderMessageID: "webui-confirmation-1", ContentRef: contentRef,
		ContentDigest: hex.EncodeToString(digest[:]), CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	coordinator := &browserConfirmationCoordinator{value: governance.Confirmation{SuspensionRequest: governance.SuspensionRequest{
		ConfirmationID: confirmationID, TenantID: route.TenantID, RequestID: "request-1", SubjectID: actor.UserID,
		ChannelBindingID: route.ChannelBindingID, SessionID: actor.SessionID, Tool: governance.VersionedRef{ID: "dangerous-tool", Version: 2},
		ExpiresAt: expiresAt}, State: governance.ConfirmationPending, Version: 1}}
	handler := BrowserHandler{Callback: http.NotFoundHandler(), Routes: routes, Secrets: secretProvider, Messages: mailbox, Results: results,
		ReplyRoutes: results, Confirmations: coordinator, Actions: governance.ConfirmationActionService{Coordinator: coordinator}, Now: func() time.Time { return now }}

	readURI := "/webui/api/replies?route_key=local-route&external_user_id=user-1&external_chat_id=chat-1"
	read := signedBrowserRequest(t, http.MethodGet, readURI, nil, token, "read", now)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, read)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"confirmation_id":"`+confirmationID+`"`) ||
		!strings.Contains(response.Body.String(), `"state":"pending"`) {
		t.Fatalf("confirmation reply code=%d body=%q", response.Code, response.Body.String())
	}

	actionURI := "/webui/api/confirmations/actions?route_key=local-route"
	body := []byte(`{"schema_version":1,"confirmation_id":"` + confirmationID + `","expected_version":1,"decision":"approve","external_user_id":"user-1","external_chat_id":"chat-1"}`)
	for index := 0; index < 2; index++ {
		actionBody := body
		if index == 1 {
			actionBody = []byte(`{"schema_version":1,"confirmation_id":"` + confirmationID + `","expected_version":1,"decision":"deny","external_user_id":"user-1","external_chat_id":"chat-1"}`)
		}
		action := signedBrowserRequest(t, http.MethodPost, actionURI, actionBody, token, "action-"+string(rune('a'+index)), now)
		response = httptest.NewRecorder()
		handler.ServeHTTP(response, action)
		if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"state":"approved"`) {
			t.Fatalf("action %d code=%d body=%q", index, response.Code, response.Body.String())
		}
	}
	if coordinator.decisionCalls != 1 {
		t.Fatalf("decision calls=%d", coordinator.decisionCalls)
	}

	forged := []byte(`{"schema_version":1,"confirmation_id":"` + confirmationID + `","expected_version":1,"decision":"approve","external_user_id":"other-user","external_chat_id":"chat-1"}`)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, signedBrowserRequest(t, http.MethodPost, actionURI, forged, token, "forged", now))
	if response.Code != http.StatusUnauthorized || coordinator.decisionCalls != 1 {
		t.Fatalf("forged code=%d calls=%d", response.Code, coordinator.decisionCalls)
	}

	tampered := httptest.NewRequest(http.MethodPost, actionURI, strings.NewReader(string(body)))
	tampered.Header.Set(headerTimestamp, "1800000000")
	tampered.Header.Set(headerNonce, "tampered")
	tampered.Header.Set(headerSignature, signatureFor(token, "1800000000", "tampered", []byte("POST\n"+actionURI+"\n"+string(forged))))
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, tampered)
	if response.Code != http.StatusUnauthorized || coordinator.decisionCalls != 1 {
		t.Fatalf("tampered code=%d calls=%d", response.Code, coordinator.decisionCalls)
	}

	coordinator.value.State, coordinator.value.Version, coordinator.value.DecisionAt = governance.ConfirmationPending, 1, time.Time{}
	coordinator.decisionCalls = 0
	deny := []byte(`{"schema_version":1,"confirmation_id":"` + confirmationID + `","expected_version":1,"decision":"deny","external_user_id":"user-1","external_chat_id":"chat-1"}`)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, signedBrowserRequest(t, http.MethodPost, actionURI, deny, token, "actual-deny", now))
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"state":"denied"`) || coordinator.decisionCalls != 1 {
		t.Fatalf("deny code=%d body=%q calls=%d", response.Code, response.Body.String(), coordinator.decisionCalls)
	}
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, signedBrowserRequest(t, http.MethodPost, actionURI, body, token, "approve-after-deny", now))
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"state":"denied"`) || coordinator.decisionCalls != 1 {
		t.Fatalf("approve after deny code=%d body=%q calls=%d", response.Code, response.Body.String(), coordinator.decisionCalls)
	}
}

func signedBrowserRequest(t *testing.T, method, uri string, body []byte, token, nonce string, now time.Time) *http.Request {
	t.Helper()
	request := httptest.NewRequest(method, uri, strings.NewReader(string(body)))
	timestamp := "1800000000"
	payload := []byte(method + "\n" + uri)
	if body != nil {
		payload = append(payload, '\n')
		payload = append(payload, body...)
	}
	request.Header.Set(headerTimestamp, timestamp)
	request.Header.Set(headerNonce, nonce)
	request.Header.Set(headerSignature, signatureFor(token, timestamp, nonce, payload))
	return request
}
