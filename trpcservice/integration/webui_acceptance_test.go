package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/webui"
	"github.com/liuzengh/trpc-agent-service/trpcservice/preprocess"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
)

func TestWebUIAdapterUsesDurableIMIngressContract(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	const tenantID, routeKey, accountID = "tenant-webui", "opaque-webui-route", "local-account"
	const token = "0123456789abcdef0123456789abcdef"
	secret, err := json.Marshal(webui.VerificationMaterial{Token: token, ExternalAccountID: accountID})
	if err != nil {
		t.Fatal(err)
	}
	adapter := &webui.Adapter{Protocol: webui.Verifier{Now: func() time.Time { return now }}}
	endpoint, intake, payloads := newIMEndpoint(t, adapter, tenantID, routeKey, accountID, webui.RouteKeyDigest(routeKey), secret, now)
	body, err := json.Marshal(map[string]any{"schema_version": 1, "external_account_id": accountID,
		"external_message_id": "browser-message-1", "external_user_id": "browser-user", "external_chat_id": "browser-chat",
		"conversation_type": "p2p", "message_type": "text", "text": "hello from browser", "occurred_at": now})
	if err != nil {
		t.Fatal(err)
	}
	request := func(signingToken, nonce string) *http.Request {
		value := httptest.NewRequest(http.MethodPost, "/callbacks/webui?route_key="+routeKey, bytes.NewReader(body))
		value.Header.Set("Content-Type", "application/json")
		value.Header.Set("X-WebUI-Timestamp", "1800000000")
		value.Header.Set("X-WebUI-Nonce", nonce)
		value.Header.Set("X-WebUI-Signature", webui.SignCallback(signingToken, "1800000000", nonce, body))
		return value
	}

	tampered := httptest.NewRecorder()
	endpoint.ServeHTTP(tampered, request("wrong-token-000000000000", "tampered"))
	if tampered.Code != http.StatusUnauthorized {
		t.Fatalf("tampered code=%d body=%q", tampered.Code, tampered.Body.String())
	}

	const attempts = 200
	start := make(chan struct{})
	failures := make(chan int, attempts)
	var wait sync.WaitGroup
	for index := 0; index < attempts; index++ {
		index := index
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			response := httptest.NewRecorder()
			endpoint.ServeHTTP(response, request(token, "nonce-"+time.Unix(int64(index), 0).Format("150405.000000000")))
			if response.Code != http.StatusOK {
				failures <- response.Code
			}
		}()
	}
	close(start)
	wait.Wait()
	close(failures)
	if code, failed := <-failures; failed {
		t.Fatalf("duplicate callback code=%d", code)
	}
	jobs, err := intake.ClaimJobs(context.Background(), preprocess.ClaimOptions{Owner: "webui-acceptance", Now: now, TTL: time.Minute, Limit: 2})
	if err != nil || len(jobs) != 1 {
		t.Fatalf("jobs=%#v err=%v", jobs, err)
	}
	key := messaging.InboxKey{TenantID: tenantID, Channel: "webui", ExternalAccountID: accountID, ExternalMessageID: "browser-message-1"}
	requestID, _ := messaging.StableInboxIdentity(key)
	record, err := payloads.GetPayload(context.Background(), tenantID, requestID)
	if err != nil || record.RequestID != jobs[0].RequestID || !bytes.Contains(record.Content, []byte("hello from browser")) {
		t.Fatalf("payload=%#v err=%v", record, err)
	}
}
