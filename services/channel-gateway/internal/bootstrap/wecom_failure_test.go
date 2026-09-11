package bootstrap

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	admission "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/admission/domain"
	connection "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/application"
	connDomain "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain"
)

type failingAcceptance struct{}

func (failingAcceptance) AcceptInbound(context.Context, admission.Inbound) (admission.Receipt, error) {
	return admission.Receipt{}, admission.ErrUnavailable
}
func TestWeComTemporaryAdmissionFailureIsRecoverable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer ws.CloseNow()
		_, data, err := ws.Read(ctx)
		if err != nil {
			return
		}
		var request struct {
			Headers map[string]string `json:"headers"`
		}
		if json.Unmarshal(data, &request) != nil {
			return
		}
		ack, _ := json.Marshal(map[string]any{"headers": request.Headers, "errcode": 0})
		if ws.Write(ctx, websocket.MessageText, ack) != nil {
			return
		}
		payload := `{"cmd":"aibot_msg_callback","headers":{"req_id":"request-1"},"body":{"msgid":"message-1","aibotid":"fixture-bot","msgtype":"text","chattype":"single","from":{"userid":"fixture-user"},"text":{"content":"hello"}}}`
		if ws.Write(ctx, websocket.MessageText, []byte(payload)) != nil {
			return
		}
		for {
			if _, _, err = ws.Read(ctx); err != nil {
				return
			}
		}
	}))
	defer server.Close()
	f := wecomClientFactory{acceptor: failingAcceptance{}, url: "ws" + strings.TrimPrefix(server.URL, "http")}
	c, err := f.New(ctx, connDomain.Account{ID: "wecom-account", BotID: "fixture-bot", Revision: 1, Enabled: true, CredentialRef: "TEST_SECRET"}, connDomain.OwnerGrant{InstanceID: "fixture-instance", Epoch: 1, Revision: 1}, connection.CredentialMaterial{Secret: "fixture-only"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		_ = c.Close(cleanup)
	}()
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("bounded handler failure did not end client")
	}
	s := c.Status()
	if !s.Terminal || !s.Retryable || s.Replaced {
		t.Fatalf("temporary admission failure incorrectly made terminal permanent: %+v", s)
	}
}
