package feishu

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	larkws "github.com/larksuite/oapi-sdk-go/v3/ws"
	"github.com/stretchr/testify/require"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

// connection is the platform side of one long connection.
type connection struct {
	platform *platform

	upgraded chan struct{}

	acks chan acknowledgement

	mu     sync.Mutex
	socket *websocket.Conn
}

type acknowledgement struct {
	messageID string
	code      int
}

func serveConnection(t *testing.T, p *platform) *connection {
	t.Helper()
	c := &connection{
		platform: p,
		upgraded: make(chan struct{}),
		acks:     make(chan acknowledgement, 32),
	}
	p.answer(bootstrapPath, func(w http.ResponseWriter, _ *http.Request) {

		writeJSON(w, http.StatusOK, map[string]any{
			"code": 0,
			"data": map[string]any{
				"URL": "ws" + strings.TrimPrefix(p.server.URL, "http") +
					socketPath + "?device_id=test-device&service_id=1",
			},
		})
	})
	p.answer(socketPath, func(w http.ResponseWriter, r *http.Request) {
		socket, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		c.mu.Lock()
		first := c.socket == nil
		c.socket = socket
		c.mu.Unlock()
		if first {
			close(c.upgraded)
		}
		go c.read(socket)
	})
	return c
}

func (c *connection) read(socket *websocket.Conn) {
	for {
		messageType, raw, err := socket.ReadMessage()
		if err != nil {
			return
		}
		if messageType != websocket.BinaryMessage {
			continue
		}
		var frame larkws.Frame
		if err := frame.Unmarshal(raw); err != nil {
			continue
		}
		if larkws.FrameType(frame.Method) != larkws.FrameTypeData {
			continue
		}
		var response larkws.Response
		if err := json.Unmarshal(frame.Payload, &response); err != nil {
			continue
		}
		c.acks <- acknowledgement{
			messageID: larkws.Headers(frame.Headers).GetString(larkws.HeaderMessageID),
			code:      response.StatusCode,
		}
	}
}

func (c *connection) deliver(t *testing.T, messageID string, payload []byte) {
	t.Helper()
	select {
	case <-c.upgraded:
	case <-time.After(10 * time.Second):
		t.Fatal("the adapter never connected")
	}
	frame := larkws.Frame{
		SeqID:   1,
		LogID:   1,
		Service: 1,
		Method:  int32(larkws.FrameTypeData),
		Headers: []larkws.Header{
			{Key: larkws.HeaderType, Value: string(larkws.MessageTypeEvent)},
			{Key: larkws.HeaderMessageID, Value: messageID},
			{Key: larkws.HeaderSum, Value: "1"},
			{Key: larkws.HeaderSeq, Value: "0"},
		},
		Payload: payload,
	}
	raw, err := frame.Marshal()
	require.NoError(t, err)
	c.mu.Lock()
	socket := c.socket
	c.mu.Unlock()
	require.NoError(t, socket.WriteMessage(websocket.BinaryMessage, raw))
}

func (c *connection) waitAck(t *testing.T) acknowledgement {
	t.Helper()
	select {
	case ack := <-c.acks:
		return ack
	case <-time.After(10 * time.Second):
		t.Fatal("the adapter never acknowledged the event")
		return acknowledgement{}
	}
}

func event(edit func(header, sender, message map[string]any)) []byte {
	header := map[string]any{
		"event_id":    "ev_1",
		"event_type":  "im.message.receive_v1",
		"create_time": "1757260800000",
		"token":       "",
		"app_id":      testAppID,
		"tenant_key":  testTenantKey,
	}
	sender := map[string]any{
		"sender_id":   map[string]any{"open_id": testOpenID},
		"sender_type": "user",
		"tenant_key":  testTenantKey,
	}
	message := map[string]any{
		"message_id":   testMessageID,
		"chat_id":      testChatID,
		"chat_type":    "p2p",
		"message_type": "text",
		"content":      `{"text":"how do I reset my password?"}`,
	}
	if edit != nil {
		edit(header, sender, message)
	}
	raw, err := json.Marshal(map[string]any{
		"schema": "2.0",
		"header": header,
		"event":  map[string]any{"sender": sender, "message": message},
	})
	if err != nil {
		panic(err)
	}
	return raw
}

func runServe(
	t *testing.T,
	adapter *Adapter,
	ctx context.Context,
	accept channels.AcceptFunc,
) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- adapter.Serve(ctx, accept) }()
	return done
}

func waitServe(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(20 * time.Second):
		t.Fatal("Serve did not return")
		return nil
	}
}

func TestServeRecordsOnlyDirectUserTextFromTheBoundApp(t *testing.T) {

	cases := []struct {
		name     string
		edit     func(header, sender, message map[string]any)
		accepted bool
	}{
		{"a direct text message", nil, true},
		{"another app's event", func(h, _, _ map[string]any) {
			h["app_id"] = "cli_someone_else"
		}, false},
		{"another enterprise in the header", func(h, _, _ map[string]any) {
			h["tenant_key"] = "tk_other"
		}, false},
		{"another enterprise on the sender", func(_, s, _ map[string]any) {
			s["tenant_key"] = "tk_other"
		}, false},
		{"a message from a bot", func(_, s, _ map[string]any) {
			s["sender_type"] = "bot"
		}, false},
		{"a sender with no open id", func(_, s, _ map[string]any) {
			s["sender_id"] = map[string]any{"user_id": "u_1"}
		}, false},
		{"a group message", func(_, _, m map[string]any) {
			m["chat_type"] = "group"
		}, false},
		{"an image", func(_, _, m map[string]any) {
			m["message_type"] = "image"
			m["content"] = `{"image_key":"img_1"}`
		}, false},
		{"content that is not JSON", func(_, _, m map[string]any) {
			m["content"] = "how do I reset my password?"
		}, false},
		{"content with no text field", func(_, _, m map[string]any) {
			m["content"] = `{"image_key":"img_1"}`
		}, false},
		{"text that is only whitespace", func(_, _, m map[string]any) {
			m["content"] = `{"text":"   \n "}`
		}, false},
		{"a message id shaped like a path", func(_, _, m map[string]any) {
			m["message_id"] = "../../om_victim"
		}, false},
		{"an oversized chat id", func(_, _, m map[string]any) {
			m["chat_id"] = strings.Repeat("c", 257)
		}, false},
		{"no chat id at all", func(_, _, m map[string]any) {
			delete(m, "chat_id")
		}, false},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			p := newPlatform(t)
			link := serveConnection(t, p)
			adapter, err := NewAdapter(newTestClient(t, p), nil)
			require.NoError(t, err)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			recorded := make(chan channels.InboundEnvelope, 4)
			done := runServe(t, adapter, ctx, func(_ context.Context, e channels.InboundEnvelope) error {
				recorded <- e
				return nil
			})

			link.deliver(t, "frame-1", event(test.edit))
			require.Equal(t, http.StatusOK, link.waitAck(t).code)
			var envelope channels.InboundEnvelope
			accepted := false
			select {
			case envelope = <-recorded:
				accepted = true
			default:
			}
			require.Equal(t, test.accepted, accepted)
			if !test.accepted {
				cancel()
				waitServe(t, done)
				return
			}

			binding := testBinding()
			principal := principalID(binding, testTenantKey, testOpenID)
			want := channels.InboundEnvelope{
				TenantID:         testTenantID,
				Channel:          channels.ChannelFeishu,
				ChannelBindingID: testBindingID,
				AgentAppID:       testAgentAppID,
				PrincipalID:      principal,
				SessionID:        directSessionID(binding, testTenantKey, principal, testChatID),
				ExternalEventID:  testMessageID,
				ReceivedAt:       envelope.ReceivedAt,
				Message:          channels.InboundMessage{Text: "how do I reset my password?"},
				DeliveryTarget:   envelope.DeliveryTarget,
			}
			if envelope.TenantID != want.TenantID || envelope.Channel != want.Channel ||
				envelope.ChannelBindingID != want.ChannelBindingID ||
				envelope.AgentAppID != want.AgentAppID ||
				envelope.PrincipalID != want.PrincipalID ||
				envelope.SessionID != want.SessionID ||
				envelope.ExternalEventID != want.ExternalEventID ||
				envelope.Message.Text != want.Message.Text ||
				len(envelope.Message.Attachments) != 0 {
				t.Fatalf("envelope = %+v, want %+v", envelope, want)
			}
			require.False(t, envelope.ReceivedAt.IsZero(), "the envelope has no arrival time")
			require.NoError(t, envelope.Validate(adapter.Identity().Scope()),
				"the accepted envelope is not storable")
			messageID, err := decodeTarget(binding, testTenantKey, envelope.DeliveryTarget)
			require.NoError(t, err)
			require.Equal(t, testMessageID, messageID)
			cancel()
			waitServe(t, done)
		})
	}
}

func TestServeAcknowledgesOnlyAfterTheEventIsRecorded(t *testing.T) {
	p := newPlatform(t)
	link := serveConnection(t, p)
	adapter, err := NewAdapter(newTestClient(t, p), nil)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	inside, release := make(chan struct{}), make(chan struct{})
	done := runServe(t, adapter, ctx, func(context.Context, channels.InboundEnvelope) error {
		close(inside)
		<-release
		return nil
	})

	link.deliver(t, "frame-1", event(nil))
	select {
	case <-inside:
	case <-time.After(10 * time.Second):
		t.Fatal("the event never reached the recorder")
	}

	select {
	case ack := <-link.acks:
		t.Fatalf("acknowledged with %d before the event was recorded", ack.code)
	case <-time.After(200 * time.Millisecond):
	}
	close(release)
	require.Equal(t, acknowledgement{messageID: "frame-1", code: http.StatusOK}, link.waitAck(t))
	cancel()
	waitServe(t, done)
}

func TestServeNeverAcknowledgesSuccessWhenTheRecordFailed(t *testing.T) {
	p := newPlatform(t)
	link := serveConnection(t, p)
	adapter, err := NewAdapter(newTestClient(t, p), nil)
	require.NoError(t, err)
	refusal := errors.New("store: the session lease was lost")
	var calls int
	var callsMu sync.Mutex
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := runServe(t, adapter, ctx, func(context.Context, channels.InboundEnvelope) error {
		callsMu.Lock()
		calls++
		callsMu.Unlock()
		return refusal
	})

	link.deliver(t, "frame-1", event(nil))

	require.ErrorIs(t, waitServe(t, done), refusal)
	// Stopping the run to drain the callbacks closes the socket, so the platform may see a
	// 500 or no answer at all and will redeliver either way.
	for {
		select {
		case ack := <-link.acks:
			if ack.code == http.StatusOK {
				t.Fatalf("acknowledged %s as recorded after the write failed", ack.messageID)
			}
		case <-time.After(200 * time.Millisecond):
			callsMu.Lock()
			defer callsMu.Unlock()
			if calls != 1 {
				t.Fatalf("the recorder was called %d times, want 1", calls)
			}
			return
		}
	}
}

func TestServeReturnsOnlyAfterEveryCallbackHasFinished(t *testing.T) {
	p := newPlatform(t)
	link := serveConnection(t, p)
	adapter, err := NewAdapter(newTestClient(t, p), nil)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	inside := make(chan struct{})
	var finished, returnedEarly bool
	var mu sync.Mutex
	done := runServe(t, adapter, ctx, func(context.Context, channels.InboundEnvelope) error {
		close(inside)

		time.Sleep(300 * time.Millisecond)
		mu.Lock()
		if returnedEarly {
			t.Error("Serve returned while a callback was still writing")
		}
		finished = true
		mu.Unlock()
		return nil
	})

	link.deliver(t, "frame-1", event(nil))
	select {
	case <-inside:
	case <-time.After(10 * time.Second):
		t.Fatal("the event never reached the recorder")
	}
	cancel()
	err = waitServe(t, done)
	mu.Lock()
	if !finished {
		returnedEarly = true
	}
	mu.Unlock()
	require.True(t, finished, "Serve returned before the callback finished")
	require.ErrorIs(t, err, context.Canceled)
}

func TestServeIsSingleUse(t *testing.T) {
	p := newPlatform(t)
	serveConnection(t, p)
	adapter, err := NewAdapter(newTestClient(t, p), nil)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := runServe(t, adapter, ctx, func(context.Context, channels.InboundEnvelope) error {
		return nil
	})
	cancel()
	waitServe(t, done)

	err = adapter.Serve(context.Background(), func(context.Context, channels.InboundEnvelope) error {
		return nil
	})
	require.ErrorIs(t, err, ErrServing, "the second Serve")
}

func TestServeNeedsSomewhereToRecord(t *testing.T) {
	p := newPlatform(t)
	adapter, err := NewAdapter(newTestClient(t, p), nil)
	require.NoError(t, err)
	require.ErrorIs(t, adapter.Serve(context.Background(), nil), ErrConfig)
}

func TestServeReportsAStoppedConnectionWithoutQuotingThePlatform(t *testing.T) {
	p := newPlatform(t)
	serveConnection(t, p)

	p.answer(bootstrapPath, func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"code": 403,
			"msg":  "long connection not enabled for cli_a1b2c3d4 at open.internal.example",
		})
	})
	adapter, err := NewAdapter(newTestClient(t, p), func() {
		t.Error("the ready callback fired on a connection that never opened")
	})
	require.NoError(t, err)
	failure := adapter.Serve(context.Background(), func(context.Context, channels.InboundEnvelope) error {
		t.Error("an event was recorded on a connection that never opened")
		return nil
	})
	require.ErrorIs(t, failure, ErrConnection)
	for _, quoted := range []string{"cli_a1b2c3d4", "open.internal.example", "not enabled", testSecret} {
		require.NotContains(t, failure.Error(), quoted, "the error quoted the platform")
	}
}

func TestServeReportsAReadyConnectionWithoutSayingAnythingAboutIt(t *testing.T) {
	p := newPlatform(t)
	serveConnection(t, p)
	ready := make(chan struct{}, 2)
	adapter, err := NewAdapter(newTestClient(t, p), func() { ready <- struct{}{} })
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := runServe(t, adapter, ctx, func(context.Context, channels.InboundEnvelope) error {
		return nil
	})
	select {
	case <-ready:
	case <-time.After(10 * time.Second):
		t.Fatal("the connection never reported itself ready")
	}
	cancel()
	waitServe(t, done)
}

func TestSendPutsOneStoredAnswerOnTheWire(t *testing.T) {
	p := newPlatform(t)
	client := newTestClient(t, p)
	adapter, err := NewAdapter(client, nil)
	require.NoError(t, err)
	target, err := encodeTarget(client.binding, testTenantKey, testMessageID)
	require.NoError(t, err)
	const answer = "Open Settings → Security, then choose \"Reset password\"."
	report := adapter.Send(context.Background(), target,
		channels.OutboundMessage{Text: answer})
	require.Equal(t, channels.Delivered, report.Outcome)
	require.Equal(t, "om_reply", report.ExternalMessageID)
	replies := p.seenPath(replyPathPrefix)
	require.Len(t, replies, 1)
	sent := replies[0]
	require.Equal(t, http.MethodPost, sent.method)
	require.Equal(t, replyPathPrefix+testMessageID+"/reply", sent.path)
	require.Equal(t, "Bearer t-live", sent.auth)
	var body replyRequest
	require.NoError(t, json.Unmarshal(sent.body, &body))
	require.Equal(t, messageTypeText, body.MsgType)
	// The text is carried as a JSON object serialized into content, and it is the
	// stored bytes: not shortened, not escaped twice, not decorated.
	var content textContent
	require.NoError(t, json.Unmarshal([]byte(body.Content), &content))
	require.Equal(t, answer, content.Text)
	require.Equal(t, replyUUID(client.binding, testTenantKey, testMessageID), body.UUID)
	require.Len(t, p.seenPath(tokenPath), 2, "one exchange at startup, one to send")
	require.NotContains(t, string(sent.body), testSecret, "the reply carried the app secret")
}

func TestSendReportsWhatThePlatformSaidAndNothingElse(t *testing.T) {

	cases := []struct {
		name   string
		answer http.HandlerFunc
		want   channels.DeliveryOutcome
	}{
		{"a stated refusal", func(w http.ResponseWriter, _ *http.Request) {
			writeJSON(w, http.StatusOK, map[string]any{"code": 230001, "msg": "bot is not in the chat"})
		}, channels.DeliveryRejected},
		{"a refusal with a client error status", func(w http.ResponseWriter, _ *http.Request) {
			writeJSON(w, http.StatusBadRequest, map[string]any{"code": 230001})
		}, channels.DeliveryRejected},
		{"a server error", func(w http.ResponseWriter, _ *http.Request) {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"code": 0})
		}, channels.DeliveryUnknown},
		{"a body that is not JSON", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("<html>gateway</html>"))
		}, channels.DeliveryUnknown},
		{"a body that states no code", func(w http.ResponseWriter, _ *http.Request) {
			writeJSON(w, http.StatusOK, map[string]any{"data": map[string]any{"message_id": "om_x"}})
		}, channels.DeliveryUnknown},
		{"a success that names no message", func(w http.ResponseWriter, _ *http.Request) {
			writeJSON(w, http.StatusOK, map[string]any{"code": 0, "data": map[string]any{}})
		}, channels.DeliveryUnknown},
		{"a success naming an unusable message id", func(w http.ResponseWriter, _ *http.Request) {
			writeJSON(w, http.StatusOK, map[string]any{
				"code": 0, "data": map[string]any{"message_id": strings.Repeat("m", 300)},
			})
		}, channels.DeliveryUnknown},
		{"a status this adapter never asked for", func(w http.ResponseWriter, _ *http.Request) {
			writeJSON(w, http.StatusAccepted, map[string]any{"code": 0})
		}, channels.DeliveryUnknown},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			p := newPlatform(t)
			client := newTestClient(t, p)
			adapter, err := NewAdapter(client, nil)
			require.NoError(t, err)
			p.answer(replyPathPrefix, test.answer)
			target, err := encodeTarget(client.binding, testTenantKey, testMessageID)
			require.NoError(t, err)
			report := adapter.Send(context.Background(), target,
				channels.OutboundMessage{Text: "answer"})
			require.Equal(t, test.want, report.Outcome)
			require.Empty(t, report.ExternalMessageID, "the report carried a platform id")
			require.Len(t, p.seenPath(replyPathPrefix), 1, "one attempt, whatever it returned")
		})
	}
}

func TestSendDoesNotFollowARedirect(t *testing.T) {
	p := newPlatform(t)
	client := newTestClient(t, p)
	adapter, err := NewAdapter(client, nil)
	require.NoError(t, err)
	// A redirect is the cheapest way to move a reply — and the token that carries
	// it — to another origin.
	p.answer(replyPathPrefix, func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, tenantQueryPath, http.StatusTemporaryRedirect)
	})
	target, err := encodeTarget(client.binding, testTenantKey, testMessageID)
	require.NoError(t, err)
	before := len(p.seenPath(tenantQueryPath))
	report := adapter.Send(context.Background(), target,
		channels.OutboundMessage{Text: "answer"})
	require.Equal(t, channels.DeliveryUnknown, report.Outcome)
	require.Equal(t, before, len(p.seenPath(tenantQueryPath)), "Send followed the redirect")
}

// The SDK fetches its websocket endpoint with a POST that carries the app
// secret. That request goes through this package's client, so a redirect does
// not move the credential to another origin.
func TestServeDoesNotFollowARedirectAwayFromTheBoundOrigin(t *testing.T) {
	p := newPlatform(t)
	var alternateCalls atomic.Int64
	alternate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		alternateCalls.Add(1)
		writeJSON(w, http.StatusOK, map[string]any{"code": 0})
	}))
	defer alternate.Close()
	p.answer(bootstrapPath, func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, alternate.URL+bootstrapPath, http.StatusTemporaryRedirect)
	})
	client := newTestClient(t, p)
	adapter, err := NewAdapter(client, nil)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := runServe(t, adapter, ctx, func(context.Context, channels.InboundEnvelope) error {
		return nil
	})
	require.Eventually(t, func() bool { return len(p.seenPath(bootstrapPath)) > 0 },
		10*time.Second, 10*time.Millisecond, "the SDK never asked for an endpoint")
	cancel()
	require.ErrorIs(t, waitServe(t, done), context.Canceled)
	require.Zero(t, alternateCalls.Load(), "the bootstrap credential left the bound origin")
}

func TestSendRefusesAnAnswerItCannotPutOnTheWire(t *testing.T) {
	p := newPlatform(t)
	client := newTestClient(t, p)
	adapter, err := NewAdapter(client, nil)
	require.NoError(t, err)
	usable, err := encodeTarget(client.binding, testTenantKey, testMessageID)
	require.NoError(t, err)
	foreign, err := encodeTarget(Binding{
		TenantID: "other", AgentAppID: testAgentAppID, BindingID: testBindingID,
		AppID: testAppID, SecretRef: testSecretRef,
	}, testTenantKey, testMessageID)
	require.NoError(t, err)

	otherApp, err := encodeTarget(Binding{
		TenantID: testTenantID, AgentAppID: "other", BindingID: testBindingID,
		AppID: testAppID, SecretRef: testSecretRef,
	}, testTenantKey, testMessageID)
	require.NoError(t, err)
	cases := []struct {
		name    string
		target  channels.DeliveryTarget
		message channels.OutboundMessage
		want    channels.DeliveryOutcome
	}{
		{"a target from another tenant", foreign,
			channels.OutboundMessage{Text: "answer"},
			channels.DeliveryTargetStale},
		{"a target from another agent app", otherApp,
			channels.OutboundMessage{Text: "answer"},
			channels.DeliveryTargetStale},
		{"no target at all", channels.DeliveryTarget{},
			channels.OutboundMessage{Text: "answer"},
			channels.DeliveryTargetStale},
		{"an empty answer", usable,
			channels.OutboundMessage{Text: ""},
			channels.DeliveryRejected},
		{"an answer longer than the limit", usable,
			channels.OutboundMessage{Text: strings.Repeat("a", maxReplyTextBytes+1)},
			channels.DeliveryRejected},
		{"an answer that is not valid UTF-8", usable,
			channels.OutboundMessage{Text: "\xff\xfe"},
			channels.DeliveryRejected},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			before := len(p.seenPath(replyPathPrefix))
			beforeToken := len(p.seenPath(tokenPath))
			report := adapter.Send(context.Background(), test.target, test.message)
			require.Equal(t, test.want, report.Outcome)
			// Nothing was attempted: a refusal this adapter can make on its own is
			// made before the platform is asked, and before a token is fetched.
			require.Equal(t, before, len(p.seenPath(replyPathPrefix)))
			require.Equal(t, beforeToken, len(p.seenPath(tokenPath)))
		})
	}
	require.Equal(t, maxReplyTextBytes, adapter.ReplyTextLimit())
}

func TestSendReportsAnUnusableCredentialAsUnknown(t *testing.T) {
	p := newPlatform(t)
	client := newTestClient(t, p)
	adapter, err := NewAdapter(client, nil)
	require.NoError(t, err)
	p.answer(tokenPath, func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"code": 10003})
	})
	target, err := encodeTarget(client.binding, testTenantKey, testMessageID)
	require.NoError(t, err)
	report := adapter.Send(context.Background(), target,
		channels.OutboundMessage{Text: "answer"})
	require.Equal(t, channels.DeliveryUnknown, report.Outcome)
	require.Empty(t, p.seenPath(replyPathPrefix), "a reply was sent without a credential")
}

func TestNewAdapterNeedsAConfirmedClient(t *testing.T) {
	_, err := NewAdapter(nil, nil)
	require.ErrorIs(t, err, ErrConfig)
	_, err = NewAdapter(&Client{binding: testBinding()}, nil)
	require.ErrorIs(t, err, ErrConfig, "an enterprise the platform never confirmed")
	p := newPlatform(t)
	adapter, err := NewAdapter(newTestClient(t, p), nil)
	require.NoError(t, err)
	require.Equal(t, channels.BindingIdentity{
		TenantID:   testTenantID,
		AgentAppID: testAgentAppID,
		BindingID:  testBindingID,
		Channel:    channels.ChannelFeishu,
	}, adapter.Identity())
}
