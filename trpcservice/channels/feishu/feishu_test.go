package feishu

import (
	"context"
	"io"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

// mockConn feeds canned events and records sends.
type mockConn struct {
	msgs [][]byte
	sent []string
	idx  int
}

func (m *mockConn) Recv(_ context.Context) ([]byte, error) {
	if m.idx >= len(m.msgs) {
		return nil, io.EOF
	}
	b := m.msgs[m.idx]
	m.idx++
	return b, nil
}

func (m *mockConn) Send(_ context.Context, target, text string) error {
	m.sent = append(m.sent, target+":"+text)
	return nil
}

func (m *mockConn) Close() error { return nil }

const eventJSON1 = `{"header":{"event_id":"ev-1","event_type":"im.message.receive_v1"},"event":{"sender":{"sender_id":{"open_id":"ou_1"}},"message":{"message_id":"om_1","chat_id":"oc_1","chat_type":"p2p","message_type":"text","content":"{\"text\":\"first\"}"}}}`

const eventJSON2 = `{"header":{"event_id":"ev-2","event_type":"im.message.receive_v1"},"event":{"sender":{"sender_id":{"open_id":"ou_2"}},"message":{"message_id":"om_2","chat_id":"oc_2","chat_type":"p2p","message_type":"text","content":"{\"text\":\"second\"}"}}}`

// ------------------------------------------------------------- verification --

func TestVerifySignature(t *testing.T) {
	const (
		timestamp = "1600000000"
		nonce     = "abcdef"
		key       = "testencryptkey"
		// sha256(timestamp + nonce + encrypt_key) computed independently.
		signature = "eaec41a424ed117a7c6fec666ad8b350d1337f6d2806b92e66cec9e586b2f3d2"
	)
	if !VerifySignature(key, timestamp, nonce, signature) {
		t.Error("VerifySignature should accept a correct signature")
	}
	if VerifySignature(key, timestamp, nonce, "deadbeef") {
		t.Error("VerifySignature should reject a wrong signature")
	}
}

// ---------------------------------------------------------- normalization --

func TestToInboundP2PText(t *testing.T) {
	ev := &Event{
		Header: Header{EventID: "ev-1", EventType: "im.message.receive_v1"},
		Event: Body{
			Sender: Sender{SenderID: SenderID{OpenID: "ou_1"}},
			Message: Message{
				MessageID:   "om_1",
				ChatID:      "oc_1",
				ChatType:    "p2p",
				MessageType: "text",
				Content:     `{"text":"hello"}`,
			},
		},
	}
	got := ToInbound("t1", ev, "bot_openid")
	if got == nil {
		t.Fatal("ToInbound returned nil for a p2p message")
	}
	if got.TenantID != "t1" {
		t.Errorf("TenantID = %q", got.TenantID)
	}
	if got.UserID != "ou_1" {
		t.Errorf("UserID = %q", got.UserID)
	}
	if got.ChatType != channels.ChatTypeSingle {
		t.Errorf("ChatType = %q, want single", got.ChatType)
	}
	if got.Content != "hello" {
		t.Errorf("Content = %q, want %q", got.Content, "hello")
	}
	if got.SessionID != "t1:feishu:user:ou_1" {
		t.Errorf("SessionID = %q", got.SessionID)
	}
	if got.PlatformMsgID != "ev-1" {
		t.Errorf("PlatformMsgID = %q, want event_id ev-1", got.PlatformMsgID)
	}
}

func TestToInboundGroupWithMention(t *testing.T) {
	ev := &Event{
		Header: Header{EventID: "ev-2", EventType: "im.message.receive_v1"},
		Event: Body{
			Sender: Sender{SenderID: SenderID{OpenID: "ou_1"}},
			Message: Message{
				MessageID:   "om_2",
				ChatID:      "oc_group",
				ChatType:    "group",
				MessageType: "text",
				Content:     `{"text":"hello bot"}`,
				Mentions:    []Mention{{Key: "bot_openid", Name: "bot"}},
			},
		},
	}
	got := ToInbound("t1", ev, "bot_openid")
	if got == nil {
		t.Fatal("ToInbound returned nil for a group message that mentions the bot")
	}
	if got.ChatType != channels.ChatTypeGroup {
		t.Errorf("ChatType = %q, want group", got.ChatType)
	}
	if got.ChatID != "oc_group" {
		t.Errorf("ChatID = %q", got.ChatID)
	}
	if got.SessionID != "t1:feishu:group:oc_group" {
		t.Errorf("SessionID = %q", got.SessionID)
	}
}

func TestToInboundGroupWithoutMention(t *testing.T) {
	ev := &Event{
		Header: Header{EventID: "ev-3", EventType: "im.message.receive_v1"},
		Event: Body{
			Sender: Sender{SenderID: SenderID{OpenID: "ou_1"}},
			Message: Message{
				MessageID:   "om_3",
				ChatID:      "oc_group",
				ChatType:    "group",
				MessageType: "text",
				Content:     `{"text":"no mention"}`,
			},
		},
	}
	if got := ToInbound("t1", ev, "bot_openid"); got != nil {
		t.Error("group message without @bot must be filtered (nil)")
	}
}

// ---------------------------------------------------------------- adapter --

func TestAdapterStartNormalizesAndDedups(t *testing.T) {
	conn := &mockConn{msgs: [][]byte{[]byte(eventJSON1), []byte(eventJSON2), []byte(eventJSON1)}}
	a := New("t1", "bot_openid", conn)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- a.Start(ctx) }()

	first := <-a.Inbound()
	if first.Content != "first" || first.UserID != "ou_1" {
		t.Errorf("first inbound = %+v", first)
	}
	second := <-a.Inbound()
	if second.Content != "second" || second.UserID != "ou_2" {
		t.Errorf("second inbound = %+v", second)
	}

	if err := <-errCh; err != io.EOF {
		t.Fatalf("Start returned %v, want io.EOF", err)
	}
	// the duplicate (event_id ev-1) must have been filtered out
	select {
	case dup := <-a.Inbound():
		t.Errorf("duplicate event not filtered: %+v", dup)
	default:
	}
}

func TestAdapterSend(t *testing.T) {
	conn := &mockConn{}
	a := New("t1", "bot_openid", conn)
	msg := &channels.OutboundMessage{
		Inbound:  &channels.InboundMessage{ChatID: "oc_group"},
		Kind:     channels.KindText,
		Segments: []channels.Segment{{Type: "text", Text: "reply"}},
	}
	if err := a.Send(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if len(conn.sent) != 1 || conn.sent[0] != "oc_group:reply" {
		t.Errorf("sent = %v", conn.sent)
	}
}
