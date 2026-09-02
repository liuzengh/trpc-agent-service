package wecom

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"encoding/binary"
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

const textXML1 = `<xml><FromUserName><![CDATA[openid-1]]></FromUserName><MsgType><![CDATA[text]]></MsgType><Content><![CDATA[first]]></Content><MsgId><![CDATA[1001]]></MsgId></xml>`

const textXML2 = `<xml><FromUserName><![CDATA[openid-2]]></FromUserName><MsgType><![CDATA[text]]></MsgType><Content><![CDATA[second]]></Content><MsgId><![CDATA[1002]]></MsgId></xml>`

// ------------------------------------------------------------- verification --

func TestVerifySignature(t *testing.T) {
	const (
		token     = "testtoken"
		timestamp = "1409659589"
		nonce     = "263014780"
		echostr   = "P9nAzCzyDtyTWESHep1vC5X9xho/qYX3Zpb4yKa9SKld1DsH3Iyt3tP3zNdtp+4RPcs8TgAE7OaBO+FZXvnaqQ=="
		// sha1(sort(token, timestamp, nonce, echostr)) computed independently.
		signature = "6acb33868c585f3cb79dde9894e2033c9269da4d"
	)
	if !VerifySignature(token, timestamp, nonce, echostr, signature) {
		t.Error("VerifySignature should accept a correct signature")
	}
	if VerifySignature(token, timestamp, nonce, echostr, "deadbeef") {
		t.Error("VerifySignature should reject a wrong signature")
	}
}

func TestDecryptMsgRoundTrip(t *testing.T) {
	const (
		encodingAESKey = "jWmYm7qr5nMoAUwZRjGtBxmz3KA1tkAj3ykkR6q2B2C"
		plain          = "<xml><ToUserName><![CDATA[ww]]></ToUserName></xml>"
	)
	encrypted := encryptForTest(t, encodingAESKey, plain)
	got, err := DecryptMsg(encodingAESKey, encrypted)
	if err != nil {
		t.Fatalf("DecryptMsg: %v", err)
	}
	if got != plain {
		t.Errorf("DecryptMsg = %q, want %q", got, plain)
	}
}

// encryptForTest mirrors the WeCom encryption scheme for round-trip checks:
// plain = random(16) + len(4, big-endian) + msg + receiveid, PKCS7 padded,
// AES-256-CBC with IV = key[:16].
func encryptForTest(t *testing.T, encodingAESKey, plain string) string {
	t.Helper()
	rawKey, err := base64.StdEncoding.DecodeString(encodingAESKey + "=")
	if err != nil {
		t.Fatalf("decode key: %v", err)
	}
	const receiveID = "corpid"
	buf := make([]byte, 16+4+len(plain)+len(receiveID))
	copy(buf[0:16], "0123456789abcdef")
	binary.BigEndian.PutUint32(buf[16:20], uint32(len(plain)))
	copy(buf[20:20+len(plain)], plain)
	copy(buf[20+len(plain):], receiveID)

	blockSize := aes.BlockSize
	padLen := blockSize - len(buf)%blockSize
	buf = append(buf, make([]byte, padLen)...)
	for i := len(buf) - padLen; i < len(buf); i++ {
		buf[i] = byte(padLen)
	}

	block, err := aes.NewCipher(rawKey)
	if err != nil {
		t.Fatalf("new cipher: %v", err)
	}
	out := make([]byte, len(buf))
	cipher.NewCBCEncrypter(block, rawKey[:16]).CryptBlocks(out, buf)
	return base64.StdEncoding.EncodeToString(out)
}

// ---------------------------------------------------------- normalization --

func TestToInboundSingleChat(t *testing.T) {
	msg := &Message{
		FromUserName: "openid-1",
		MsgType:      "text",
		Content:      "hello",
		MsgId:        "6537597555634206977",
		AgentID:      "1000002",
	}
	got := ToInbound("t1", msg)
	if got.TenantID != "t1" {
		t.Errorf("TenantID = %q", got.TenantID)
	}
	if got.UserID != "openid-1" {
		t.Errorf("UserID = %q", got.UserID)
	}
	if got.ChatType != channels.ChatTypeSingle {
		t.Errorf("ChatType = %q, want single", got.ChatType)
	}
	if got.PlatformMsgID != "6537597555634206977" {
		t.Errorf("PlatformMsgID = %q", got.PlatformMsgID)
	}
	if got.SessionID != "t1:wecom:user:openid-1" {
		t.Errorf("SessionID = %q", got.SessionID)
	}
}

func TestToInboundGroupChat(t *testing.T) {
	msg := &Message{
		FromUserName: "openid-1",
		MsgType:      "text",
		Content:      "hello group",
		MsgId:        "msg-2",
		ChatId:       "chat-456",
	}
	got := ToInbound("t1", msg)
	if got.ChatType != channels.ChatTypeGroup {
		t.Errorf("ChatType = %q, want group", got.ChatType)
	}
	if got.ChatID != "chat-456" {
		t.Errorf("ChatID = %q", got.ChatID)
	}
	if got.SessionID != "t1:wecom:group:chat-456" {
		t.Errorf("SessionID = %q", got.SessionID)
	}
}

// ---------------------------------------------------------------- adapter --

func TestAdapterStartNormalizesAndDedups(t *testing.T) {
	conn := &mockConn{msgs: [][]byte{[]byte(textXML1), []byte(textXML2), []byte(textXML1)}}
	a := New("t1", conn)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- a.Start(ctx) }()

	first := <-a.Inbound()
	if first.Content != "first" || first.UserID != "openid-1" {
		t.Errorf("first inbound = %+v", first)
	}
	second := <-a.Inbound()
	if second.Content != "second" || second.UserID != "openid-2" {
		t.Errorf("second inbound = %+v", second)
	}

	if err := <-errCh; err != io.EOF {
		t.Fatalf("Start returned %v, want io.EOF", err)
	}
	// the duplicate (msgId 1001) must have been filtered out
	select {
	case dup := <-a.Inbound():
		t.Errorf("duplicate message not filtered: %+v", dup)
	default:
	}
}

func TestAdapterSend(t *testing.T) {
	conn := &mockConn{}
	a := New("t1", conn)
	msg := &channels.OutboundMessage{
		Inbound:  &channels.InboundMessage{ChatID: "openid-1"},
		Kind:     channels.KindText,
		Segments: []channels.Segment{{Type: "text", Text: "hello"}},
	}
	if err := a.Send(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if len(conn.sent) != 1 || conn.sent[0] != "openid-1:hello" {
		t.Errorf("sent = %v", conn.sent)
	}
}
