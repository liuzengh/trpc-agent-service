// Package wecom adapts the enterprise WeChat platform into the unified
// channels contract. It provides callback signature verification, AES message
// decryption, normalization of inbound WeCom messages, and the Adapter that
// assembles those pieces on top of a channels.Conn.
package wecom

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

// Name is the stable channel identifier used by this adapter.
const Name = "wecom"

// ---------------------------------------------------------------- protocol --

// VerifySignature checks a WeCom callback signature:
//
//	signature = sha1(sort(token, timestamp, nonce, encrypt))
//
// encrypt is the echostr for URL validation or the encrypted body for
// message callbacks.
func VerifySignature(token, timestamp, nonce, encrypt, signature string) bool {
	parts := []string{token, timestamp, nonce, encrypt}
	sort.Strings(parts)
	sum := sha1.Sum([]byte(strings.Join(parts, "")))
	return hex.EncodeToString(sum[:]) == signature
}

// DecryptMsg decrypts a WeCom encrypted message and returns the inner XML
// payload (msg), stripping the random prefix, length prefix and receiveid.
// encodingAESKey is the 43-char base64 key from the WeCom admin console.
func DecryptMsg(encodingAESKey, encrypt string) (string, error) {
	rawKey, err := base64.StdEncoding.DecodeString(encodingAESKey + "=")
	if err != nil {
		return "", fmt.Errorf("wecom: decode aes key: %w", err)
	}
	if len(rawKey) != 32 {
		return "", fmt.Errorf("wecom: invalid aes key length %d", len(rawKey))
	}
	ciphertext, err := base64.StdEncoding.DecodeString(encrypt)
	if err != nil {
		return "", fmt.Errorf("wecom: decode ciphertext: %w", err)
	}
	if len(ciphertext) == 0 || len(ciphertext)%aes.BlockSize != 0 {
		return "", fmt.Errorf("wecom: invalid ciphertext length %d", len(ciphertext))
	}

	block, err := aes.NewCipher(rawKey)
	if err != nil {
		return "", fmt.Errorf("wecom: new cipher: %w", err)
	}
	plain := make([]byte, len(ciphertext))
	cipher.NewCBCDecrypter(block, rawKey[:aes.BlockSize]).CryptBlocks(plain, ciphertext)

	plain, err = pkcs7Unpad(plain)
	if err != nil {
		return "", err
	}
	// plain = random(16) + msg_len(4, big-endian) + msg + receiveid
	if len(plain) < 20 {
		return "", fmt.Errorf("wecom: plaintext too short (%d)", len(plain))
	}
	msgLen := int(binary.BigEndian.Uint32(plain[16:20]))
	if 20+msgLen > len(plain) {
		return "", fmt.Errorf("wecom: invalid msg length %d", msgLen)
	}
	return string(plain[20 : 20+msgLen]), nil
}

func pkcs7Unpad(b []byte) ([]byte, error) {
	if len(b) == 0 {
		return nil, fmt.Errorf("wecom: empty plaintext")
	}
	padLen := int(b[len(b)-1])
	if padLen == 0 || padLen > aes.BlockSize || padLen > len(b) {
		return nil, fmt.Errorf("wecom: invalid pkcs7 padding")
	}
	return b[:len(b)-padLen], nil
}

// Message is the decrypted inbound WeCom message (XML unmarshaled).
type Message struct {
	ToUserName   string `xml:"ToUserName"`
	FromUserName string `xml:"FromUserName"`
	CreateTime   int64  `xml:"CreateTime"`
	MsgType      string `xml:"MsgType"`
	Content      string `xml:"Content"`
	MsgId        string `xml:"MsgId"`
	AgentID      string `xml:"AgentID"`
	ChatId       string `xml:"ChatId"` // present only in group chat
}

// ToInbound converts a decrypted WeCom message into a normalized InboundMessage.
// tenantID is resolved by the platform before this call; AgentID may be empty
// and is filled in later by the agent-binding lookup.
func ToInbound(tenantID string, msg *Message) *channels.InboundMessage {
	chatType := channels.ChatTypeSingle
	chatID := msg.FromUserName
	if msg.ChatId != "" {
		chatType = channels.ChatTypeGroup
		chatID = msg.ChatId
	}
	return &channels.InboundMessage{
		PlatformMsgID: msg.MsgId,
		TenantID:      tenantID,
		AgentID:       msg.AgentID,
		SessionID:     channels.BuildSessionID(tenantID, Name, chatType, chatID),
		UserID:        msg.FromUserName,
		ChatType:      chatType,
		ChatID:        chatID,
		Content:       msg.Content,
		MsgType:       normalizeMsgType(msg.MsgType),
	}
}

// normalizeMsgType maps WeCom message types onto the unified contract. Types
// we don't surface specially (voice/video/event/recall) collapse to event.
func normalizeMsgType(t string) string {
	switch t {
	case channels.MsgTypeText, channels.MsgTypeImage, channels.MsgTypeFile:
		return t
	default:
		return channels.MsgTypeEvent
	}
}

// ----------------------------------------------------------------- adapter --

// Adapter implements channels.Adapter for enterprise WeChat on top of a Conn.
// The Conn already owns connection establishment and verification/decryption;
// Adapter normalizes and dedups inbound messages.
type Adapter struct {
	tenantID string
	conn     channels.Conn
	inbound  chan *channels.InboundMessage

	mu   sync.Mutex
	seen map[string]struct{}
}

// New returns a WeCom adapter bound to the given tenant.
func New(tenantID string, conn channels.Conn) *Adapter {
	return &Adapter{
		tenantID: tenantID,
		conn:     conn,
		inbound:  make(chan *channels.InboundMessage, 64),
		seen:     make(map[string]struct{}),
	}
}

// Name returns the stable channel identifier.
func (a *Adapter) Name() string { return Name }

// Inbound returns the channel of normalized inbound messages.
func (a *Adapter) Inbound() <-chan *channels.InboundMessage { return a.inbound }

// Start consumes raw events from the Conn until ctx is done.
func (a *Adapter) Start(ctx context.Context) error {
	for {
		raw, err := a.conn.Recv(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil // graceful shutdown
			}
			return err
		}
		var msg Message
		if err := xml.Unmarshal(raw, &msg); err != nil || msg.MsgId == "" {
			continue // skip malformed or undeduplicatable events
		}
		if a.isDup(msg.MsgId) {
			continue
		}
		in := ToInbound(a.tenantID, &msg)
		select {
		case a.inbound <- in:
		case <-ctx.Done():
			return nil
		}
	}
}

// Send delivers a normalized outbound message back to the platform.
func (a *Adapter) Send(ctx context.Context, msg *channels.OutboundMessage) error {
	if msg == nil || msg.Inbound == nil {
		return fmt.Errorf("wecom: outbound requires inbound context")
	}
	return a.conn.Send(ctx, msg.Inbound.ChatID, msg.Inbound.ChatType, msg.Text())
}

// Stop closes the underlying connection.
func (a *Adapter) Stop(_ context.Context) error {
	return a.conn.Close()
}

// isDup records and reports whether a platform message id was already seen.
// ponytail: in-memory set; swap for Redis idem:{msg_key} in phase 6.
func (a *Adapter) isDup(id string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, ok := a.seen[id]; ok {
		return true
	}
	a.seen[id] = struct{}{}
	return false
}
