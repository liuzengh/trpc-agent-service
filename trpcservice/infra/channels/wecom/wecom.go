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

	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/channels"
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

// DecryptMedia decrypts a WeCom media download. Media uses the same AES-256-CBC
// + PKCS7 scheme as callback messages but without the random/msg-len/receiveid
// framing, so the whole plaintext is the file content. aesKey is the 43-char
// base64 key delivered with the message ("aeskey" field).
func DecryptMedia(encodingAESKey string, ciphertext []byte) ([]byte, error) {
	if encodingAESKey == "" {
		return ciphertext, nil
	}
	rawKey, err := base64.StdEncoding.DecodeString(encodingAESKey + "=")
	if err != nil {
		return nil, fmt.Errorf("wecom: decode media aes key: %w", err)
	}
	if len(rawKey) != 32 {
		return nil, fmt.Errorf("wecom: invalid media aes key length %d", len(rawKey))
	}
	if len(ciphertext) == 0 || len(ciphertext)%aes.BlockSize != 0 {
		return nil, fmt.Errorf("wecom: invalid media ciphertext length %d", len(ciphertext))
	}
	block, err := aes.NewCipher(rawKey)
	if err != nil {
		return nil, fmt.Errorf("wecom: new media cipher: %w", err)
	}
	plain := make([]byte, len(ciphertext))
	cipher.NewCBCDecrypter(block, rawKey[:aes.BlockSize]).CryptBlocks(plain, ciphertext)
	return pkcs7Unpad(plain)
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
	// Media carries one attachment descriptor per non-text message. WeCom hands
	// out a download URL plus the AES key that protects it.
	Media []MediaPart `xml:"Media"`
}

// MediaPart is the XML shape of one WeCom attachment descriptor.
type MediaPart struct {
	Kind   string `xml:"Kind,attr"`   // image | file | voice
	URL    string `xml:"URL,attr"`    // download url
	AesKey string `xml:"AesKey,attr"` // media decryption key
	Name   string `xml:"Name,attr"`
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
		Media:         toMedia(msg.Media),
	}
}

// toMedia maps the XML descriptors onto the unified attachment type.
func toMedia(parts []MediaPart) []channels.MediaAttachment {
	if len(parts) == 0 {
		return nil
	}
	out := make([]channels.MediaAttachment, 0, len(parts))
	for _, p := range parts {
		kind := p.Kind
		if kind == "" {
			kind = channels.MediaFile
		}
		out = append(out, channels.MediaAttachment{
			Kind:   kind,
			Name:   p.Name,
			URL:    p.URL,
			AesKey: p.AesKey,
		})
	}
	return out
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
// The Conn owns connection establishment and verification/decryption; the
// adapter only decodes and normalizes inbound messages. The shared pump,
// dedup and send plumbing lives in channels.BaseAdapter.
type Adapter struct {
	tenantID string
	*channels.BaseAdapter
}

// New returns a WeCom adapter bound to the given tenant.
func New(tenantID string, conn channels.Conn) *Adapter {
	return &Adapter{tenantID: tenantID, BaseAdapter: channels.NewBaseAdapter(Name, conn)}
}

// Start consumes raw events from the Conn until ctx is done.
func (a *Adapter) Start(ctx context.Context) error {
	return a.BaseAdapter.Run(ctx, func(raw []byte) (*channels.InboundMessage, bool) {
		var msg Message
		if err := xml.Unmarshal(raw, &msg); err != nil || msg.MsgId == "" {
			return nil, false // skip malformed or undeduplicatable events
		}
		return ToInbound(a.tenantID, &msg), true
	})
}
