// Package protocol implements the encrypted XML callback contract used by
// WeCom custom applications. It has no tenant, Inbox, or Gateway dependency.
package protocol

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha1" // #nosec G505 -- SHA-1 is mandated by the WeCom callback protocol.
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

const maxCallbackBytes = 1 << 20

// VerificationMaterial is stored as a channel_verify scoped secret. ReceiveID
// is the CorpID for an Agent callback; AgentID pins the callback application.
type VerificationMaterial struct {
	Token          string `json:"token"`
	EncodingAESKey string `json:"encoding_aes_key"`
	ReceiveID      string `json:"receive_id"`
	AgentID        int64  `json:"agent_id"`
}

type Verifier struct {
	Now     func() time.Time
	MaxSkew time.Duration
}

func IsChallengeRequest(request channel.CallbackRequest) bool {
	return query(request.Query, "echostr") != ""
}

func (v Verifier) VerifyChallenge(_ context.Context, request channel.CallbackRequest, secret []byte) (channel.VerifiedProtocolPayload, error) {
	material, err := parseMaterial(secret)
	if err != nil {
		return channel.VerifiedProtocolPayload{}, err
	}
	timestamp := query(request.Query, "timestamp")
	nonce := query(request.Query, "nonce")
	signature := query(request.Query, "msg_signature")
	echo := query(request.Query, "echostr")
	if nonce == "" || signature == "" || echo == "" || !v.validTimestamp(timestamp) {
		return channel.VerifiedProtocolPayload{}, runtime.ErrInvalidEnvelope
	}
	expected := Signature(material.Token, timestamp, nonce, echo)
	if len(signature) != len(expected) || subtle.ConstantTimeCompare([]byte(signature), []byte(expected)) != 1 {
		return channel.VerifiedProtocolPayload{}, runtime.ErrVersionMismatch
	}
	plaintext, receiveID, err := decrypt(echo, material.EncodingAESKey)
	if err != nil || receiveID != material.ReceiveID || len(plaintext) == 0 || len(plaintext) > 4096 || !utf8.Valid(plaintext) {
		return channel.VerifiedProtocolPayload{}, runtime.ErrVersionMismatch
	}
	return channel.VerifiedProtocolPayload{Body: plaintext, Headers: map[string]string{"content-type": "text/plain; charset=utf-8"},
		ProtocolIdentityDigest: digest(material.ReceiveID, "url_verification", string(plaintext))}, nil
}

type encryptedEnvelope struct {
	XMLName    xml.Name `xml:"xml"`
	ToUserName string   `xml:"ToUserName"`
	Encrypt    string   `xml:"Encrypt"`
	AgentID    int64    `xml:"AgentID"`
}

type callbackMessage struct {
	XMLName      xml.Name `xml:"xml"`
	ToUserName   string   `xml:"ToUserName"`
	FromUserName string   `xml:"FromUserName"`
	CreateTime   int64    `xml:"CreateTime"`
	MsgType      string   `xml:"MsgType"`
	Content      string   `xml:"Content"`
	MediaID      string   `xml:"MediaId"`
	FileName     string   `xml:"FileName"`
	FileSize     int64    `xml:"FileSize"`
	MsgID        string   `xml:"MsgId"`
	AgentID      int64    `xml:"AgentID"`
}

func (v Verifier) Verify(_ context.Context, request channel.CallbackRequest, secret []byte) (channel.VerifiedProtocolPayload, error) {
	material, err := parseMaterial(secret)
	if err != nil {
		return channel.VerifiedProtocolPayload{}, err
	}
	if len(request.Body) == 0 || len(request.Body) > maxCallbackBytes {
		return channel.VerifiedProtocolPayload{}, runtime.ErrInvalidEnvelope
	}
	timestamp := query(request.Query, "timestamp")
	nonce := query(request.Query, "nonce")
	signature := query(request.Query, "msg_signature")
	seconds, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil || nonce == "" || signature == "" {
		return channel.VerifiedProtocolPayload{}, runtime.ErrInvalidEnvelope
	}
	if !v.timestampWithinWindow(seconds) {
		return channel.VerifiedProtocolPayload{}, runtime.ErrInvalidEnvelope
	}

	var encrypted encryptedEnvelope
	if err := decodeXML(request.Body, &encrypted); err != nil || encrypted.XMLName.Local != "xml" ||
		encrypted.ToUserName != material.ReceiveID || encrypted.AgentID != material.AgentID || encrypted.Encrypt == "" {
		return channel.VerifiedProtocolPayload{}, runtime.ErrVersionMismatch
	}
	expected := Signature(material.Token, timestamp, nonce, encrypted.Encrypt)
	if len(signature) != len(expected) || subtle.ConstantTimeCompare([]byte(signature), []byte(expected)) != 1 {
		return channel.VerifiedProtocolPayload{}, runtime.ErrVersionMismatch
	}
	plaintext, receiveID, err := decrypt(encrypted.Encrypt, material.EncodingAESKey)
	if err != nil || receiveID != material.ReceiveID {
		return channel.VerifiedProtocolPayload{}, runtime.ErrVersionMismatch
	}
	message, err := parseMessage(plaintext)
	if err != nil || message.ToUserName != material.ReceiveID || message.AgentID != material.AgentID {
		return channel.VerifiedProtocolPayload{}, runtime.ErrVersionMismatch
	}
	identity := digest(material.ReceiveID, strconv.FormatInt(material.AgentID, 10), message.MsgID)
	return channel.VerifiedProtocolPayload{Body: plaintext, Headers: map[string]string{
		"x-wecom-receive-id": material.ReceiveID,
		"x-wecom-agent-id":   strconv.FormatInt(material.AgentID, 10),
	}, ProtocolIdentityDigest: identity}, nil
}

func (v Verifier) validTimestamp(value string) bool {
	seconds, err := strconv.ParseInt(value, 10, 64)
	return err == nil && v.timestampWithinWindow(seconds)
}

func (v Verifier) timestampWithinWindow(seconds int64) bool {
	now := time.Now().UTC()
	if v.Now != nil {
		now = v.Now().UTC()
	}
	maxSkew := v.MaxSkew
	if maxSkew <= 0 {
		maxSkew = 5 * time.Minute
	}
	requestTime := time.Unix(seconds, 0).UTC()
	return !requestTime.Before(now.Add(-maxSkew)) && !requestTime.After(now.Add(maxSkew))
}

func DecodeMessage(payload channel.VerifiedCallback) (channel.ProviderEvent, error) {
	message, err := parseMessage(payload.Body)
	if err != nil {
		return channel.ProviderEvent{}, err
	}
	receiveID := header(payload.Headers, "x-wecom-receive-id")
	agentID, err := strconv.ParseInt(header(payload.Headers, "x-wecom-agent-id"), 10, 64)
	if err != nil || receiveID == "" || message.ToUserName != receiveID || message.AgentID != agentID {
		return channel.ProviderEvent{}, runtime.ErrVersionMismatch
	}
	event := channel.ProviderEvent{
		SchemaVersion: 1, Channel: "wecom", ExternalAccountID: receiveID,
		ExternalMessageID: message.MsgID, ConversationType: "p2p",
		ExternalUserID: message.FromUserName, MessageType: message.MsgType,
		OccurredAt: time.Unix(message.CreateTime, 0).UTC(),
	}
	if message.MsgType == "text" {
		event.Text = strings.TrimSpace(message.Content)
	} else {
		event.MediaRefs = []channel.MediaRef{{ID: message.MediaID, MessageID: message.MsgID, Kind: message.MsgType, Size: message.FileSize}}
	}
	return event, nil
}

func parseMaterial(secret []byte) (VerificationMaterial, error) {
	var material VerificationMaterial
	if len(secret) == 0 || len(secret) > 16<<10 {
		return material, runtime.ErrVersionMismatch
	}
	decoder := json.NewDecoder(bytes.NewReader(secret))
	decoder.DisallowUnknownFields()
	decodeErr := decoder.Decode(&material)
	var trailing any
	trailingErr := decoder.Decode(&trailing)
	key, keyErr := decodeAESKey(material.EncodingAESKey)
	if decodeErr != nil || trailingErr != io.EOF || material.Token == "" || material.ReceiveID == "" ||
		material.AgentID <= 0 || keyErr != nil || len(key) != 32 {
		return VerificationMaterial{}, runtime.ErrVersionMismatch
	}
	return material, nil
}

func parseMessage(body []byte) (callbackMessage, error) {
	var message callbackMessage
	if len(body) == 0 || len(body) > maxCallbackBytes || decodeXML(body, &message) != nil || message.XMLName.Local != "xml" ||
		message.ToUserName == "" || message.FromUserName == "" || message.CreateTime <= 0 ||
		message.MsgID == "" || message.AgentID <= 0 || message.FileSize < 0 {
		return callbackMessage{}, runtime.ErrInvalidEnvelope
	}
	switch message.MsgType {
	case "text":
		if strings.TrimSpace(message.Content) == "" || !utf8.ValidString(message.Content) || message.MediaID != "" || message.FileSize != 0 {
			return callbackMessage{}, runtime.ErrInvalidEnvelope
		}
	case "image":
		if message.MediaID == "" || message.Content != "" || message.FileName != "" {
			return callbackMessage{}, runtime.ErrInvalidEnvelope
		}
	case "file":
		if message.MediaID == "" || message.Content != "" || (message.FileName != "" && !utf8.ValidString(message.FileName)) {
			return callbackMessage{}, runtime.ErrInvalidEnvelope
		}
	default:
		return callbackMessage{}, runtime.ErrCapabilityUnsupported
	}
	return message, nil
}

func decodeXML(body []byte, destination any) error {
	decoder := xml.NewDecoder(bytes.NewReader(body))
	decoder.Strict = true
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return runtime.ErrInvalidEnvelope
	}
	return nil
}

func decrypt(value, encodingAESKey string) ([]byte, string, error) {
	key, err := decodeAESKey(encodingAESKey)
	if err != nil {
		return nil, "", err
	}
	ciphertext, err := base64.StdEncoding.DecodeString(value)
	if err != nil || len(ciphertext) == 0 || len(ciphertext)%aes.BlockSize != 0 {
		return nil, "", runtime.ErrInvalidEnvelope
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, "", runtime.ErrInvalidEnvelope
	}
	plaintext := append([]byte(nil), ciphertext...)
	cipher.NewCBCDecrypter(block, key[:aes.BlockSize]).CryptBlocks(plaintext, plaintext)
	plaintext, err = unpad(plaintext)
	if err != nil || len(plaintext) < 20 {
		return nil, "", runtime.ErrInvalidEnvelope
	}
	messageLength := uint64(binary.BigEndian.Uint32(plaintext[16:20]))
	if messageLength == 0 || messageLength > uint64(len(plaintext)-20) {
		return nil, "", runtime.ErrInvalidEnvelope
	}
	end := 20 + int(messageLength)
	if end == len(plaintext) {
		return nil, "", runtime.ErrInvalidEnvelope
	}
	return append([]byte(nil), plaintext[20:end]...), string(plaintext[end:]), nil
}

func decodeAESKey(value string) ([]byte, error) {
	if len(value) != 43 {
		return nil, runtime.ErrVersionMismatch
	}
	return base64.RawStdEncoding.DecodeString(value)
}

func unpad(value []byte) ([]byte, error) {
	if len(value) == 0 || len(value)%aes.BlockSize != 0 {
		return nil, runtime.ErrInvalidEnvelope
	}
	padding := int(value[len(value)-1])
	if padding < 1 || padding > 32 || padding > len(value) {
		return nil, runtime.ErrInvalidEnvelope
	}
	for _, current := range value[len(value)-padding:] {
		if int(current) != padding {
			return nil, runtime.ErrInvalidEnvelope
		}
	}
	return value[:len(value)-padding], nil
}

// Signature implements the WeCom callback signature algorithm.
func Signature(token, timestamp, nonce, encrypted string) string {
	values := []string{token, timestamp, nonce, encrypted}
	sort.Strings(values)
	hash := sha1.New() // #nosec G401 -- SHA-1 is mandated by the WeCom protocol.
	_, _ = hash.Write([]byte(strings.Join(values, "")))
	return hex.EncodeToString(hash.Sum(nil))
}

func RouteKeyDigest(routeKey string) string { return digest("wecom-route-v1", routeKey) }

func digest(fields ...string) string {
	hash := sha256.New()
	for _, field := range fields {
		_, _ = fmt.Fprintf(hash, "%d:%s", len(field), field)
	}
	return base64.RawURLEncoding.EncodeToString(hash.Sum(nil))
}

func query(values map[string]string, name string) string {
	for key, value := range values {
		if strings.EqualFold(key, name) {
			return value
		}
	}
	return ""
}

func header(values map[string]string, name string) string { return query(values, name) }
