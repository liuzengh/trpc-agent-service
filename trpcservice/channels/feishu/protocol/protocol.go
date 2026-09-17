// Package protocol implements Feishu webhook verification and strict message
// decoding. It does not know tenant, Inbox, Session, or Gateway concepts.
package protocol

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	larkevent "github.com/larksuite/oapi-sdk-go/v3/event"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

const EventTypeMessageReceive = "im.message.receive_v1"

const challengeType = "url_verification"

type VerificationMaterial struct {
	EncryptKey        string `json:"encrypt_key"`
	VerificationToken string `json:"verification_token"`
	AppID             string `json:"app_id"`
	BotOpenID         string `json:"bot_open_id"`
}

type Verifier struct {
	Now     func() time.Time
	MaxSkew time.Duration
}

// IsChallengeRequest recognizes both plaintext challenge envelopes and the
// encrypted challenge form used when an EncryptKey is configured. Encrypted
// challenge requests do not carry the normal callback signature headers.
func IsChallengeRequest(request channel.CallbackRequest) bool {
	var fuzzy struct {
		Type    string `json:"type"`
		Encrypt string `json:"encrypt"`
	}
	if json.Unmarshal(request.Body, &fuzzy) != nil {
		return false
	}
	return fuzzy.Type == challengeType || (fuzzy.Encrypt != "" && header(request.Headers, larkevent.EventSignature) == "")
}

func (v Verifier) VerifyChallenge(_ context.Context, request channel.CallbackRequest, secret []byte) (channel.VerifiedProtocolPayload, error) {
	material, err := parseMaterial(secret)
	if err != nil || len(request.Body) == 0 || len(request.Body) > 1<<20 {
		return channel.VerifiedProtocolPayload{}, runtime.ErrVersionMismatch
	}
	plaintext := append([]byte(nil), request.Body...)
	var wrapper larkevent.EventEncryptMsg
	if json.Unmarshal(request.Body, &wrapper) == nil && wrapper.Encrypt != "" {
		decrypted, decryptErr := larkevent.EventDecrypt(wrapper.Encrypt, material.EncryptKey)
		if decryptErr != nil {
			return channel.VerifiedProtocolPayload{}, runtime.ErrInvalidEnvelope
		}
		plaintext = decrypted
	}
	var challenge struct {
		Schema    string                 `json:"schema"`
		Token     string                 `json:"token"`
		Type      string                 `json:"type"`
		Challenge string                 `json:"challenge"`
		Header    *larkevent.EventHeader `json:"header"`
		Event     json.RawMessage        `json:"event"`
	}
	decoder := json.NewDecoder(bytes.NewReader(plaintext))
	decoder.DisallowUnknownFields()
	decodeErr := decoder.Decode(&challenge)
	var trailing any
	trailingErr := decoder.Decode(&trailing)
	token, appID := challenge.Token, ""
	if challenge.Header != nil {
		token, appID = challenge.Header.Token, challenge.Header.AppID
	}
	if decodeErr != nil || trailingErr != io.EOF || challenge.Type != challengeType || challenge.Challenge == "" ||
		len(challenge.Challenge) > 4096 || token == "" || (challenge.Header != nil && (challenge.Schema != "2.0" || appID != material.AppID)) ||
		len(token) != len(material.VerificationToken) || subtle.ConstantTimeCompare([]byte(token), []byte(material.VerificationToken)) != 1 {
		return channel.VerifiedProtocolPayload{}, runtime.ErrVersionMismatch
	}
	response, err := json.Marshal(struct {
		Challenge string `json:"challenge"`
	}{Challenge: challenge.Challenge})
	if err != nil {
		return channel.VerifiedProtocolPayload{}, err
	}
	return channel.VerifiedProtocolPayload{Body: response, Headers: map[string]string{"content-type": "application/json"},
		ProtocolIdentityDigest: digest(material.AppID, challengeType, challenge.Challenge)}, nil
}

func (v Verifier) Verify(_ context.Context, request channel.CallbackRequest, secret []byte) (channel.VerifiedProtocolPayload, error) {
	material, err := parseMaterial(secret)
	if err != nil || len(request.Body) == 0 {
		return channel.VerifiedProtocolPayload{}, runtime.ErrVersionMismatch
	}
	timestamp := header(request.Headers, larkevent.EventRequestTimestamp)
	nonce := header(request.Headers, larkevent.EventRequestNonce)
	signature := header(request.Headers, larkevent.EventSignature)
	seconds, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil || nonce == "" || signature == "" {
		return channel.VerifiedProtocolPayload{}, runtime.ErrInvalidEnvelope
	}
	now := time.Now().UTC()
	if v.Now != nil {
		now = v.Now().UTC()
	}
	maxSkew := v.MaxSkew
	if maxSkew <= 0 {
		maxSkew = 5 * time.Minute
	}
	requestTime := time.Unix(seconds, 0).UTC()
	if requestTime.Before(now.Add(-maxSkew)) || requestTime.After(now.Add(maxSkew)) {
		return channel.VerifiedProtocolPayload{}, runtime.ErrInvalidEnvelope
	}
	expected := larkevent.Signature(timestamp, nonce, material.EncryptKey, string(request.Body))
	if len(signature) != len(expected) || subtle.ConstantTimeCompare([]byte(signature), []byte(expected)) != 1 {
		return channel.VerifiedProtocolPayload{}, runtime.ErrVersionMismatch
	}
	var wrapper larkevent.EventEncryptMsg
	if err := json.Unmarshal(request.Body, &wrapper); err != nil || wrapper.Encrypt == "" {
		return channel.VerifiedProtocolPayload{}, runtime.ErrInvalidEnvelope
	}
	plaintext, err := larkevent.EventDecrypt(wrapper.Encrypt, material.EncryptKey)
	if err != nil {
		return channel.VerifiedProtocolPayload{}, runtime.ErrInvalidEnvelope
	}
	var envelope struct {
		Schema string                 `json:"schema"`
		Header *larkevent.EventHeader `json:"header"`
	}
	if err := json.Unmarshal(plaintext, &envelope); err != nil || envelope.Schema != "2.0" || envelope.Header == nil ||
		envelope.Header.Token != material.VerificationToken || envelope.Header.AppID != material.AppID ||
		envelope.Header.EventID == "" || envelope.Header.EventType != EventTypeMessageReceive || envelope.Header.TenantKey == "" {
		return channel.VerifiedProtocolPayload{}, runtime.ErrVersionMismatch
	}
	identity := digest(material.AppID, envelope.Header.TenantKey, envelope.Header.EventID)
	return channel.VerifiedProtocolPayload{Body: plaintext, Headers: map[string]string{"x-feishu-bot-open-id": material.BotOpenID}, ProtocolIdentityDigest: identity}, nil
}

func parseMaterial(secret []byte) (VerificationMaterial, error) {
	var material VerificationMaterial
	decoder := json.NewDecoder(strings.NewReader(string(secret)))
	decoder.DisallowUnknownFields()
	decodeErr := decoder.Decode(&material)
	var trailing any
	trailingErr := decoder.Decode(&trailing)
	if decodeErr != nil || trailingErr != io.EOF || material.EncryptKey == "" || material.VerificationToken == "" || material.AppID == "" {
		return VerificationMaterial{}, runtime.ErrVersionMismatch
	}
	return material, nil
}

type DecodeResult struct {
	Event   channel.ProviderEvent
	Ignored bool
}

func DecodeMessage(payload channel.VerifiedCallback) (DecodeResult, error) {
	var event larkim.P2MessageReceiveV1
	if err := json.Unmarshal(payload.Body, &event); err != nil || event.EventV2Base == nil || event.EventV2Base.Header == nil ||
		event.Event == nil || event.Event.Message == nil || event.Event.Sender == nil || event.Event.Sender.SenderId == nil {
		return DecodeResult{}, runtime.ErrInvalidEnvelope
	}
	headerValue := event.EventV2Base.Header
	message, sender := event.Event.Message, event.Event.Sender
	if headerValue.EventType != EventTypeMessageReceive || headerValue.EventID == "" || headerValue.AppID == "" ||
		message.MessageId == nil || *message.MessageId == "" || message.ChatId == nil || *message.ChatId == "" ||
		message.ChatType == nil || message.MessageType == nil || message.Content == nil ||
		sender.SenderId.OpenId == nil || *sender.SenderId.OpenId == "" || sender.TenantKey == nil || *sender.TenantKey == "" {
		return DecodeResult{}, runtime.ErrInvalidEnvelope
	}
	if *message.ChatType != "p2p" && *message.ChatType != "group" {
		return DecodeResult{}, runtime.ErrInvalidEnvelope
	}
	botOpenID := header(payload.Headers, "x-feishu-bot-open-id")
	// A local real-IM smoke can start from a p2p conversation without knowing
	// the bot Open ID ahead of time. Group messages remain fail-closed until it
	// is configured, because their @mention must be proven before execution.
	if *message.ChatType == "group" && botOpenID == "" {
		return DecodeResult{Ignored: true}, nil
	}
	messageType, text, media, ignored, err := decodeContent(*message.MessageType, *message.Content, message.Mentions, botOpenID, *message.ChatType)
	if err != nil {
		return DecodeResult{}, err
	}
	occurredAt, err := eventTime(message.CreateTime, headerValue.CreateTime)
	if err != nil {
		return DecodeResult{}, err
	}
	return DecodeResult{Ignored: ignored, Event: channel.ProviderEvent{
		SchemaVersion: 1, Channel: "feishu", ExternalAccountID: headerValue.AppID,
		ExternalMessageID: *message.MessageId, ConversationType: *message.ChatType,
		ExternalUserID: *sender.SenderId.OpenId, ExternalChatID: *message.ChatId,
		MessageType: messageType, Text: text, MediaRefs: bindMediaMessage(media, *message.MessageId), OccurredAt: occurredAt,
	}}, nil
}

func bindMediaMessage(media []channel.MediaRef, messageID string) []channel.MediaRef {
	for index := range media {
		media[index].MessageID = messageID
	}
	return media
}

func decodeContent(messageType, content string, mentions []*larkim.MentionEvent, botOpenID, chatType string) (string, string, []channel.MediaRef, bool, error) {
	switch messageType {
	case "text":
		text, mentionedBot, err := normalizeText(content, mentions, botOpenID)
		if err != nil {
			return "", "", nil, false, err
		}
		ignored := chatType == "group" && (!mentionedBot || strings.TrimSpace(text) == "")
		if ignored {
			text = ""
		}
		return "text", text, nil, ignored, nil
	case "image", "file":
		// Media-only group events carry no mention span that can prove the bot was
		// addressed, so the first safe slice accepts them only in p2p chats.
		if chatType != "p2p" || len(mentions) != 0 {
			return "", "", nil, false, runtime.ErrInvalidEnvelope
		}
		var body struct {
			ImageKey string `json:"image_key"`
			FileKey  string `json:"file_key"`
		}
		decoder := json.NewDecoder(strings.NewReader(content))
		decoder.DisallowUnknownFields()
		decodeErr := decoder.Decode(&body)
		var trailing any
		trailingErr := decoder.Decode(&trailing)
		id := body.ImageKey
		if messageType == "file" {
			id = body.FileKey
		}
		if decodeErr != nil || trailingErr != io.EOF || id == "" ||
			(messageType == "image" && body.FileKey != "") || (messageType == "file" && body.ImageKey != "") {
			return "", "", nil, false, runtime.ErrInvalidEnvelope
		}
		return messageType, "", []channel.MediaRef{{ID: id, Kind: messageType}}, false, nil
	default:
		return "", "", nil, false, runtime.ErrCapabilityUnsupported
	}
}

func normalizeText(content string, mentions []*larkim.MentionEvent, botOpenID string) (string, bool, error) {
	var body struct {
		Text string `json:"text"`
	}
	decoder := json.NewDecoder(strings.NewReader(content))
	decoder.DisallowUnknownFields()
	decodeErr := decoder.Decode(&body)
	var trailing any
	trailingErr := decoder.Decode(&trailing)
	if decodeErr != nil || trailingErr != io.EOF || body.Text == "" {
		return "", false, runtime.ErrInvalidEnvelope
	}
	type span struct {
		start, end  int
		replacement string
		bot         bool
	}
	spans := make([]span, 0, len(mentions))
	keys := make(map[string]struct{}, len(mentions))
	for _, mention := range mentions {
		if mention == nil || mention.Key == nil || *mention.Key == "" || mention.TenantKey == nil || *mention.TenantKey == "" {
			return "", false, runtime.ErrInvalidEnvelope
		}
		key := *mention.Key
		if _, exists := keys[key]; exists || strings.Count(body.Text, key) != 1 {
			return "", false, runtime.ErrInvalidEnvelope
		}
		keys[key] = struct{}{}
		start := strings.Index(body.Text, key)
		isAll := key == "@_all" || key == "@all"
		openID := ""
		if mention.Id != nil && mention.Id.OpenId != nil {
			openID = *mention.Id.OpenId
		}
		if !isAll && openID == "" {
			return "", false, runtime.ErrInvalidEnvelope
		}
		name := ""
		if mention.Name != nil {
			name = strings.TrimSpace(*mention.Name)
		}
		isBot := openID == botOpenID
		replacement := ""
		if !isBot {
			if isAll {
				replacement = "@所有人"
			} else if name != "" {
				replacement = "@" + name
			} else {
				return "", false, runtime.ErrInvalidEnvelope
			}
		}
		spans = append(spans, span{start: start, end: start + len(key), replacement: replacement, bot: isBot})
	}
	for _, unresolved := range []string{"@_user_", "@_all", "@all"} {
		if strings.Contains(body.Text, unresolved) {
			found := false
			for key := range keys {
				if strings.Contains(key, unresolved) {
					found = true
				}
			}
			if !found {
				return "", false, runtime.ErrInvalidEnvelope
			}
		}
	}
	for i := 1; i < len(spans); i++ {
		for j := i; j > 0 && spans[j].start < spans[j-1].start; j-- {
			spans[j], spans[j-1] = spans[j-1], spans[j]
		}
	}
	var out strings.Builder
	position, mentionedBot := 0, false
	for _, current := range spans {
		if current.start < position {
			return "", false, runtime.ErrInvalidEnvelope
		}
		out.WriteString(body.Text[position:current.start])
		out.WriteString(current.replacement)
		position = current.end
		mentionedBot = mentionedBot || current.bot
	}
	out.WriteString(body.Text[position:])
	normalized := strings.TrimSpace(out.String())
	if strings.Contains(normalized, "@_user_") || strings.Contains(normalized, "@_all") || strings.Contains(normalized, "@all") {
		return "", false, runtime.ErrInvalidEnvelope
	}
	return normalized, mentionedBot, nil
}

func eventTime(messageTime *string, headerTime string) (time.Time, error) {
	value := headerTime
	if messageTime != nil && *messageTime != "" {
		value = *messageTime
	}
	millis, err := strconv.ParseInt(value, 10, 64)
	if err != nil || millis <= 0 {
		return time.Time{}, runtime.ErrInvalidEnvelope
	}
	return time.UnixMilli(millis).UTC(), nil
}

func header(headers map[string]string, name string) string {
	for key, value := range headers {
		if strings.EqualFold(key, name) {
			return value
		}
	}
	return ""
}

func digest(fields ...string) string {
	hash := sha256.New()
	for _, field := range fields {
		_, _ = fmt.Fprintf(hash, "%d:%s", len(field), field)
	}
	return base64.RawURLEncoding.EncodeToString(hash.Sum(nil))
}

func RouteKeyDigest(routeKey string) string { return digest("feishu-route-v1", routeKey) }
