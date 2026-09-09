package wecom

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

var (
	// ErrUnsupportedMessage marks authenticated provider events that are not
	// valid Agent inputs and should be acknowledged without retry.
	ErrUnsupportedMessage = errors.New("wecom: unsupported message")
	// ErrBindingMismatch marks a decrypted callback for another CorpID/AgentID.
	ErrBindingMismatch = errors.New("wecom: callback binding mismatch")
)

// Binding is the immutable server-owned scope for one WeCom application.
type Binding struct {
	TenantID, AppID, BindingID string
	CorpID, AgentID            string
	AppSecret                  string
	ConfigVersion              tenant.ConfigVersion
	Crypt                      *Crypt
}

// EncryptedEnvelope is the outer XML callback body.
type EncryptedEnvelope struct {
	XMLName    xml.Name `xml:"xml"`
	ToUserName string   `xml:"ToUserName"`
	AgentID    string   `xml:"AgentID"`
	Encrypt    string   `xml:"Encrypt"`
}

// CallbackMessage is the decrypted common WeCom message shape.
type CallbackMessage struct {
	XMLName      xml.Name `xml:"xml"`
	ToUserName   string   `xml:"ToUserName"`
	FromUserName string   `xml:"FromUserName"`
	CreateTime   int64    `xml:"CreateTime"`
	MsgType      string   `xml:"MsgType"`
	Content      string   `xml:"Content"`
	MsgID        string   `xml:"MsgId"`
	AgentID      string   `xml:"AgentID"`
	ChatID       string   `xml:"ChatId"`
	RoomID       string   `xml:"RoomId"`
	Event        string   `xml:"Event"`
	EventKey     string   `xml:"EventKey"`
	MediaID      string   `xml:"MediaId"`
	PicURL       string   `xml:"PicUrl"`
	FileName     string   `xml:"FileName"`
	FileSize     int64    `xml:"FileSize"`
	Recognition  string   `xml:"Recognition"`
	LocationX    string   `xml:"Location_X"`
	LocationY    string   `xml:"Location_Y"`
	Scale        string   `xml:"Scale"`
	Label        string   `xml:"Label"`
	Title        string   `xml:"Title"`
	Description  string   `xml:"Description"`
	URL          string   `xml:"Url"`
}

func (binding Binding) validate() error {
	if binding.TenantID == "" || binding.AppID == "" || binding.BindingID == "" || binding.CorpID == "" || binding.ConfigVersion == 0 || binding.Crypt == nil {
		return errors.New("wecom: complete tenant binding and callback crypt are required")
	}
	return nil
}

func decodeEnvelope(value []byte) (EncryptedEnvelope, error) {
	var envelope EncryptedEnvelope
	decoder := xml.NewDecoder(strings.NewReader(string(value)))
	decoder.Strict = true
	if err := decoder.Decode(&envelope); err != nil {
		return EncryptedEnvelope{}, fmt.Errorf("wecom: decode encrypted callback: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return EncryptedEnvelope{}, errors.New("wecom: trailing callback data")
	}
	if strings.TrimSpace(envelope.Encrypt) == "" {
		return EncryptedEnvelope{}, errors.New("wecom: encrypted callback body is required")
	}
	return envelope, nil
}

func decodeCallback(value []byte) (CallbackMessage, error) {
	var message CallbackMessage
	decoder := xml.NewDecoder(strings.NewReader(string(value)))
	decoder.Strict = true
	if err := decoder.Decode(&message); err != nil {
		return CallbackMessage{}, fmt.Errorf("wecom: decode callback message: %w", err)
	}
	return message, nil
}

func normalize(binding Binding, message CallbackMessage, now time.Time) (gateway.InboundMessage, error) {
	if err := binding.validate(); err != nil {
		return gateway.InboundMessage{}, err
	}
	if message.ToUserName != binding.CorpID {
		return gateway.InboundMessage{}, fmt.Errorf("%w: CorpID", ErrBindingMismatch)
	}
	if binding.AgentID != "" && message.AgentID != "" && message.AgentID != binding.AgentID {
		return gateway.InboundMessage{}, fmt.Errorf("%w: AgentID", ErrBindingMismatch)
	}
	externalUserID := strings.TrimSpace(message.FromUserName)
	if externalUserID == "" {
		return gateway.InboundMessage{}, errors.New("wecom: callback user is required")
	}
	text, err := callbackText(message)
	if err != nil {
		return gateway.InboundMessage{}, err
	}
	externalMessageID := strings.TrimSpace(message.MsgID)
	if externalMessageID == "" {
		externalMessageID = eventMessageID(message)
	}
	userID, err := tenant.CanonicalUserID(tenant.ChannelTypeWeCom, binding.BindingID, externalUserID)
	if err != nil {
		return gateway.InboundMessage{}, err
	}
	conversationID := strings.TrimSpace(message.ChatID)
	if conversationID == "" {
		conversationID = strings.TrimSpace(message.RoomID)
	}
	var sessionID string
	if conversationID == "" {
		sessionID, err = tenant.DirectSessionID(binding.BindingID, externalUserID)
	} else {
		sessionID, err = tenant.GroupSessionID(binding.BindingID, conversationID)
	}
	if err != nil {
		return gateway.InboundMessage{}, err
	}
	traceHash := sha256.Sum256([]byte(binding.TenantID + "\x00" + binding.BindingID + "\x00" + externalMessageID))
	inbound := gateway.InboundMessage{
		TenantID:          binding.TenantID,
		AppID:             binding.AppID,
		BindingID:         binding.BindingID,
		ExternalMessageID: externalMessageID,
		ExternalUserID:    externalUserID,
		ConversationID:    conversationID,
		UserID:            userID,
		SessionID:         sessionID,
		Text:              text,
		TraceID:           "wecom-" + hex.EncodeToString(traceHash[:8]),
		ConfigVersion:     binding.ConfigVersion,
		ReceivedAt:        now.UTC(),
	}
	if kind := mediaKind(message.MsgType); kind != "" {
		inbound.Media = &gateway.MediaReference{Kind: kind, Key: strings.TrimSpace(message.MediaID), Name: strings.TrimSpace(message.FileName)}
	}
	return inbound, nil
}

func callbackText(message CallbackMessage) (string, error) {
	switch strings.ToLower(strings.TrimSpace(message.MsgType)) {
	case "text":
		if text := strings.TrimSpace(message.Content); text != "" {
			return text, nil
		}
	case "image":
		return attachmentText("image", ""), nil
	case "file":
		name := strings.TrimSpace(message.FileName)
		if message.FileSize > 0 {
			name = strings.TrimSpace(name + " " + strconv.FormatInt(message.FileSize, 10) + " bytes")
		}
		return attachmentText("file", name), nil
	case "voice":
		if text := strings.TrimSpace(message.Recognition); text != "" {
			return text, nil
		}
		return attachmentText("voice", ""), nil
	case "location":
		return fmt.Sprintf("[WeCom location: %s (%s,%s), scale=%s]", strings.TrimSpace(message.Label), message.LocationX, message.LocationY, message.Scale), nil
	case "link":
		return fmt.Sprintf("[WeCom link: %s — %s]", strings.TrimSpace(message.Title), strings.TrimSpace(message.Description)), nil
	case "event":
		return "", fmt.Errorf("%w: event %q", ErrUnsupportedMessage, message.Event)
	}
	return "", fmt.Errorf("%w: type %q", ErrUnsupportedMessage, message.MsgType)
}

func attachmentText(kind, name string) string {
	detail := strings.TrimSpace(name)
	if detail == "" {
		return "[WeCom " + kind + "]"
	}
	return "[WeCom " + kind + ": " + detail + "]"
}

func mediaKind(messageType string) string {
	switch strings.ToLower(strings.TrimSpace(messageType)) {
	case "image":
		return "image"
	case "file":
		return "file"
	default:
		return ""
	}
}

func eventMessageID(message CallbackMessage) string {
	value := strings.Join([]string{message.FromUserName, strconv.FormatInt(message.CreateTime, 10), message.MsgType, message.Event, message.EventKey}, "\x00")
	sum := sha256.Sum256([]byte(value))
	return "event-" + hex.EncodeToString(sum[:16])
}
