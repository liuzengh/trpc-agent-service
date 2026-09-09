package platform

import (
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"sort"
	"strconv"
	"strings"
	"time"
)

// ProviderSender is intentionally injectable. The default sender records a
// deterministic delivery and never requires provider credentials or network.
type ProviderSender func(context.Context, ChannelBinding, ChannelReply) error

type EnterpriseWeChatChannel struct{ Sender ProviderSender }

type wechatMessage struct {
	XMLName    xml.Name `xml:"xml"`
	FromUser   string   `xml:"FromUserName"`
	ToUser     string   `xml:"ToUserName"`
	MsgType    string   `xml:"MsgType"`
	Content    string   `xml:"Content"`
	MsgID      string   `xml:"MsgId"`
	CreateTime int64    `xml:"CreateTime"`
	MediaID    string   `xml:"MediaId"`
}

func (e EnterpriseWeChatChannel) Receive(ctx context.Context, callback ChannelCallback) (ChannelMessage, error) {
	if callback.Channel != ChannelEnterpriseWeChat {
		return ChannelMessage{}, channelError{code: "channel_mismatch"}
	}
	if err := verifyEnterpriseWeChat(ctx, callback); err != nil {
		return ChannelMessage{}, err
	}
	var msg wechatMessage
	if err := xml.Unmarshal(callback.Body, &msg); err != nil || msg.FromUser == "" || msg.MsgID == "" {
		return ChannelMessage{}, channelError{code: "channel_callback_invalid"}
	}
	if msg.MsgType != "text" || msg.Content == "" {
		return ChannelMessage{}, channelError{code: "channel_attachment_rejected"}
	}
	sequence, _ := strconv.ParseUint(msg.MsgID, 10, 64)
	if sequence == 0 {
		sequence = uint64(msg.CreateTime)
	}
	if sequence == 0 {
		sequence = 1
	}
	return ChannelMessage{MessageID: msg.MsgID, UserID: msg.FromUser, ConversationID: msg.FromUser,
		Text: msg.Content, ProviderSequence: sequence, AttachmentName: msg.MediaID, ReceivedAt: time.Now().UTC()}, nil
}

func (e EnterpriseWeChatChannel) Send(ctx context.Context, binding ChannelBinding, reply ChannelReply) (ChannelDelivery, error) {
	if reply.MessageID == "" || strings.TrimSpace(reply.Text) == "" {
		return ChannelDelivery{}, channelError{code: "channel_callback_invalid"}
	}
	if len([]rune(reply.Text)) > 2048 {
		return ChannelDelivery{}, channelError{code: "channel_message_too_long"}
	}
	if e.Sender != nil {
		if err := e.Sender(ctx, binding, reply); err != nil {
			code := channelErrorCode(err)
			return ChannelDelivery{MessageID: reply.MessageID, Status: "failed", Code: code, Attempts: 1, LastAttempt: time.Now().UTC()}, channelError{code: code}
		}
	}
	return ChannelDelivery{MessageID: reply.MessageID, Status: "delivered", Attempts: 1, LastAttempt: time.Now().UTC()}, nil
}

func verifyEnterpriseWeChat(ctx context.Context, callback ChannelCallback) error {
	if err := ctx.Err(); err != nil {
		return channelError{code: "channel_timeout"}
	}
	if callback.Credential.Secret == "" || callback.Signature == "" {
		return channelError{code: "channel_signature_invalid"}
	}
	if callback.Timestamp != "" && callback.Nonce != "" {
		parts := []string{callback.Credential.Secret, callback.Timestamp, callback.Nonce}
		sort.Strings(parts)
		h := sha1.Sum([]byte(strings.Join(parts, "")))
		if hmac.Equal([]byte(strings.ToLower(callback.Signature)), []byte(hex.EncodeToString(h[:]))) {
			return nil
		}
	}
	mac := hmac.New(sha256.New, []byte(callback.Credential.Secret))
	_, _ = mac.Write(callback.Body)
	if hmac.Equal([]byte(strings.ToLower(callback.Signature)), []byte(hex.EncodeToString(mac.Sum(nil)))) {
		return nil
	}
	return channelError{code: "channel_signature_invalid"}
}

type TelegramChannel struct{ Sender ProviderSender }

type telegramUpdate struct {
	UpdateID int64 `json:"update_id"`
	Message  struct {
		MessageID int64 `json:"message_id"`
		Chat      struct {
			ID   int64  `json:"id"`
			Type string `json:"type"`
		} `json:"chat"`
		From struct {
			ID int64 `json:"id"`
		} `json:"from"`
		Text string `json:"text"`
	} `json:"message"`
}

func (t TelegramChannel) Receive(ctx context.Context, callback ChannelCallback) (ChannelMessage, error) {
	if callback.Channel != ChannelTelegram {
		return ChannelMessage{}, channelError{code: "channel_mismatch"}
	}
	if err := ctx.Err(); err != nil {
		return ChannelMessage{}, channelError{code: "channel_timeout"}
	}
	if callback.Credential.Secret == "" || callback.Signature == "" ||
		!hmac.Equal([]byte(callback.Signature), []byte(callback.Credential.Secret)) {
		return ChannelMessage{}, channelError{code: "channel_signature_invalid"}
	}
	var update telegramUpdate
	if err := json.Unmarshal(callback.Body, &update); err != nil || update.UpdateID <= 0 || update.Message.MessageID <= 0 || update.Message.Chat.ID == 0 || update.Message.Text == "" {
		return ChannelMessage{}, channelError{code: "channel_callback_invalid"}
	}
	return ChannelMessage{MessageID: "telegram-" + strconv.FormatInt(update.Message.MessageID, 10), UserID: strconv.FormatInt(update.Message.From.ID, 10),
		ConversationID: strconv.FormatInt(update.Message.Chat.ID, 10), Text: update.Message.Text,
		ProviderSequence: uint64(update.UpdateID), ReceivedAt: time.Now().UTC()}, nil
}

func (t TelegramChannel) Send(ctx context.Context, binding ChannelBinding, reply ChannelReply) (ChannelDelivery, error) {
	if reply.MessageID == "" || strings.TrimSpace(reply.Text) == "" {
		return ChannelDelivery{}, errors.New("platform: invalid channel reply")
	}
	if len([]rune(reply.Text)) > 4096 {
		return ChannelDelivery{}, channelError{code: "channel_message_too_long"}
	}
	if t.Sender != nil {
		if err := t.Sender(ctx, binding, reply); err != nil {
			code := channelErrorCode(err)
			return ChannelDelivery{MessageID: reply.MessageID, Status: "failed", Code: code, Attempts: 1, LastAttempt: time.Now().UTC()}, channelError{code: code}
		}
	}
	return ChannelDelivery{MessageID: reply.MessageID, Status: "delivered", Attempts: 1, LastAttempt: time.Now().UTC()}, nil
}
