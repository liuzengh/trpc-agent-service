package telegramadapter

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/go-telegram/bot/models"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/admission/domain"
)

// Normalize translates a raw Telegram Update without losing unknown fields.
// Both receive modes call this exact function; transport authorization is local
// metadata added by the caller and is never part of the source digest.
func Normalize(data []byte, accountID string, now time.Time) (domain.Inbound, error) {
	if len(data) > maxBodyBytes || !identifier(accountID, 128, true) {
		return domain.Inbound{}, domain.ErrInvalidInput
	}
	return normalize(data, accountID, now)
}

func normalize(data []byte, accountID string, now time.Time) (domain.Inbound, error) {
	canonical, fields, err := canonicalObject(data)
	if err != nil {
		return domain.Inbound{}, err
	}
	id, present := fields["update_id"]
	if !present || id == nil {
		return domain.Inbound{}, domain.ErrInvalidInput
	}
	var update models.Update
	if err := json.Unmarshal(canonical, &update); err != nil || update.ID < 0 {
		return domain.Inbound{}, domain.ErrInvalidInput
	}
	// The Bot API Update union permits at most one optional event member.
	eventCount := 0
	for key, value := range fields {
		// encoding/json matches struct field names case-insensitively. Provider
		// protocol members are case-sensitive; reject aliases instead of letting
		// them bypass the union check or override canonical known members.
		lower := strings.ToLower(key)
		if key != lower && (lower == "update_id" || updateEventKinds[lower]) {
			return domain.Inbound{}, domain.ErrInvalidInput
		}
		if value != nil && updateEventKinds[key] {
			eventCount++
		}
	}
	if eventCount > 1 {
		return domain.Inbound{}, domain.ErrInvalidInput
	}
	digest := sha256.Sum256(canonical)
	in := domain.Inbound{
		Key:  domain.EventKey{Provider: "telegram", AccountID: accountID, EventID: strconv.FormatInt(update.ID, 10)},
		Kind: "ignore", SourceDigest: hex.EncodeToString(digest[:]), ReceivedAt: now,
	}
	var reply ReplyContext
	switch {
	case update.CallbackQuery != nil:
		callback := update.CallbackQuery
		if callback.ID == "" || callback.From.ID <= 0 {
			return domain.Inbound{}, domain.ErrInvalidInput
		}
		in.SenderID = strconv.FormatInt(callback.From.ID, 10)
		reply.CallbackQueryID = callback.ID
		reply.InlineMessageID = callback.InlineMessageID
		if message := callback.Message.Message; message != nil {
			setMessageAddress(&in, &reply, message.Chat.ID, message.ID, message.MessageThreadID)
		} else if message := callback.Message.InaccessibleMessage; message != nil {
			setMessageAddress(&in, &reply, message.Chat.ID, message.MessageID, 0)
		}
		rawCallback, _ := fields["callback_query"].(map[string]any)
		if !callback.From.IsBot && actualHuman(rawCallback["from"]) {
			in.Kind = "interaction"
		}
	case update.Message != nil:
		message := update.Message
		setMessageAddress(&in, &reply, message.Chat.ID, message.ID, message.MessageThreadID)
		if message.From != nil {
			in.SenderID = strconv.FormatInt(message.From.ID, 10)
		}
		// First slice: only actual human text in a private chat. Group command
		// and reply-to-bot triggers require verified bot identity and remain
		// ignored until that capability is implemented. No media caption,
		// service event, edit, anonymous sender or bot becomes model input.
		rawMessage, _ := fields["message"].(map[string]any)
		if message.Chat.Type == models.ChatTypePrivate && message.Chat.ID > 0 && message.ID > 0 &&
			message.From != nil && message.From.ID > 0 && !message.From.IsBot &&
			message.SenderChat == nil && message.SenderBusinessBot == nil &&
			message.ViaBot == nil && message.EditDate == 0 && strings.TrimSpace(message.Text) != "" &&
			onlyTextMessage(rawMessage) && actualHuman(rawMessage["from"]) {
			in.Kind = "text"
			in.Text = message.Text
		}
	}
	in.ReplyContext, err = json.Marshal(reply)
	return in, err
}

func setMessageAddress(in *domain.Inbound, reply *ReplyContext, chatID int64, messageID, threadID int) {
	if chatID != 0 {
		in.ConversationID = strconv.FormatInt(chatID, 10)
		reply.ChatID = in.ConversationID
	}
	if messageID > 0 {
		reply.SourceMessageID = strconv.Itoa(messageID)
	}
	if threadID > 0 {
		in.ThreadID = strconv.Itoa(threadID)
		reply.MessageThreadID = in.ThreadID
	}
}

// Require the actual provider fields, not zero values supplied by JSON decoding.
func actualHuman(value any) bool {
	user, ok := value.(map[string]any)
	if !ok {
		return false
	}
	isBot, ok := user["is_bot"].(bool)
	if !ok || isBot {
		return false
	}
	id, ok := user["id"].(json.Number)
	if !ok {
		return false
	}
	number, err := id.Int64()
	return err == nil && number > 0
}

// A curated text envelope avoids treating a service/media payload containing a
// forged text member as a prompt. Unknown new message capabilities are audited
// as ignored rather than implicitly opting into execution.
func onlyTextMessage(fields map[string]any) bool {
	for key := range fields {
		if !textMessageFields[key] {
			return false
		}
	}
	return true
}

var textMessageFields = map[string]bool{
	"message_id": true, "message_thread_id": true, "from": true, "sender_chat": true,
	"sender_business_bot": true, "date": true, "chat": true, "text": true, "entities": true,
	"reply_to_message": true, "external_reply": true, "quote": true, "forward_origin": true,
	"is_topic_message": true, "is_automatic_forward": true, "via_bot": true, "edit_date": true,
	"has_protected_content": true, "is_from_offline": true, "link_preview_options": true,
	"effect_id": true, "reply_markup": true, "sender_boost_count": true, "sender_tag": true,
}

var updateEventKinds = map[string]bool{
	"message": true, "edited_message": true, "channel_post": true, "edited_channel_post": true,
	"business_connection": true, "business_message": true, "edited_business_message": true,
	"deleted_business_messages": true, "message_reaction": true, "message_reaction_count": true,
	"inline_query": true, "chosen_inline_result": true, "callback_query": true, "shipping_query": true,
	"pre_checkout_query": true, "purchased_paid_media": true, "poll": true, "poll_answer": true,
	"managed_bot": true, "guest_message": true, "my_chat_member": true, "chat_member": true,
	"chat_join_request": true, "chat_boost": true, "removed_chat_boost": true, "subscription": true,
	"stopped_message_generation": true,
}

// canonicalObject preserves JSON numbers losslessly, sorts object members on
// marshal, and rejects duplicate keys at every depth. This makes retry digests
// independent of whitespace/member order without silently accepting ambiguous
// event identities. Raw provider payload is retained only during the request.
func canonicalObject(data []byte) ([]byte, map[string]any, error) {
	if !utf8.Valid(data) {
		return nil, nil, domain.ErrInvalidInput
	}
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.UseNumber()
	value, err := readJSONValue(decoder, 0)
	if err != nil {
		return nil, nil, domain.ErrInvalidInput
	}
	fields, ok := value.(map[string]any)
	if !ok {
		return nil, nil, domain.ErrInvalidInput
	}
	if _, err = decoder.Token(); err != io.EOF {
		return nil, nil, domain.ErrInvalidInput
	}
	canonical, err := json.Marshal(fields)
	return canonical, fields, err
}

func readJSONValue(decoder *json.Decoder, depth int) (any, error) {
	if depth > 64 {
		return nil, domain.ErrInvalidInput
	}
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	delim, container := token.(json.Delim)
	if !container {
		return token, nil
	}
	switch delim {
	case '{':
		object := make(map[string]any)
		for decoder.More() {
			token, err := decoder.Token()
			if err != nil {
				return nil, err
			}
			key, ok := token.(string)
			if !ok {
				return nil, domain.ErrInvalidInput
			}
			if _, exists := object[key]; exists {
				return nil, domain.ErrInvalidInput
			}
			object[key], err = readJSONValue(decoder, depth+1)
			if err != nil {
				return nil, err
			}
		}
		_, err := decoder.Token()
		return object, err
	case '[':
		array := make([]any, 0)
		for decoder.More() {
			value, err := readJSONValue(decoder, depth+1)
			if err != nil {
				return nil, err
			}
			array = append(array, value)
		}
		_, err := decoder.Token()
		return array, err
	default:
		return nil, domain.ErrInvalidInput
	}
}
