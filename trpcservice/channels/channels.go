package channels

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
)

type Incoming struct{ ID, TenantID, Channel, UserID, ChatID, ThreadID, Text string }
type Adapter interface {
	Name() string
	Verify(*http.Request, []byte) error
	Parse([]byte) (Incoming, error)
	Reply(string, string) ([]byte, error)
}

func SessionID(tenant, channel, user, chat string) string {
	scope := chat
	if scope == "" {
		scope = user
	}
	h := sha256.Sum256([]byte(tenant + "|" + channel + "|" + scope))
	return hex.EncodeToString(h[:])
}

type WebAdapter struct{}

func (WebAdapter) Name() string                           { return "web" }
func (WebAdapter) Verify(_ *http.Request, _ []byte) error { return nil }
func (WebAdapter) Parse(b []byte) (Incoming, error)       { return parseJSON(b, "web") }
func (WebAdapter) Reply(_, text string) ([]byte, error)   { return []byte(text), nil }

type TelegramAdapter struct{}

func (TelegramAdapter) Name() string { return "telegram" }
func (TelegramAdapter) Verify(r *http.Request, _ []byte) error {
	if r.Header.Get("X-Telegram-Bot-Api-Secret-Token") == "" {
		return fmt.Errorf("missing telegram secret token")
	}
	return nil
}
func (TelegramAdapter) Parse(b []byte) (Incoming, error) {
	var x struct {
		UpdateID int64 `json:"update_id"`
		Message  struct {
			MessageID int64  `json:"message_id"`
			Text      string `json:"text"`
			Chat      struct {
				ID int64 `json:"id"`
			} `json:"chat"`
			From struct {
				ID int64 `json:"id"`
			} `json:"from"`
		} `json:"message"`
	}
	if err := decode(b, &x); err != nil {
		return Incoming{}, err
	}
	return Incoming{ID: fmt.Sprint(x.UpdateID), Channel: "telegram", UserID: fmt.Sprint(x.Message.From.ID), ChatID: fmt.Sprint(x.Message.Chat.ID), Text: x.Message.Text}, nil
}
func (TelegramAdapter) Reply(chat, text string) ([]byte, error) {
	return []byte(fmt.Sprintf(`{"chat_id":%q,"text":%q}`, chat, text)), nil
}

type WeComAdapter struct{}

func (WeComAdapter) Name() string { return "wecom" }
func (WeComAdapter) Verify(r *http.Request, _ []byte) error {
	if r.Header.Get("X-WeCom-Signature") == "" {
		return fmt.Errorf("missing wecom signature")
	}
	return nil
}
func (WeComAdapter) Parse(b []byte) (Incoming, error) {
	var x struct {
		MsgID   string `json:"msgid"`
		UserID  string `json:"userid"`
		ChatID  string `json:"chatid"`
		Content string `json:"content"`
	}
	if err := decode(b, &x); err != nil {
		return Incoming{}, err
	}
	return Incoming{ID: x.MsgID, TenantID: "", Channel: "wecom", UserID: x.UserID, ChatID: x.ChatID, Text: x.Content}, nil
}
func (WeComAdapter) Reply(_, text string) ([]byte, error) {
	return []byte(fmt.Sprintf(`{"msgtype":"text","text":{"content":%q}}`, text)), nil
}

func parseJSON(b []byte, channel string) (Incoming, error) {
	var x struct {
		ID      string `json:"id"`
		UserID  string `json:"user_id"`
		ChatID  string `json:"chat_id"`
		Text    string `json:"text"`
		Message string `json:"message"`
	}
	if err := decode(b, &x); err != nil {
		return Incoming{}, err
	}
	if x.Text == "" {
		x.Text = x.Message
	}
	return Incoming{ID: x.ID, Channel: channel, UserID: x.UserID, ChatID: x.ChatID, Text: x.Text}, nil
}
func decode(b []byte, dst any) error {
	if err := json.Unmarshal(b, dst); err != nil {
		return err
	}
	return nil
}
