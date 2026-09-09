package platform

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strings"
	"testing"
)

func TestRealProviderBindingsAreRejected(t *testing.T) {
	client := newChannelTestClient(t, EchoRunner{})
	client.activateApp("app-one", "deploy-one")
	response := client.do(http.MethodPost, "/api/v1/chat/bindings", `{"channel":"telegram","app_id":"app-one","conversation_type":"single","external_conversation_id":"chat-1","external_user_id":"user-1","secret":"bot-secret"}`, nil)
	defer response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("real provider binding status = %d", response.StatusCode)
	}
}

func TestEnterpriseWeChatChannelParsesSignedXML(t *testing.T) {
	body := []byte(`<xml><FromUserName><![CDATA[user-1]]></FromUserName><ToUserName><![CDATA[corp]]></ToUserName><MsgType><![CDATA[text]]></MsgType><Content><![CDATA[hello]]></Content><MsgId>42</MsgId><CreateTime>1700000000</CreateTime></xml>`)
	mac := hmac.New(sha256.New, []byte("token"))
	_, _ = mac.Write(body)
	adapter := EnterpriseWeChatChannel{}
	message, err := adapter.Receive(context.Background(), ChannelCallback{Channel: ChannelEnterpriseWeChat, Body: body, Signature: hex.EncodeToString(mac.Sum(nil)), Credential: ChannelCredential{Secret: "token"}})
	if err != nil || message.MessageID != "42" || message.UserID != "user-1" || message.Text != "hello" || message.ProviderSequence != 42 {
		t.Fatalf("message=%#v err=%v", message, err)
	}
}

func TestEnterpriseWeChatChannelRejectsUnsupportedMedia(t *testing.T) {
	body := []byte(`<xml><FromUserName>user</FromUserName><MsgType>image</MsgType><MsgId>1</MsgId></xml>`)
	_, err := (EnterpriseWeChatChannel{}).Receive(context.Background(), ChannelCallback{Channel: ChannelEnterpriseWeChat, Body: body, Signature: "bad", Credential: ChannelCredential{Secret: "token"}})
	if channelErrorCode(err) != "channel_signature_invalid" {
		t.Fatalf("error code=%q", channelErrorCode(err))
	}
}

func TestTelegramChannelParsesAuthenticatedUpdate(t *testing.T) {
	body := []byte(`{"update_id":7,"message":{"message_id":9,"chat":{"id":123},"from":{"id":456},"text":"hello"}}`)
	message, err := (TelegramChannel{}).Receive(context.Background(), ChannelCallback{Channel: ChannelTelegram, Body: body, Signature: "bot-secret", Credential: ChannelCredential{Secret: "bot-secret"}})
	if err != nil || message.MessageID != "telegram-9" || message.UserID != "456" || message.ConversationID != "123" || message.ProviderSequence != 7 {
		t.Fatalf("message=%#v err=%v", message, err)
	}
	if strings.Contains(message.Text, "secret") {
		t.Fatal("provider secret leaked into message")
	}
}

func TestTelegramChannelRejectsInvalidSecret(t *testing.T) {
	_, err := (TelegramChannel{}).Receive(context.Background(), ChannelCallback{Channel: ChannelTelegram, Body: []byte(`{}`), Signature: "bad", Credential: ChannelCredential{Secret: "good"}})
	if channelErrorCode(err) != "channel_signature_invalid" {
		t.Fatalf("error code=%q", channelErrorCode(err))
	}
}
