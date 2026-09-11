package wecombot

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"
)

const (
	DefaultEndpoint       = "wss://openws.work.weixin.qq.com"
	DefaultOrigin         = "https://open.work.weixin.qq.com"
	DefaultRequestTimeout = 10 * time.Second
	DefaultHeartbeatTimer = 30 * time.Second
	DefaultMaxMissedPongs = 2
)

var (
	ErrNotConnected       = errors.New("wecombot: not connected")
	ErrAuthFailed         = errors.New("wecombot: authentication failed")
	ErrRequestTimeout     = errors.New("wecombot: request timeout")
	ErrRequestRejected    = errors.New("wecombot: request rejected")
	ErrConnectionReplaced = errors.New("wecombot: connection replaced by another instance")
)

type Config struct {
	BotID          string
	Secret         string
	Endpoint       string
	Origin         string
	RequestTimeout time.Duration
	HeartbeatTimer time.Duration
	HTTPClient     *http.Client
	MaxFileBytes   int64
	OnReady        func()
	OnDisconnected func(error)
}

type Headers struct {
	RequestID string `json:"req_id"`
}

type WireFrame struct {
	Command         string          `json:"cmd,omitempty"`
	Headers         Headers         `json:"headers"`
	Body            json.RawMessage `json:"body,omitempty"`
	ErrorCode       *int            `json:"errcode,omitempty"`
	ErrorMessage    string          `json:"errmsg,omitempty"`
	ProviderSession string          `json:"provider_session_id,omitempty"`
}

type InboundMsgBody struct {
	MsgID      string `json:"msgid"`
	AIBotID    string `json:"aibotid"`
	ChatID     string `json:"chatid"`
	ChatType   string `json:"chattype"`
	CreateTime int64  `json:"create_time"`
	MsgType    string `json:"msgtype"`
	From       struct {
		UserID string `json:"userid"`
	} `json:"from"`
	Text struct {
		Content string `json:"content"`
	} `json:"text"`
	Image mediaContent `json:"image"`
	File  mediaContent `json:"file"`
	Video mediaContent `json:"video"`
	Voice struct {
		Content string `json:"content"`
	} `json:"voice"`
	Mixed struct {
		Items []mixedItem `json:"msg_item"`
	} `json:"mixed"`
}

type mediaContent struct {
	URL    string `json:"url"`
	AESKey string `json:"aeskey"`
}

type mixedItem struct {
	MsgType string `json:"msgtype"`
	Text    struct {
		Content string `json:"content"`
	} `json:"text"`
	Image mediaContent `json:"image"`
}

type EventCallbackBody struct {
	MsgID      string `json:"msgid"`
	AIBotID    string `json:"aibotid"`
	ChatID     string `json:"chatid"`
	ChatType   string `json:"chattype"`
	CreateTime int64  `json:"create_time"`
	From       struct {
		UserID string `json:"userid"`
		CorpID string `json:"corpid,omitempty"`
	} `json:"from"`
	Event struct {
		Type              string `json:"eventtype"`
		EventKey          string `json:"event_key,omitempty"`
		TaskID            string `json:"task_id,omitempty"`
		TemplateCardEvent struct {
			CardType string `json:"card_type,omitempty"`
			EventKey string `json:"event_key,omitempty"`
			TaskID   string `json:"task_id,omitempty"`
		} `json:"template_card_event,omitempty"`
	} `json:"event"`
}

// DisconnectedEventBody is kept as an alias for compatibility with focused
// protocol tests; all event callbacks are decoded through EventCallbackBody.
type DisconnectedEventBody = EventCallbackBody
